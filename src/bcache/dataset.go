// Copyright 2020 Baidu, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package bcache

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
)

import (
	"utils"
)

type FileNode struct {
	fileId   uint32
	cached   bool
	fileSize uint64
	body     []byte
}

type Group struct {
	id      uint32 // id of group
	startId uint32 // start id in dataset
	endId   uint32 // end id in dataset

	dataset *Dataset

	isTrain        bool
	groupSize      uint64 // total size of group
	allowCacheSize uint64
	cachedSize     uint64

	leftFileNum uint32

	fileNum  uint32
	endpoint string
	next     *Group
	pre      *Group

	lock *sync.Mutex
	cond *sync.Cond
}

// now, sample implement
func (g *Group) getRandomUnreadedFile() (uint32, bool) {
	dlt := g.dataset.dlts[0]
	group := dlt.groups[g.id]
	return group.getRandomUnreadedFile()
}

type fileShard struct {
	groups sync.Map // from name to id
}

// max file num uint32
type Dataset struct {
	id      uint32
	kindNum int

	groupNum      uint32
	trainGroupNum uint32
	valGroupNum   uint32

	fileNum      uint32
	trainFileNum uint32
	valFileNum   uint32

	totalSize      uint64
	trainTotalSize uint64
	valTotalSize   uint64

	allowCacheSize uint64

	groups         []Group  // |<- train -> | <- val -> |
	leftGroupLevel []*Group // it is a list
	levelGap       uint32

	fileIdToGroups []uint32    // file id to group
	shards         []fileShard // filename to id
	idToFilename   []*string
	cachedFiles    []*FileNode

	dlts      []*DLT
	dltMap    sync.Map // job id (int) => dlt index
	masterDLT *DLT

	groupHaveCache []uint32
	prereadPool    chan *readInfo

	clients []*http.Client
}

type DatasetManager struct {
	datasets sync.Map // datasetName to Dataset
}

func NewDatasetManager() *DatasetManager {
	return &DatasetManager{}
}

func (d *Dataset) GetFileId(fileName string) (uint32, bool) {
	shardId := utils.StringToInt(fileName, DEFAULT_FILE_SHARD)
	fileId, ok := d.shards[shardId].groups.Load(fileName)
	return fileId.(uint32), ok
}

func (d *Dataset) genGroupLevel() {
	// set train group level
	// split group to mutli-group
	tempTotalfileNum := d.fileNum
	tempGroupNum := d.groupNum

	if !confCacheVal {
		tempTotalfileNum = d.trainFileNum
		tempGroupNum = d.trainGroupNum
	}

	totalLevel := uint32(GROUP_LEVEL * 2)
	levelGap := tempTotalfileNum / tempGroupNum / GROUP_LEVEL
	if levelGap == 0 {
		levelGap = 1
	}

	d.leftGroupLevel = make([]*Group, totalLevel)
	d.levelGap = levelGap

	for i := uint32(0); i < tempGroupNum; i++ {
		level := d.groups[i].fileNum / levelGap
		if level >= totalLevel {
			level = totalLevel - 1
		}

		headGroup := d.leftGroupLevel[level]
		curGroup := &(d.groups[i])

		if headGroup == nil {
			d.leftGroupLevel[level] = curGroup
			curGroup.next = nil
			curGroup.pre = nil
		} else {
			curGroup.next = headGroup.next
			if headGroup.next != nil {
				headGroup.next.pre = curGroup
			}
			headGroup.next = curGroup
			curGroup.pre = headGroup
		}
	}
}

// start a new DLT
func (m *DatasetManager) Start(datasetName string, jobId uint32, maxCacheSize uint64) (*DLT, error) {
	var (
		dataset *Dataset
		dlt     *DLT
		ok      bool
		err     error
	)

	d, ok := m.datasets.Load(datasetName)
	dataset = d.(*Dataset)
	if !ok {
		dataset, err = m.getDatasetMap(datasetName, maxCacheSize)
		if err != nil {
			return nil, fmt.Errorf("failed get datasetMap, error: %v", err)
		}
	}

	t, ok := dataset.dltMap.Load(jobId)
	dlt = t.(*DLT)
	if ok {
		return nil, fmt.Errorf("repeatedly init job")
	}

	dlt = &DLT{
		id:            jobId,
		fileNum:       dataset.fileNum,
		dataset:       dataset,
		readedFileNum: 0,
	}

	err = dlt.init(dataset)
	if err != nil {
		return nil, fmt.Errorf("failed init DLT file group, error: %v", err)
	}

	dataset.dlts = append(dataset.dlts, dlt)
	dataset.dltMap.Store(jobId, len(dataset.dlts)-1)
	dataset.masterDLT = dlt // Simple set the first dlt is master

	return dlt, nil
}

func (m *DatasetManager) getDatasetMap(datasetName string, maxCacheSize uint64) (*Dataset, error) {
	d, ok := m.datasets.Load(datasetName)
	if ok {
		return d.(*Dataset), nil
	}

	dataset := &Dataset{
		shards:         make([]fileShard, DEFAULT_FILE_SHARD),
		allowCacheSize: maxCacheSize,
	}

	err := m.getDatasetMapfromFile(datasetName, dataset)
	if err != nil {
		return nil, err
	}

	// gen level
	dataset.genGroupLevel()

	return dataset, nil
}

// get datasetMap from file
func (m *DatasetManager) getDatasetMapfromFile(datasetMapPath string, dataset *Dataset) error {

	fd, err := os.Open(datasetMapPath)
	if err != nil {
		return err
	}
	defer fd.Close()

	scanner := bufio.NewScanner(fd)

	// get dataset info: file_num, group_num, dataset_size
	scanner.Scan()
	datasetInfo := strings.TrimSpace(scanner.Text())
	if datasetInfo == "" {
		return fmt.Errorf("invalid dataset info 1")
	}

	info := strings.Split(datasetInfo, ",")
	if len(info) != 3 {
		return fmt.Errorf("Invalid dataset info 2")
	}

	// get file num
	if fileNum, err := strconv.ParseUint(info[0], 10, 32); err != nil {
		return fmt.Errorf("failed get file num, error: %v", err)
	} else {
		dataset.fileNum = uint32(fileNum)
		dataset.fileIdToGroups = make([]uint32, fileNum)
		dataset.cachedFiles = make([]*FileNode, fileNum)
		dataset.idToFilename = make([]*string, fileNum)
	}

	// get group num
	if groupNum, err := strconv.ParseUint(info[1], 10, 32); err != nil {
		return fmt.Errorf("failed get group num, error: %v", err)
	} else {
		dataset.groupNum = uint32(groupNum)
		dataset.groups = make([]Group, groupNum)
	}

	// get dataset size
	if datasetSize, err := strconv.ParseUint(info[2], 10, 64); err != nil {
		return fmt.Errorf("failed get dataset size, error: %v", err)
	} else {
		dataset.totalSize = datasetSize
	}

	var (
		scanedNum   uint32
		lastGroupId int = -1
		lastGroup   *Group
	)

	// start read file list from file
	for scanner.Scan() {
		fileInfo := strings.TrimSpace(scanner.Text())
		if fileInfo == "" {
			continue
		}

		// filename groupId fileId fileSize shardId
		info := strings.Split(fileInfo, ",")
		if len(info) != 4 {
			return fmt.Errorf("Invalid file list")
		}

		tempNum, err := strconv.ParseUint(info[1], 10, 32)
		if err != nil {
			return err
		}
		groupId := uint32(tempNum)

		tempNum, err = strconv.ParseUint(info[2], 10, 32)
		if err != nil {
			return err
		}
		fileId := uint32(tempNum)

		fileSize, err := strconv.ParseUint(info[3], 10, 64)
		if err != nil {
			return err
		}

		shardId, err := strconv.Atoi(info[4])
		if err != nil {
			return err
		}

		// groups are split to two part
		// |  <-  train group -> | <- val group -> |
		realGroupId := groupId
		if groupId > VAL_GROUP_ID_START {
			realGroupId = dataset.groupNum - (groupId % VAL_GROUP_ID_START) - 1
		}

		dataset.fileIdToGroups[fileId] = realGroupId

		if lastGroupId != int(realGroupId) { // new group
			curGroup := &(dataset.groups[realGroupId])
			curGroup.startId = fileId
			curGroup.cond = sync.NewCond(curGroup.lock)
			curGroup.dataset = dataset
			if groupId > VAL_GROUP_ID_START {
				dataset.valGroupNum++
			} else {
				dataset.trainGroupNum++
				curGroup.isTrain = true
			}

			if lastGroupId >= 0 {
				lastGroup.endId = fileId - 1
				if lastGroup.isTrain {
					dataset.trainFileNum += lastGroup.fileNum
					dataset.trainTotalSize += lastGroup.groupSize
				} else {
					dataset.valFileNum += lastGroup.fileNum
					dataset.valTotalSize += lastGroup.groupSize
				}

				if confCacheVal {
					lastGroup.allowCacheSize =
						uint64(float64(dataset.allowCacheSize) / float64(dataset.totalSize) *
							float64(lastGroup.groupSize))
				} else {
					lastGroup.allowCacheSize =
						uint64(float64(dataset.allowCacheSize) / float64(dataset.trainTotalSize) *
							float64(lastGroup.groupSize))
				}
				lastGroup.leftFileNum = lastGroup.fileNum
			}

			lastGroup = curGroup
			lastGroupId = int(realGroupId)
		}

		dataset.groups[realGroupId].groupSize += fileSize
		dataset.groups[realGroupId].fileNum += 1

		// update fileToId
		dataset.shards[shardId].groups.Store(info[0], fileId)
		dataset.idToFilename[fileId] = &info[0]

		scanedNum++
	}

	if scanedNum != dataset.fileNum {
		return fmt.Errorf("wrong file num %d != %d", scanedNum, dataset.fileNum)
	}

	// update last group
	lastGroup.endId = dataset.fileNum - 1
	lastGroup.leftFileNum = lastGroup.fileNum
	if lastGroup.isTrain {
		dataset.trainFileNum += lastGroup.fileNum
		dataset.trainTotalSize += lastGroup.groupSize
	} else {
		dataset.valFileNum += lastGroup.fileNum
		dataset.valTotalSize += lastGroup.groupSize
	}
	if confCacheVal {
		lastGroup.allowCacheSize =
			uint64(float64(dataset.allowCacheSize) / float64(dataset.totalSize) *
				float64(lastGroup.groupSize))
	} else {
		lastGroup.allowCacheSize =
			uint64(float64(dataset.allowCacheSize) / float64(dataset.trainTotalSize) *
				float64(lastGroup.groupSize))
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}
