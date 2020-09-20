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
	"fmt"
	"os"
	"strings"
	"sync"
)

type FileNode struct {
    fileId uint32
    cached bool
    fileSize uint64
    body []byte
}

type Group struct {
	Id          uint32 // id of group
	startId     uint32 // start id in dataset
	endId       uint32 // end id in dataset

    isTrain     bool
	groupSize   uint64 // total size of group
    allowCacheSize uint64
    cachedSize     uint64

    leftFileNum uint32

	fileNum     int
	endpoint    string
    next        *Group
    pre         *Group
}

// now, sample implement
func (g *Group) getRandomUnreadedFile() (uint32, bool) {
    dlt := g.dataset.dlts[0]
    group := dlt.groups[g.Id]
    return group.getRandomUnreadedFile()
}

type fileShard struct {
	groups   sync.Map // from name to id
}

// max file num uint32
type Dataset struct {
	id       int
    kindNum  int

	groupNum int
    trainGroupNum int
    valGroupNum int

	fileNum  uint32
    trainFileNum uint32
    valFileNum uint32

	totalSize uint64
    trainTotalSize uint64
    valTotalSize uint64

    allowCacheSize uint64

	groups []Group // |<- train -> | <- val -> |
    leftGroupLevel []*Group // it is a list
    levelGap int

    fileIdToGroups []int // file id to group
	shards []fileShard // filename to id
    idToFilename []*string
	cachedFiles []*FileNode

    dlts []*DLT
	dltMap sync.Map // job id (int) => dlt index

    groupHaveCache []uint32
    prereadPool chan *readInfo
}

type DatasetManage struct {
	datasets sync.Map // datasetName to Dataset
}

func (d *Dataset) GetFileId(fileName string) (uint32, bool) {
    shardId := StringToInt(fileName, DEFAULT_FILE_SHARD)
    return d.shards[shardId].groups.Load(fileName)
}

func (d *Dataset) genGroupLevel() {
    // set train group level
    // split group to mutli-group
    tempTotalfileNum := d.fileNum
    tempGroupNum := d.groupNum

    if !conf_cache_val {
        tempTotalfileNum = d.trainFileNum
        tempGroupNum = d.trainGroupNum
    }

    totalLevel := GROUP_LEVEL*2
    levelGap := tempTotalfileNum / tempGroupNum / GROUP_LEVEL
    if levelGap == 0 {
        levelGap = 1
    }

    d.leftGroupLevel = make([]*Group, totalLevel)
    d.levelGap = levelGap

    for i :=0; i < tempGroupNum; ++i {
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
func (m *DatasetManage) Start(datasetName string, jobId int, maxCacheSize uint64) (*DLT, error) {
	var (
		dataset *Dataset
		dlt     *DLT
		ok      bool
		err     error
	)

	dataset, ok = m.datasets.Load(datasetName)
	if ! ok {
		dataset, err = m.getDatasetMap(datasetName)
		if err != nil {
			return nil, fmt.Errorf("failed get datasetMap, error: %v", err)
		}
	}

	dlt, ok = m.dltMap.Load(jobId)
	if ok {
		return nil, fmt.Errorf("repeatedly init job")
	}

	dlt = &DLT {
		id: jobId,
		fileNum: dataset.fileNum,
		dataset: dataset,
		readedFileNum: 0,
	}

	err = dlt.initGroup(dataset)
	if err != nil {
		return nil, fmt.Errorf("failed init DLT file group, error: %v", err)
	}

    m.dlts = append(m.dlts, dlt)
    m.dltMap.Store(jobId, len(m) - 1)

	return dlt, nil
}

func (m *DatasetManage) getDatasetMap(datasetName string, maxCacheSize uint64) (*Dataset, error) {
	dataset, ok :=  m.datasets.Load(datasetName)
	if ok {
		return dataset, nill
	}

	dataset = &Dataset{
		shards: make([]fileShard, DEFAULT_FILE_SHARD),
        allowCacheSize: maxCacheSize,
	}

	err := m.getDatasetMapfromFile(datasetName, datset)
	if err != nil {
		return nil, err
	}

    // gen level
    dataset.genGroupLevel()

	return dataset, nil
}

// get datasetMap from file
func (m *DatasetManage) getDatasetMapfromFile(datasetMapPath string, dataset *Dataset) error {

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
	if fileNum, err := strconv.Atoi(info[0]); err != nil {
		return fmt.Errorf("failed get file num, error: %v", err)
	} else {
		dataset.fileNum = fileNum
		dataset.fileIdToGroups = make([]int, fileNum)
        dataset.cachedFiles = make([]*FileNode, fileNum)
        dataset.idToFilename =  make([]*string, fileNum)
	}

	// get group num
	if groupNum, err := strconv.Atoi(info[1]); err != nil {
		return fmt.Errorf("failed get group num, error: %v", err)
	} else {
		dataset.groupNum = groupNum
		dataset.groups = make([]Group, groupNum)
	}

	// get dataset size
	if datasetSize, err := strconv.ParseUint(info[2], 10, 64); err != nil {
		return fmt.Errorf("failed get dataset size, error: %v", err)
	} else {
		dataset.totalSize = datasetSize
	}

	scanedNum := 0
	lastGroupId := -1
    var lastGroup *Group

	// start read file list from file
	for scanner.Scan() {
		fileInfo := strings.TrimSpace(scanner.Text())
		if fileInfo == "" {
			continue
		}

		// filename groupId fileId fileSize shardId
		info := strings.Split(oneFileListInfo, ",")
		if len(info) != 4 {
			return fmt.Errorf("Invalid file list")
		}

		groupId, err := strconv.Atoi(info[1])
		if err != nil {
			return err
		}

		fileId, err := strconv.Atoi(info[2])
		if err != nil {
			return err
		}

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
            realGroupId := groupNum - (groupId % VAL_GROUP_ID_START) - 1
        }

		dataset.fileIdToGroups[fileId] = realGroupId

		if lastGroupId != realGroupId { // new group
            curGroup := &(dataset.groups[realGroupId])
            curGroup.startId = fileId
            if groupId > VAL_GROUP_ID_START {
                valGroupNum ++
            } else {
                trainGroupNum ++
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

                if cache_val {
                    lastGroup.allowCacheSize = 
                        int64(float64(dataset.allowCacheSize) / float64(dataset.totalSize) *
                        float64(lastGroup.groupSize))
                } else {
                    lastGroup.allowCacheSize = 
                        int64(float64(dataset.allowCacheSize) / float64(dataset.trainTotalSize) *
                        float64(lastGroup.groupSize))
                }
                lastGroup.leftFileNum = lastGroup.fileNum
            }

            lastGroup = curGroup
            lastGroupId = realGroupId
		}

        dataset.groups[realGroupId].groupSize += fileSize
        dataset.groups[realGroupId].fileNum += 1

		// update fileToId
		dataset.shards[shardId].groups.Store(info[0], fileId)
        dataset.idToFilename[fileId] = &info[0]

		scanedNum += fileNum
	}

	if scanedNum != dataset.fileNum {
		return fmt.Errorf("wrong file num %d != %d", scanedNum, dataset.fileNum)
	}

    // update last group
	lastGroup.endId = fileNum - 1
    lastGroup.leftFileNum = lastGroup.fileNum
    if lastGroup.isTrain {
        dataset.trainFileNum += lastGroup.fileNum
        dataset.trainTotalSize += lastGroup.groupSize
    } else {
        dataset.valFileNum += lastGroup.fileNum
        dataset.valTotalSize += lastGroup.groupSize
    }
    if cache_val {
        lastGroup.allowCacheSize = 
            int64(float64(dataset.allowCacheSize) / float64(dataset.totalSize) *
            float64(lastGroup.groupSize))
    } else {
        lastGroup.allowCacheSize = 
            int64(float64(dataset.allowCacheSize) / float64(dataset.trainTotalSize) *
            float64(lastGroup.groupSize))
    }

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}
