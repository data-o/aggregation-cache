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
	"os"
	"strconv"
	"strings"
	"sync"
)

import (
	"utils"
)

type DatasetManager struct {
	datasets sync.Map // datasetName to Dataset
}

func NewDatasetManager() *DatasetManager {
	return &DatasetManager{}
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
	if !ok {
		dataset, err = m.getDatasetMap(datasetName, maxCacheSize)
		if err != nil {
			return nil, fmt.Errorf("failed get datasetMap, error: %v", err)
		}
	} else {
		dataset = d.(*Dataset)
	}

	_, ok = dataset.dltMap.Load(jobId)
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

	if dataset.masterDLT == nil {
		dataset.masterDLT = dlt // Simple set the first dlt is master
		dataset.PrereadStart()
	}

	return dlt, nil
}

func (m *DatasetManager) Finish(datasetId, jobId uint32) error {
	return nil
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
	dataset.groupInit()

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
		if len(info) != 5 {
			return fmt.Errorf("Invalid file list %v", info)
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

		shardId := utils.StringToInt(info[0], DEFAULT_FILE_SHARD)
		//shardId, err := strconv.ParseUint(info[4], 10, 32)
		//if err != nil {
		//	return err
		//}
		//shardId = shardId % DEFAULT_FILE_SHARD

		// groups are split to two part
		// |  <-  train group -> | <- val group -> |
		realGroupId := groupId
		if groupId >= VAL_GROUP_ID_START {
			realGroupId = dataset.groupNum - (groupId % VAL_GROUP_ID_START) - 1
		}

		dataset.fileIdToGroups[fileId] = realGroupId

		if lastGroupId != int(realGroupId) { // new group
			curGroup := &(dataset.groups[realGroupId])
			curGroup.startId = fileId
			curGroup.dataset = dataset
			curGroup.id = realGroupId
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
	if lastGroup.isTrain {
		dataset.trainFileNum += lastGroup.fileNum
		dataset.trainTotalSize += lastGroup.groupSize
	} else {
		dataset.valFileNum += lastGroup.fileNum
		dataset.valTotalSize += lastGroup.groupSize
	}

	dataset.avgSize = dataset.totalSize / uint64(dataset.fileNum)

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}
