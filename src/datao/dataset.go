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

package datao

import (
	"fmt"
	"os"
	"strings"
	"sync"
)

type FileNode struct {
    cached bool
    fileSize uint64
    body []byte
}

type Group struct {
	Id          uint32 // id of group
	startId     uint32 // start id in dataset
	endId       uint32 // end id in dataset
	fileNum     int
	groupSize   uint64 // total size of group
	endpoint    string
	cachedFiles []*FileNode
}

type fileShard struct {
	groups   sync.Map // from name to id
}

// max file num uint32
type Dataset struct {
	id       int
    kindNum  int
	groupNum int
	fileNum  uint32
	totalSize uint64
	groups []Group
    fileIdToGroups []int // file id to group
	shards []fileShard // filename to id
	dlts     map[int]*DLT
}

type DatasetManage struct {
	datasets sync.Map // datasetName to Dataset
}

func (d *Dataset) GetFileId(fileName string) (uint32, bool) {
    shardId := StringToInt(fileName, DEFAULT_FILE_SHARD)
    return d.shards[shardId].groups.Load(fileName)
}

// start a new DLT
func (m *DatasetManage) Start(datasetName string, jobId int) (*DLT, error) {
	var (
		dataset *Dataset
		dlt     *DLT
		ok      bool
		err     error
	)

	dataset, ok = m.datsets[datasetName]
	if ! ok {
		dataset, err = m.getDatasetMap(datasetName)
		if err != nil {
			return nil, fmt.Errorf("failed get datasetMap, error: %v", err)
		}
	}

	dlt, ok = m.dlts[jobId]
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

	return dlt, nil
}

func (m *DatasetManage) getDatasetMap(datasetName) (*Dataset, error) {
	dataset, ok :=  m.datasets.Load(datasetName)
	if ok {
		return dataset, nill
	}

	dataset = &Dataset{
		shards: make([]fileShard, DEFAULT_FILE_SHARD),
	}

	err := m.getDatasetMapfromFile(datasetName, datset)
	if err != nil {
		return nil, err
	}
	return dataset, nil
}

// get datasetMap from file
func (m *DatasetManage) getDatasetMapfromFile(datasetMapPath string,
		dataset *Dataset) error {

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
	}

	// get group num
	if groupNum, err := strconv.Atoi(info[1]); err != nil {
		return fmt.Errorf("failed get group num, error: %v", err)
	} else {
		dataset.groupNum = groupNum
		dataset.groups = make([]Group, fileNum)
	}

	// get dataset size
	if datasetSize, err := strconv.ParseUint(info[2], 10, 64); err != nil {
		return fmt.Errorf("failed get dataset size, error: %v", err)
	} else {
		dataset.totalSize = datasetSize
	}

	scanedNum := 0
	lastGroupId := -1

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

		// update group
		dataset.fileIdToGroups[fileId] = groupId
		if lastGroupId != groupId {
            lastGroupId = groupId
            dataset.groups[groupId].startId = fileId
			if lastGroupId >= 0 {
				dataset.groups[lastGroupId].endId = fileId - 1
			}
		}

        dataset.groups[groupId].totalSize += fileSize
        dataset.groups[groupId].fileNum += 1

		// update fileToId
		dataset.shards[shardId].groups.Store(info[0], fileId)

		scanedNum += fileNum
	}

	if scanedNum != dataset.fileNum {
		return fmt.Errorf("wrong file num %d != %d", scanedNum, dataset.fileNum)
	}

	dataset.groups[groupNum-1].endId = fileNum - 1

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}
