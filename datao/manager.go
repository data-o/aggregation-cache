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
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

import (
	"github.com/data-o/aggregation-cache/dconfig"
	"github.com/data-o/aggregation-cache/utils"
)

type DatasetManager struct {
	serverId   int
	clientType dconfig.ClientType
	datasets   sync.Map // bucketName to Dataset
	lock       sync.Mutex
	dlts       []*DLT
}

func NewDatasetManager(serverId int, t dconfig.ClientType, confPath string) (*DatasetManager,
	error) {
	if err := dconfig.LoadConfigFromFile(confPath); err != nil {
		return nil, fmt.Errorf("failed load config file %s", err.Error())
	}

	if t != dconfig.CLIENT_TYPE_CLIENT && t != dconfig.CLIENT_TYPE_SERVER {
		return nil, fmt.Errorf("invaild client type %d", t)
	}

	if t != dconfig.ConfClientType {
		return nil, fmt.Errorf("invaild client type %d != %d", t, dconfig.ConfClientType)
	}

	if serverId >= int(dconfig.ConfEndpointNum) {
		return nil, fmt.Errorf("invaild server id")
	}

	contentInit()

	m := &DatasetManager{
		serverId:   serverId,
		clientType: t,
		dlts:       make([]*DLT, 100),
	}

	startPrereadWorker(t, uint32(serverId))

	return m, nil
}

func (m *DatasetManager) GetJob(jobId int) (*DLT, bool) {
	if jobId > len(m.dlts) {
		return nil, false
	}
	dlt := m.dlts[jobId]
	if dlt == nil {
		return nil, false
	}
	return dlt, true
}

func (m *DatasetManager) StartWithoutJob(datasetMapPath, bucketName string) error {
	var (
		dataset *Dataset
		ok      bool
		err     error
	)

	if m.clientType != dconfig.CLIENT_TYPE_SERVER {
		fmt.Errorf("only server can start without job")
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	d, ok := m.datasets.Load(bucketName)
	if !ok {
		dataset, err = m.getDatasetMap(datasetMapPath, bucketName, dconfig.ConfCacheSize)
		if err != nil {
			return fmt.Errorf("failed get datasetMap, error: %v", err)
		}
	} else {
		dataset = d.(*Dataset)
	}

	err = dataset.newTempJob(m.clientType)
	return err
}

// start a new DLT
func (m *DatasetManager) Start(datasetMapPath, bucketName string, jobId uint32) (*DLT, error) {
	var (
		dataset *Dataset
		dlt     *DLT
		ok      bool
		err     error
	)

	if jobId <= 0 {
		return nil, fmt.Errorf("job id must greater than zero")
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	d, ok := m.datasets.Load(bucketName)
	if !ok {
		dataset, err = m.getDatasetMap(datasetMapPath, bucketName, dconfig.ConfCacheSize)
		if err != nil {
			return nil, fmt.Errorf("failed get datasetMap, error: %v", err)
		}
	} else {
		dataset = d.(*Dataset)
	}

	dlt, err = dataset.newJob(jobId, m.clientType)
	if err != nil {
		return nil, err
	}

	m.dlts[jobId] = dlt

	return dlt, nil
}

func (m *DatasetManager) Finish(bucketName string, jobId uint32) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	d, ok := m.datasets.Load(bucketName)
	if !ok {
		return fmt.Errorf("dataset %s is not start", bucketName)
	}

	dataset := d.(*Dataset)
	err := dataset.deleteJob(jobId)
	if err != nil {
		return err
	}

	m.dlts[jobId] = nil
	return nil
}

func (m *DatasetManager) getDatasetMap(datasetMapPath, bucketName string,
	maxCacheSize uint64) (*Dataset, error) {
	d, ok := m.datasets.Load(bucketName)
	if ok {
		return d.(*Dataset), nil
	}

	dataset := &Dataset{
		serverId:       m.serverId,
		clientType:     m.clientType,
		bucketName:     bucketName,
		datasetMapPath: datasetMapPath,
		shards:         make([]fileShard, DEFAULT_FILE_SHARD),
		dltMap:         make(map[uint32]int),
		allowCacheSize: maxCacheSize,
	}

	dataset.waitDLTCon = sync.NewCond(&dataset.lock)
	err := m.getDatasetMapfromFile(datasetMapPath, dataset)
	if err != nil {
		return nil, err
	}

	// gen level
	dataset.groupInit()

	m.datasets.Store(bucketName, dataset)

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

		shardId := utils.StringToIntV2(info[0], DEFAULT_FILE_SHARD)

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

			if lastGroupId >= 0 && (m.clientType == dconfig.CLIENT_TYPE_CLIENT ||
				lastGroupId%int(dconfig.ConfEndpointNum) == m.serverId) {

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
	if m.clientType == dconfig.CLIENT_TYPE_CLIENT ||
		lastGroupId%int(dconfig.ConfEndpointNum) == m.serverId {
		lastGroup.endId = dataset.fileNum - 1
		if lastGroup.isTrain {
			dataset.trainFileNum += lastGroup.fileNum
			dataset.trainTotalSize += lastGroup.groupSize
		} else {
			dataset.valFileNum += lastGroup.fileNum
			dataset.valTotalSize += lastGroup.groupSize
		}
	}

	dataset.avgSize = dataset.totalSize / uint64(dataset.fileNum)
	dataset.avgBlockNum = int32((dataset.avgSize + utils.ConfMempoolBlockSize - 1) / utils.ConfMempoolBlockSize)

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}
