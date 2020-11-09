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
	"net/http"
	"sync"
)

import (
	"github.com/data-o/aggregation-cache/dconfig"
	"github.com/data-o/aggregation-cache/utils"
)

type Group struct {
	id uint32 // id of group

	dataset *Dataset

	fileNum uint32
	startId uint32 // start id in dataset
	endId   uint32 // end id in dataset

	bm     *utils.BlockManage
	extrBm *utils.BlockManage

	isTrain        bool
	groupSize      uint64 // total size of group
	allowCacheSize uint64
	cachedSize     uint64
}

type fileShard struct {
	groups sync.Map // from name to id
}

// max file num uint32
type Dataset struct {
	id             uint32
	clientType     dconfig.ClientType
	serverId       int
	bucketName     string
	datasetMapPath string
	kindNum        int
	waitDLTNum     int

	groupNum      uint32
	trainGroupNum uint32
	valGroupNum   uint32
	groups        []Group // |<- train -> | <- val -> |

	fileNum      uint32
	trainFileNum uint32
	valFileNum   uint32

	totalSize      uint64
	trainTotalSize uint64
	valTotalSize   uint64
	avgSize        uint64
	avgBlockNum    int32

	allowCacheSize uint64
	allocSize      int64

	fileIdToGroups []uint32    // file id to group
	shards         []fileShard // filename to id
	idToFilename   []*string
	cachedFiles    []*FileNode

	dlts       []*DLT
	dltMap     map[uint32]int // job id (int) => dlt index
	lock       sync.Mutex
	waitDLTCon *sync.Cond
	masterDLT  *DLT

	httpClient *http.Client

	isPrereadStart bool
}

// file name to file id
func (d *Dataset) GetFileId(fileName string) (uint32, bool) {
	shardId := utils.StringToIntV2(fileName, DEFAULT_FILE_SHARD)
	fileId, ok := d.shards[shardId].groups.Load(fileName)
	if ok {
		return fileId.(uint32), ok
	} else {
		LogFromtln("failed get id of file", fileName)
		return 0, false
	}
}

// preprocess group
func (d *Dataset) groupInit() {
	wg := sync.WaitGroup{}

	totalFileSize := d.trainTotalSize
	groupEnd := d.trainGroupNum
	if dconfig.ConfCacheVal {
		totalFileSize += d.valTotalSize
		groupEnd = d.groupNum
	}

	ratio := float64(d.allowCacheSize) / float64(totalFileSize)
	if d.allowCacheSize > totalFileSize {
		ratio = float64(1)
	}

	allocBlocks := func(group *Group, realCacheSize, extrCacheSize uint64, wg *sync.WaitGroup) {
		defer wg.Done()
		group.bm = utils.NewBlocks(realCacheSize, extrCacheSize, group.id)
		group.bm.Alloc()
	}

	extrBm := utils.NewBlocks(dconfig.ConfAllowGlobalExtrMemSize, 0, 100000000)
	extrBm.Alloc()

	for i := uint32(0); i < groupEnd; i++ {
		group := &d.groups[i]

		if d.clientType == dconfig.CLIENT_TYPE_SERVER &&
			(group.id%dconfig.ConfEndpointNum != uint32(d.serverId)) {
			continue
		}

		group.allowCacheSize = uint64(ratio * float64(group.groupSize))
		group.extrBm = extrBm

		extrCacheSize := uint64(dconfig.ConfAllowAmpMemSize)
		if group.allowCacheSize >= group.groupSize {
			extrCacheSize = 0
		} else if group.groupSize < group.allowCacheSize+extrCacheSize {
			extrCacheSize = group.groupSize - group.allowCacheSize
		}

		wg.Add(1)
		go allocBlocks(group, group.allowCacheSize, extrCacheSize, &wg)
	}

	wg.Wait()

	d.httpClient = utils.NewHttpClient()
}

// try add cached file to slave DLT group
func (d *Dataset) addCachedFileToSlave(groupId, fileId uint32) bool {
	for _, dlt := range d.dlts {
		g := &dlt.groups[groupId]
		g.lock.Lock()
		g.moveFromUnreadToCachedFile(fileId)
		g.lock.Unlock()
	}
	return true
}

// try remove cached file from slave DLT group
func (d *Dataset) removeCachedFileToSlave(groupId, fileId uint32) bool {
	for _, dlt := range d.dlts {
		g := &dlt.groups[groupId]
		g.lock.Lock()
		g.moveFromCachedToUnread(fileId)
		g.lock.Unlock()
	}
	return true
}

func (d *Dataset) choosetNewJobAsMaster() (*DLT, error) {
	lastDlt := len(d.dlts)
	if lastDlt < 1 {
		return nil, nil
	}

	lastDlt--
	newDLT := d.dlts[lastDlt]
	d.dlts[lastDlt] = nil
	d.dlts = d.dlts[:lastDlt]
	delete(d.dltMap, newDLT.id)

	return newDLT, nil
}

func (d *Dataset) newTempJob(clientType dconfig.ClientType) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	if clientType != dconfig.CLIENT_TYPE_SERVER {
		return fmt.Errorf("can't start temp job in client")
	}

	if len(d.dltMap) > 0 || d.masterDLT != nil {
		return fmt.Errorf("can't start temp job")
	}

	dlt := &DLT{
		id:            0,
		fileNum:       d.fileNum,
		dataset:       d,
		readedFileNum: 0,
	}

	err := dlt.init(d, true)
	if err != nil {
		return fmt.Errorf("failed init DLT file group, error: %v", err)
	}

	dlt.initCachedFiles(d, true)

	// it not be the master DLT
	d.masterDLT = dlt // Simple set the first dlt is master
	d.PrereadStart(clientType)
	return nil
}

func (d *Dataset) cachedNum() uint32 {
	var (
		cachedNum uint32
	)
	for i := uint32(0); i < d.fileNum; i++ {
		if d.cachedFiles[i] != nil && d.cachedFiles[i].Cached {
			cachedNum++
		}
	}
	return cachedNum
}

func (d *Dataset) newJob(jobId uint32, clientType dconfig.ClientType) (*DLT, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	_, ok := d.dltMap[jobId]
	if ok {
		return nil, fmt.Errorf("repeatedly init job")
	}

	if d.masterDLT != nil && d.masterDLT.id == jobId {
		return nil, fmt.Errorf("repeatedly init job")
	}

	dlt := &DLT{
		id:            jobId,
		fileNum:       d.fileNum,
		dataset:       d,
		readedFileNum: 0,
	}

	dltIsMaster := (d.masterDLT == nil)
	err := dlt.init(d, dltIsMaster)
	if err != nil {
		return nil, fmt.Errorf("failed init DLT file group, error: %v", err)
	}

	LogFromtln("new job", jobId, "is master?:", dltIsMaster)
	// tell backend server, a new job will start
	if clientType == dconfig.CLIENT_TYPE_CLIENT && dconfig.ConfWithCacheServer {
		err = dlt.tellBackendNewDLTStart()
		if err != nil {
			return nil, err
		}
	}

	// it not be the master DLT
	if dltIsMaster {
		d.masterDLT = dlt // Simple set the first dlt is master
		dlt.initCachedFiles(d, dltIsMaster)
		d.PrereadStart(clientType)
		dlt.tryNotifyAllGroup()
		if d.waitDLTNum > 0 {
			d.waitDLTCon.Broadcast()
		}
	} else {
		d.dlts = append(d.dlts, dlt)
		d.dltMap[jobId] = len(d.dlts) - 1
		dlt.initCachedFiles(d, dltIsMaster)
	}

	if jobId > 0 && d.masterDLT.id == 0 { // we need delete temp job
		d.deleteJobLockless(0)
	}

	return dlt, nil
}

func (d *Dataset) deleteJob(jobId uint32) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.deleteJobLockless(jobId)
}

func (d *Dataset) deleteJobLockless(jobId uint32) error {
	var (
		dlt *DLT
	)

	if d.masterDLT == nil {
		return fmt.Errorf("master DLT is nil")
	}

	// it is master?
	if d.masterDLT.id == jobId {
		dlt = d.masterDLT
		newMaster, err := d.choosetNewJobAsMaster()
		if err != nil {
			return err
		}
		d.masterDLT = newMaster
		dlt.isStop = true
		dlt.tryNotifyAllGroup()

		if newMaster == nil {
			LogFromtln("delete master job", jobId, "new master is nil")
		} else {
			newMaster.isMaster = true
			if d.waitDLTNum > 0 {
				d.waitDLTCon.Broadcast()
			}
			LogFromtln("delete master job", jobId, "new master is job", newMaster.id)
		}
	} else {
		dltIndex, ok := d.dltMap[jobId]
		if !ok {
			return fmt.Errorf("job %d of dataset %s is not start", jobId, d.bucketName)
		}
		dlt = d.dlts[dltIndex]
		delete(d.dltMap, jobId)
		lastDlt := len(d.dlts) - 1
		if lastDlt != dltIndex {
			d.dlts[dltIndex] = d.dlts[lastDlt]
		}
		d.dlts[lastDlt] = nil
		d.dlts = d.dlts[:lastDlt]
		dlt.isStop = true
	}

	// tell backend server, a new job will stop
	if d.clientType == dconfig.CLIENT_TYPE_CLIENT && dconfig.ConfWithCacheServer {
		if err := dlt.tellBackendNewDLTStop(); err != nil {
			return err
		}
	}
	return nil
}

func (g *Group) doChangeDLTMaster(mg *DLTGroup) error {
	mg.lock.Lock() // add lock for master group
	defer mg.lock.Unlock()

	for j := g.startId; j <= g.endId; j++ {
		node := g.dataset.cachedFiles[j]
		// not cached
		if node == nil || !node.Cached {
			continue
		}

		fileId := node.FileId

		index := mg.dlt.cachedFilesCache[fileId]
		if index != 0 {
			continue
		}

		if mg.dlt.unreadFilesIndexs[fileId] != 0 {
			return fmt.Errorf("Warning file not in cached file lists")
		}

		mg.readedCachedFiles.put(fileId)
		mg.readedCachedFileNum++
	}
	return nil
}

// must be protect by the lock of master group
func (g *Group) addFileToCache(mg *DLTGroup, fileId uint32, fileSize uint64, md5val string,
	body *utils.RefReadCloserBase, notExist, isPreread bool) {
	var (
		isReaded bool
	)

	needBlock := utils.NeedBlock(fileSize)
	// add to readed cache
	if mg.readedFilesCache.Get(fileId-g.startId) || // if this file has been readed
		!isPreread { // it is directly read
		if !g.bm.Reserve(needBlock) {
			LogFromtln("Warning it is not preread")
			return
		} else {
			isReaded = true
		}
	}

	node := g.dataset.cachedFiles[fileId]
	if node != nil {
		g.cachedSize -= node.Save(fileId, fileSize, body, notExist) // may release old cache
		node.Md5val = md5val
	} else {
		node = &FileNode{
			FileId:   fileId,
			Cached:   true,
			NotExist: notExist,
			FileSize: fileSize,
			Body:     body,
			Md5val:   md5val,
		}
		g.dataset.cachedFiles[fileId] = node
	}
	g.cachedSize += fileSize

	if isReaded {
		mg.readedCachedFiles.put(fileId) // may core
		mg.readedCachedFileNum++
		g.dataset.addCachedFileToSlave(g.id, fileId)
		LogFromtln("Warning it has been readed", fileId)
	} else if mg.dlt.cachedFilesCache[fileId] == 0 { // not cached
		mg.cachedFiles[mg.cachedFileNum] = fileId
		mg.cachedFileNum++
		mg.dlt.cachedFilesCache[fileId] = mg.cachedFileNum
		if mg.hasWiated > 0 {
			mg.condWaitCache.Signal()
		}
		g.dataset.addCachedFileToSlave(g.id, fileId)
	} else { // have been cached
		LogFromtln("Warning: file %d has been cached", fileId)
	}
}

// should protect by the lock of master group
// mg: master DLTGroup
func (g *Group) releaseMem(mg *DLTGroup, force bool) {
	if mg.readedCachedFileNum == 0 { // don't have cache
		return
	} else if mg.unreadFileNum == 0 {
		return
	}

	needBlock := utils.NeedBlock(uint64(mg.unreadFileNum) * g.dataset.avgSize)

	if force || !g.bm.Reserve(needBlock) {
		releaseNum := mg.readedCachedFileNum
		if releaseNum > mg.unreadFileNum {
			releaseNum = mg.unreadFileNum
		}

		// at least release 10
		if !force && releaseNum < 4 {
			return
		}

		for i := uint32(0); mg.readedCachedFileNum > 0 && i < releaseNum; i++ {
			mg.readedCachedFileNum--
			fileId, ok := mg.readedCachedFiles.get()
			if !ok {
				panic("Error: don't have node in readed queue")
			}

			if len(g.dataset.dlts) > 0 {
				g.dataset.removeCachedFileToSlave(g.id, fileId)
			}

			node := g.dataset.cachedFiles[fileId]
			if node == nil {
				continue
			}
			node.Release()
		}

		// try release more node
		for mg.readedCachedFileNum > 0 && !g.bm.Reserve(needBlock) {
			mg.readedCachedFileNum--
			fileId, ok := mg.readedCachedFiles.get()
			if !ok {
				panic("Error: don't have node in readed queue 1")
			}

			if len(g.dataset.dlts) > 0 {
				g.dataset.removeCachedFileToSlave(g.id, fileId)
			}

			node := g.dataset.cachedFiles[fileId]
			if node == nil {
				continue
			}
			node.Release()
		}
	}
}
