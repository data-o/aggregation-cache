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
	"math/rand"
	"sync"
	"sync/atomic"
)

import (
	"utils"
)

const (
	BITMAP_LENGTH = 64
)

type DLTGroup struct {
	lock      sync.Mutex

	condWaitCache *sync.Cond
	condPreread   *sync.Cond

	cachedBitmap []byte
	groupEpoch int32
	remoteEpoch  int32

	hasWiated int32
	dlt       *DLT

	group            *Group
	readedFilesReal  *utils.BitMap // bitmap 1 or 0
	readedFilesCache *utils.BitMap // bitmap 1 or 0

	unreadFileNum     uint32
	unreadFiles       []uint32

	readedCachedFileNum uint32 // don't contail unreaded files
	readedCachedFiles   []uint32

	prereadFileNum uint32

	cachedFileNum     uint32   // don't contail unreaded files
	cachedFiles       []uint32 // value is file id
}

type DLT struct {
	id            uint32 // id of DLT
	fileNum       uint32

	epoch        int32

	dataset       *Dataset
	readedFileNum uint32
	groups        []DLTGroup
	inited        bool

	unreadFilesIndexs []uint32 // don't contain cached files
	cacheedFilesCache []uint32 // index to cachedFiles
}

func (t *DLT) Dump() {
	fmt.Println("dlt id", t.id)
	fmt.Println("fileNum ", t.fileNum)
	fmt.Println("readedFileNum ", t.readedFileNum)
}

func (t *DLT) init(dataset *Dataset) error {
	var (
		err error
	)

	t.groups = make([]DLTGroup, dataset.groupNum)

	t.unreadFilesIndexs = make([]uint32, dataset.fileNum)
	t.cacheedFilesCache = make([]uint32, dataset.fileNum)

	for i := uint32(0); i < dataset.groupNum; i++ {
		g := &(t.groups[i])

		// init condition variable
		g.condWaitCache = sync.NewCond(&g.lock)
		g.condPreread = sync.NewCond(&g.lock)
		g.dlt = t

		g.group = &(dataset.groups[i])
        // build bit map
		g.readedFilesReal, err = utils.NewBitMap(g.group.fileNum)
		if err != nil {
			return err
		}
		g.readedFilesCache, err = utils.NewBitMap(g.group.fileNum)
		if err != nil {
			return err
		}

		// init unread file list
		g.unreadFiles = make([]uint32, g.group.fileNum)
		for j := g.group.startId; j <= g.group.endId; j ++ {
			g.unreadFiles[g.unreadFileNum] = j
			g.unreadFileNum ++
			g.dlt.unreadFilesIndexs[j] = g.unreadFileNum
		}

		g.readedCachedFiles = make([]uint32, g.group.fileNum)
		g.cachedFiles = make([]uint32, g.group.fileNum)
	}

	t.inited = true

	return nil
}

func (t *DLT) Get(fileName string) (*FileNode, ErrorCode, error) {
	var (
		node *FileNode
		err  error
	)

	if !t.inited {
		return nil, CODE_DLT_NOT_INIT, nil
	}

	// get file id
	fileId, ok := t.dataset.GetFileId(fileName)
	if !ok {
		return nil, CODE_NOT_FOUND, nil
	}

	// get group id
	groupId := t.dataset.fileIdToGroups[fileId]
	group := &t.groups[groupId] // get DLT group

	// add lock
	lockReleased := false
	group.lock.Lock()
	defer func() {
		if !lockReleased {
			group.lock.Unlock()
		}
	}()

	// have been readed?
	if group.readedFilesReal.Get(fileId - group.group.startId) {
		return nil, CODE_EMPTY, fmt.Errorf("read angin %d", fileId)
	}

	if group.cachedFileNum > 0 { // have cache, read from cache
		// try to read from cache
		node, err = t.getFileFromCache(fileId, group)
		if err != nil {
			return nil, CODE_EMPTY, err
		} else if node == nil {
			return nil, CODE_EMPTY,
				fmt.Errorf("don't have cache, but cachedFileNum is %d", group.cachedFileNum)
		}
		// mark file has been readed
		group.readedFilesCache.Set(node.fileId - group.group.startId)
		// mark this file have been read (maybe replaceed by other file)
		group.readedFilesReal.Set(fileId - group.group.startId)

	} else if group.unreadFileNum > 0 {// have unreaded file, try to read it directly
		// don't have cache
		tempId := fileId
		if group.readedFilesCache.Get(fileId - group.group.startId) { // maybe replaced
			tempId, ok = group.getRandomUnreadedFile()
			if !ok {
				return nil, CODE_EMPTY,
					fmt.Errorf("don't have unreaded file, but unreadFileNum is %d", group.unreadFileNum)
			}
		}

		// remove from unread files
		index := t.unreadFilesIndexs[tempId]
		if index != 0 {
			lastId := group.unreadFileNum - 1
			if index != lastId {
				group.unreadFiles[index] = group.unreadFiles[lastId]
				t.unreadFilesIndexs[group.unreadFiles[lastId]] = index
			}
			// mark this file is readed
			t.unreadFilesIndexs[tempId] = 0
			group.unreadFileNum--
		}

		// try to read from endpoint
		lockReleased = true
		group.lock.Unlock()
		node, err := group.readFromBackend(tempId, false)
		// maybe , we can cache all files
		if err != nil {
			return nil, CODE_EMPTY, err
		}

		// mark this file have been read (maybe replaceed by other file)
		group.readedFilesReal.Set(fileId - group.group.startId)
		group.readedFilesCache.Set(node.fileId - group.group.startId)
	} else if group.prereadFileNum > 0 { // waiting for preread
		atomic.AddInt32(&group.hasWiated, 1)
		group.condWaitCache.Wait()
		atomic.AddInt32(&group.hasWiated, -1)
		// try to read from cache
		node, err = t.getFileFromCache(fileId, group)
		if err != nil {
			return nil, CODE_EMPTY, err
		} else if node == nil {
			return nil, CODE_EMPTY, fmt.Errorf("don't have cache, but get condition")
		}
		// mark file has been readed
		group.readedFilesCache.Set(node.fileId - group.group.startId)
		// mark this file have been read (maybe replaceed by other file)
		group.readedFilesReal.Set(fileId - group.group.startId)
	} else {
		if group.groupEpoch != atomic.LoadInt32(&t.epoch) {
			group.newEpoch(t.epoch)
			return nil, CODE_AGAIN, nil
		}
		return nil, CODE_EMPTY, fmt.Errorf("don't have unread file, when try to read %d", fileId)
	}

	atomic.AddUint32(&t.readedFileNum, 1)
	if atomic.LoadUint32(&t.readedFileNum) == t.fileNum {
		if ok := atomic.CompareAndSwapUint32(&t.readedFileNum, t.fileNum, 0); ok {
			atomic.AddInt32(&t.epoch, 1)
		}
	}

	return node, CODE_OK, err
}

// for batfs
func (t *DLT) getFileFromCache(fileId uint32, group *DLTGroup) (*FileNode, error) {
	var (
		ok bool
	)

	tempId := fileId

	if group.readedFilesCache.Get(fileId - group.group.startId) { // have been replace, return a random file
		tempId, ok = group.getRandomCachedFile()
		if !ok {
			return nil, fmt.Errorf("can't get cached file")
		}
	} else if val := t.cacheedFilesCache[tempId]; val == 0 { // this file hasn't been cached
		tempId, ok = group.getRandomCachedFile()
		if !ok {
			return nil, fmt.Errorf("can't get cached file")
		}
	} else {
		group.markCachedFileReaded(fileId, val-1)
	}

	node := t.dataset.cachedFiles[tempId]

	if node == nil || node.cached == false {
		return nil, fmt.Errorf("file %d is mark as cached , but not cached", tempId)
	}

	if group.group.allowCacheSize <= group.group.cachedSize {
		group.condPreread.Signal()
	}

	return node, nil
}

// must have enough cache
func (g *DLTGroup) getRandomCachedFile() (uint32, bool) {
	randNum := rand.Uint32() % g.cachedFileNum
	fileId := g.cachedFiles[randNum]
	g.markCachedFileReaded(fileId, randNum)
	return fileId, true
}

// mark file is readed
func (g *DLTGroup) markCachedFileReaded(fileId, index uint32) {
	// mark this file is readed from cache
	g.readedCachedFiles[g.readedCachedFileNum] = fileId
	g.readedCachedFileNum++

	// move last cahce Id, fill in gap
	lastId := g.cachedFileNum - 1

	if index != lastId {
		g.cachedFiles[index] = g.cachedFiles[lastId]
		g.dlt.cacheedFilesCache[g.cachedFiles[lastId]] = (index + 1)
	}

	// mark this file is readed
	g.dlt.cacheedFilesCache[fileId] = 0
	g.cachedFileNum--
}

// on condition that cache is empty
func (g *DLTGroup) getRandomUnreadedFile() (uint32, bool) {
	if g.unreadFileNum == 0 {
		return 0, false
	}

	randNum := rand.Uint32() % g.unreadFileNum
	fileId := g.unreadFiles[randNum]

	// move last cahce Id, fill in gap
	lastId := g.unreadFileNum - 1
	if randNum != lastId {
		g.unreadFiles[randNum] = g.unreadFiles[lastId]
		g.dlt.unreadFilesIndexs[g.unreadFiles[lastId]] = (randNum + 1)
	}

	// mark this file is readed
	g.dlt.unreadFilesIndexs[fileId] = 0

	g.unreadFileNum--

	return fileId, true
}

// add file to unread files
func (g *DLTGroup) addUnreadedFile(fileId uint32) {
	index := g.dlt.unreadFilesIndexs[fileId]
	if index != 0 {
		fmt.Println("Warning: addUnreadedFile already in  unread files", fileId)
		return
	}

	if g.readedFilesCache.Get(fileId - g.group.startId) {
		fmt.Println("Warning: addUnreadedFile already in  readed files", fileId)
		return
	}

	if g.dlt.cacheedFilesCache[fileId] != 0 {
		fmt.Println("Warning: addUnreadedFile already in cached files", fileId)
		return
	}
	g.unreadFiles[g.unreadFileNum] = fileId
	g.unreadFileNum++
	g.dlt.unreadFilesIndexs[fileId] = g.unreadFileNum

	if g.unreadFileNum == 1 {
		g.condPreread.Signal()
	}
}

// must be protect by lock
func (g *DLTGroup) addFileToCache(node *FileNode) {
	fileId := node.fileId
	// if this file has been readed
	// add to readed cache
	if g.readedFilesCache.Get(fileId - g.group.startId) {
		if g.group.allowCacheSize >= g.group.cachedSize {
			g.readedCachedFiles[g.readedCachedFileNum] = fileId // may core
			g.readedCachedFileNum++
			g.group.cachedSize += node.fileSize
		}
		return
	}

	// remove this file from unreaded files
	orgIndex := g.dlt.unreadFilesIndexs[fileId]
	if orgIndex != 0 { // in unread file list
		fmt.Println("Warning: not remove from unread files", fileId)
		if orgIndex != g.unreadFileNum {
			g.unreadFiles[orgIndex-1] = g.unreadFiles[g.unreadFileNum-1]
		}
		g.unreadFileNum--
		g.dlt.unreadFilesIndexs[fileId] = 0
	}
	
	if g.dlt.cacheedFilesCache[fileId] == 0 { // not cached
		g.cachedFiles[g.cachedFileNum] = fileId
		g.cachedFileNum++
		g.dlt.cacheedFilesCache[fileId] = g.cachedFileNum
		g.group.cachedSize += node.fileSize
		fmt.Println("group", g.group.id, "fileNum", g.group.fileNum, "cached", g.cachedFileNum, 
		"unreaded", g.unreadFileNum, "preread", g.prereadFileNum, "allowCacheSize", g.group.allowCacheSize,
		"cachedSize", g.group.cachedSize)

		if atomic.LoadInt32(&g.hasWiated) > 0 {
			g.condWaitCache.Signal()
		}
	} else { // have been cached
		fmt.Println("Warning: file %d has been cached", fileId)
	}
}

func (g *DLTGroup) newEpoch(epoch int32) {
	if g.groupEpoch == epoch {
		return
	}
    g.lock.Lock()
    defer g.lock.Unlock()

	// init condition variable
    // build bit map
	g.readedFilesReal.Clear()
	g.readedFilesCache.Clear()
	g.unreadFileNum = 0
	g.cachedFileNum = 0
	g.prereadFileNum = 0
	g.readedCachedFileNum = 0

    dataset := g.dlt.dataset
	g.groupEpoch = epoch

	// clear bitmap
	length := g.group.fileNum
	mapLength := length / 8
	if length%8 != 0 {
		mapLength += 1
	}
	if len(g.cachedBitmap) == 0 {
		g.cachedBitmap = make([]byte, mapLength)
	} else {
		for i := uint32(0); i < mapLength; i ++ {
			g.cachedBitmap[i] = 0
		}
	}

    for i := g.group.startId; i <= g.group.endId; i++ {
        if dataset.cachedFiles[i] != nil && dataset.cachedFiles[i].cached {
			// set cache
			g.cachedFiles[g.cachedFileNum] = i
			g.cachedFileNum ++
			g.dlt.cacheedFilesCache[i] = g.cachedFileNum
			// clear unread file
			g.dlt.unreadFilesIndexs[i] = 0
			g.cachedBitmap[(i - g.group.startId) / 8] |= uint8(1) << ((i - g.group.startId) % 8)
        } else {
			// clear cache
			g.dlt.cacheedFilesCache[i] = 0
			// update unread files
			g.unreadFiles[g.unreadFileNum] = i
			g.unreadFileNum ++
			g.dlt.unreadFilesIndexs[i] = g.unreadFileNum
		}
    }
}

// should protect by lock
func (g *DLTGroup) releaseMem() {
	if g.readedCachedFileNum == 0 { // don't have cache
		return
	}

	var (
		i uint32
	)

	releaseNum := g.readedCachedFileNum
	if releaseNum > g.unreadFileNum {
		releaseNum = g.unreadFileNum
	}

	for ; i < releaseNum; i ++ {
		if g.readedCachedFileNum > 0 {
			g.readedCachedFileNum -- 
		} else {
			break
		}

		fileId := g.readedCachedFiles[g.readedCachedFileNum]
		node := g.dlt.dataset.cachedFiles[fileId]
		if node == nil || node.cached == false {
			continue
		}
		n := node.Release()
		g.group.cachedSize -= n
	}
}

// should not be lock
func (g *DLTGroup) readFromBackend(fileId uint32, isPreread bool) (*FileNode, error) {
	var (
		data []byte
		sendCacheInfo bool
	)

	fmt.Println("input fileid", fileId)

	dataset := g.dlt.dataset
	oldEpoch := g.remoteEpoch
	// need send cache info to backend
	if g.groupEpoch != oldEpoch {
		data = g.cachedBitmap
		sendCacheInfo = true
	}

	// build request
	httpClient := g.dlt.dataset.clients[g.group.id % confHttpClientNum]
	endpoint := confEndpoints[g.group.id % confEndpointNum]

	// start get file from backend
	body, size, realId, err := readFromBackend(g.dlt.dataset, httpClient, endpoint, dataset.id, fileId, g.group.id,
		g.dlt.id, data)

	if err != nil {
		fmt.Println("failed to get file", fileId, "from backend", err)
		g.lock.Lock()
		g.addUnreadedFile(fileId)
		g.lock.Unlock()
		fmt.Println("unreadFileNum", g.group.fileNum,  g.unreadFileNum, g.prereadFileNum)
		return nil, err
	}

	if sendCacheInfo {
		if ok := atomic.CompareAndSwapInt32(&g.remoteEpoch, oldEpoch, g.groupEpoch); ok {
			g.remoteEpoch = g.groupEpoch
		}
	}

	// add to cache
	if isPreread ||  (g.group.allowCacheSize - g.group.cachedSize)/dataset.avgSize >
		uint64(g.unreadFileNum + g.prereadFileNum) {

		g.lock.Lock()
		node := dataset.cachedFiles[realId]
		if node != nil {
			node.Save(body, size)
		} else {
			 node =  &FileNode{
				fileId:   realId,
				cached:   true,
				fileSize: size,
				body:     body,
			}
			dataset.cachedFiles[realId] = node
		}

		// add to cache files
		g.addFileToCache(node)
		if realId != fileId {
			g.addUnreadedFile(fileId)
		}
		g.lock.Unlock()

		return node, nil
	}

	return &FileNode {
		fileId: realId,
		fileSize: size,
		body:    body,
	}, nil
}
