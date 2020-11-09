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
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	// 	"time"
)

import (
	"github.com/data-o/aggregation-cache/dconfig"
	"github.com/data-o/aggregation-cache/utils"
)

const (
	BITMAP_LENGTH = 64
)

type DLTGroup struct {
	lock sync.Mutex

	condWaitCache *sync.Cond
	condPreread   *sync.Cond

	cachedBitmap []byte
	groupEpoch   int32

	hasWiated int32
	dlt       *DLT

	group            *Group
	readedFilesReal  *utils.BitMap // bitmap 1 or 0
	readedFilesCache *utils.BitMap // bitmap 1 or 0

	unreadFileNum uint32
	unreadFiles   []uint32

	readedCachedFileNum uint32 // don't contail unreaded files
	readedCachedFiles   *FIFO

	prereadFileNum uint32

	cachedFileNum uint32   // don't contail unreaded files
	cachedFiles   []uint32 // value is file id

	retQueue chan *ReadRet

	t        DataoGroup
	endpoint string
}

type DLT struct {
	id      uint32 // id of DLT
	fileNum uint32

	isStop bool

	isMaster bool

	readFromCache uint32
	readDirectly  uint32
	waitRead      uint32

	trainEpoch int32
	valEpoch   int32

	dataset        *Dataset
	readedFileNum  uint32
	trainReadedNum uint32
	valReadedNum   uint32
	groups         []DLTGroup
	inited         bool

	replaceFilesIndex []uint32
	unreadFilesIndexs []uint32 // don't contain cached files
	cachedFilesCache  []uint32 // index to cachedFiles
}

func (t *DLT) Dump() {
	fmt.Println("dlt id", t.id)
	fmt.Println("fileNum ", t.fileNum)
	fmt.Println("readedFileNum ", t.readedFileNum)
}

func (t *DLT) init(dataset *Dataset, isMaster bool) error {
	var (
		err error
	)

	t.isMaster = isMaster
	t.groups = make([]DLTGroup, dataset.groupNum)
	t.unreadFilesIndexs = make([]uint32, dataset.fileNum)
	t.cachedFilesCache = make([]uint32, dataset.fileNum)
	t.replaceFilesIndex = make([]uint32, dataset.fileNum)

	for i := uint32(0); i < dataset.groupNum; i++ {
		g := &(t.groups[i])
		group := &(dataset.groups[i])

		// init condition variable
		g.condWaitCache = sync.NewCond(&g.lock)
		g.condPreread = sync.NewCond(&g.lock)
		g.dlt = t
		g.group = group
		g.readedCachedFiles = NewFIFO(int(group.fileNum))
		g.unreadFiles = make([]uint32, group.fileNum)
		g.cachedFiles = make([]uint32, group.fileNum)
		g.retQueue = make(chan *ReadRet, dconfig.ConfGroupRetQueueLength)

		// choose endpoint
		if t.dataset.clientType == dconfig.CLIENT_TYPE_CLIENT {
			g.t = &ClientCacheGroup{}
			g.endpoint = dconfig.ConfEndpoints[group.id%dconfig.ConfEndpointNum]
		} else {
			g.t = &ServerCacheGroup{}
			g.endpoint = dconfig.ConfEndpoints[t.dataset.serverId]
		}

		// build bit map
		g.readedFilesReal, err = utils.NewBitMap(group.fileNum)
		if err != nil {
			return err
		}
		g.readedFilesCache, err = utils.NewBitMap(group.fileNum)
		if err != nil {
			return err
		}
	}

	t.inited = true

	return nil
}

func (t *DLT) initCachedFiles(dataset *Dataset, isMaster bool) {
	var (
		masterGroup *DLTGroup
		cachedNum   int
	)

	for i := uint32(0); i < dataset.groupNum; i++ {
		g := &(t.groups[i])
		group := &(dataset.groups[i])
		// init unread file list

		if !isMaster {
			masterGroup = &(dataset.masterDLT.groups[i])
			masterGroup.lock.Lock() // add lock for master group
		}

		g.cachedFileNum = 0
		g.unreadFileNum = 0

		for j := group.startId; j <= group.endId; j++ {
			node := dataset.cachedFiles[j]
			if node != nil && node.Cached {
				g.cachedFiles[g.cachedFileNum] = j
				g.cachedFileNum++
				g.dlt.cachedFilesCache[j] = g.cachedFileNum
				cachedNum++
			} else {
				g.unreadFiles[g.unreadFileNum] = j
				g.unreadFileNum++
				g.dlt.unreadFilesIndexs[j] = g.unreadFileNum
			}
		}

		if !isMaster {
			masterGroup.lock.Unlock()
		}
	}
	LogFromtln("success init cached file for job", t.id, "cached", cachedNum)
}

func (t *DLT) GetFileLists() []*string {
	fileLists := make([]*string, t.dataset.fileNum)
	copy(fileLists, t.dataset.idToFilename)
	return fileLists
}

func (t *DLT) GetFileId(fileName string) (uint32, bool) {
	return t.dataset.GetFileId(fileName)
}

func (t *DLT) Get(fileName string, replace bool) (uint32, uint64, string, io.ReadCloser, ErrorCode,
	error) {

	var (
		retFileId   uint32
		retFileSize uint64
		retBody     io.ReadCloser
		retMd5val   string
		ret         *ReadRet
		err         error
		code        ErrorCode = CODE_OK
	)

	if !t.inited {
		return 0, 0, "", nil, CODE_DLT_NOT_INIT, nil
	}

	// get file id
	fileId, ok := t.dataset.GetFileId(fileName)
	if !ok {
		return 0, 0, "", nil, CODE_NOT_FOUND, fmt.Errorf("Not Found")
	}

	// 	startTime := time.Now().UnixNano()
	// 	readKind := 1

	// get group id
	groupId := t.dataset.fileIdToGroups[fileId]
	group := &t.groups[groupId] // get DLT group

	// add lock
	group.lock.Lock()
	defer group.lock.Unlock()

	// read with out replace
	if !replace {
		return t.processReadDirect(fileName, fileId, group)
	}

	// have been readed?
	if group.readedFilesReal.Get(fileId - group.group.startId) {
		retFileId, retFileSize, retMd5val, retBody, code, err = group.t.ProcessRepeatRead(group,
			fileName, fileId)
		if code != CODE_CONTINUE {
			return retFileId, retFileSize, retMd5val, retBody, code, err
		}
	}

	if group.cachedFileNum > 0 { // have cache, read from cache
		node, err := t.getFileFromCache(fileId, group)
		if err != nil {
			return 0, 0, "", nil, CODE_EMPTY, err
		} else if node == nil || !node.Cached {
			return 0, 0, "", nil, CODE_EMPTY,
				fmt.Errorf("don't have cache, but cachedFileNum is %d", group.cachedFileNum)
		} else if node.NotExist {
			code = CODE_NOT_FOUND
		} else {
			retBody, err = node.Body.NewReadCloser()
			if err != nil {
				return 0, 0, "", nil, CODE_EMPTY, err
			}

			// start test md5
			if dconfig.ConfCheckBodyMd5 { // test md5
				md5Body, err := node.Body.NewReadCloser()
				if err != nil {
					return 0, 0, "", nil, CODE_EMPTY, err
				}
				if md5val, err := utils.ReaderMd5Check(md5Body, node.Md5val); err != nil {
					LogFromtln("Error: get cache wrong md5", md5val, "for", fileName)
					return 0, 0, "", nil, CODE_EMPTY, err
				}
			}
			// end
		}

		retFileId = node.FileId
		retFileSize = node.FileSize
		retMd5val = node.Md5val

		// mark file has been readed
		group.readedFilesCache.Set(node.FileId - group.group.startId)
		// mark this file have been read (maybe replaceed by other file)
		group.readedFilesReal.Set(fileId - group.group.startId)

		t.readFromCache++
	} else if (dconfig.ConfTryReadDirect || !t.isMaster) && group.unreadFileNum > 0 {
		// if we are not the master DLT, try read unreaded file from backend
		// if we are master and have unreaded file, try to read it directly
		// 		readKind = 3

		// don't have cache
		tempId := fileId
		if group.readedFilesCache.Get(fileId-group.group.startId) || // maybe replaced
			group.dlt.unreadFilesIndexs[fileId] == 0 { // maybe process by preread
			tempId, ok = group.getRandomUnreadedFile()
			if !ok {
				return 0, 0, "", nil, CODE_EMPTY,
					fmt.Errorf("don't have unreaded file, but unreadFileNum is %d", group.unreadFileNum)
			}
		} else {
			// remove from unread files
			group.removeFromUnreadFile(tempId)
		}

		// try to read from endpoint
		group.lock.Unlock()
		ret, code, err = group.readFromBackend(tempId, true)
		// maybe , we can cache all files
		group.lock.Lock()
		if code != CODE_OK && code != CODE_NOT_FOUND {
			group.addUnreadedFile(tempId, 6)
			readRetPool.Put(ret)
			return 0, 0, "", nil, code, err // this process will exit
		} else if code == CODE_NOT_FOUND {
			err = nil
		}

		// mark this file have been read (maybe replaceed by other file)
		group.readedFilesReal.Set(fileId - group.group.startId)
		group.readedFilesCache.Set(ret.FileId - group.group.startId)
		retBody = ret.Body

		retMd5val = ret.Md5val
		retFileId = ret.FileId
		retFileSize = ret.FileSize
		readRetPool.Put(ret)

		t.readDirectly++
	} else if group.unreadFileNum > 0 || group.prereadFileNum > 0 { // waiting for preread
		// 		readKind = 2
		group.hasWiated += 1
		for true {
			group.condWaitCache.Wait()
			if group.cachedFileNum > 0 {
				break
			}
			LogFromtln(group.group.id, "End", group.unreadFileNum, group.prereadFileNum, group.cachedFileNum, fileId)
		}
		group.hasWiated -= 1
		// try to read from cache
		node, err := t.getFileFromCache(fileId, group)
		if err != nil {
			return 0, 0, "", nil, CODE_EMPTY, err
		} else if node == nil || !node.Cached {
			return 0, 0, "", nil, CODE_EMPTY, fmt.Errorf("don't have cache, but get condition")
		} else if node.NotExist {
			code = CODE_NOT_FOUND
		} else {
			retBody, err = node.Body.NewReadCloser()
			if err != nil {
				return 0, 0, "", nil, CODE_EMPTY, err
			}
		}

		// start test md5
		if dconfig.ConfCheckBodyMd5 { // test md5
			md5Body, err := node.Body.NewReadCloser()
			if err != nil {
				return 0, 0, "", nil, CODE_EMPTY, err
			}
			if md5val, err := utils.ReaderMd5Check(md5Body, node.Md5val); err != nil {
				LogFromtln("Error: get cache 1 wrong md5", md5val, "for", fileName)
				return 0, 0, "", nil, CODE_EMPTY, err
			}
		}
		// end

		// mark file has been readed
		group.readedFilesCache.Set(node.FileId - group.group.startId)
		// mark this file have been read (maybe replaceed by other file)
		group.readedFilesReal.Set(fileId - group.group.startId)

		retMd5val = node.Md5val
		retFileId = node.FileId
		retFileSize = node.FileSize

		t.waitRead++
	} else {
		ok, err := group.t.ProcessAfterReadAll(group)
		if err != nil {
			return 0, 0, "", nil, CODE_EMPTY, err
		} else if ok {
			return 0, 0, "", nil, CODE_AGAIN, nil
		}
		return 0, 0, "", nil, CODE_EMPTY, fmt.Errorf("don't have unread file, when try to read %d", fileId)
	}

	if group.group.id >= t.dataset.trainGroupNum { // read from val
		newFileNum := atomic.AddUint32(&t.valReadedNum, 1)
		if newFileNum == t.dataset.valFileNum/2 {
			atomic.AddInt32(&t.trainEpoch, 1)
			atomic.StoreUint32(&t.trainReadedNum, 0)
		}
	} else { // read from train
		newFileNum := atomic.AddUint32(&t.trainReadedNum, 1)
		if newFileNum == t.dataset.trainFileNum/2 && t.trainEpoch > t.valEpoch {
			atomic.AddInt32(&t.valEpoch, 1)
			atomic.StoreUint32(&t.valReadedNum, 0)
		}
	}

	if (t.trainReadedNum+t.valReadedNum)%100000 == 0 {
		LogFromtln("Job", t.id, "ReadType", t.readFromCache, t.readDirectly, t.waitRead, "cached", t.dataset.cachedNum())
	}

	t.replaceFilesIndex[fileId] = retFileId + 1

	// 	if readKind == 2 || readKind == 3 {
	// 		useTime := (time.Now().UnixNano() - startTime) / 1000 // us
	// 		LogFromtln("group", group.group.id, "file num get", fileName, retFileId, "use", useTime, "us")
	// 	}

	return retFileId, retFileSize, retMd5val, retBody, code, err
}

func (t *DLT) processReadDirect(fileName string, realId uint32, group *DLTGroup) (uint32, uint64, string,
	io.ReadCloser, ErrorCode, error) {

	// try read from cache
	tnode := t.dataset.cachedFiles[realId]
	if tnode != nil && tnode.Cached {
		LogFromtln("server Repeat read from cache ", fileName)
		body, err := tnode.Body.NewReadCloser()
		if err != nil {
			return 0, 0, "", body, CODE_EMPTY, err
		}
		return tnode.FileId, tnode.FileSize, tnode.Md5val, body, CODE_OK, nil
	}

	// try read from backend
	LogFromtln("server Repeat read from backend ", fileName)
	// read without replace
	ret, code, err := group.readFromBackend(realId, false)

	defer readRetPool.Put(ret)
	// don't add to cache
	if code == CODE_OK {
		return ret.FileId, ret.FileSize, ret.Md5val, ret.Body, code, err
	} else if code == CODE_NOT_FOUND {
		return ret.FileId, 0, ret.Md5val, nil, code, nil
	} else {
		return 0, 0, "", nil, code, err
	}
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
	} else if val := t.cachedFilesCache[tempId]; val == 0 { // this file hasn't been cached
		tempId, ok = group.getRandomCachedFile()
		if !ok {
			return nil, fmt.Errorf("can't get cached file")
		}
	} else {
		group.markCachedFileReaded(fileId, val-1)
	}

	node := t.dataset.cachedFiles[tempId]
	if node == nil || node.Cached == false {
		return nil, fmt.Errorf("file %d is mark as cached , but not cached", tempId)
	}

	group.group.bm.TryWakeup(group.group.id)

	return node, nil
}

func (t *DLT) SetReadedFiles(groupId, epochId int, bitmaps []byte) error {
	var (
		i   uint32
		num int
	)

	g := &t.groups[groupId] // get DLT group

	// add lock
	g.lock.Lock()
	defer func() {
		g.lock.Unlock()
	}()

	g.t.NewEpoch(g, int32(epochId), bitmaps)

	for ; i < g.group.fileNum; i++ {
		id := i / 8
		offset := i % 8
		if uint8(bitmaps[id]&(1<<offset)) > 0 {
			fileId := g.group.startId + i

			g.removeFromUnreadFile(fileId)

			// mark file has been readed
			g.readedFilesCache.Set(i)

			// mark this file have been read (maybe replaceed by other file)
			g.readedFilesReal.Set(i)

			if val := t.cachedFilesCache[fileId]; val != 0 { // this file hasn't been cached
				g.markCachedFileReaded(fileId, val-1)
			}

			t.replaceFilesIndex[fileId] = fileId + 1
			num++
		}
	}

	return nil
}

func (t *DLT) tellBackendNewDLTStart() error {
	if t.dataset.clientType != dconfig.CLIENT_TYPE_CLIENT {
		return fmt.Errorf("only client need to tell backend new job will start")
	}

	for _, endpoint := range dconfig.ConfEndpoints {
		prefix := "/start"
		query := fmt.Sprintf("datamap=%s&bucketname=%s&jobid=%d", t.dataset.datasetMapPath,
			t.dataset.bucketName, t.id)

		if _, err := utils.SendToBackend(t.dataset.httpClient, dconfig.HttpScheme, endpoint,
			prefix, query, nil); err != nil {
			LogFromtln("Error: job", t.id, "failed send start command", err)
			return fmt.Errorf("failed start job on %s error: %s", endpoint, err.Error())
		}
	}
	return nil
}

func (t *DLT) tellBackendNewDLTStop() error {
	if t.dataset.clientType != dconfig.CLIENT_TYPE_CLIENT {
		return fmt.Errorf("only client need to tell backend new job will stop")
	}

	for _, endpoint := range dconfig.ConfEndpoints {
		prefix := "/stop"
		query := fmt.Sprintf("bucketname=%s&jobid=%d", t.dataset.bucketName, t.id)
		if _, err := utils.SendToBackend(t.dataset.httpClient, dconfig.HttpScheme, endpoint,
			prefix, query, nil); err != nil {
			LogFromtln("Error: job", t.id, "failed send stop command", err)
			return fmt.Errorf("failed stop job on %s error: %s", endpoint, err.Error())
		}
	}
	return nil
}

func (t *DLT) tryNotifyAllGroup() {
	dataset := t.dataset

	avgFileNeedBlock := utils.NeedBlock(dataset.avgSize) / 2
	if avgFileNeedBlock == 0 {
		avgFileNeedBlock = 1
	}

	for i := uint32(0); i < t.dataset.groupNum; i++ {
		group := &t.groups[i]
		if dataset.clientType == dconfig.CLIENT_TYPE_SERVER &&
			(group.group.id%dconfig.ConfEndpointNum != uint32(dataset.serverId)) {
			continue
		}

		if group.unreadFileNum == 0 {
			group.lock.Lock()
			group.condPreread.Signal()
			group.lock.Unlock()
		}

		if ok := group.group.bm.Reserve(avgFileNeedBlock); !ok {
			group.lock.Lock()
			group.group.bm.TryWakeup(group.group.id)
			group.lock.Unlock()
		}
	}
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
	g.readedCachedFiles.put(fileId)
	g.readedCachedFileNum++

	// move last cahce Id, fill in gap
	lastId := g.cachedFileNum - 1

	if index != lastId {
		g.cachedFiles[index] = g.cachedFiles[lastId]
		g.dlt.cachedFilesCache[g.cachedFiles[lastId]] = (index + 1)
	}

	// mark this file is readed
	g.dlt.cachedFilesCache[fileId] = 0
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
func (g *DLTGroup) addUnreadedFile(fileId uint32, id int) bool {
	if index := g.dlt.unreadFilesIndexs[fileId]; index == 0 {
		if g.dlt.cachedFilesCache[fileId] == 0 {
			g.unreadFiles[g.unreadFileNum] = fileId
			g.unreadFileNum++
			g.dlt.unreadFilesIndexs[fileId] = g.unreadFileNum
			if g.unreadFileNum == 1 {
				g.condPreread.Signal()
			}
			return true
		}

		LogFromtln("Warning: addUnreadedFile already in cached files", id, fileId)
		return false
	}
	LogFromtln("Warning: addUnreadedFile already in  unread files", id, fileId)
	return false
}

// remove this file from unreaded files
func (g *DLTGroup) removeFromUnreadFile(fileId uint32) bool {
	if orgIndex := g.dlt.unreadFilesIndexs[fileId]; orgIndex != 0 { // in unread file list
		lastId := g.unreadFileNum - 1
		if orgIndex != g.unreadFileNum {
			g.unreadFiles[orgIndex-1] = g.unreadFiles[lastId]
			g.dlt.unreadFilesIndexs[g.unreadFiles[lastId]] = orgIndex
		}
		g.unreadFileNum--
		g.dlt.unreadFilesIndexs[fileId] = 0
		return true
	}
	return false
}

// add to cached file list
func (g *DLTGroup) moveFromUnreadToCachedFile(fileId uint32) bool {
	ok := g.removeFromUnreadFile(fileId)
	if !ok {
		return false
	}

	// add to cached
	if index := g.dlt.cachedFilesCache[fileId]; index != 0 {
		LogFromtln("Warning: addUnreadedFile already in cached files", fileId)
		return false
	}

	g.cachedFiles[g.cachedFileNum] = fileId
	g.cachedFileNum++
	g.dlt.cachedFilesCache[fileId] = g.cachedFileNum

	return true
}

func (g *DLTGroup) moveFromCachedToUnread(fileId uint32) bool {
	// try to remove from cached files
	index := g.dlt.cachedFilesCache[fileId]
	if index == 0 {
		return false
	}

	lastId := g.cachedFileNum
	if index != lastId {
		g.cachedFiles[index-1] = g.cachedFiles[lastId-1]
		g.dlt.cachedFilesCache[g.cachedFiles[lastId-1]] = index
	}

	g.dlt.cachedFilesCache[fileId] = 0
	g.cachedFileNum--

	return g.addUnreadedFile(fileId, 10)
}

func (g *DLTGroup) sendBitmapToBackend() error {
	if len(g.cachedBitmap) == 0 {
		return nil
	}

	if g.dlt.dataset.clientType != dconfig.CLIENT_TYPE_CLIENT {
		return fmt.Errorf("only client need to send bitmap to server")
	}

	endpoint := dconfig.ConfEndpoints[g.group.id%dconfig.ConfEndpointNum]
	prefix := "/putbitmap"
	query := fmt.Sprintf("group=%d&jobid=%d&epoch=%d", g.group.id, g.dlt.id, g.groupEpoch+1)
	_, err := utils.SendToBackend(g.dlt.dataset.httpClient, dconfig.HttpScheme, endpoint,
		prefix, query, g.cachedBitmap)
	if err != nil {
		LogFromtln("Error: group", g.group.id, "failed send bitmap", err)
	}

	return err
}

// should not be lock
func (g *DLTGroup) readFromBackend(fileId uint32, replace bool) (*ReadRet, ErrorCode, error) {
	// build request
	dataset := g.dlt.dataset

	// start get file from backend
	ret, code, err := readFromBackend(g.dlt.dataset, dataset.httpClient, g.endpoint, dataset.id,
		fileId, g.group.id, g.dlt.id, replace)

	return ret, code, err
}

func (g *DLTGroup) NewEpochClear(epoch int32) bool {
	if g.groupEpoch == epoch {
		return false
	}

	if g.unreadFileNum != 0 || g.prereadFileNum != 0 && g.cachedFileNum != 0 {
		LogFromtln("Error unreadFileNum is", g.unreadFileNum, "prereadFileNum is", g.prereadFileNum)
		return false
	}

	// init condition variable
	// build bit map
	g.readedFilesReal.Clear()
	g.readedFilesCache.Clear()
	g.unreadFileNum = 0
	g.cachedFileNum = 0
	g.prereadFileNum = 0
	g.readedCachedFileNum = 0
	g.readedCachedFiles.clear()

	g.groupEpoch = epoch

	if g.hasWiated != 0 {
		LogFromtln("hasWiated ==", g.hasWiated)
		g.hasWiated = 0
	}

	return true
}

func (g *DLTGroup) groupPriority() PriorityCode {
	if g.unreadFileNum == 0 {
		return PRIORITY_LOW
	}

	if g.cachedFileNum == 0 {
		return PRIORITY_UG
	}

	if g.cachedFileNum <= 2 {
		return PRIORITY_HIGH
	}

	if uint64((g.cachedFileNum)*1000000/(g.cachedFileNum+g.unreadFileNum)) <
		g.dlt.dataset.allowCacheSize*200000/g.dlt.dataset.totalSize {
		return PRIORITY_HIGH
	}

	return PRIORITY_LOW
}
