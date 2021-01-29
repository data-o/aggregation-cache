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
)

import (
	"github.com/data-o/aggregation-cache/dconfig"
	"github.com/data-o/aggregation-cache/utils"
)

type ClientCacheGroup struct {
	cachedBitmap []byte
}

func (c *ClientCacheGroup) processReadAgain(fileName string, fileId uint32, g *DLTGroup,
	dlt *DLT) (uint32, uint64, string, io.ReadCloser, ErrorCode, error) {

	// try read from cache
	realId := dlt.replaceFilesIndex[fileId] - 1
	tnode := dlt.dataset.cachedFiles[realId]
	if tnode != nil && tnode.Cached {
		LogFromtln("client Repeat read from cache ", fileName)
		body, err := tnode.Body.NewReadCloser()
		if err != nil {
			return 0, 0, "", body, CODE_EMPTY, err
		}
		return tnode.FileId, tnode.FileSize, tnode.Md5val, body, CODE_OK, nil
	}

	// try read from backend
	LogFromtln("client Repeat read from backend ", fileName)
	// read without replace
	ret, code, err := g.readFromBackend(realId, false, false)
	defer readRetPool.Put(ret)
	// don't add to cache
	if code != CODE_OK {
		return 0, 0, "", nil, code, err
	}
	return ret.FileId, ret.FileSize, ret.Md5val, ret.Body, code, err
}

func (c *ClientCacheGroup) isNewEpoch(g *DLTGroup) (int32, bool) {
	dltEpoch := g.dlt.trainEpoch
	if g.group.id >= g.dlt.dataset.trainGroupNum {
		dltEpoch = g.dlt.valEpoch
	}

	if g.groupEpoch != dltEpoch {
		return dltEpoch, true
	}

	return 0, false
}

func (c *ClientCacheGroup) NewEpoch(g *DLTGroup, epoch int32, cachedBitmap []byte) error {
	if len(cachedBitmap) > 0 {
		return fmt.Errorf("not support set bitmap")
	}

	ok := g.NewEpochClear(epoch)
	if !ok {
		return nil
	}

	group := g.group

	// clear bitmap
	mapLength := group.fileNum / 8
	if group.fileNum%8 != 0 {
		mapLength += 1
	}

	if len(g.cachedBitmap) == 0 {
		g.cachedBitmap = make([]byte, mapLength)
	} else {
		for i := uint32(0); i < mapLength; i++ {
			g.cachedBitmap[i] = 0
		}
	}

	for i := group.startId; i <= group.endId; i++ {
		node := group.dataset.cachedFiles[i]
		if node != nil && node.Cached {
			// set cache
			if node.AollocType == utils.ALLOC_TYPE_NEW ||
				node.AollocType == utils.ALLOC_TYPE_EXTR {
				g.highPriorityCaches[g.highPriorityNum] = i
				g.highPriorityNum++
				g.dlt.cachedFilesCache[i] = g.highPriorityNum + PRIORITY_GAP_BASE
			} else {
				g.cachedFiles[g.cachedFileNum] = i
				g.cachedFileNum++
				g.dlt.cachedFilesCache[i] = g.cachedFileNum
			}
			// clear unread file
			g.dlt.unreadFilesIndexs[i] = 0
			g.cachedBitmap[(i-group.startId)/8] |= uint8(1) << ((i - group.startId) % 8)
		} else {
			// clear cache
			g.dlt.cachedFilesCache[i] = 0
			// update unread files
			g.unreadFiles[g.unreadFileNum] = i
			g.unreadFileNum++
			g.dlt.unreadFilesIndexs[i] = g.unreadFileNum
		}
		g.dlt.replaceFilesIndex[i] = 0
	}

	if dconfig.ConfWithCacheServer {
		// send the info of cached files to backend
		if err := g.sendBitmapToBackend(); err != nil {
			return err
		}
	}

	g.condPreread.Signal()
	return nil
}

// need lock
func (c *ClientCacheGroup) ProcessRepeatRead(g *DLTGroup, fileName string, fileId uint32) (uint32,
	uint64, string, io.ReadCloser, ErrorCode, error) {

	if newEpochId, ok := c.isNewEpoch(g); ok {
		if err := c.NewEpoch(g, newEpochId, nil); err != nil {
			return 0, 0, "", nil, CODE_EMPTY, err
		} else {
			return 0, 0, "", nil, CODE_CONTINUE, nil
		}
	} else if g.dlt.replaceFilesIndex[fileId] != 0 { // have read this object
		return c.processReadAgain(fileName, fileId, g, g.dlt)
	}

	return 0, 0, "", nil, CODE_EMPTY, fmt.Errorf("it is read angin %d", fileId)
}

func (c *ClientCacheGroup) ProcessAfterReadAll(g *DLTGroup) (bool, error) {
	if newEpochId, ok := c.isNewEpoch(g); ok {
		if err := c.NewEpoch(g, newEpochId, nil); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}
