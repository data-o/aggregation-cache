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
	"github.com/data-o/aggregation-cache/utils"
)

type ServerCacheGroup struct {
}

func (s *ServerCacheGroup) NewEpoch(g *DLTGroup, epoch int32, cachedBitmap []byte) error {
	var (
		num    int
		offset uint32
	)

	ok := g.NewEpochClear(epoch)
	if !ok {
		return nil
	}

	group := g.group

	for i := group.startId; i <= group.endId; i++ {
		// maybe cached by client, we don't need to cache it again
		clientCached := false
		id := offset / 8
		o := offset % 8
		if uint8(cachedBitmap[id]&(1<<o)) > 0 { // have been cached by client
			clientCached = true
		}

		node := g.dlt.dataset.cachedFiles[i]
		if node != nil && node.Cached {
			if clientCached {
				g.dlt.cachedFilesCache[i] = 0
				g.readedFilesCache.Set(offset)
				// mark this file have been read (maybe replaceed by other file)
				g.readedFilesReal.Set(offset)
				g.dlt.replaceFilesIndex[i] = i + 1
				if node.AollocType == utils.ALLOC_TYPE_NEW ||
					node.AollocType == utils.ALLOC_TYPE_EXTR {
					g.readedPriorityCachedFiles.put(i)
				} else {
					g.readedCachedFiles.put(i)
				}
				g.readedCachedFileNum++
			} else {
				num++
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
				g.dlt.replaceFilesIndex[i] = 0
			}
		} else {
			// clear cache
			g.dlt.cachedFilesCache[i] = 0
			if clientCached {
				// mark file has been readed
				g.readedFilesCache.Set(offset)
				// mark this file have been read (maybe replaceed by other file)
				g.readedFilesReal.Set(offset)
				g.dlt.replaceFilesIndex[i] = i + 1
			} else {
				// update unread files
				g.unreadFiles[g.unreadFileNum] = i
				g.unreadFileNum++
				g.dlt.unreadFilesIndexs[i] = g.unreadFileNum
				g.dlt.replaceFilesIndex[i] = 0
			}
		}
		offset++
	}

	g.condPreread.Signal()

	return nil
}

func (s *ServerCacheGroup) ProcessRepeatRead(group *DLTGroup, fileName string,
	fileId uint32) (uint32, uint64, string, io.ReadCloser, ErrorCode, error) {
	// have been readed?
	if group.dlt.replaceFilesIndex[fileId] != 0 { // have read this object
		realId := group.dlt.replaceFilesIndex[fileId] - 1
		LogFromtln("Rep", realId, "for", fileId)
		return group.dlt.processReadDirect(fileName, realId, group)
	}
	return 0, 0, "", nil, CODE_EMPTY, fmt.Errorf("it is read angin %d", fileId)
}

func (s *ServerCacheGroup) ProcessAfterReadAll(group *DLTGroup) (bool, error) {
	return false, nil
}
