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
	"strings"
	"sync"
)

const (
	BITMAP_LENGTH = 64
)

type DLTGroup struct {
	group *Group
	emptyBitMap []uint64 // from clear bitmap
	readedFilesReal []uint64 // bitmap 1 or 0
	readedFilesCache []uint64 // bitmap 1 or 0

    unreadFileNum uint32
    unreadFilesIndexs []int
    unreadFiles []uint32

	cachedFileNum uint32
	cacheedFilesCache []int // index to cachedFiles
	cachedFiles []uint32 // value is file id
}

type DLT struct {
	id int // id of DLT
	fileNum uint32
    dataset *Dataset
	readedFileNum uint32
	groups []DLTGroup
    inited bool
    isMaster bool
}

func (t *DLT) init(dataset *Dataset)  error {
	t.groups = make([]DLTGroup, dataset.groupNum)
	for i := 0; i < dataset.groupNum; ++i {
		g := &(t.groups[i])
		g.group = &(dataset.groups[i])
		g.emptyBitMap = make([]uint64, (g.group.fileNum+BITMAP_LENGTH -1)/BITMAP_LENGTH)
		g.readedFilesReal = g.emptyBitMap
		g.readedFilesCache = g.emptyBitMap

        g.unreadFileNum = g.group.fileNum
        g.unreadFilesIndexs = make([]int, g.group.fileNum)
        g.unreadFiles = make([]uint32, g.group.fileNum)

		g.cachedFileId = g.emptyBitMap
		g.cacheedFilesCache = make([]int, g.group.fileNum)
		g.cachedFiles = make([]int32, g.group.fileNum)
        g.isMaster = true
	}
    inited = true
}

func (t *DLT) Get(fileName string) (*FileNode, ErrorCode, error) {
    if !inited {
        return nil, CODE_DLT_NOT_INIT, nil
    }

    // get file id
    fileId, ok := t.dataset.GetFileId(fileName)
    if !ok {
        return  nil, CODE_NOT_FOUND, nil
    }

    // get group id
    groupId := t.dataset.fileIdToGroups[fileId]
    group := &t.groups[groupId] // get DLT group

    // offset and mask
    bitmapIndex := (fileId - group.group.startId)/BITMAP_LENGTH
    bitmapOffset := fileId - (bitmapIndex*BITMAP_LENGTH)
    mask := (uint64(1) << bitmapOffset)

    // have been readed?
    if group.readedFilesReal[bitmapIndex] & mask {
        return nil, fmt.Errorf("read angin %d", fileId)
    }

    // try to read from cache
    node, err := t.getFileFromCache(fileId, group, bitmapIndex, mask)
    if err != nil {
        return nil, CODE_EMPTY, err
    }

    if node != nil {
        return node, CODE_OK, nil
    }

    // try to read from endpoint
    node, err = t.readFromBackend(fileName)

    // try add file to cache
    if err != nil && !g.isMaster {
        t.group.addToCache(fileId, node, group)
    }

    return node, CODE_EMPTY, err
}

// for batfs
func (t *DLT) getFileFromCache(fileId uint32, group *DLTGroup, index uint32, 
    mask uint64) (*FileNode, error) {

    var (
        ok bool
    )

    tempId := fileId
    
    // have been replace
    if group.readedFilesCache[bitmapIndex] & mask {
        node, ok := group.findRandomCachedFile()
        if ok { // have cache
            return node, nil
        }

        // don't have cache
        tempId, ok := group.getRandomUnreadedFile()
        if !ok {
            return nil, fmt.Errorf("can't find unreaded file")
        }
        return 
    }

    if !g.isMaster {
        if group.readedFilesCache
    }
}
