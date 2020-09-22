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
	"net/http"
	"sync"
)

import (
	"utils"
)

type FileNode struct {
	FileId   uint32
	Cached   bool
	NotExist bool
	FileSize uint64
	Body     *[]byte
}

type Group struct {
	id      uint32 // id of group
	startId uint32 // start id in dataset
	endId   uint32 // end id in dataset

	dataset *Dataset

	isTrain        bool
	groupSize      uint64 // total size of group
	allowCacheSize uint64
	cachedSize     uint64

	fileNum uint32
	next    *Group
	pre     *Group
}

type fileShard struct {
	groups sync.Map // from name to id
}

// max file num uint32
type Dataset struct {
	id      uint32
	kindNum int

	avgSize uint64

	groupNum      uint32
	trainGroupNum uint32
	valGroupNum   uint32

	fileNum      uint32
	trainFileNum uint32
	valFileNum   uint32

	totalSize      uint64
	trainTotalSize uint64
	valTotalSize   uint64

	allowCacheSize uint64

	groups         []Group  // |<- train -> | <- val -> |
	leftGroupLevel []*Group // it is a list
	levelGap       uint32

	fileIdToGroups []uint32    // file id to group
	shards         []fileShard // filename to id
	idToFilename   []*string
	cachedFiles    []*FileNode

	dlts      []*DLT
	dltMap    sync.Map // job id (int) => dlt index
	masterDLT *DLT

	groupHaveCache []uint32
	prereadPool    chan *readInfo

	clients []*http.Client
}

func (f *FileNode) Release() uint64 {
	n := len(*f.Body)
	f.Body = nil
	f.Cached = false
	f.NotExist = false
	return uint64(n)
}

func (f *FileNode) Save(ret *ReadRet, notExist bool) {
	f.Body = ret.Body
	f.FileId = ret.FileId
	f.Cached = true
	f.FileSize = ret.FileSize
	f.NotExist = notExist
}

// now, sample implement
func (g *Group) getRandomUnreadedFile() (uint32, bool) {
	dlt := g.dataset.dlts[0]
	group := dlt.groups[g.id]
	return group.getRandomUnreadedFile()
}

func (d *Dataset) GetFileId(fileName string) (uint32, bool) {
	shardId := utils.StringToInt(fileName, DEFAULT_FILE_SHARD)
	fileId, ok := d.shards[shardId].groups.Load(fileName)
	if ok {
		return fileId.(uint32), ok
	} else {
		fmt.Println("failed get id of file", fileName)
		return 0, false
	}
}

// preprocess group
func (d *Dataset) groupInit() {
	totalFileSize := d.totalSize
	groupEnd := d.groupNum
	if !confCacheVal {
		totalFileSize = d.trainTotalSize
		groupEnd = d.trainGroupNum
	}

	ratio := float64(d.allowCacheSize) / float64(totalFileSize)

	allSize := uint64(0)
	for i := uint32(0); i < groupEnd; i++ {
		group := &d.groups[i]
		group.allowCacheSize = uint64(ratio * float64(group.groupSize))
		allSize += group.allowCacheSize
	}

	d.genGroupLevel()
}

func (d *Dataset) genGroupLevel() {
	// set train group level
	// split group to mutli-group
	tempTotalfileNum := d.fileNum
	tempGroupNum := d.groupNum

	if !confCacheVal {
		tempTotalfileNum = d.trainFileNum
		tempGroupNum = d.trainGroupNum
	}

	totalLevel := uint32(GROUP_LEVEL * 2)
	levelGap := tempTotalfileNum / tempGroupNum / GROUP_LEVEL
	if levelGap == 0 {
		levelGap = 1
	}

	d.leftGroupLevel = make([]*Group, totalLevel)
	d.levelGap = levelGap

	for i := uint32(0); i < tempGroupNum; i++ {
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
