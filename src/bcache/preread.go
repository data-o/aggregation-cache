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
	"strings"
	"sync"
    "sync/atomic"
)

import (
    "utils"
)

type readInfo struct {
    dltId uint32
    datasetId uint32
    groupId uint32
    fileId uint32
}


type readThread struct {
    dataset *Dataset
    readQueue chan *readInfo
}



func (d *Dataset) singleReadthread(threadId int) {
    var (
        err error
    )

    for info := range d.prereadPool {
        node := d.cachedFiles[info.fileId]
        if  node == nil {
            node, err = readFromBackend(info.dltId, info.datasetId, info.groupId, info.fileId)
            if err != nil {
                fmt.Println("failed read from backe", err)
            }
            d.cachedFiles[info.fileId] = node
        }

        dlt, ok := m.dlts.Load(info.dltId)
        if !ok {
            fmt.Println("failed get job id", info.dltId)
        }

        group := &(dlt.groups[info.groupId])

        group.addFileToCache(node)
    }
}

func (d *Dataset) getFreeGroup() {



}

func (d *Dataset) PrereadStart(threadNum int) {
    d.prereadPool = make(chan *readInfo,  PREREAD_QUEUE_LENGTH)

    for i := 0; i < threadNum; i ++ {
        go d.singleReadthread(i)
    }

    for i := GROUP_LEVEL*2 - 1; i >= 0; i-- {
        groupHead := d.leftGroupLevel[i]
        if groupHead == nil {
            continue
        }

        tempGroup := groupHead
        for tempGroup != nil {
            if tempGroup.allowCacheSize > tempGroup.cachedSize {
                fileId, ok := tempGroup.getRandomUnreadedFile()
                d.prereadPool 
            } else {

            }
            if !ok { // don't have unread file

            }
        }
    }
}

func readFromBackend(dltId, datasetId, groupId, fileId uint32) (*FileNode, error) {

}


