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
	"io/ioutil"
	"net/http"
)

import (
	"utils"
)

type readInfo struct {
	dltId     uint32
	datasetId uint32
	groupId   uint32
	fileId    uint32
}

func (g *DLTGroup) preReadProcess() {
	for g.group.allowCacheSize > g.group.cachedSize {
		g.lock.Lock()
		fileId, ok := g.getRandomUnreadedFile()
		if !ok { // don't have unread file
			g.group.cond.Wait() // wait unread file
			g.lock.Unlock()
			continue
		}

		g.lock.Unlock()
		node, err := readFromBackend(g.dlt.dataset, g.dlt.id, g.dlt.dataset.id, g.group.id, fileId)
		if err != nil {
			g.lock.Lock()
			g.addUnreadedFile(fileId)
			g.lock.Unlock()
		}

		g.lock.Lock()
		g.group.dataset.cachedFiles[fileId] = node
		g.addFileToCache(node)
		g.lock.Unlock()
	}
}

func (d *Dataset) PrereadStart(threadNum int) {
	d.clients = make([]*http.Client, confHttpClientNum)
	for i := 0; i < confHttpClientNum; i++ {
		d.clients[i] = utils.NewHttpClient()
	}

	for i := GROUP_LEVEL*2 - 1; i >= 0; i-- {
		groupHead := d.leftGroupLevel[i]
		if groupHead == nil {
			continue
		}

		tempGroup := groupHead
		for tempGroup != nil {
			// master dlt start preprocess
			go d.masterDLT.groups[tempGroup.id].preReadProcess()
			tempGroup = tempGroup.next
		}
	}
}

func readFromBackend(dataset *Dataset, dltId, datasetId, groupId, fileId uint32) (*FileNode, error) {
	httpClient := dataset.clients[groupId%confHttpClientNum]
	endpoint := confEndpoints[groupId%confEndpointNum]
	prefix := fmt.Sprintf("/%d/%d", datasetId, fileId)
	query := fmt.Sprintf("group=%d&dlt=%d", groupId, dltId)
	httpRequest := utils.NewHttpRequest(defaultHttpScheme, endpoint, prefix, query)
	httpRequest.Method = http.MethodGet
	httpClient.Timeout = defaultRequestTimeOut

	httpResponse, err := httpClient.Do(httpRequest)
	if err != nil {
		return nil, err
	} else if httpResponse == nil {
		return nil, fmt.Errorf("http response is empty!")
	}

	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != 200 {
		message, _ := ioutil.ReadAll(httpResponse.Body)
		return nil, fmt.Errorf("Http code %d, message %s!", httpResponse.StatusCode, message)
	}

	data, err := ioutil.ReadAll(httpResponse.Body)
	if err != nil {
		return nil, err
	}

	node := &FileNode{
		fileId:   fileId,
		cached:   true,
		fileSize: uint64(httpResponse.ContentLength),
		body:     data,
	}
	return node, nil
}
