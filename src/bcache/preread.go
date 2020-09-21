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
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	//"strconv"
)

import (
	"utils"
)

var (
	content []byte
)

func init() {
	content = GetRandomString(100*1024)
}

type readInfo struct {
	dltId     uint32
	datasetId uint32
	groupId   uint32
	fileId    uint32
}

func (g *DLTGroup) preReadProcess() {
	var (
		err error
		node *FileNode
	)
	fmt.Println(" start proread ", g.group.id, g.group.allowCacheSize, g.group.cachedSize)
	for true {
		g.lock.Lock()
		if g.group.allowCacheSize <= g.group.cachedSize {
			// relase some mem
			g.releaseMem()
			if g.group.allowCacheSize <= g.group.cachedSize {
				g.condPreread.Wait() // wait relase file
				g.lock.Unlock()
				continue
			}
		}

		fileId, ok := g.getRandomUnreadedFile()
		if !ok { // don't have unread file
			fmt.Println("failed get unread file")
			g.condPreread.Wait() // wait unread file
			g.lock.Unlock()
			continue
		}

		node = g.group.dataset.cachedFiles[fileId]
		if node == nil || node.cached == false {
			g.lock.Unlock()
			// start get file from backend
			g.prereadFileNum ++
			node, err =  g.readFromBackend(fileId, true)
			g.prereadFileNum --
			if err != nil {
				fmt.Println("failed to get file", fileId, "from backend", err)
				continue
			}

			g.lock.Lock()
		}
		g.lock.Unlock()
	}
}

// TODO: for quick implementaion, each group start a goroutine
func (d *Dataset) PrereadStart() {
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

func readFromBackend(dataset *Dataset, httpClient *http.Client, endpoint string, datasetId, fileId, groupId, 
	dltId uint32, data []byte) (*[]byte, uint64, uint32, error) {

	/** for test */
	fileName := *(dataset.idToFilename[fileId])
	datasetName := "images1"
	prefix := "/"+datasetName+"/"+fileName

	/* test end */

	fmt.Println("read", prefix, "fileid", fileId, "groupId", groupId)

	contentLen := rand.Uint64() % 102400
	data = make([]byte, contentLen)
	copy(data, content)
	return &data, contentLen, fileId, nil



	//prefix := fmt.Sprintf("/%d/%d/%d/%d", datasetId, fileId, groupId, dltId)
	httpRequest := utils.NewHttpRequest(defaultHttpScheme, endpoint, prefix, "")
	httpClient.Timeout = defaultRequestTimeOut
	
	// need send cache info to backend
	//length := len(data)
	length := 0
	if length > 0 {
		httpRequest.Body = ioutil.NopCloser(bytes.NewBuffer(data))
		httpRequest.ContentLength = int64(length)
		httpRequest.Method = http.MethodPut
	} else {
		httpRequest.Method = http.MethodGet
	}

	// send request
	httpResponse, err := httpClient.Do(httpRequest)
	if err != nil {
		return nil, 0, 0, err
	} else if httpResponse == nil {
		return nil, 0, 0, fmt.Errorf("http response is empty!")
	}

	// read data
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != 200 {
		message, _ := ioutil.ReadAll(httpResponse.Body)
		return nil, 0, 0, fmt.Errorf("Http code %d, message %s!", httpResponse.StatusCode, message)
	}

	// get size
	size := uint64(httpResponse.ContentLength)

	// get real file id
	realFileIdStr := httpResponse.Header.Get(HEADER_REAL_FILE_ID)
	if len(realFileIdStr) == 0 {
		return nil, 0, 0, fmt.Errorf("can't get real file id")
	}
	//realFileId, err := strconv.ParseUint(realFileIdStr, 10, 32)
	//if err != nil {
	//	return nil, 0, 0, fmt.Errorf("can't get conv real file id %s", realFileIdStr)
	//}

	realFileId := fileId

	body, err := ioutil.ReadAll(httpResponse.Body)
	if err != nil {
		return nil, 0, 0, err
	}

	return &body, size, uint32(realFileId),  nil
}
