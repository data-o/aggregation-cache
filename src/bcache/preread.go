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
	"io"
	"io/ioutil"
	//"math/rand"
	"net/http"
	//"strconv"
)

import (
	"utils"
)

/*
var (
	content []byte
)

func init() {
	content = GetRandomString(200*1024)
}
*/

type readInfo struct {
	dltId     uint32
	datasetId uint32
	groupId   uint32
	fileId    uint32
}

func (g *DLTGroup) preReadProcess() {
	var (
		err  error
		node *FileNode
	)
	for true {
		g.lock.Lock()
		g.releaseMem(false)
		if g.group.allowCacheSize <= g.group.cachedSize {
			// relase some mem
			g.releaseMem(true)
			if g.group.allowCacheSize <= g.group.cachedSize {
				g.preReadWaitMem = true
				g.condPreread.Wait() // wait relase file
				g.preReadWaitMem = false
				g.lock.Unlock()
				continue
			}
		}

		fileId, ok := g.getRandomUnreadedFile()
		if !ok { // don't have unread file
			g.condPreread.Wait() // wait unread file
			g.lock.Unlock()
			continue
		}

		node = g.group.dataset.cachedFiles[fileId]
		if node == nil || node.Cached == false {
			g.prereadFileNum++
			g.lock.Unlock()
			// start get file from backend
			_, _, err = g.readFromBackend(fileId, true)
			g.lock.Lock()
			g.prereadFileNum--
			if err != nil {
				fmt.Println("failed to get file", fileId, "from backend", err)
			}
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
	dltId uint32, data []byte) (*ReadRet, ErrorCode, error) {

	/** for test */
	fileName := *(dataset.idToFilename[fileId])
	datasetName := "imagenet"
	prefix := "/" + datasetName + "/" + fileName
	/* test end */

	//prefix := fmt.Sprintf("/%d/%d/%d/%d", datasetId, fileId, groupId, dltId)
	httpRequest := utils.NewHttpRequest(defaultHttpScheme, endpoint, prefix, "")
	httpClient.Timeout = defaultRequestTimeOut

	// need send cache info to backend
	length := 0
	//length := len(data)
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
		return nil, CODE_EMPTY, err
	} else if httpResponse == nil {
		return nil, CODE_EMPTY, fmt.Errorf("http response is empty!")
	}

	// read data
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != 200 {
		message, _ := ioutil.ReadAll(httpResponse.Body)
		if httpResponse.StatusCode == 404 { //not found
			return &ReadRet{
				FileId: fileId, // TODO get from header
			}, CODE_NOT_FOUND, fmt.Errorf("Http code %d, message %s!", httpResponse.StatusCode, message)
		}
		return nil, CODE_EMPTY, fmt.Errorf("Http code %d, message %s!", httpResponse.StatusCode, message)
	}

	// get size
	size := uint64(httpResponse.ContentLength)

	// get real file id
	/*
		realFileIdStr := httpResponse.Header.Get(HEADER_REAL_FILE_ID)
		if len(realFileIdStr) == 0 {
			return nil, 0, 0, fmt.Errorf("can't get real file id")
		}
		realFileId, err := strconv.ParseUint(realFileIdStr, 10, 32)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("can't get conv real file id %s", realFileIdStr)
		}
	*/

	realFileId := fileId // test

	body := make([]byte, size)
	n, err := io.ReadFull(httpResponse.Body, body)
	if err != nil {
		return nil, CODE_EMPTY, err
	} else if uint64(n) != size {
		return nil, CODE_EMPTY, fmt.Errorf("only copy %d of %d", n, size)
	}

	return &ReadRet{
		FileId:   uint32(realFileId),
		FileSize: size,
		Body:     &body,
	}, CODE_OK, nil
}
