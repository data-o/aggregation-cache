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
	// 	"bytes"
	"fmt"
	//"io"
	"io/ioutil"
	// 	"math/rand"
	"net/http"
	"strconv"
	//"encoding/hex"
	//"crypto/md5"
	// 	"sync/atomic"
)

import (
	"github.com/data-o/aggregation-cache/dconfig"
	"github.com/data-o/aggregation-cache/utils"
)

func (g *Group) preReadProcess() {
	var (
		ok     bool
		fileId uint32
	)

	bm := g.bm
	avgFileNeedBlock := utils.NeedBlock(g.dataset.avgSize)
	if avgFileNeedBlock == 0 {
		avgFileNeedBlock = 1
	}

	groupIndex := g.id % dconfig.ConfEndpointNum
	dataset := g.dataset
	mg := &dataset.masterDLT.groups[g.id]
	masterJobId := dataset.masterDLT.id

	// start preread
	for true {
		for !mg.dlt.isStop {
			mg.lock.Lock()
			if mg.dlt.isStop {
				mg.lock.Unlock()
				break
			}

			// try to reserve mem
			if ok = bm.Reserve(avgFileNeedBlock); !ok {
				g.releaseMem(mg, true)
				// relase some mem
				if ok = bm.Reserve(avgFileNeedBlock); !ok {
					mg.lock.Unlock()
					bm.WaitMem(avgFileNeedBlock)
					continue
				}
			}

			pri := mg.groupPriority()

			fileId, ok = mg.getRandomUnreadedFile()
			if !ok { // don't have unread file
				mg.condPreread.Wait() // wait unread file
				mg.lock.Unlock()
				continue
			}

			mg.prereadFileNum++
			mg.lock.Unlock()

			job := jobInfoPool.Get().(*jobInfo)
			job.group = mg
			job.fileId = fileId

			// start get file from backend
			if pri == PRIORITY_UG {
				ugPriorityJobQueue[groupIndex] <- job
			} else if pri == PRIORITY_HIGH {
				highPriorityJobQueue[groupIndex] <- job
			} else {
				lowPriorityJobQueue[groupIndex] <- job
			}

			ret := <-mg.retQueue

			if ret == nil { // error
				mg.lock.Lock()
				mg.addUnreadedFile(fileId, 2)
			} else {
				if ret.Code == CODE_OK {
					mg.lock.Lock()
					body := ret.Body.(*utils.RefReadCloserBase)
					g.addFileToCache(mg, ret.FileId, ret.FileSize, ret.Md5val, body,
						ret.Type, false, true)
				} else {
					mg.lock.Lock()
					g.addFileToCache(mg, ret.FileId, ret.FileSize, ret.Md5val, nil,
						utils.ALLOC_TYPE_NONE, true, true)
				}
				readRetPool.Put(ret)
			}

			mg.prereadFileNum--
			mg.lock.Unlock()
		}

		dataset.lock.Lock()
		// don't have DLT, waitting
		if dataset.masterDLT == nil {
			dataset.waitDLTNum++
			dataset.waitDLTCon.Wait()
			dataset.waitDLTNum--
		}

		if masterJobId == dataset.masterDLT.id {
			dataset.lock.Unlock()
			continue
		}

		// master DLT has been changed
		mg = &dataset.masterDLT.groups[g.id]
		masterJobId = dataset.masterDLT.id
		dataset.lock.Unlock()
		g.doChangeDLTMaster(mg)
	}
}

// TODO: for quick implementaion, each group start a goroutine
func (d *Dataset) PrereadStart(clientType dconfig.ClientType) {
	if d.isPrereadStart {
		return
	}
	// start train group preread
	for i := uint32(0); i < d.groupNum; i++ {
		if clientType == dconfig.CLIENT_TYPE_SERVER &&
			d.groups[i].id%dconfig.ConfEndpointNum != uint32(d.serverId) {
			continue
		}
		go d.groups[i].preReadProcess()
	}

	d.isPrereadStart = true
}

func getRealFileId(rsp *http.Response) (uint32, bool, error) {
	// get real file id
	realFileIdStr := rsp.Header.Get(HEADER_REAL_FILE_ID)
	if len(realFileIdStr) == 0 {
		return 0, false, nil
	}
	realFileId, err := strconv.ParseUint(realFileIdStr, 10, 32)
	if err != nil {
		return 0, false, fmt.Errorf("can't get conv real file id %s", realFileIdStr)
	}
	return uint32(realFileId), true, nil
}

func readFromBackend(dataset *Dataset, g *Group, httpClient *http.Client, endpoint string,
	fileId, dltId uint32, replace, needCache bool) (*ReadRet, ErrorCode, error) {

	var (
		query        string
		prefix       string
		ok           bool
		retErr       error
	)

	fileName := *(dataset.idToFilename[fileId])

	if dconfig.ConfWithCacheServer {
		prefix = "/get/" + fileName
		query = fmt.Sprintf("group=%d&replace=%v&jobid=%d", g.id, replace, dltId)
	} else {
		prefix = "/" + dataset.bucketName + "/" + fileName
	}

	httpRequest := utils.NewHttpRequest(dconfig.HttpScheme, endpoint, prefix, query)
	httpClient.Timeout = dconfig.RequestTimeOut
	httpRequest.Method = http.MethodGet
	defer utils.CloseHttpRequest(httpRequest)

	realFileId := fileId

	for retry := 0; retry < DEFAULT_RETRY_NUM; retry++ {
		httpResponse, err := httpClient.Do(httpRequest)
		if err != nil {
			return nil, CODE_EMPTY, err
		} else if httpResponse == nil {
			return nil, CODE_EMPTY, fmt.Errorf("http response is empty!")
		}

		if dconfig.ConfWithCacheServer {
			realFileId, ok, err = getRealFileId(httpResponse)
			if err != nil {
				return nil, CODE_EMPTY, err
			} else if !ok && (httpResponse.StatusCode == 200 || httpResponse.StatusCode == 404) {
				return nil, CODE_EMPTY, fmt.Errorf("failed get real file id for %s %s", prefix,
					httpResponse.Header.Get(HEADER_REAL_FILE_ID))
			}
		}

		if httpResponse.StatusCode == 200 {
			size := uint64(httpResponse.ContentLength)
			/*
				// md5 check
				mbody := string(body[:])
				h := md5.New()
				io.WriteString(h, mbody)
				md5Val := hex.EncodeToString(h.Sum(nil))
				if md5Val !=  etag {
					err = fmt.Errorf("get warning content md5 of %s etag is %s is %s size is %d",
						fileName, md5Val, etag, size)
					fmt.Println(err)
					continue
				}
			*/

			ret := readRetPool.Get().(*ReadRet)
			if needCache {
				body, aType, err := utils.NewRefReadCloserBase(uint32(realFileId), g.bm, g.extrBm,
					size, httpResponse.Body)
				if err != nil {
					LogFromtln("failed create new reader", err)
					panic("failed careate new reader")
				}

				ret.Init(uint32(realFileId), size, body, CODE_OK)
				ret.Type = aType
				httpResponse.Body.Close()
			} else {
				ret.Init(uint32(realFileId), size, httpResponse.Body, CODE_OK)
			}

			ret.Md5val = httpResponse.Header.Get("ETag")
			if len(ret.Md5val) > 0 && !dconfig.ConfWithCacheServer {
				ret.Md5val = ret.Md5val[1 : len(ret.Md5val)-1]
			}

			return ret, CODE_OK, nil
		} else {
			message, _ := ioutil.ReadAll(httpResponse.Body)
			httpResponse.Body.Close()
			retErr = fmt.Errorf("Http code %d, message %s!", httpResponse.StatusCode, message)
			if httpResponse.StatusCode == 404 {
				ret := readRetPool.Get().(*ReadRet)
				ret.Init(realFileId, 0, nil, CODE_NOT_FOUND)
				return ret, CODE_NOT_FOUND, retErr
			} else if httpResponse.StatusCode >= 500 {
				continue
			}
			return nil, CODE_EMPTY, retErr
		}
	}

	return nil, CODE_EMPTY, retErr
}
