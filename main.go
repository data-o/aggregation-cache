// Copyright 2017 Baidu, Inc.
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

package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"
)

import (
	"github.com/data-o/aggregation-cache/bcache"
	"github.com/data-o/aggregation-cache/utils"
)

const (
	TEST_VERSION = "0.0.1"
)

var (
	isStop bool
)

func readThread(dlt *bcache.DLT, fileLists []*string, startId, endId, threadId int, wg *sync.WaitGroup) {
	//startTime := time.Now().UnixNano()
	defer wg.Done()

	i := startId
	for i < endId && !isStop {
		node, code, err := dlt.Get(*fileLists[i], threadId)
		if code == bcache.CODE_AGAIN {
			fmt.Println("read", *fileLists[i], " again")
			continue
		} else if code == bcache.CODE_NOT_FOUND {
			fmt.Println("Failed read", *fileLists[i], " not found")
		} else if err != nil {
			fmt.Println("Failed read exit ", *fileLists[i], err, code)
			isStop = true
			break
		} else {
			if node == nil {
				panic("get empty node")
			}
		}
		i++
	}

	//endTime := time.Now().UnixNano()
	// fmt.Println("Thread", threadId, "read", endId - startId, "files, use", (endTime - startTime)/1000000, "ms")
}

func readOneSplit(dlt *bcache.DLT, fileLists []*string, threadNum, turn int) {
	var (
		wg  sync.WaitGroup
		err error
	)

	// fmt.Println("Start trun", turn)

	valStart := 1281167
	fileNum := len(fileLists)
	valFileNum := fileNum - valStart
	fileLists, err = utils.ShuffleStringList(fileLists, 0, valStart)
	if err != nil {
		fmt.Println("failed shuffle 1", 0, valStart)
		return
	}
	fileLists, err = utils.ShuffleStringList(fileLists, valStart, valFileNum)
	if err != nil {
		fmt.Println("failed shuffle 1", valStart, valFileNum)
		return
	}

	//startTime := time.Now().UnixNano()
	eachThreadFileNum := (valStart + threadNum - 1) / threadNum
	if eachThreadFileNum == 0 {
		eachThreadFileNum = 1
	}

	for i := 0; i < threadNum; i++ {
		start := i * eachThreadFileNum
		end := (i + 1) * eachThreadFileNum
		if end > valStart {
			end = valStart
		}
		wg.Add(1)
		go readThread(dlt, fileLists, start, end, i, &wg)
	}
	wg.Wait()

	eachThreadFileNum = (valFileNum + threadNum - 1) / threadNum
	if eachThreadFileNum == 0 {
		eachThreadFileNum = 1
	}

	for i := 0; i < threadNum; i++ {
		start := valStart + i*eachThreadFileNum
		end := valStart + (i+1)*eachThreadFileNum
		if end > fileNum {
			end = fileNum
		}
		wg.Add(1)
		go readThread(dlt, fileLists, start, end, i, &wg)
	}
	wg.Wait()

	// endTime := time.Now().UnixNano()
	// fmt.Println("Trun", turn, "read", fileNum, "files, use", (endTime - startTime)/1000000, "ms")
}

func readOne(dlt *bcache.DLT, fileLists []*string, threadNum, turn int) {
	var (
		wg  sync.WaitGroup
		err error
	)

	fmt.Println("Start trun", turn)

	fileNum := len(fileLists)
	fileLists = utils.ShuffleStringListAll(fileLists)
	if err != nil {
		fmt.Println("failed shuffle")
		return
	}

	startTime := time.Now().UnixNano()
	eachThreadFileNum := (fileNum + threadNum - 1) / threadNum
	if eachThreadFileNum == 0 {
		eachThreadFileNum = 1
	}

	for i := 0; i < threadNum; i++ {
		start := i * eachThreadFileNum
		end := (i + 1) * eachThreadFileNum
		if end > fileNum {
			end = fileNum
		}
		wg.Add(1)
		go readThread(dlt, fileLists, start, end, i, &wg)
	}
	wg.Wait()
	endTime := time.Now().UnixNano()
	fmt.Println("Trun", turn, "read", fileNum, "files, use", (endTime-startTime)/1000000, "ms")
}

func main() {
	var (
		jobId uint32 = 1
	)

	go func() {
		http.ListenAndServe("0.0.0.0:8080", nil)
	}()

	datasetName := "imagenet1k"
	maxCacheSize := (uint64(150) << 30)

	dm := bcache.NewDatasetManager()
	dlt, err := dm.Start(datasetName, jobId, maxCacheSize)
	if err != nil {
		fmt.Println("error in main", err)
	} else {
		dlt.Dump()
	}

	fileLists := dlt.GetFileLists()

	startTime := time.Now().UnixNano()
	for turn := 0; turn < 10 && !isStop; turn++ {
		readOneSplit(dlt, fileLists, 10, turn)
	}
	endTime := time.Now().UnixNano()
	fmt.Println("Total  use", (endTime-startTime)/1000000, "ms")
}
