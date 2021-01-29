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
	"flag"
	"fmt"
	"io"
    "os"
	"os/signal"
	"net/http"
	"strconv"
	"syscall"
	"sync"
)

import (
	"github.com/data-o/aggregation-cache/datao"
	"github.com/data-o/aggregation-cache/dconfig"
)

const (
	SERVER_VERSION = "cacheserver-0.0.1"
	QUERY_JOB_ID  = "jobid"
	QUERY_REPLACE = "replace"
)

var (
	dlt     *datao.DLT
	dm *datao.DatasetManager
	isDebug bool
    lastJobIdStr string
	lastJobId int
	datasetName  string
	fileListPath string
	lock     sync.Mutex
)

func DebugFromt(format string, args ...interface{}) {
	if !isDebug {
		return
	}
	if format != "" {
		fmt.Printf(format+"\n", args...)
	}
}

func OneProcess(rsp http.ResponseWriter, req *http.Request) {
	fileName := (req.URL.Path)
	if len(fileName) < 2 {
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, "invalid file name")
		DebugFromt("Failed get %s invaild file name", fileName)
		return
	}

	fileName = fileName[1:]
	replace := true
	q := req.URL.Query()
	replaceStr := q.Get(QUERY_REPLACE)
	if replaceStr == "false" {
		replace = false
	}

	for true {
		_, retFileSize, md5Val, retBody, code, err := dlt.Get(fileName, replace)
		if code == datao.CODE_AGAIN {
			DebugFromt("read %s again", fileName)
			continue
		} else if code == datao.CODE_NOT_FOUND {
			rsp.WriteHeader(http.StatusNotFound)
			io.WriteString(rsp, "object not found")
			DebugFromt("Failed read %s not found", fileName)
			return
		} else if err != nil {
			rsp.WriteHeader(http.StatusInternalServerError)
			io.WriteString(rsp, err.Error())
			DebugFromt("Failed read %s exit %s %d", fileName, err.Error(), code)
			return
		} else {
			if retBody == nil {
				rsp.WriteHeader(http.StatusInternalServerError)
				io.WriteString(rsp, "get empty content")
				DebugFromt("Failed get empty node when read %s", fileName)
				return
			}

			rsp.Header().Set("Content-Type", "application/text")
			rsp.Header().Set("Content-Length",  strconv.FormatUint(retFileSize, 10))
			rsp.Header().Set("ETag", md5Val)
			rsp.WriteHeader(http.StatusOK)
			n, err := io.Copy(rsp, retBody)
			if err != nil {
				DebugFromt("Failed, respose file %s %v", fileName, err)
			} else if uint64(n) != retFileSize {
				DebugFromt("Failed, respose file %s only send %d of %d", fileName, n, retFileSize)
			} else {
				DebugFromt("Success, get file %s size %d", fileName, retFileSize)
			}

			retBody.Close()
			return
		}
	}
}

func Version() {
	fmt.Println("VERSION:", SERVER_VERSION)
}

func InitDLT(jobId uint32, datasetName, fileListPath, confPath string) error {
	var (
		err error
	)

	dm, err = datao.NewDatasetManager(0, dconfig.CLIENT_TYPE_CLIENT, confPath)
	if err != nil {
		return err
	}

	dlt, err = dm.Start(fileListPath, datasetName, jobId)
	if err != nil {
		return err
	}
	return nil
}

func startDLT(datasetName, fileListPath string, jobId int)  error {
	var (
		err error
	)

    fmt.Println("start job ", jobId)
    dlt, err = dm.Start(fileListPath, datasetName, uint32(jobId))
	if err != nil {
		fmt.Println("faile start job", jobId, err)
	}
	return err
}

func stopDLT(datasetName string, jobId int) error {
    fmt.Println("stop job ", jobId)
    err := dm.Finish(datasetName, uint32(jobId))
	if err != nil {
		fmt.Println("faile stop job", jobId, err)
	}
	return err
}

func main() {
	var (
		showVersion  bool
		listenAdd    string
		confPath string
        isStop   bool
	)

	flag.StringVar(&datasetName, "b", "", "the name of dataset")
	flag.StringVar(&fileListPath, "f", "", "the path of DatasetMap")
	flag.StringVar(&listenAdd, "a", "0.0.0.0:8081", "the listend address")
	flag.StringVar(&confPath, "c", "./datao.cfg", "the path of config")
	flag.IntVar(&lastJobId, "j", 0, "job id, must greater than 0")
	flag.BoolVar(&isDebug, "d", false, "debug")
	flag.BoolVar(&showVersion, "v", false, "version")

	flag.Parse()

	// show version
	if showVersion {
		Version()
		return
	}

	if datasetName == "" {
		fmt.Println("the name of dataset is empty")
		flag.PrintDefaults()
		return
	} else if fileListPath == "" {
		fmt.Println("the path of datasetMap is empty")
		flag.PrintDefaults()
		return
	}

	if lastJobId <= 0 {
		fmt.Println("invaild job id", lastJobId)
		flag.PrintDefaults()
		return
	}

    defer func() {
		err := recover()

        if !isStop {
            if dm != nil {
                stopDLT(datasetName, lastJobId)
            }
            isStop = true
        }

        if err != nil {
			fmt.Printf("Error: %v!\n", err)
		    os.Exit(1)
        }
        os.Exit(0)
	}()

	// handling interrupt (ctrl+c)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalChan
        fmt.Println("get signal, stop")
		if !isStop {
            stopDLT(datasetName, lastJobId)
		}
        isStop = true
		os.Exit(1)
	}()

	lastJobIdStr = strconv.Itoa(lastJobId)

	// init data-o
	err := InitDLT(uint32(lastJobId), datasetName, fileListPath, confPath)
	if err != nil {
		fmt.Println("Failed init DLT", err)
		return
	}

	http.HandleFunc("/", OneProcess)
	http.ListenAndServe(listenAdd, nil)
}
