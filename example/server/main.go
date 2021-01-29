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
	"net/http"
	//_ "net/http/pprof"
	//"sync"
	//"strings"
	"strconv"
)

import (
	"github.com/data-o/aggregation-cache/datao"
	"github.com/data-o/aggregation-cache/dconfig"
	//"github.com/data-o/aggregation-cache/utils"
)

const (
	SERVER_VERSION = "cacheserver-1.0.1"

	GET_PROCESS_PREFIX   = "/get/"
	PUT_PROCESS_PREFIX   = "/putbitmap"
	START_PROCESS_PREFXI = "/start"
	STOP_PROCESS_PREFXI = "/stop"

	QUERY_JOB_ID  = "jobid"
	QUERY_REPLACE = "replace"
	QUERY_GROUP   = "group"
	QUERY_EPOCH   = "epoch"
	QUERY_DATAMAP = "datamap"
	QUERY_BUCKET = "bucketname"
)

var (
	mg                  *datao.DatasetManager
	isDebug             bool
	get_prefix_length   int
)

func init() {
	get_prefix_length = len(GET_PROCESS_PREFIX)
}

func DebugFromt(format string, args ...interface{}) {
	if !isDebug {
		return
	}
	if format != "" {
		fmt.Printf(format+"\n", args...)
	}
}

func OneProcess(rsp http.ResponseWriter, fileName string, jobId int, replace bool) {
	dlt, ok := mg.GetJob(jobId)
	if !ok {
		DebugFromt("Failed get job id %d", jobId)
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, "job id not found")
		return
	}

	for true {
		realId, retFileSize, md5val, retBody, code, err := dlt.Get(fileName, replace)
		if code == datao.CODE_AGAIN {
			DebugFromt("read %s again", fileName)
			continue
		} else if code == datao.CODE_NOT_FOUND {
			if err == nil {
				rsp.Header().Set("File-Id", strconv.FormatUint(uint64(realId), 10))
				DebugFromt("Failed read %s not found realy, id %d", fileName, realId)
			} else {
				DebugFromt("Failed read %s not found in backend %v", fileName, err)
			}
			rsp.WriteHeader(http.StatusNotFound)
			io.WriteString(rsp, "object not found")
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
			rsp.Header().Set("Content-Length", strconv.FormatUint(retFileSize, 10))
			rsp.Header().Set("ETag", md5val)
			rsp.Header().Set("File-Id", strconv.FormatUint(uint64(realId), 10))
			rsp.WriteHeader(http.StatusOK)
			n, err := io.Copy(rsp, retBody)
			if err != nil {
				DebugFromt("Failed, respose file %s %v", fileName, err)
			} else if uint64(n) != retFileSize {
				DebugFromt("Failed, respose file %s only send %d of %d", fileName, n, retFileSize)
			} else {
				DebugFromt("Success, get file %s size %d %s %d", fileName, retFileSize, fileName, realId)
			}

			retBody.Close()
			return
		}
	}
}

func preprocessReq(req *http.Request) (int, bool, error) {
	q := req.URL.Query()
	jobIdStr := q.Get(QUERY_JOB_ID)
	if jobIdStr == "" {
		return 0, false, fmt.Errorf("job id is empty")
	}

	jobId, err := strconv.Atoi(jobIdStr)
	if err != nil {
		return 0, false, fmt.Errorf("can't get job id")
	}

	replace := q.Get(QUERY_REPLACE)
	if replace == "false" {
		return jobId, false, nil
	}

	return jobId, true, nil
}

func GetProcess(rsp http.ResponseWriter, req *http.Request) {
	fileName := req.URL.Path[get_prefix_length:]
	if len(fileName) < 2 {
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, "invalid file name")
		DebugFromt("Failed get %s invaild file name", fileName)
		return
	}

	jobId, replace, err := preprocessReq(req)
	if err != nil {
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, err.Error())
		DebugFromt("Failed get %s invaild query %s", fileName, err.Error())
		return
	}

	OneProcess(rsp, fileName, jobId, replace)
}

func PutProcess(rsp http.ResponseWriter, req *http.Request) {
	// get group id
	groupIdStr := req.URL.Query().Get(QUERY_GROUP)
	if groupIdStr == "" {
		fmt.Println(req.URL.Query())
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, "group id is empty")
		DebugFromt("Failed get group id")
		return
	}

	groupId, err := strconv.Atoi(groupIdStr)
	if err != nil {
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, "invalid group id")
		DebugFromt("Failed bad group id %s", groupIdStr)
		return
	}

	// get epoch id
	epochStr := req.URL.Query().Get(QUERY_EPOCH)
	if epochStr == "" {
		fmt.Println(req.URL.Query())
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, "epoch id is empty")
		DebugFromt("Failed get epoch id")
		return
	}

	epochId, err := strconv.Atoi(epochStr)
	if err != nil {
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, "invalid epoch id")
		DebugFromt("Failed bad epoch id %s", epochStr)
		return
	}

	// get job id and replace
	jobId, _, err := preprocessReq(req)
	if err != nil {
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, err.Error())
		DebugFromt("Failed get invaild query %s", err.Error())
		return
	}

	// get content length
	contentLength := req.ContentLength
	if contentLength == 0 {
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, "bitmap length is 0")
		DebugFromt("Failed get empty bitmap")
		return
	}

	// get bitmap
	bitmaps := make([]byte, contentLength)
	n, err := io.ReadFull(req.Body, bitmaps)
	req.Body.Close()
	if err != nil {
		rsp.WriteHeader(http.StatusInternalServerError)
		io.WriteString(rsp, err.Error())
		DebugFromt("Failed read bitmap with error %s", err.Error())
		return
	} else if int64(n) != contentLength {
		rsp.WriteHeader(http.StatusInternalServerError)
		io.WriteString(rsp, "fail to read bitmap")
		DebugFromt("Failed read %d bytes of bitmap but expect %d", n, contentLength)
		return
	}

	dlt, ok := mg.GetJob(jobId)
	if !ok {
		DebugFromt("Failed get job id %d", jobId)
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, "job id not found")
		return
	}

	// set readed files
	err = dlt.SetReadedFiles(groupId, epochId, bitmaps)
	if err != nil {
		rsp.WriteHeader(http.StatusInternalServerError)
		io.WriteString(rsp, err.Error())
		DebugFromt("Failed set readed files of job %d with error  %s", jobId, err.Error())
		return
	}

	rsp.WriteHeader(http.StatusOK)
	DebugFromt("Success, get bitmap of group %d jobid %d", groupId, jobId)
}

func NewDLT(rsp http.ResponseWriter, req *http.Request) {
	DebugFromt("start new job")
	// get group id
	datasetMapPath := req.URL.Query().Get(QUERY_DATAMAP)
	if datasetMapPath == "" {
		fmt.Println(req.URL.Query())
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, "dataset map path is empty")
		DebugFromt("Failed get dataset map path")
		return
	}

	// get epoch id
	bucketName := req.URL.Query().Get(QUERY_BUCKET)
	if bucketName == "" {
		fmt.Println(req.URL.Query())
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, "bucketname is empty")
		DebugFromt("Failed get bucketname")
		return
	}

	// get job id and replace
	jobId, _, err := preprocessReq(req)
	if err != nil {
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, err.Error())
		DebugFromt("Failed get invaild query %s", err.Error())
		return
	}

	_, err = mg.Start(datasetMapPath, bucketName, uint32(jobId))
	if err != nil {
		rsp.WriteHeader(http.StatusInternalServerError)
		io.WriteString(rsp, err.Error())
		DebugFromt("Failed start new job %d with error  %s", jobId, err.Error())
		return
	}

	rsp.WriteHeader(http.StatusOK)
	DebugFromt("Success, start new job  %d jobid", jobId)
}

func StopDLT(rsp http.ResponseWriter, req *http.Request) {
	DebugFromt("stop job")
	// get bucket name
	bucketName := req.URL.Query().Get(QUERY_BUCKET)
	if bucketName == "" {
		fmt.Println(req.URL.Query())
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, "bucketname is empty")
		DebugFromt("Failed get bucketname")
		return
	}

	// get job id and replace
	jobId, _, err := preprocessReq(req)
	if err != nil {
		rsp.WriteHeader(http.StatusBadRequest)
		io.WriteString(rsp, err.Error())
		DebugFromt("Failed get invaild query %s", err.Error())
		return
	}

	err = mg.Finish(bucketName, uint32(jobId))
	if err != nil {
		rsp.WriteHeader(http.StatusInternalServerError)
		io.WriteString(rsp, err.Error())
		DebugFromt("Failed stop new job %d with error  %s", jobId, err.Error())
		return
	}

	rsp.WriteHeader(http.StatusOK)
	DebugFromt("Success, stop job  %d jobid", jobId)
}

func Version() {
	fmt.Println("VERSION:", SERVER_VERSION)
}

func InitDLT(serverId int, jobId uint32, datasetName, fileListPath, configPath string) error {
	var (
		err error
	)

	mg, err = datao.NewDatasetManager(serverId, dconfig.CLIENT_TYPE_SERVER, configPath)
	if err != nil {
		return err
	}

	err = mg.StartWithoutJob(fileListPath, datasetName)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	var (
		jobId        uint32 = 1
		serverId     int
		datasetName  string
		fileListPath string
		showVersion  bool
		configPath   string
	)

	flag.IntVar(&serverId, "i", 0, "the id of server, start with 1")
	flag.StringVar(&datasetName, "b", "", "the name of dataset")
	flag.StringVar(&fileListPath, "f", "", "the path of DatasetMap")
	flag.StringVar(&configPath, "c", "./datao.cfg", "the path of config file")
	flag.BoolVar(&isDebug, "d", false, "debug")
	flag.BoolVar(&showVersion, "v", false, "version")

	flag.Parse()

	// show version
	if showVersion {
		Version()
		return
	}

	if serverId == 0 {
		fmt.Println("the id of server is invaild")
		flag.PrintDefaults()
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

	// init data-o
	err := InitDLT(serverId-1, jobId, datasetName, fileListPath, configPath)
	if err != nil {
		fmt.Println("Failed init DLT", err)
		return
	}

	http.HandleFunc(GET_PROCESS_PREFIX, GetProcess)
	http.HandleFunc(PUT_PROCESS_PREFIX, PutProcess)
	http.HandleFunc(START_PROCESS_PREFXI, NewDLT)
	http.HandleFunc(STOP_PROCESS_PREFXI, StopDLT)
	http.ListenAndServe(dconfig.ConfListendAddress, nil)
}
