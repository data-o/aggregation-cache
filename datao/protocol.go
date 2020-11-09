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
	"io"
	"sync"
)

import (
	"github.com/data-o/aggregation-cache/utils"
)

type DataoGroup interface {
	ProcessRepeatRead(*DLTGroup, string, uint32) (uint32, uint64, string, io.ReadCloser,
		ErrorCode, error)
	ProcessAfterReadAll(*DLTGroup) (bool, error)
	NewEpoch(*DLTGroup, int32, []byte) error
}

type ReadRet struct {
	FileId   uint32
	FileSize uint64
	Body     io.ReadCloser
	Code     ErrorCode
	Md5val   string
}

func (r *ReadRet) Init(fileId uint32, size uint64, body io.ReadCloser, code ErrorCode) {
	r.FileId = fileId
	r.FileSize = size
	r.Body = body
	r.Code = code
}

var readRetPool = sync.Pool{
	New: func() interface{} {
		return &ReadRet{}
	},
}

func GetReadRet() *ReadRet {
	return readRetPool.Get().(*ReadRet)
}

func PutReadRet(r *ReadRet) {
	readRetPool.Put(r)
}

type FileNode struct {
	FileId   uint32
	Cached   bool
	NotExist bool
	FileSize uint64
	Body     *utils.RefReadCloserBase
	Md5val   string
}

func (f *FileNode) Release() uint64 {
	retSize := f.FileSize
	if f.Body != nil {
		err := f.Body.Close()
		if err != nil {
			fmt.Println("Failed release 1", err)
		}
		f.Body = nil
	}

	f.Cached = false
	f.NotExist = false
	return retSize
}

func (f *FileNode) Save(fileId uint32, fileSize uint64, body *utils.RefReadCloserBase,
	notExist bool) uint64 {
	var (
		retSize uint64
	)
	if f.Cached {
		retSize = f.FileSize
	}
	if f.Body != nil {
		if err := f.Body.Close(); err != nil {
			fmt.Println("Failed release 2", err)
		}
	}

	f.Body = body
	f.FileId = fileId
	f.Cached = true
	f.FileSize = fileSize
	f.NotExist = notExist
	return retSize
}
