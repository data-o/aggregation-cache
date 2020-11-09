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
	"time"
)

func LogFromtf(format string, args ...interface{}) {
	format = time.Now().Format("2006-01-02 15:04:05.9999") + " " + format

	fmt.Printf(format+"\n", args...)
}

func LogFromtln(args ...interface{}) {
	args = append(args, nil)
	copy(args[1:], args)
	args[0] = time.Now().Format("2006-01-02 15:04:05.9999")
	fmt.Println(args...)
}

type FIFO struct {
	queue   []uint32
	start   int
	end     int
	length  int
	nodeNum int
}

func NewFIFO(length int) *FIFO {
	f := &FIFO{
		queue:  make([]uint32, length+1),
		length: length + 1,
	}
	return f
}

func (f *FIFO) put(val uint32) bool {
	if f.nodeNum+1 == f.length {
		return false
	}

	f.queue[f.end] = val
	f.end = (f.end + 1) % f.length
	f.nodeNum++
	return true
}

func (f *FIFO) get() (uint32, bool) {
	if f.nodeNum == 0 {
		return 0, false
	}

	val := f.queue[f.start]
	f.start = (f.start + 1) % f.length
	return val, true
}

func (f *FIFO) clear() {
	f.start = 0
	f.end = 0
	f.nodeNum = 0
}
