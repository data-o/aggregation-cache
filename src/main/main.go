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
	"time"
)

import (
	"bcache"
)

const (
	TEST_VERSION = "0.0.1"
)

func main() {
	var (
		jobId uint32 = 1
	)
	datasetName := "imagenet1k"
	maxCacheSize := (uint64(30) << 30)

	dm := bcache.NewDatasetManager()
	dlt, err := dm.Start(datasetName, jobId, maxCacheSize)

	if err != nil {
		fmt.Println("error in main", err)
	} else {
		dlt.Dump()
	}

	time.Sleep(1000 * time.Second)
}
