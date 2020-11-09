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
	"sync"
)

import (
	"github.com/data-o/aggregation-cache/dconfig"
)

var (
	ugPriorityJobQueue   [](chan *jobInfo)
	lowPriorityJobQueue  [](chan *jobInfo)
	highPriorityJobQueue [](chan *jobInfo)
	inited               bool
	prereadStarted       bool
)

func contentInit() {
	if inited {
		return
	}

	ugPriorityJobQueue = make([](chan *jobInfo), dconfig.ConfEndpointNum)
	highPriorityJobQueue = make([](chan *jobInfo), dconfig.ConfEndpointNum)
	lowPriorityJobQueue = make([](chan *jobInfo), dconfig.ConfEndpointNum)

	for i := uint32(0); i < dconfig.ConfEndpointNum; i++ {
		ugPriorityJobQueue[i] = make(chan *jobInfo, dconfig.ConfWokerJobQueueLength)
		highPriorityJobQueue[i] = make(chan *jobInfo, dconfig.ConfWokerJobQueueLength)
		lowPriorityJobQueue[i] = make(chan *jobInfo, dconfig.ConfWokerJobQueueLength)
	}

	content = make([]byte, 262144)
	for i := 0; i < 262144; i++ {
		content[i] = 1
	}
	inited = true
}

type jobInfo struct {
	fileId uint32
	group  *DLTGroup
}

var jobInfoPool = sync.Pool{
	New: func() interface{} {
		return &jobInfo{}
	},
}

type worker struct {
	workerId uint32
	index    uint32
}

var (
	workers []worker
	content []byte
)

func (w *worker) ReadOne(job *jobInfo) {
	ret, code, err := job.group.readFromBackend(job.fileId, true)
	if code != CODE_OK {
		LogFromtln("failed read", job.fileId, code, err)
	}

	job.group.retQueue <- ret
	jobInfoPool.Put(job)
}

func (w *worker) Start() {
	for {
		select {
		case jobUG := <-ugPriorityJobQueue[w.index]:
			w.ReadOne(jobUG)
		case jobHigh := <-highPriorityJobQueue[w.index]:
			w.ReadOne(jobHigh)
		case jobLow := <-lowPriorityJobQueue[w.index]:
			w.ReadOne(jobLow)
		}
	}
}

func startPrereadWorker(clientType dconfig.ClientType, serverId uint32) {
	if prereadStarted {
		return
	}

	workers = make([]worker, dconfig.ConfWokerThreadNum)
	for i := uint32(0); i < dconfig.ConfWokerThreadNum; i++ {
		worker := &workers[i]
		worker.workerId = i
		if clientType == dconfig.CLIENT_TYPE_CLIENT {
			worker.index = i % dconfig.ConfEndpointNum
		} else {
			worker.index = serverId
		}
		go worker.Start()
	}

	prereadStarted = true
}
