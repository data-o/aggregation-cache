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
	"time"
)

const (
	DEFAULT_FILE_SHARD   = 100
	GROUP_LEVEL          = 100
	VAL_GROUP_ID_START   = 1000000
	PREREAD_QUEUE_LENGTH = 10000

	HEADER_LAST_MODIFIED = "Last-Modified"
	S3_TIME_FORMAT       = "Mon, 02 Jan 2006 15:04:05 GMT"
	HEADER_REAL_FILE_ID  =  "File-Id"
)

type ErrorCode int

const (
	CODE_OK           = 0
	CODE_NOT_FOUND    = 404
	CODE_EMPTY        = 10000
	CODE_DLT_NOT_INIT = 10001
	CODE_AGAIN = 10002

)

const (
	confNewEpochthread    = 100
	confCacheVal          = false
	confEndpointNum       = 2
	confHttpClientNum     = 10
	defaultRequestTimeOut = 1200 * time.Second
	defaultHttpScheme     = "http"
)

var (
	confEndpoints = [2]string{"10.130.48.86:8080", "10.130.48.86:8080"}
)
