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

package utils

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	DEFAULT_RETRY_NUM = 3

	defaultMaxIdleConnsPerHost   = 300
	defaultMaxIdleConns          = 300
	defaultIdleConnTimeout       = 0
	defaultResponseHeaderTimeout = 60 * time.Second
	defaultDialTimeout           = 30 * time.Second
)

var httpRequestPool = sync.Pool{
	New: func() interface{} {
		return &http.Request{
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			URL:        &url.URL{},
		}
	},
}

func SplitHttpPrefix(endpoint string) (string, string) {
	if strings.HasPrefix(endpoint, "https://") {
		return "https", endpoint[8:]
	} else if strings.HasPrefix(endpoint, "http://") {
		return "http", endpoint[7:]
	}
	return "http", endpoint
}

// build a new http client
func NewHttpClient() *http.Client {
	httpClient := &http.Client{}
	transport := &http.Transport{
		MaxIdleConns:          defaultMaxIdleConns,
		MaxIdleConnsPerHost:   defaultMaxIdleConnsPerHost,
		ResponseHeaderTimeout: defaultResponseHeaderTimeout,
		IdleConnTimeout:       defaultIdleConnTimeout,
		Dial: func(network, address string) (net.Conn, error) {
			conn, err := net.DialTimeout(network, address, defaultDialTimeout)
			if err != nil {
				return nil, err
			}
			return conn, nil
		},
	}
	httpClient.Transport = transport
	return httpClient
}

func NewHttpRequest(httpScheme, endpoint, path, query string) *http.Request {
	request := httpRequestPool.Get().(*http.Request)
	request.URL.Scheme = httpScheme
	request.URL.Host = endpoint
	request.URL.Path = path
	request.URL.RawQuery = query
	return request
}

func CloseHttpRequest(request *http.Request) {
	httpRequestPool.Put(request)
}

func SendToBackend(httpClient *http.Client, scheme, endpoint, prefix, query string,
	data []byte) (int, error) {

	length := len(data)
	httpRequest := NewHttpRequest(scheme, endpoint, prefix, query)
	httpClient.Timeout = defaultResponseHeaderTimeout
	httpRequest.ContentLength = int64(length)
	httpRequest.Method = http.MethodPut

	for retry := 0; retry < DEFAULT_RETRY_NUM; retry++ {
		httpRequest.Body = ioutil.NopCloser(bytes.NewBuffer(data))

		// send request
		httpResponse, err := httpClient.Do(httpRequest)
		if err != nil {
			return 0, err
		} else if httpResponse == nil {
			return 0, fmt.Errorf("http response is empty!")
		}

		defer httpResponse.Body.Close()
		message, _ := ioutil.ReadAll(httpResponse.Body)
		// read data
		if httpResponse.StatusCode != 200 {
			err = fmt.Errorf("Http code %d, message %s!", httpResponse.StatusCode, message)
			if httpResponse.StatusCode >= 500 {
				continue
			}
			return httpResponse.StatusCode, err
		}
		return httpResponse.StatusCode, nil
	}

	return 200, nil
}
