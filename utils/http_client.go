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
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultMaxIdleConnsPerHost   = 1000
	defaultMaxIdleConns          = 1000
	defaultIdleConnTimeout       = 0
	defaultResponseHeaderTimeout = 60 * time.Second
	defaultDialTimeout           = 30 * time.Second
)

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
	httpRequest := &http.Request{
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
	}
	internalUrl := &url.URL{
		Scheme:   httpScheme,
		Host:     endpoint,
		Path:     path,
		RawQuery: query,
	}
	httpRequest.URL = internalUrl
	return httpRequest
}
