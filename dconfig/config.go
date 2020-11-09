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

// This file provide the major operations on configuration

package dconfig

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

import (
	"github.com/data-o/aggregation-cache/utils"
)

type ClientType int

const (
	CLIENT_TYPE_CLIENT = 1023
	CLIENT_TYPE_SERVER = 1024
)

var (
	RequestTimeOut = 1200 * time.Second
	HttpScheme     = "http"

	ConfClientType             ClientType
	ConfWokerThreadNum         uint32 = 126
	ConfAllowGlobalExtrMemSize uint64 = 6442450944
	ConfAllowAmpMemSize        uint64 = 65536
	ConfWithCacheServer        bool
	ConfTryReadDirect          bool = true

	ConfListendAddress string = "0.0.0.0:8081"
	ConfDatasetMapPath string
	ConfBucketName     string

	ConfWokerJobQueueLength int    = 20000
	ConfGroupRetQueueLength int    = 20
	ConfEndpointNum         uint32 = 0
	ConfCheckBodyMd5        bool   = false
	ConfCacheVal            bool   = true
	ConfEndpoints           []string
	ConfCacheSize           uint64 = 75161927680 //70GB
)

// Store the default configuration.
// The default value of int is zero, therefore each parameter is special as string .
type DefaultsCfg struct {
	ClientType             string
	WokerThreadNum         string
	AllowGlobalExtrMemSize string
	AllowAmpMemSize        string
	CheckBodyMd5           string
	WithCacheServer        string
	Endpoints              string
	Address                string // ip + port "192.168.0.1:8080"
	CacheSize              string
	TryReadDirect          string
}

type ServerConfig struct {
	Defaults DefaultsCfg
}

// When configuration file exist, loading configuration from a file
func LoadConfigFromFile(configFilePath string) error {
	if ok := utils.DoesFileExist(configFilePath); !ok {
		return fmt.Errorf("config file %s not exist!", configFilePath)
	}

	cfg := &ServerConfig{}

	if err := LoadConfig(configFilePath, cfg); err != nil {
		return fmt.Errorf("load configuration error: %s", err)
	}
	if err := setConfig(cfg); err != nil {
		return err
	}
	return nil
}

func setConfig(cfg *ServerConfig) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}

	// client type
	if cfg.Defaults.ClientType == "" {
		return fmt.Errorf("config: client type is empty")
	} else {
		if cfg.Defaults.ClientType == "client" {
			ConfClientType = CLIENT_TYPE_CLIENT
		} else if cfg.Defaults.ClientType == "server" {
			ConfClientType = CLIENT_TYPE_SERVER
		} else {
			return fmt.Errorf("config: invaild client type '%s'", cfg.Defaults.ClientType)
		}
	}

	// worker thread num
	if cfg.Defaults.WokerThreadNum != "" {
		val, ok := strconv.ParseUint(cfg.Defaults.WokerThreadNum, 10, 32)
		if ok != nil || val < 1 {
			return fmt.Errorf("worker thread number must be integer and  greater than zero!")
		}
		ConfWokerThreadNum = uint32(val)
	}

	// global extr mem size
	if cfg.Defaults.AllowGlobalExtrMemSize != "" {
		val, ok := strconv.ParseUint(cfg.Defaults.AllowGlobalExtrMemSize, 10, 64)
		if ok != nil || val < 1 {
			return fmt.Errorf("allow global extr mem size  must greater than zero!")
		}
		ConfAllowGlobalExtrMemSize = val
	}

	// amp mem size
	if cfg.Defaults.AllowAmpMemSize != "" {
		val, ok := strconv.ParseUint(cfg.Defaults.AllowAmpMemSize, 10, 64)
		if ok != nil || val < 1 {
			return fmt.Errorf("part size must greater than zero!")
		}
		ConfAllowAmpMemSize = val
	}

	// with cache server
	if cfg.Defaults.WithCacheServer != "" {
		if cfg.Defaults.WithCacheServer == "true" {
			ConfWithCacheServer = true
		} else if cfg.Defaults.WithCacheServer != "false" {
			return fmt.Errorf("with cache server must be true or false!")
		}
	}

	// set endpoints
	if cfg.Defaults.Endpoints == "" {
		return fmt.Errorf("endpoints is empty!")
	}

	ConfEndpoints = strings.Split(cfg.Defaults.Endpoints, ",")
	ConfEndpointNum = uint32(len(ConfEndpoints))
	if ConfEndpointNum == 0 {
		return fmt.Errorf("endpoint num is 0")
	}

	// set address
	if cfg.Defaults.Address != "" {
		ConfListendAddress = cfg.Defaults.Address
	}

	// set cache size
	if cfg.Defaults.CacheSize != "" {
		val, ok := strconv.ParseUint(cfg.Defaults.CacheSize, 10, 64)
		if ok != nil || val < 1 {
			return fmt.Errorf("cache size must greater than zero!")
		}
		ConfCacheSize = val
	}

	// set direct read
	if cfg.Defaults.TryReadDirect != "" {
		if cfg.Defaults.TryReadDirect == "true" {
			ConfTryReadDirect = true
		} else if cfg.Defaults.TryReadDirect == "false" {
			ConfTryReadDirect = false
		} else {
			return fmt.Errorf("try read direct must be 'true' or 'false'!")
		}
	}

	return nil
}
