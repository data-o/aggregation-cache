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
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"os/signal"
	"syscall"
	"crypto/md5"
	"encoding/hex"
	"strings"
	"sync"
	"time"

)

import (
	"github.com/data-o/aggregation-cache/datao"
	"github.com/data-o/aggregation-cache/dconfig"
	"github.com/data-o/aggregation-cache/utils"
)

const (
	TEST_VERSION    = "0.0.1"
	CACHE_SIZE      = (uint64(70) << 30)
	TEST_TYPE_CACHE = 1
	TEST_TYPE_FS    = 2
	TEST_TYPE_HTTP  = 3
	TEST_TYPE_LRU   = 4

	defaultRequestTimeOut = 1200 * time.Second
	defaultHttpScheme     = "http"
)

var (
	isStop        bool
	confEndpoints = [3]string{"192.168.0.2:8080", "192.168.0.3:8080", "192.168.0.4:8080"}
	dm *datao.DatasetManager
)

type TestTemplate interface {
	OneJob(int, int, int, int, *sync.WaitGroup)
	GetFileLists() []*string
	ShuffleALLFileList() error
	ShufflePart() error
	GetValStartId() int
	GetFileNum() int
}

type FileTest struct {
	prefix    string
	fileLists []*string
	valStart  int
	fileNum   int
}

func (f *FileTest) GetFileLists() []*string {
	return f.fileLists
}

func (f *FileTest) GetValStartId() int {
	return f.valStart
}

func (f *FileTest) GetFileNum() int {
	return f.fileNum
}

func (f *FileTest) ShuffleALLFileList() error {
	f.fileLists = utils.ShuffleStringListAll(f.fileLists)
	return nil
}

func (f *FileTest) ShufflePart() error {
	var (
		err error
	)
	valFileNum := f.fileNum - f.valStart
	f.fileLists, err = utils.ShuffleStringList(f.fileLists, 0, f.valStart)
	if err != nil {
		return err
	}
	f.fileLists, err = utils.ShuffleStringList(f.fileLists, f.valStart, valFileNum)
	return err
}

func (f *FileTest) OneJob(turn, startId, endId, threadId int, wg *sync.WaitGroup) {
	defer wg.Done()
	objectContent := make([]byte, 20<<10)

	for i := startId; i < endId && !isStop; i++ {
		fileName := f.prefix + *(f.fileLists[i])

		var offset int64

		fd, err := os.Open(fileName)
		if err != nil {
			fmt.Println("Failed open", fileName, err)
			//isStop = true
			// break
			continue
		}
		isFail := false
		isFirst := true
		var realId string
		for {
			//read_num, err := fd.ReadAt(objectContent, offset)
			read_num, err := fd.Read(objectContent)
			if err != nil && err != io.EOF {
				fmt.Println("Failed read", fileName, err)
				fd.Close()
				isFail = true
				if !strings.Contains(err.Error(), "not found") {
					isStop = true
				}
				break
			}
			if isFirst {
				str := string(objectContent[:10])
				infos := strings.Split(str, ",")
				realId = infos[0]
				isFirst = false
			}
			offset += int64(read_num)
			if err == io.EOF {
				break
			}
		}
		fd.Close()

		if isFail {
			continue
		}

		//var realId string
		//body, err := ioutil.ReadFile(fileName)
		//if err != nil {
		//	fmt.Println("Failed read", fileName, err)
		//	continue
		//} else if len(body) <= 10 {
		//	fmt.Println("Failed read empty file ", fileName)
		//	isStop = true
		//	break
		//}

		//str := string(objectContent[:10])
		//infos := strings.Split(str, ",")
		//fmt.Println("Success read", fileName, "id", infos[0], "size", offset)
		fmt.Println("Success read", fileName, "size", offset, "id", realId)
	}
	//endTime := time.Now().UnixNano()
	// fmt.Println("Thread", threadId, "read", endId - startId, "files, use", (endTime - startTime)/1000000, "ms")
}

func NewFileTest(fileListsPath, prefix string, valStart int) (TestTemplate, error) {
	fsTest := &FileTest{
		prefix:   prefix,
		valStart: valStart,
	}

	var err error

	fsTest.fileLists, err = utils.GetListsFromFile(fileListsPath)
	if err != nil {
		return nil, err
	}

	fsTest.fileNum = len(fsTest.fileLists)
	return fsTest, nil
}

type DLTTest struct {
	dlt       *datao.DLT
	fileLists []*string
	valStart  int
	fileNum   int
	testMd5   bool
}

func (d *DLTTest) GetFileLists() []*string {
	return d.fileLists
}

func (d *DLTTest) GetValStartId() int {
	return d.valStart
}

func (d *DLTTest) GetFileNum() int {
	return d.fileNum
}

func (d *DLTTest) ShuffleALLFileList() error {
	d.fileLists = utils.ShuffleStringListAll(d.fileLists)
	return nil
}

func (d *DLTTest) ShufflePart() error {
	var (
		err error
	)
	valFileNum := d.fileNum - d.valStart
	d.fileLists, err = utils.ShuffleStringList(d.fileLists, 0, d.valStart)
	if err != nil {
		return err
	}
	d.fileLists, err = utils.ShuffleStringList(d.fileLists, d.valStart, valFileNum)
	return err
}

func (d *DLTTest) OneJob(turn, startId, endId, threadId int, wg *sync.WaitGroup) {
	//startTime := time.Now().UnixNano()
	defer wg.Done()

	i := startId

	for i < endId && !isStop {
		fileName := *(d.fileLists[i])
		_, fileSize, retMd5val, body, code, err := d.dlt.Get(fileName, true)
		if code == datao.CODE_AGAIN {
			fmt.Println("read", fileName, " again")
			continue
		} else if code == datao.CODE_NOT_FOUND {
			fmt.Println("Failed read", fileName, " not found")
		} else if err != nil {
			fmt.Println("Failed read exit ", fileName, err, code)
			isStop = true
			break
		} else if d.testMd5 {
			h := md5.New()
			n, err := io.Copy(h, body)
			if err != nil {
				fmt.Println("Failed, couldn't get file content ", fileName, err)
			} else if uint64(n) != fileSize {
				fmt.Println("Failed, only copy", n, "of", fileSize)
			}
			md5Val := hex.EncodeToString(h.Sum(nil))
			if md5Val == retMd5val {
				//fmt.Println("Success m", fileName, retMd5val)
			} else {
				fmt.Println("Failed", fileName, "expect md5", retMd5val, "but get", md5Val)
				isStop = true
				break
			}
			if err := body.Close(); err != nil {
				fmt.Println("Failed release 3", err)
			}
		} else {
			n, err := io.Copy(ioutil.Discard, body)
			if err != nil {
				fmt.Println("Failed, couldn't get file content ", fileName, err)
			} else if uint64(n) != fileSize {
				fmt.Println("Failed, only copy", n, "of", fileSize)
			}
			//fmt.Println("Success n", fileName, retMd5val)
			body.Close()
		}
		i++
	}
	//endTime := time.Now().UnixNano()
	// fmt.Println("Thread", threadId, "read", endId - startId, "files, use", (endTime - startTime)/1000000, "ms")
}

func NewAggCache(datasetMapPath, bucketName, confPath string, valStart int, jobId uint32,
	testMd5 bool) (TestTemplate, error) {
	
	var (
		err error
	)

	dm, err = datao.NewDatasetManager(0, dconfig.CLIENT_TYPE_CLIENT, confPath)
	if err != nil {
		return nil, err
	}

	dlt, err := dm.Start(datasetMapPath, bucketName, jobId)
	if err != nil {
		fmt.Println("error in main", err)
		return nil, err
	} else {
		dlt.Dump()
	}

	dltTest := &DLTTest{
		dlt:       dlt,
		fileLists: dlt.GetFileLists(),
		valStart:  valStart,
		testMd5:   testMd5,
	}
	dltTest.fileNum = len(dltTest.fileLists)
	time.Sleep(10)

	return dltTest, nil
}

type HTTPTest struct {
	endpoint  string
	fileLists []*string
	valStart  int
	fileNum   int
	scheme    string
	clients   []*http.Client
	clientNum int
	jobId     string
	checkMd5  bool
}

func (h *HTTPTest) GetFileLists() []*string {
	return h.fileLists
}

func (h *HTTPTest) GetValStartId() int {
	return h.valStart
}

func (h *HTTPTest) GetFileNum() int {
	return h.fileNum
}

func (h *HTTPTest) ShuffleALLFileList() error {
	h.fileLists = utils.ShuffleStringListAll(h.fileLists)
	return nil
}

func (h *HTTPTest) ShufflePart() error {
	var (
		err error
	)
	valFileNum := h.fileNum - h.valStart
	h.fileLists, err = utils.ShuffleStringList(h.fileLists, 0, h.valStart)
	if err != nil {
		return err
	}
	h.fileLists, err = utils.ShuffleStringList(h.fileLists, h.valStart, valFileNum)
	return err
}

func readFromHttpCache(httpClient *http.Client, defaultHttpScheme, endpoint, path,
	jobId string, check bool) ([]byte, uint64, error) {

	httpClient.Timeout = defaultRequestTimeOut
	// send request
	httpResponse, err := httpClient.Get("http://127.0.0.1:8081/" + path + "?jobid=" + jobId)
	if err != nil {
		return nil, 0, err
	} else if httpResponse == nil {
		return nil, 0, fmt.Errorf("http response is empty!")
	}

	// read data
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != 200 {
		message, _ := ioutil.ReadAll(httpResponse.Body)
		return nil, 0, fmt.Errorf("Http code %d, message %s!", httpResponse.StatusCode, message)
	}

	// get size
	size := uint64(httpResponse.ContentLength)

    if check {
		retMd5val := httpResponse.Header.Get("ETag")
		if retMd5val == "" {
			return nil, 0, fmt.Errorf("failed get etag")
		}
		h := md5.New()
		n, err := io.Copy(h, httpResponse.Body)
		if err != nil {
			return nil, 0, err
		} else if uint64(n) != size {
			return nil, 0, fmt.Errorf("only copy %d of %d", n, size)
		}
		md5Val := hex.EncodeToString(h.Sum(nil))
		if md5Val != retMd5val {
			return nil, 0, fmt.Errorf("expect md5", retMd5val, "but get", md5Val)
		} else {
			fmt.Println("get etag:", retMd5val)
		}
		return nil, size, nil
	} else {
		n, err := io.Copy(ioutil.Discard, httpResponse.Body)
		if err != nil {
			return nil, 0, err
		} else if uint64(n) != size {
			return nil, 0, fmt.Errorf("only copy %d of %d", n, size)
		}
		return nil, size, nil
	}
}

func (h *HTTPTest) OneJob(turn, startId, endId, threadId int, wg *sync.WaitGroup) {
	defer wg.Done()
	i := startId

	httpClient := h.clients[threadId%h.clientNum]

	for i < endId && !isStop {
		fileName := (*(h.fileLists[i]))
		_, _, err := readFromHttpCache(httpClient, h.scheme, h.endpoint, fileName, h.jobId, h.checkMd5)
		if err != nil {
			fmt.Println("Failed read", fileName, err)
		}
		i++
	}
}

func NewHttpCache(fileListsPath, cacheEndpoint string, valStart, conNum int,
	jobId int, check bool) (TestTemplate, error) {

	httpTest := &HTTPTest{
		endpoint:  cacheEndpoint,
		valStart:  valStart,
		scheme:    defaultHttpScheme,
		clients:   make([]*http.Client, conNum),
		clientNum: conNum,
		jobId:     strconv.Itoa(jobId),
		checkMd5:  check,
	}
	for i := 0; i < conNum; i++ {
		httpTest.clients[i] = utils.NewHttpClient()
	}

	var err error

	httpTest.fileLists, err = utils.GetListsFromFile(fileListsPath)
	if err != nil {
		return nil, err
	}

	httpTest.fileNum = len(httpTest.fileLists)
	return httpTest, nil
}

// for LRU test

type Node struct {
	Key      uint32
	Body     *[]byte
	FileSize uint64
	pre      *Node
	next     *Node
}

type LRUCache struct {
	limit   int
	HashMap map[uint32]*Node
	head    *Node
	end     *Node
	lock    sync.Mutex
}

func Constructor(capacity int) LRUCache {
	fmt.Println("Make lru", capacity)
	lruCache := LRUCache{limit: capacity}
	lruCache.HashMap = make(map[uint32]*Node, capacity)
	return lruCache
}

func (l *LRUCache) Get(key uint32) *Node {
	l.lock.Lock()
	defer l.lock.Unlock()

	if v, ok := l.HashMap[key]; ok {
		l.refreshNode(v)
		return v
	} else {
		return nil
	}
}

func (l *LRUCache) Put(key uint32, body *[]byte, size uint64) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if v, ok := l.HashMap[key]; !ok {
		if len(l.HashMap) >= l.limit {
			oldKey := l.removeNode(l.head)
			delete(l.HashMap, oldKey)
		}

		node := Node{
			Key:      key,
			Body:     body,
			FileSize: size,
		}

		l.addNode(&node)
		l.HashMap[key] = &node
	} else {
		l.refreshNode(v)
	}
}

func (l *LRUCache) refreshNode(node *Node) {
	if node == l.end {
		return
	}
	l.removeNode(node)
	l.addNode(node)
}

func (l *LRUCache) removeNode(node *Node) uint32 {
	if node == l.end {
		l.end = l.end.pre
	} else if node == l.head {
		l.head = l.head.next
	} else {
		node.pre.next = node.next
		node.next.pre = node.pre
	}
	return node.Key
}

func (l *LRUCache) addNode(node *Node) {
	if l.end != nil {
		l.end.next = node
		node.pre = l.end
		node.next = nil
	}
	l.end = node
	if l.head == nil {
		l.head = node
	}
}

type LRUTest struct {
	endpoints   []string
	endpointNum int

	fileLists []*string

	lrus   []*LRUCache
	lruNum int

	valStart  int
	fileNum   int
	scheme    string
	clients   []*http.Client
	clientNum int
}

func (h *LRUTest) GetFileLists() []*string {
	return h.fileLists
}

func (h *LRUTest) GetValStartId() int {
	return h.valStart
}

func (h *LRUTest) GetFileNum() int {
	return h.fileNum
}

func (h *LRUTest) ShuffleALLFileList() error {
	h.fileLists = utils.ShuffleStringListAll(h.fileLists)
	return nil
}

func (h *LRUTest) ShufflePart() error {
	var (
		err error
	)
	valFileNum := h.fileNum - h.valStart
	h.fileLists, err = utils.ShuffleStringList(h.fileLists, 0, h.valStart)
	if err != nil {
		return err
	}
	h.fileLists, err = utils.ShuffleStringList(h.fileLists, h.valStart, valFileNum)
	return err
}

func (h *LRUTest) readFromLRUCache(httpClient *http.Client, defaultHttpScheme,
	endpoint, path string, fileId uint32) ([]byte, uint64, error) {

	lru := h.lrus[fileId%uint32(h.lruNum)]
	node := lru.Get(fileId)
	if node != nil {
		return *(node.Body), node.FileSize, nil
	}

	httpClient.Timeout = defaultRequestTimeOut
	endUrl := defaultHttpScheme + "://" + endpoint + "/imagenet/" + path

	// send request
	httpResponse, err := httpClient.Get(endUrl)
	if err != nil {
		return nil, 0, err
	} else if httpResponse == nil {
		return nil, 0, fmt.Errorf("http response is empty!")
	}

	// read data
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != 200 {
		message, _ := ioutil.ReadAll(httpResponse.Body)
		return nil, 0, fmt.Errorf("Http code %d, message %s!", httpResponse.StatusCode, message)
	}

	// get size
	size := uint64(httpResponse.ContentLength)
	body := make([]byte, size)

	n, err := io.ReadFull(httpResponse.Body, body)
	if err != nil {
		return nil, 0, err
	} else if uint64(n) != size {
		return nil, 0, fmt.Errorf("only copy %d of %d", n, size)
	}

	lru.Put(fileId, &body, size)

	return body, size, nil
}

func (h *LRUTest) OneJob(turn, startId, endId, threadId int, wg *sync.WaitGroup) {
	defer wg.Done()
	i := startId

	httpClient := h.clients[threadId%h.clientNum]

	for i < endId && !isStop {
		fileName := (*(h.fileLists[i]))
		endpoint := h.endpoints[i%3]
		_, length, err := h.readFromLRUCache(httpClient, h.scheme, endpoint, fileName, uint32(i))
		if err != nil {
			fmt.Println("Failed read", fileName, err)
		} else {
			fmt.Println("Success read", fileName, length)
		}
		i++
	}
}

func NewLRUTEST() (TestTemplate, error) {
	fmt.Println("new http test")
	datasetName := "imagenet1k.files"
	clientNum := 10
	lruNum := 10

	lruTest := &LRUTest{
		endpoints:   confEndpoints[:],
		endpointNum: 3,
		valStart:    1281167,
		scheme:      defaultHttpScheme,
		clients:     make([]*http.Client, clientNum),
		clientNum:   clientNum,
		lrus:        make([]*LRUCache, lruNum),
		lruNum:      lruNum,
	}

	var err error

	lruTest.fileLists, err = utils.GetListsFromFile(datasetName)
	if err != nil {
		return nil, err
	}

	lruTest.fileNum = len(lruTest.fileLists)

	// imagenet1k
	avgSize := 153614448851 / lruTest.fileNum
	groupNum := 2000

	for i := 0; i < clientNum; i++ {
		lruTest.clients[i] = utils.NewHttpClient()
	}

	for i := 0; i < lruNum; i++ {
		lru := Constructor((int(CACHE_SIZE/uint64(avgSize)) / groupNum))
		lruTest.lrus[i] = &lru
	}

	return lruTest, nil
}

func readOneSplit(threadNum, turn int, t TestTemplate) {
	var (
		wg  sync.WaitGroup
		err error
	)

	fmt.Println("thread num", threadNum)

	err = t.ShufflePart()
	if err != nil {
		fmt.Println("failed shuffle", err)
		return
	}
	valStart := t.GetValStartId()

	eachThreadFileNum := (valStart + threadNum - 1) / threadNum
	if eachThreadFileNum == 0 {
		eachThreadFileNum = 1
	}

	for i := 0; i < threadNum; i++ {
		start := i * eachThreadFileNum
		end := (i + 1) * eachThreadFileNum
		if end > valStart {
			end = valStart
		}
		wg.Add(1)
		go t.OneJob(turn, start, end, i, &wg)
	}

	wg.Wait()

	fileNum := t.GetFileNum()
	valFileNum := fileNum - valStart
	eachThreadFileNum = (valFileNum + threadNum - 1) / threadNum
	if eachThreadFileNum == 0 {
		eachThreadFileNum = 1
	}

	for i := 0; i < threadNum; i++ {
		start := valStart + i*eachThreadFileNum
		end := valStart + (i+1)*eachThreadFileNum
		if end > fileNum {
			end = fileNum
		}
		wg.Add(1)
		go t.OneJob(turn, start, end, i, &wg)
	}
	wg.Wait()
}

func readOne(threadNum, turn int, t TestTemplate) {
	var (
		wg  sync.WaitGroup
		err error
	)

	fmt.Println("Start trun", turn)

	err = t.ShuffleALLFileList()
	if err != nil {
		fmt.Println("failed shuffle", err)
		return
	}

	fileNum := t.GetFileNum()

	startTime := time.Now().UnixNano()
	eachThreadFileNum := (fileNum + threadNum - 1) / threadNum
	if eachThreadFileNum == 0 {
		eachThreadFileNum = 1
	}

	for i := 0; i < threadNum; i++ {
		start := i * eachThreadFileNum
		end := (i + 1) * eachThreadFileNum
		if end > fileNum {
			end = fileNum
		}
		wg.Add(1)
		go t.OneJob(turn, start, end, i, &wg)
	}
	wg.Wait()
	endTime := time.Now().UnixNano()
	fmt.Println("Trun", turn, "read", fileNum, "files, use", (endTime-startTime)/1000000, "ms")
}

func testStart(testType, threadNum, totalTurn int) {
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
		t   TestTemplate
		err error
	)

	go func() {
		http.ListenAndServe("0.0.0.0:8082", nil)
	}()

	var (
		datasetName  string
		fileListPath string
		pathPrefix   string
		confPath     string
		threadNum    int
		epochNum     int
		testType     int
		valStart     int
		testMd5      bool

		// cache server test
		cacheEndpoint string
		conNum        int
		jobId        int

        testStop   bool
	)

	flag.IntVar(&testType, "k", 0, "test type: cache 1, fs 2, http 3, lru 4")
	flag.IntVar(&threadNum, "t", 16, "number of thread")
	flag.StringVar(&confPath, "c", "./datao.cfg", "the path of config file")
	flag.IntVar(&epochNum, "e", 2, "number of epoch")
	flag.IntVar(&jobId, "j", 0, "job id, must greater than 0")
	flag.StringVar(&cacheEndpoint, "endpoint", "127.0.0.1:8081", "the enpdoint of cache server")
	flag.IntVar(&conNum, "con", 10, "the number of http client")
	flag.IntVar(&valStart, "v", 1281167, "the start id of val")
	flag.StringVar(&datasetName, "b", "", "the name of dataset")
	flag.StringVar(&fileListPath, "f", "", "the path of DatasetMap or filelist")
	flag.StringVar(&pathPrefix, "p", "", "the prefix of fs")
	flag.BoolVar(&testMd5, "m", false, "check the md5 of content")

	//flag.BoolVar(&isDebug, "d", false, "debug")
	//flag.BoolVar(&showVersion, "v", false, "version")
	flag.Parse()

	// check arguments
	if testType <= 0 || testType > 4 {
		fmt.Println("invaild test type")
		flag.PrintDefaults()
		return
	} else if fileListPath == "" {
		fmt.Println("the path of fileList or datasetMap is empty")
		flag.PrintDefaults()
		return
	}

	if testType == TEST_TYPE_CACHE {
		if datasetName == "" {
			fmt.Println("the name of dataset is empty")
			flag.PrintDefaults()
			return
		}
		if jobId <= 0 {
			fmt.Println("invaild job id", jobId)
			flag.PrintDefaults()
			return
		}
		t, err = NewAggCache(fileListPath, datasetName, confPath, valStart, uint32(jobId), testMd5)
		fmt.Println("Start cache test")
	} else if testType == TEST_TYPE_FS {
		if pathPrefix == "" {
			fmt.Println("the prefix of fs is empty")
			flag.PrintDefaults()
			return
		}
		fmt.Println("Start file test")
		t, err = NewFileTest(fileListPath, pathPrefix, valStart)
	} else if testType == TEST_TYPE_HTTP {
		if cacheEndpoint == "" {
			fmt.Println("the endpoint fs is empty")
			flag.PrintDefaults()
			return
		}
		t, err = NewHttpCache(fileListPath, cacheEndpoint, valStart, conNum, jobId, testMd5)
	} else if testType == TEST_TYPE_LRU {
		t, err = NewLRUTEST()
	} else {
		fmt.Println("error tyep")
		return
	}

	if err != nil {
		fmt.Println("Failed create test template", err)
		return
	}

    defer func() {
		err := recover()

        if !testStop && testType == TEST_TYPE_CACHE {
            stopDLT(datasetName, jobId)
            testStop = true
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
		if !testStop && testType == TEST_TYPE_CACHE {
            stopDLT(datasetName, jobId)
            testStop = true
		}
		os.Exit(1)
	}()

	fmt.Println("turn num", epochNum)

	// start test
	startTime := time.Now().UnixNano()
	for turn := 0; turn < epochNum && !isStop; turn++ {
		readOneSplit(threadNum, turn, t)
	}

	endTime := time.Now().UnixNano()
	fmt.Println("Total  use", (endTime-startTime)/1000000, "ms")

	if testType == TEST_TYPE_CACHE {
		err = dm.Finish(datasetName, uint32(jobId))
		if err != nil {
			fmt.Println("faile stop job", jobId, err)
		}
	}
}
