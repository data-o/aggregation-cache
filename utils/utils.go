package utils

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	BIT_MAP_MAX_LENGTH            = 1 << 30
	MAX_CONNECT_NUM               = 300
	DEFAULT_THREAD_IN_ONE_CONNECT = 10
	BOS_PATH_SEPARATOR            = "/"
)

var (
	OsPathSeparator = fmt.Sprintf("%c", os.PathSeparator)
)

type BitMap struct {
	maps     []uint64
	emptyBit []uint64
	myLength uint32
}

func NewBitMap(length uint32) (*BitMap, error) {

	if length < 1 || length > BIT_MAP_MAX_LENGTH {
		return nil, fmt.Errorf("Invalid bit map length!")
	}

	t := &BitMap{}
	t.init(length)
	return t, nil
}

func (b *BitMap) init(length uint32) {
	mapLength := length / 64
	if length%64 != 0 {
		mapLength += 1
	}
	b.maps = make([]uint64, mapLength)
	b.emptyBit = make([]uint64, mapLength)
	b.myLength = length
}

func (b *BitMap) Set(index uint32) {
	// don't check index, because it is heavy
	b.maps[index/64] |= uint64(1) << (index % 64)
}

func (b *BitMap) Get(index uint32) bool {
	// don't check index, because it is heavy
	return (b.maps[index/64] & (uint64(1) << (index % 64))) != 0
}

func (b *BitMap) Clear() {
	copy(b.maps, b.emptyBit)
}

func DoesDirExist(path string) bool {
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return true
	}
	return false
}

// Check whether file path exist.
func DoesFileExist(filePath string) bool {
	fileInfo, err := os.Stat(filePath)
	if err == nil && !fileInfo.IsDir() {
		return true
	}
	return false
}

// Check whether path exist
func DoesPathExist(filePath string) bool {
	_, err := os.Stat(filePath)
	if err == nil {
		return true
	}
	return false
}

func CountdownStart(cond *sync.Cond, sleepTime time.Duration, retry int) {
	// retry 3 times
	for i := 0; i < retry; i++ {
		time.Sleep(sleepTime)
		cond.L.Lock()
		cond.Broadcast()
		cond.L.Unlock()
	}
}

func WaitForCountdown(cond *sync.Cond) {
	// wait for time out
	cond.L.Lock()
	cond.Wait()
	cond.L.Unlock()
}

func ShuffleList(lists []int) []int {
	totalNum := len(lists)
	for i := totalNum - 1; i >= 0; i-- {
		cId := rand.Intn(totalNum)
		temp := lists[i]
		lists[i] = lists[cId]
		lists[cId] = temp
	}
	return lists
}

// too many connection may cause too many socket
func GetProperConnNum(connNum int, threadNum int) int {
	var (
		eachConnectThreadNum int
	)
	if connNum != 0 {
		eachConnectThreadNum = threadNum / connNum
		if threadNum%connNum != 0 {
			eachConnectThreadNum++
		}
	} else {
		eachConnectThreadNum = DEFAULT_THREAD_IN_ONE_CONNECT
		if threadNum > MAX_CONNECT_NUM {
			eachConnectThreadNum = threadNum/MAX_CONNECT_NUM + 1
			if threadNum%MAX_CONNECT_NUM != 0 {
				eachConnectThreadNum++
			}
		}
	}
	return eachConnectThreadNum
}

// get file lists info from file
func GetListsFromFile(listPath string) ([]*string, error) {
	fd, err := os.Open(listPath)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	lists := []*string{}

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		objectName := strings.TrimSpace(scanner.Text())
		if objectName == "" {
			continue
		}
		lists = append(lists, &objectName)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lists, nil
}

func ShuffleStringListAll(lists []*string) []*string {
	listsLength := len(lists)
	for i := listsLength - 1; i >= 0; i-- {
		cId := rand.Intn(listsLength)
		temp := lists[i]
		lists[i] = lists[cId]
		lists[cId] = temp
	}
	return lists
}

func ShuffleStringList(lists []*string, start int, length int) ([]*string, error) {
	listsLength := len(lists)
	end := start + length
	if end > listsLength {
		return lists, fmt.Errorf("invalid end")
	}

	for i := start; i < end; i++ {
		cId := rand.Intn(length)
		temp := lists[i]
		lists[i] = lists[start+cId]
		lists[start+cId] = temp
	}

	return lists, nil
}

// Change UTC time string to timestamp
func TranUTCTimeStringToTimeStamp(utcTimeString, oldTimeForm string) (int64, error) {
	utcTime, err := time.Parse(oldTimeForm, utcTimeString)
	if err != nil {
		return 0, err
	}
	timestamp := utcTime.Unix()
	return timestamp, nil
}

func StringToInt(str string, mode int) int {
	val := int(crc32.ChecksumIEEE([]byte(str)))
	if val >= 0 {
		return val % mode
	} else {
		return (-val) % mode
	}
}
