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

package utils

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ConfMempoolBlockSize = 4096 //4k
	MEM_POOL_START       = 10000000000
	EXTR_MEM_POOL_START  = 20000000000
	EXTR_POOL_START      = 30000000000
	POOL_TYPE_MASK       = 10000000000

	ALLOC_TYPE_NONE   = 10000
	ALLOC_TYPE_NORMAL = 10001 // alloc from memory pool
	ALLOC_TYPE_EXTR   = 10002 // alloc from extra memory pool
	ALLOC_TYPE_NEW    = 10003 // alloc new node from system
)

type ReaderAllocType int

type Callback interface {
	Call(uint64) error
}

type Block struct {
	data []byte
}

var (
	newBlockNum int
)

var blocksPool = sync.Pool{
	New: func() interface{} {
		newBlockNum++
		if newBlockNum%10000 == 0 {
			fmt.Println(time.Now().Format("2006-01-02 15:04:05.9999"), "New block", newBlockNum)
		}
		return &Block{
			data: make([]byte, ConfMempoolBlockSize),
		}
	},
}
var poolRefReadCloser = sync.Pool{
	New: func() interface{} {
		return &RefReadCloser{}
	},
}

type BlockManage struct {
	id         uint32
	data       []byte
	freeList   []int64
	freeBlock  int64
	poolBlock  int64
	extrBlock  int64
	totalBlock int64

	isAlloced bool
	hasWait   bool
	lock      sync.Mutex
	cond      *sync.Cond
}

func NewBlocks(size, extrSize uint64, id uint32) *BlockManage {
	b := &BlockManage{
		id: id,
	}
	b.cond = sync.NewCond(&b.lock)

	b.Init(size, extrSize)
	return b
}

func (b *BlockManage) Init(size, extrSize uint64) {
	b.lock.Lock()
	defer b.lock.Unlock()

	blockNum := int64((size + ConfMempoolBlockSize - 1) / ConfMempoolBlockSize)
	totalBlockNum := int64((size + extrSize + ConfMempoolBlockSize - 1) / ConfMempoolBlockSize)

	totalSize := totalBlockNum * ConfMempoolBlockSize
	b.data = make([]byte, totalSize)
	atomic.StoreInt64(&b.freeBlock, totalBlockNum)

	b.poolBlock = blockNum
	b.extrBlock = totalBlockNum - blockNum
	b.totalBlock = totalBlockNum

	b.freeList = make([]int64, totalBlockNum)
	for i := int64(0); i < totalBlockNum; i++ {
		b.freeList[i] = i
	}
}

func (b *BlockManage) Alloc() {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.isAlloced {
		return
	}

	dataLen := len(b.data)
	for i := 0; i < dataLen; i++ {
		b.data[i] = 1
	}
	b.isAlloced = true
}

func NeedBlock(fileSize uint64) int64 {
	if fileSize == 0 {
		return 0
	}

	blockNum := (fileSize + ConfMempoolBlockSize - 1) / ConfMempoolBlockSize
	return int64(blockNum)
}

func (b *BlockManage) Reserve(blockNum int64) bool {
	if blockNum+b.extrBlock > b.totalBlock {
		return true
	}

	if blockNum+b.extrBlock <= atomic.LoadInt64(&b.freeBlock) {
		return true
	}

	return false
}

func (b *BlockManage) GetFreeSize() int64 {
	return b.freeBlock
}

func (b *BlockManage) WaitMem(blockNum int64) {
	if atomic.LoadInt64(&b.freeBlock) > blockNum+b.extrBlock {
		return
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	if atomic.LoadInt64(&b.freeBlock) > blockNum+b.extrBlock {
		return
	}

	b.hasWait = true
	b.cond.Wait()
	b.hasWait = false
}

func (b *BlockManage) TryWakeup(id uint32) {
	if b.hasWait == true {
		b.cond.Signal()
	}
}

// must protect by lock
func (b *BlockManage) Get(needBlockNum int64, blocks []int64) ([]int64, int64) {
	if atomic.LoadInt64(&b.freeBlock) == 0 {
		return blocks, 0
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	if needBlockNum > b.freeBlock {
		needBlockNum = b.freeBlock
	}

	for i := int64(0); i < needBlockNum; i++ {
		atomic.AddInt64(&b.freeBlock, -1)
		blocks = append(blocks, b.freeList[b.freeBlock])
	}

	return blocks, needBlockNum
}

// must protect by lock
func (b *BlockManage) GetOne() (int64, bool) {
	if atomic.LoadInt64(&b.freeBlock) == 0 {
		return 0, false
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	if b.freeBlock == 0 {
		return 0, false
	}

	atomic.AddInt64(&b.freeBlock, -1)
	return b.freeList[b.freeBlock], true
}

func (b *BlockManage) Put(blocks []int64, blockNum int64, notify bool) error {
	if blockNum < 0 {
		return fmt.Errorf("invaild block number %d", blockNum)
	} else if blockNum == 0 {
		return nil
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	for i := int64(0); i < blockNum; i++ {
		if blocks[i] >= b.totalBlock {
			return fmt.Errorf("invaild id %d", blocks[i])
		}

		b.freeList[b.freeBlock] = blocks[i]
		atomic.AddInt64(&b.freeBlock, 1)
	}

	if notify && b.hasWait {
		b.cond.Signal()
	}

	return nil
}

type MemBlocks struct {
	bm     *BlockManage
	extrBm *BlockManage

	memPoolBlocks []int64
	usedBlockNum  int64

	extrPoolBlocks   []int64
	extrPoolBlockNum int64

	extrBlocks   []*Block
	extrBlockNum int64

	dataMap []int64 // 0xxxx block  1xxxx extrPoolBlocks 2xxx extrBlocks

	size uint64
}

func (m *MemBlocks) Init(bm, extrBm *BlockManage, size uint64, data io.ReadCloser) (uint64,
	ReaderAllocType, error) {
	var (
		n        int
		readed   uint64
		err      error
		blockNum int64
	)

	needBlockNum := int64((size + ConfMempoolBlockSize - 1) / ConfMempoolBlockSize)
	if int64(cap(m.memPoolBlocks)) < needBlockNum {
		m.memPoolBlocks = make([]int64, 0, needBlockNum+4)
	}

	if int64(cap(m.dataMap)) < needBlockNum {
		m.dataMap = make([]int64, 0, needBlockNum+4)
	}

	m.bm = bm
	m.extrBm = extrBm
	m.size = size

	extrBmNum := 0
	poolBmBum := 0

	for readed < size {
		// try get from self bm
		if m.memPoolBlocks, blockNum = bm.Get(needBlockNum, m.memPoolBlocks); blockNum != 0 {
			needBlockNum -= blockNum
			for j := int64(0); j < blockNum; j++ {
				needByte := ConfMempoolBlockSize
				if readed+ConfMempoolBlockSize > size {
					needByte = int(size - readed)
				}

				startId := m.memPoolBlocks[m.usedBlockNum] * ConfMempoolBlockSize
				endId := startId + int64(needByte)

				n, err = io.ReadAtLeast(data, m.bm.data[startId:endId], needByte)
				if n > 0 {
					readed += uint64(n)
					m.dataMap = append(m.dataMap, MEM_POOL_START+m.usedBlockNum)
					m.usedBlockNum++
				} else if err != nil {
					break
				}
			}
			// try get mem for extr blocks
		} else if blockId, ok := extrBm.GetOne(); ok {
			needBlockNum--
			if cap(m.extrPoolBlocks) == 0 {
				m.extrPoolBlocks = make([]int64, 0, 8)
			}

			needByte := ConfMempoolBlockSize
			if readed+ConfMempoolBlockSize > size {
				needByte = int(size - readed)
			}

			extrBmNum++

			startId := blockId * ConfMempoolBlockSize
			endId := startId + int64(needByte)

			n, err = io.ReadAtLeast(data, extrBm.data[startId:endId], needByte)
			if n > 0 {
				readed += uint64(n)
				m.extrPoolBlocks = append(m.extrPoolBlocks, blockId)
				m.dataMap = append(m.dataMap, EXTR_MEM_POOL_START+m.extrPoolBlockNum)
				m.extrPoolBlockNum++
			}
		} else {
			if cap(m.extrBlocks) == 0 {
				m.extrBlocks = make([]*Block, 0, 4)
			}

			poolBmBum++
			block := blocksPool.Get().(*Block)
			needByte := ConfMempoolBlockSize
			if readed+ConfMempoolBlockSize > size {
				needByte = int(size - readed)
			}
			n, err = io.ReadAtLeast(data, block.data, needByte)
			if n > 0 {
				readed += uint64(n)
				m.extrBlocks = append(m.extrBlocks, block)
				m.dataMap = append(m.dataMap, EXTR_POOL_START+m.extrBlockNum)
				m.extrBlockNum++
			}
		}

		if err != nil && err != io.EOF { // at end
			return readed, ALLOC_TYPE_NONE, err
		} else if err == io.EOF {
			err = nil
			break
		} else if n == 0 {
			return 0, ALLOC_TYPE_NONE, fmt.Errorf("Read 0 bytes, but error is not EOF")
		}
	}

	if readed != size {
		return 0, ALLOC_TYPE_NONE,
			fmt.Errorf("only read %d byte from ReadCloser instead of %d", readed, size)
	}

	if poolBmBum > 0 {
		return readed, ALLOC_TYPE_NEW, nil
	} else if extrBmNum > 0 {
		return readed, ALLOC_TYPE_EXTR, nil
	}

	return readed, ALLOC_TYPE_NORMAL, nil
}

// return the release size of mempool
// return size not contain the size of extrBlocks
func (m *MemBlocks) Release(notify bool) {
	err := m.bm.Put(m.memPoolBlocks, m.usedBlockNum, notify)
	if err != nil {
		fmt.Println("Wraning: fail to release mem blocks", err)
	}

	if m.extrPoolBlockNum > 0 {
		err := m.extrBm.Put(m.extrPoolBlocks, m.extrPoolBlockNum, false)
		if err != nil {
			fmt.Println("Wraning: fail to release extr blocks", err)
		}
	}

	for _, value := range m.extrBlocks {
		blocksPool.Put(value)
	}

	m.bm = nil
	m.extrBm = nil
	m.memPoolBlocks = m.memPoolBlocks[:0]
	m.usedBlockNum = 0
	m.extrPoolBlocks = m.extrPoolBlocks[:0]
	m.extrPoolBlockNum = 0
	m.extrBlocks = m.extrBlocks[:0]
	m.extrBlockNum = 0
	m.dataMap = m.dataMap[:0]
	m.size = 0
}

func (m *MemBlocks) Read(b []byte, off int64) (int, error) {
	var (
		readByte int
	)
	offUint64 := uint64(off)
	needByte := len(b)

	if offUint64 >= m.size {
		return 0, io.EOF
	}

	var n int

	for n < needByte && offUint64 < m.size {
		blockNum := int64(offUint64 / ConfMempoolBlockSize)
		start := offUint64 % ConfMempoolBlockSize
		end := uint64(ConfMempoolBlockSize)
		if uint64(blockNum+1)*ConfMempoolBlockSize > m.size {
			end = m.size % ConfMempoolBlockSize
		}

		index := m.dataMap[blockNum]
		memType := index / POOL_TYPE_MASK
		memOffset := index % POOL_TYPE_MASK

		if memType == 1 { // mem pool
			blockId := m.memPoolBlocks[memOffset]
			blockOffset := uint64(blockId*ConfMempoolBlockSize) + start
			blockEnd := uint64(blockId*ConfMempoolBlockSize) + end
			readByte = copy(b[n:], m.bm.data[blockOffset:blockEnd])
		} else if memType == 2 { // extr mem pool
			blockId := m.extrPoolBlocks[memOffset]
			blockOffset := uint64(blockId*ConfMempoolBlockSize) + start
			blockEnd := uint64(blockId*ConfMempoolBlockSize) + end
			readByte = copy(b[n:], m.extrBm.data[blockOffset:blockEnd])
		} else if memType == 3 { // extr block
			block := m.extrBlocks[memOffset]
			readByte = copy(b[n:], block.data[start:end])
		} else {
			return 0, fmt.Errorf("unexpect type", index)
		}
		n += readByte
		offUint64 += uint64(readByte)
	}

	return n, nil
}

// copy data from http respose body to []byte
// id is file id
func NewRefReadCloserBase(id uint32, bm, extrBm *BlockManage, size uint64,
	body io.ReadCloser) (*RefReadCloserBase, ReaderAllocType, error) {

	r := readerPool.Get().(*RefReadCloserBase)

	r.fileId = id
	r.groupId = bm.id
	r.fileSize = size
	r.ref = 1
	r.isClose = false
	r.blocks = memBlockPool.Get().(*MemBlocks)
	n, aType, err := r.blocks.Init(bm, extrBm, size, body)
	if err != nil {
		return nil, ALLOC_TYPE_NONE, err
	} else if n != size {
		return nil, ALLOC_TYPE_NONE,
			fmt.Errorf("read %d bytes from body. but expect %d bytes", n, size)
	}

	return r, aType, nil
}

var readerPool = sync.Pool{
	New: func() interface{} {
		return &RefReadCloserBase{}
	},
}

var memBlockPool = sync.Pool{
	New: func() interface{} {
		return &MemBlocks{}
	},
}

type RefReadCloserBase struct {
	fileId   uint32
	groupId  uint32
	blocks   *MemBlocks
	fileSize uint64
	ref      int32
	isClose  bool
}

func (r *RefReadCloserBase) MemPoolCap() uint64 {
	if r.blocks == nil {
		return 0
	}
	return uint64(r.blocks.usedBlockNum) * ConfMempoolBlockSize
}

func (r *RefReadCloserBase) Cap() uint64 {
	if r.blocks == nil {
		return 0
	}

	blockNum := r.blocks.usedBlockNum + r.blocks.extrPoolBlockNum + r.blocks.extrBlockNum

	return uint64(blockNum) * ConfMempoolBlockSize
}

func (r *RefReadCloserBase) Len() uint64 {
	return r.fileSize
}

func (r *RefReadCloserBase) Close() error {
	ref := atomic.AddInt32(&r.ref, -1)
	if ref < 0 {
		return fmt.Errorf("Base have been closed")
	} else if ref > 0 {
		return nil
	}

	r.isClose = true
	r.blocks.Release(true)
	memBlockPool.Put(r.blocks)
	r.blocks = nil

	readerPool.Put(r)
	return nil
}

func (r *RefReadCloserBase) CloseInter() error {
	ref := atomic.AddInt32(&r.ref, -1)
	if ref < 0 {
		return fmt.Errorf("Base have been closed")
	} else if ref > 0 {
		return nil
	}

	r.isClose = true
	r.blocks.Release(false)
	memBlockPool.Put(r.blocks)
	r.blocks = nil

	readerPool.Put(r)
	return nil
}

func (r *RefReadCloserBase) NewReadCloser() (io.ReadCloser, error) {
	if r.isClose {
		return nil, fmt.Errorf("Base is closed")
	}

	// just return the body of http
	newRef := atomic.AddInt32(&r.ref, 1) // add ref
	if newRef < 2 {
		return nil, fmt.Errorf("Base is closed")
	}

	closer := poolRefReadCloser.Get().(*RefReadCloser)
	closer.isClose = 0
	closer.off = 0
	closer.base = r
	return closer, nil
}

func (r *RefReadCloserBase) Read(b []byte) (n int, err error) {
	panic("RefReadCloserBase not support read")
	return 0, nil
}

type RefReadCloser struct {
	base    *RefReadCloserBase
	off     int64
	isClose int32
}

func (r *RefReadCloser) Read(b []byte) (n int, err error) {
	if r.isClose == 1 {
		err = fmt.Errorf("RefReadCloser is closed")
		return
	}

	if r.off >= int64(r.base.fileSize) {
		return 0, io.EOF
	}

	n, err = r.base.blocks.Read(b, r.off)
	r.off += int64(n)
	return
}

func (r *RefReadCloser) Close() error {
	if ok := atomic.CompareAndSwapInt32(&r.isClose, 0, 1); !ok {
		return fmt.Errorf("Have been closed")
	}

	err := r.base.Close()
	poolRefReadCloser.Put(r)
	return err
}

func ReaderMd5Check(body io.ReadCloser, md5val string) (string, error) {
	h := md5.New()
	n, err := io.Copy(h, body)
	body.Close()

	if err != nil {
		return "", fmt.Errorf("fail to get content %v", err)
	} else if n == 0 {
		return "", fmt.Errorf("get empty content")
	}
	retMd5val := hex.EncodeToString(h.Sum(nil))
	if md5val != retMd5val {
		return retMd5val, fmt.Errorf("wrong md5 value, expect %s but get %s", md5val, retMd5val)
	}
	return retMd5val, nil
}
