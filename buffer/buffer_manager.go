package buffer

import (
	"errors"
	"sync"
	"time"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
	"github.com/adieumonks/simple-db/util"
)

const (
	MAX_TIME = 10 * time.Second
)

var ErrBufferAbort = errors.New("buffer abort")

type BufferManager struct {
	bufferPool   []*Buffer
	numAvailable int32
	cond         *sync.Cond
}

func NewBufferManager(fm *file.FileManager, lm *log.LogManager, numBuffs int32) *BufferManager {
	bufferPool := make([]*Buffer, numBuffs)
	numAvailable := numBuffs
	for i := int32(0); i < numBuffs; i++ {
		bufferPool[i] = NewBuffer(fm, lm)
	}

	return &BufferManager{
		bufferPool:   bufferPool,
		numAvailable: numAvailable,
		cond:         sync.NewCond(&sync.Mutex{}),
	}
}

func (bm *BufferManager) Available() int32 {
	return bm.numAvailable

}

func (bm *BufferManager) FlushAll(txnum int32) {
	for _, buffer := range bm.bufferPool {
		if buffer.ModifyingTx() == txnum {
			buffer.Flush()
		}
	}
}

func (bm *BufferManager) Unpin(buffer *Buffer) {
	bm.cond.L.Lock()
	defer bm.cond.L.Unlock()

	buffer.Unpin()
	if !buffer.IsPinned() {
		bm.numAvailable++
		bm.cond.Broadcast()
	}
}

func (bm *BufferManager) Pin(block file.BlockID) (*Buffer, error) {
	bm.cond.L.Lock()
	defer bm.cond.L.Unlock()

	timestamp := time.Now()
	buffer, err := bm.tryToPin(block)
	if err != nil {
		return nil, err
	}
	for buffer == nil && !bm.waitTooLong(timestamp) {
		util.Wait(bm.cond, MAX_TIME)
		buffer, err = bm.tryToPin(block)
		if err != nil {
			return nil, err
		}
	}
	if buffer == nil {
		return nil, ErrBufferAbort
	}
	return buffer, nil
}

func (bm *BufferManager) waitTooLong(startTime time.Time) bool {
	return time.Since(startTime) > MAX_TIME
}

func (bm *BufferManager) tryToPin(block file.BlockID) (*Buffer, error) {
	buffer := bm.findExistingBuffer(block)
	if buffer == nil {
		buffer = bm.chooseUnpinnedBuffer()
		if buffer == nil {
			return nil, nil
		}
		if err := buffer.AssignToBlock(block); err != nil {
			return nil, err
		}
	}
	if !buffer.IsPinned() {
		bm.numAvailable--
	}
	buffer.Pin()
	return buffer, nil
}

func (bm *BufferManager) findExistingBuffer(block file.BlockID) *Buffer {
	for _, buffer := range bm.bufferPool {
		b := buffer.Block()
		if b == block {
			return buffer
		}
	}
	return nil
}

func (bm *BufferManager) chooseUnpinnedBuffer() *Buffer {
	for _, buffer := range bm.bufferPool {
		if !buffer.IsPinned() {
			return buffer
		}
	}
	return nil
}
