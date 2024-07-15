package buffer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
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

func (bm *BufferManager) Pin(block *file.BlockID) (*Buffer, error) {
	bm.cond.L.Lock()
	defer bm.cond.L.Unlock()

	timestamp := time.Now()
	buffer := bm.tryToPin(block)
	for buffer == nil && !bm.waitTooLong(timestamp) {
		wait(bm.cond, MAX_TIME)
		buffer = bm.tryToPin(block)
	}
	if buffer == nil {
		return nil, ErrBufferAbort
	}
	return buffer, nil
}

// 通知されるか、タイムアウトするまで待機する
func wait(cond *sync.Cond, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	stopf := context.AfterFunc(ctx, func() {
		cond.L.Lock()
		defer cond.L.Unlock()

		cond.Broadcast()
	})
	defer stopf()

	cond.Wait()
}

func (bm *BufferManager) waitTooLong(startTime time.Time) bool {
	return time.Since(startTime) > MAX_TIME
}

func (bm *BufferManager) tryToPin(block *file.BlockID) *Buffer {
	buffer := bm.findExistingBuffer(block)
	if buffer == nil {
		buffer = bm.chooseUnpinnedBuffer()
		if buffer == nil {
			return nil
		}
		buffer.AssignToBlock(block)
	}
	if !buffer.IsPinned() {
		bm.numAvailable--
	}
	buffer.Pin()
	return buffer
}

func (bm *BufferManager) findExistingBuffer(block *file.BlockID) *Buffer {
	for _, buffer := range bm.bufferPool {
		b := buffer.Block()
		if b != nil && b.Equals(block) {
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
