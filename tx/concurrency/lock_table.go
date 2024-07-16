package concurrency

import (
	"errors"
	"sync"
	"time"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/util"
)

const (
	MAX_TIME = 10 * time.Second
)

var ErrLockAbort = errors.New("lock abort")

type LockTable struct {
	locks map[file.BlockID]int32
	cond  *sync.Cond
}

func NewLockTable() *LockTable {
	return &LockTable{
		locks: make(map[file.BlockID]int32),
		cond:  sync.NewCond(&sync.Mutex{}),
	}
}

func (lt *LockTable) SLock(block file.BlockID) error {
	lt.cond.L.Lock()
	defer lt.cond.L.Unlock()

	timestamp := time.Now()
	for lt.hasXLock(block) && !lt.waitingTooLong(timestamp) {
		util.Wait(lt.cond, MAX_TIME)
	}
	if lt.hasXLock(block) {
		return ErrLockAbort
	}
	val := lt.getLockVal(block)
	lt.locks[block] = val + 1
	return nil
}

func (lt *LockTable) XLock(block file.BlockID) error {
	lt.cond.L.Lock()
	defer lt.cond.L.Unlock()

	timestamp := time.Now()
	for lt.hasOtherSLock(block) && !lt.waitingTooLong(timestamp) {
		util.Wait(lt.cond, MAX_TIME)
	}
	if lt.hasOtherSLock(block) {
		return ErrLockAbort
	}
	lt.locks[block] = -1
	return nil
}

func (lt *LockTable) Unlock(block file.BlockID) {
	lt.cond.L.Lock()
	defer lt.cond.L.Unlock()

	val := lt.getLockVal(block)
	if val > 1 {
		lt.locks[block] = val - 1
	} else {
		delete(lt.locks, block)
		lt.cond.Broadcast()
	}
}

func (lt *LockTable) hasXLock(block file.BlockID) bool {
	return lt.getLockVal(block) < 0
}

func (lt *LockTable) hasOtherSLock(block file.BlockID) bool {
	return lt.getLockVal(block) > 1
}

func (lt *LockTable) waitingTooLong(startTime time.Time) bool {
	return time.Since(startTime) > MAX_TIME
}

func (lt *LockTable) getLockVal(block file.BlockID) int32 {
	ival, ok := lt.locks[block]
	if !ok {
		return 0
	}
	return ival
}
