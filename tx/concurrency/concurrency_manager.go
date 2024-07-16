package concurrency

import (
	"fmt"

	"github.com/adieumonks/simple-db/file"
)

var lockTable = NewLockTable()

type ConcurrencyManager struct {
	locks map[file.BlockID]string
}

func NewConcurrencyManager() *ConcurrencyManager {
	return &ConcurrencyManager{
		locks: make(map[file.BlockID]string),
	}
}

func (cm *ConcurrencyManager) SLock(block file.BlockID) error {
	if cm.locks[block] != "" {
		return nil
	}

	if err := lockTable.SLock(block); err != nil {
		return fmt.Errorf("failed to acquire SLock: %w", err)
	}
	cm.locks[block] = "S"
	return nil
}

func (cm *ConcurrencyManager) XLock(block file.BlockID) error {
	if cm.hasXLock(block) {
		return nil
	}

	if err := lockTable.SLock(block); err != nil {
		return fmt.Errorf("failed to acquire SLock: %w", err)
	}
	if err := lockTable.XLock(block); err != nil {
		return fmt.Errorf("failed to acquire XLock: %w", err)
	}
	cm.locks[block] = "X"
	return nil
}

func (cm *ConcurrencyManager) Release() {
	for block := range cm.locks {
		lockTable.Unlock(block)
	}
	clear(cm.locks)
}

func (cm *ConcurrencyManager) hasXLock(block file.BlockID) bool {
	lockType := cm.locks[block]
	return lockType == "X"
}
