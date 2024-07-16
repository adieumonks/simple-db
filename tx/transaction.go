package tx

import (
	"fmt"
	"sync"

	"github.com/adieumonks/simple-db/buffer"
	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
	"github.com/adieumonks/simple-db/tx/concurrency"
	"github.com/adieumonks/simple-db/tx/recovery"
)

const END_OF_FILE = -1

var (
	mu        sync.Mutex
	nextTxNum int32 = 0
)

type Transaction struct {
	rm        *recovery.RecoveryManager
	cm        *concurrency.ConcurrencyManager
	bm        *buffer.BufferManager
	fm        *file.FileManager
	txnum     int32
	myBuffers *BufferList
}

func NewTransaction(fm *file.FileManager, lm *log.LogManager, bm *buffer.BufferManager) *Transaction {
	txnum := nextTxNumber()
	tx := &Transaction{
		bm:        bm,
		fm:        fm,
		txnum:     txnum,
		cm:        concurrency.NewConcurrencyManager(),
		myBuffers: NewBufferList(bm),
	}

	tx.rm = recovery.NewRecoveryManager(tx, txnum, lm, bm)
	return tx
}

func (tx *Transaction) Commit() {
	tx.rm.Commit()
	tx.cm.Release()
	tx.myBuffers.UnpinAll()
}

func (tx *Transaction) Rollback() {
	tx.rm.RollBack()
	tx.cm.Release()
	tx.myBuffers.UnpinAll()
}

func (tx *Transaction) Recover() {
	tx.bm.FlushAll(tx.txnum)
	tx.rm.Recover()
}

func (tx *Transaction) Pin(block file.BlockID) {
	tx.myBuffers.Pin(block)
}

func (tx *Transaction) Unpin(block file.BlockID) {
	tx.myBuffers.Unpin(block)
}

func (tx *Transaction) GetInt(block file.BlockID, offset int32) (int32, error) {
	err := tx.cm.SLock(block)
	if err != nil {
		return 0, fmt.Errorf("failed to get int: %w", err)
	}
	buffer := tx.myBuffers.GetBuffer(block)
	return buffer.Contents().GetInt(offset), nil
}

func (tx *Transaction) GetString(block file.BlockID, offset int32) (string, error) {
	err := tx.cm.SLock(block)
	if err != nil {
		return "", fmt.Errorf("failed to get string: %w", err)
	}
	buffer := tx.myBuffers.GetBuffer(block)
	return buffer.Contents().GetString(offset), nil
}

func (tx *Transaction) SetInt(block file.BlockID, offset int32, val int32, okToLog bool) error {
	err := tx.cm.XLock(block)
	if err != nil {
		return fmt.Errorf("failed to set int: %w", err)
	}
	buffer := tx.myBuffers.GetBuffer(block)
	var lsn int32 = -1
	if okToLog {
		var err error
		lsn, err = tx.rm.SetInt(buffer, offset, val)
		if err != nil {
			return fmt.Errorf("failed to set int: %w", err)
		}
	}
	p := buffer.Contents()
	p.SetInt(offset, val)
	buffer.SetModified(tx.txnum, lsn)
	return nil
}

func (tx *Transaction) SetString(block file.BlockID, offset int32, val string, okToLog bool) error {
	err := tx.cm.XLock(block)
	if err != nil {
		return fmt.Errorf("failed to set string: %w", err)
	}
	buffer := tx.myBuffers.GetBuffer(block)
	var lsn int32 = -1
	if okToLog {
		var err error
		lsn, err = tx.rm.SetString(buffer, offset, val)
		if err != nil {
			return fmt.Errorf("failed to set string: %w", err)
		}
	}
	p := buffer.Contents()
	p.SetString(offset, val)
	buffer.SetModified(tx.txnum, lsn)
	return nil
}

func (tx *Transaction) Size(filename string) (int32, error) {
	dummyBlock := file.NewBlockID(filename, END_OF_FILE)
	err := tx.cm.SLock(dummyBlock)
	if err != nil {
		return 0, fmt.Errorf("failed to get size: %w", err)
	}
	return tx.fm.Length(filename)
}

func (tx *Transaction) Append(filename string) (file.BlockID, error) {
	dummyBlock := file.NewBlockID(filename, END_OF_FILE)
	err := tx.cm.XLock(dummyBlock)
	if err != nil {
		return file.NewBlockID("", 0), fmt.Errorf("failed to append: %w", err)
	}
	return tx.fm.Append(filename)
}

func (tx *Transaction) BlockSize() int32 {
	return tx.fm.BlockSize()
}

func (tx *Transaction) AvailableBuffers() int32 {
	return tx.bm.Available()
}

func nextTxNumber() int32 {
	mu.Lock()
	defer mu.Unlock()

	nextTxNum++
	return nextTxNum
}
