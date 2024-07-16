package recovery

import (
	"fmt"

	"github.com/adieumonks/simple-db/buffer"
	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
)

type Transaction interface {
	Pin(block file.BlockID)
	Unpin(block file.BlockID)
	SetInt(block file.BlockID, offset int32, val int32, okToLog bool) error
	SetString(block file.BlockID, offset int32, val string, okToLog bool) error
}

type RecoveryManager struct {
	lm    *log.LogManager
	bm    *buffer.BufferManager
	tx    Transaction
	txnum int32
}

func NewRecoveryManager(tx Transaction, txnum int32, lm *log.LogManager, bm *buffer.BufferManager) *RecoveryManager {
	rm := RecoveryManager{
		lm:    lm,
		bm:    bm,
		tx:    tx,
		txnum: txnum,
	}

	NewStartRecord(txnum).WriteToLog(lm)

	return &rm
}

func (rm *RecoveryManager) Commit() error {
	rm.bm.FlushAll(rm.txnum)
	lsn, err := NewCommitRecord(rm.txnum).WriteToLog(rm.lm)
	if err != nil {
		return fmt.Errorf("failed to write commit record to log: %w", err)
	}
	rm.lm.Flush(lsn)
	return nil
}

func (rm *RecoveryManager) RollBack() error {
	err := rm.doRollBack()
	if err != nil {
		return fmt.Errorf("failed to rollback: %w", err)
	}

	rm.bm.FlushAll(rm.txnum)
	lsn, err := NewRollbackRecord(rm.txnum).WriteToLog(rm.lm)
	if err != nil {
		return fmt.Errorf("failed to write rollback record to log: %w", err)
	}
	rm.lm.Flush(lsn)
	return nil
}

func (rm *RecoveryManager) Recover() error {
	err := rm.doRecover()
	if err != nil {
		return fmt.Errorf("failed to recover: %w", err)
	}
	rm.bm.FlushAll(rm.txnum)
	lsn, error := NewCheckpointRecord().WriteToLog(rm.lm)
	if error != nil {
		return fmt.Errorf("failed to write checkpoint record to log: %w", error)
	}
	rm.lm.Flush(lsn)
	return nil
}

func (rm *RecoveryManager) SetInt(buffer *buffer.Buffer, offset int32, newVal int32) (int32, error) {
	oldVal := buffer.Contents().GetInt(offset)
	block := buffer.Block()
	return NewSetIntRecord(rm.txnum, block, offset, oldVal).WriteToLog(rm.lm)
}

func (rm *RecoveryManager) SetString(buffer *buffer.Buffer, offset int32, newVal string) (int32, error) {
	oldVal := buffer.Contents().GetString(offset)
	block := buffer.Block()
	return NewSetStringRecord(rm.txnum, block, offset, oldVal).WriteToLog(rm.lm)
}

func (rm *RecoveryManager) doRollBack() error {

	iter, err := rm.lm.Iterator()
	if err != nil {
		return fmt.Errorf("failed to get log iterator: %w", err)
	}

	for iter.HasNext() {
		bytes := iter.Next()
		rec, err := NewLogRecord(bytes)
		if err != nil {
			return fmt.Errorf("failed to create log record: %w", err)
		}
		if rec.TxNumber() == rm.txnum {
			if rec.Op() == START {
				return nil
			}
			rec.Undo(rm.tx)
		}
	}
	return nil
}

func (rm *RecoveryManager) doRecover() error {
	finishedTxs := make(map[int32]bool)
	iter, err := rm.lm.Iterator()
	if err != nil {
		return fmt.Errorf("failed to get log iterator: %w", err)
	}

	for iter.HasNext() {
		bytes := iter.Next()
		rec, err := NewLogRecord(bytes)
		if err != nil {
			return fmt.Errorf("failed to create log record: %w", err)
		}
		if rec.Op() == CHECKPOINT {
			return nil
		}
		if rec.Op() == COMMIT || rec.Op() == ROLLBACK {
			finishedTxs[rec.TxNumber()] = true
		} else if !finishedTxs[rec.TxNumber()] {
			rec.Undo(rm.tx)
		}
	}
	return nil
}
