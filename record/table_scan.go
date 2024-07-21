package record

import (
	"fmt"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/tx"
)

type TableScan struct {
	tx          *tx.Transaction
	layout      *Layout
	rp          *RecordPage
	filename    string
	currentSlot int32
}

func NewTableScan(tx *tx.Transaction, tableName string, layout *Layout) (*TableScan, error) {
	ts := &TableScan{
		tx:       tx,
		layout:   layout,
		filename: tableName + ".tbl",
	}

	fileSize, err := tx.Size(ts.filename)
	if err != nil {
		return nil, fmt.Errorf("failed to get file size: %v", err)
	}
	if fileSize == 0 {
		ts.moveToNewBlock()
	} else {
		ts.moveToBlock(0)
	}
	return ts, nil
}

func (ts *TableScan) BeforeFirst() {
	ts.moveToBlock(0)
}

func (ts *TableScan) Next() (bool, error) {
	currentSlot, err := ts.rp.NextAfter(ts.currentSlot)
	if err != nil {
		return false, fmt.Errorf("failed to get next slot: %v", err)
	}
	ts.currentSlot = currentSlot
	for ts.currentSlot < 0 {
		atLastBlock, err := ts.atLastBlock()
		if err != nil {
			return false, fmt.Errorf("failed to check if at last block: %v", err)
		}
		if atLastBlock {
			return false, nil
		}
		ts.moveToBlock(ts.rp.Block().Number() + 1)
		currentSlot, err = ts.rp.NextAfter(ts.currentSlot)
		if err != nil {
			return false, fmt.Errorf("failed to get next slot: %v", err)
		}
		ts.currentSlot = currentSlot
	}
	return true, nil
}

func (ts *TableScan) GetInt(fieldName string) (int32, error) {
	return ts.rp.GetInt(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetString(fieldName string) (string, error) {
	return ts.rp.GetString(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetVal(fieldName string) (*Constant, error) {
	if ts.layout.Schema().Type(fieldName) == INTEGER {
		val, err := ts.GetInt(fieldName)
		if err != nil {
			return nil, fmt.Errorf("failed to get int value: %v", err)
		}
		return NewConstantFromInt(val), nil
	} else {
		val, err := ts.GetString(fieldName)
		if err != nil {
			return nil, fmt.Errorf("failed to get string value: %v", err)
		}
		return NewConstantFromString(val), nil
	}
}

func (ts *TableScan) HasField(fieldName string) bool {
	return ts.layout.Schema().HasField(fieldName)
}

func (ts *TableScan) Close() {
	if ts.rp != nil {
		ts.tx.Unpin(ts.rp.Block())
	}
}

func (ts *TableScan) SetInt(fieldName string, val int32) error {
	return ts.rp.SetInt(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetString(fieldName string, val string) error {
	return ts.rp.SetString(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetVal(fieldName string, val *Constant) error {
	if ts.layout.Schema().Type(fieldName) == INTEGER {
		err := ts.SetInt(fieldName, val.AsInt())
		if err != nil {
			return fmt.Errorf("failed to set int value: %w", err)
		}
	} else {
		err := ts.SetString(fieldName, val.AsString())
		if err != nil {
			return fmt.Errorf("failed to set string value: %w", err)
		}
	}
	return nil
}

func (ts *TableScan) Insert() error {
	currentSlot, err := ts.rp.InsertAfter(ts.currentSlot)
	if err != nil {
		return fmt.Errorf("failed to insert after: %v", err)
	}
	ts.currentSlot = currentSlot
	for ts.currentSlot < 0 {
		atLastBlock, err := ts.atLastBlock()
		if err != nil {
			return fmt.Errorf("failed to check if at last block: %v", err)
		}
		if atLastBlock {
			ts.moveToNewBlock()
		} else {
			ts.moveToBlock(ts.rp.Block().Number() + 1)
		}
		currentSlot, err = ts.rp.InsertAfter(ts.currentSlot)
		if err != nil {
			return fmt.Errorf("failed to insert after: %v", err)
		}
		ts.currentSlot = currentSlot
	}
	return nil
}

func (ts *TableScan) Delete() error {
	return ts.rp.Delete(ts.currentSlot)
}

func (ts *TableScan) MoveToRID(rid *RID) {
	ts.Close()
	block := file.NewBlockID(ts.filename, rid.BlockNumber())
	ts.rp = NewRecordPage(ts.tx, block, ts.layout)
	ts.currentSlot = rid.Slot()
}

func (ts *TableScan) GetRID() *RID {
	return NewRID(ts.rp.Block().Number(), ts.currentSlot)
}

func (ts *TableScan) moveToBlock(blockNum int32) {
	ts.Close()
	block := file.NewBlockID(ts.filename, blockNum)
	ts.rp = NewRecordPage(ts.tx, block, ts.layout)
	ts.currentSlot = -1
}

func (ts *TableScan) moveToNewBlock() error {
	ts.Close()
	block, err := ts.tx.Append(ts.filename)
	if err != nil {
		return fmt.Errorf("failed to append block: %v", err)
	}
	ts.rp = NewRecordPage(ts.tx, block, ts.layout)
	ts.rp.Format()
	ts.currentSlot = -1
	return nil
}

func (ts *TableScan) atLastBlock() (bool, error) {
	fileSize, err := ts.tx.Size(ts.filename)
	if err != nil {
		return false, fmt.Errorf("failed to get file size: %v", err)
	}
	return ts.rp.Block().Number() == fileSize-1, nil
}
