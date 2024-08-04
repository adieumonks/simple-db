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
		if err := ts.moveToNewBlock(); err != nil {
			return nil, err
		}
	} else {
		if err := ts.moveToBlock(0); err != nil {
			return nil, err
		}
	}
	return ts, nil
}

func (ts *TableScan) BeforeFirst() error {
	return ts.moveToBlock(0)
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
		if err := ts.moveToBlock(ts.rp.Block().Number() + 1); err != nil {
			return false, err
		}
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
		return NewConstantWithInt(val), nil
	} else {
		val, err := ts.GetString(fieldName)
		if err != nil {
			return nil, fmt.Errorf("failed to get string value: %v", err)
		}
		return NewConstantWithString(val), nil
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
			if err := ts.moveToNewBlock(); err != nil {
				return err
			}
		} else {
			if err := ts.moveToBlock(ts.rp.Block().Number() + 1); err != nil {
				return err
			}
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

func (ts *TableScan) MoveToRID(rid *RID) error {
	ts.Close()
	block := file.NewBlockID(ts.filename, rid.BlockNumber())

	var err error
	ts.rp, err = NewRecordPage(ts.tx, block, ts.layout)
	if err != nil {
		return err
	}
	ts.currentSlot = rid.Slot()
	return nil
}

func (ts *TableScan) GetRID() *RID {
	return NewRID(ts.rp.Block().Number(), ts.currentSlot)
}

func (ts *TableScan) moveToBlock(blockNum int32) error {
	ts.Close()
	block := file.NewBlockID(ts.filename, blockNum)
	var err error
	ts.rp, err = NewRecordPage(ts.tx, block, ts.layout)
	if err != nil {
		return err
	}
	ts.currentSlot = -1
	return nil
}

func (ts *TableScan) moveToNewBlock() error {
	ts.Close()
	block, err := ts.tx.Append(ts.filename)
	if err != nil {
		return fmt.Errorf("failed to append block: %v", err)
	}
	ts.rp, err = NewRecordPage(ts.tx, block, ts.layout)
	if err != nil {
		return err
	}
	if err := ts.rp.Format(); err != nil {
		return err
	}
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
