package record

import (
	"fmt"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/tx"
)

const (
	EMPTY = iota
	USED
)

type RecordPage struct {
	tx     *tx.Transaction
	block  file.BlockID
	layout *Layout
}

func NewRecordPage(tx *tx.Transaction, block file.BlockID, layout *Layout) *RecordPage {
	tx.Pin(block)
	return &RecordPage{tx, block, layout}
}

func (rp *RecordPage) GetInt(slot int32, fieldName string) (int32, error) {
	fpos := rp.offset(slot) + rp.layout.Offset(fieldName)
	val, err := rp.tx.GetInt(rp.block, fpos)
	if err != nil {
		return 0, fmt.Errorf("failed to get int: %v", err)
	}
	return val, nil
}

func (rp *RecordPage) GetString(slot int32, fieldName string) (string, error) {
	fpos := rp.offset(slot) + rp.layout.Offset(fieldName)
	val, err := rp.tx.GetString(rp.block, fpos)
	if err != nil {
		return "", fmt.Errorf("failed to get string: %v", err)
	}
	return val, nil
}

func (rp *RecordPage) SetInt(slot int32, fieldName string, val int32) error {
	fpos := rp.offset(slot) + rp.layout.Offset(fieldName)
	err := rp.tx.SetInt(rp.block, fpos, val, true)
	if err != nil {
		return fmt.Errorf("failed to set int: %v", err)
	}
	return nil
}

func (rp *RecordPage) SetString(slot int32, fieldName string, val string) error {
	fpos := rp.offset(slot) + rp.layout.Offset(fieldName)
	err := rp.tx.SetString(rp.block, fpos, val, true)
	if err != nil {
		return fmt.Errorf("failed to set string: %v", err)
	}
	return nil
}

func (rp *RecordPage) Delete(slot int32) error {
	err := rp.setFlag(slot, EMPTY)
	if err != nil {
		return fmt.Errorf("failed to delete: %v", err)
	}
	return nil
}

func (rp *RecordPage) Format() {
	slot := int32(0)
	for rp.isValidSlot(slot) {
		rp.tx.SetInt(rp.block, rp.offset(slot), EMPTY, false)
		sch := rp.layout.Schema()
		for _, fieldName := range sch.Fields() {
			fpos := rp.offset(slot) + rp.layout.Offset(fieldName)
			if sch.Type(fieldName) == INTEGER {
				rp.tx.SetInt(rp.block, fpos, 0, false)
			} else {
				rp.tx.SetString(rp.block, fpos, "", false)
			}
		}
		slot++
	}
}

func (rp *RecordPage) NextAfter(slot int32) (int32, error) {
	return rp.searchAfter(slot, USED)
}
func (rp *RecordPage) InsertAfter(slot int32) (int32, error) {
	newSlot, err := rp.searchAfter(slot, EMPTY)
	if err != nil {
		return 0, fmt.Errorf("failed to insert after: %v", err)
	}
	if newSlot >= 0 {
		err = rp.setFlag(newSlot, USED)
		if err != nil {
			return 0, fmt.Errorf("failed to insert after: %v", err)
		}
	}
	return newSlot, nil
}

func (rp *RecordPage) Block() file.BlockID {
	return rp.block
}

func (rp *RecordPage) setFlag(slot int32, flag int32) error {
	err := rp.tx.SetInt(rp.block, rp.offset(slot), flag, true)
	if err != nil {
		return fmt.Errorf("failed to set flag: %v", err)
	}
	return nil
}
func (rp *RecordPage) searchAfter(slot int32, flag int32) (int32, error) {
	slot++
	for rp.isValidSlot(slot) {
		flagVal, err := rp.getFlag(slot)
		if err != nil {
			return 0, fmt.Errorf("failed to search after: %v", err)
		}
		if flagVal == flag {
			return slot, nil
		}
		slot++
	}
	return -1, nil
}

func (rp *RecordPage) getFlag(slot int32) (int32, error) {
	val, err := rp.tx.GetInt(rp.block, rp.offset(slot))
	if err != nil {
		return 0, fmt.Errorf("failed to get flag: %v", err)
	}
	return val, nil
}

func (rp *RecordPage) isValidSlot(slot int32) bool {
	return rp.offset(slot+1) <= rp.tx.BlockSize()
}

func (rp *RecordPage) offset(slot int32) int32 {
	return slot * rp.layout.SlotSize()
}
