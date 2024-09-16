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

func NewRecordPage(tx *tx.Transaction, block file.BlockID, layout *Layout) (*RecordPage, error) {
	if err := tx.Pin(block); err != nil {
		return nil, err
	}
	return &RecordPage{tx, block, layout}, nil
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

func (rp *RecordPage) Format() error {
	slot := int32(0)
	for rp.isValidSlot(slot) {
		if err := rp.tx.SetInt(rp.block, rp.offset(slot), EMPTY, false); err != nil {
			return err
		}
		sch := rp.layout.Schema()
		for _, fieldName := range sch.Fields() {
			fpos := rp.offset(slot) + rp.layout.Offset(fieldName)
			if sch.Type(fieldName) == INTEGER {
				if err := rp.tx.SetInt(rp.block, fpos, 0, false); err != nil {
					return err
				}
			} else {
				if err := rp.tx.SetString(rp.block, fpos, "", false); err != nil {
					return err
				}
			}
		}
		slot++
	}
	return nil
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

func (rp *RecordPage) Compare(slot1, slot2 int32, fields []string) (int, error) {
	for _, fieldName := range fields {
		if rp.layout.Schema().Type(fieldName) == INTEGER {
			val1, err := rp.GetInt(slot1, fieldName)
			if err != nil {
				return 0, err
			}
			val2, err := rp.GetInt(slot2, fieldName)
			if err != nil {
				return 0, err
			}
			if val1 < val2 {
				return -1, nil
			} else if val1 > val2 {
				return 1, nil
			}
		} else {
			val1, err := rp.GetString(slot1, fieldName)
			if err != nil {
				return 0, err
			}
			val2, err := rp.GetString(slot2, fieldName)
			if err != nil {
				return 0, err
			}
			if val1 < val2 {
				return -1, nil
			} else if val1 > val2 {
				return 1, nil
			}
		}
	}
	return 0, nil
}

func (rp *RecordPage) Swap(slot1, slot2 int32) error {
	for _, fieldName := range rp.layout.Schema().Fields() {
		if rp.layout.Schema().Type(fieldName) == INTEGER {
			val1, err := rp.GetInt(slot1, fieldName)
			if err != nil {
				return err
			}
			val2, err := rp.GetInt(slot2, fieldName)
			if err != nil {
				return err
			}
			if err := rp.SetInt(slot1, fieldName, val2); err != nil {
				return err
			}
			if err := rp.SetInt(slot2, fieldName, val1); err != nil {
				return err
			}
		} else {
			val1, err := rp.GetString(slot1, fieldName)
			if err != nil {
				return err
			}
			val2, err := rp.GetString(slot2, fieldName)
			if err != nil {
				return err
			}
			if err := rp.SetString(slot1, fieldName, val2); err != nil {
				return err
			}
			if err := rp.SetString(slot2, fieldName, val1); err != nil {
				return err
			}
		}
	}
	return nil
}
