package index

import (
	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

type BTPage struct {
	tx           *tx.Transaction
	currentBlock *file.BlockID
	layout       *record.Layout
}

func NewBTPage(tx *tx.Transaction, currentBlock *file.BlockID, layout *record.Layout) (*BTPage, error) {
	p := &BTPage{
		tx:           tx,
		currentBlock: currentBlock,
		layout:       layout,
	}
	if err := tx.Pin(*p.currentBlock); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *BTPage) FindSlotBefore(seachKey *query.Constant) (int32, error) {
	slot := int32(0)
	for {
		numRecords, err := p.GetNumRecs()
		if err != nil {
			return 0, err
		}
		if slot >= numRecords {
			break
		}

		dataVal, err := p.GetDataVal(slot)
		if err != nil {
			return 0, err
		}
		if dataVal.CompareTo(seachKey) >= 0 {
			break
		}

		slot++
	}
	return slot - 1, nil
}

func (p *BTPage) Close() {
	if p.currentBlock != nil {
		p.tx.Unpin(*p.currentBlock)
	}
	p.currentBlock = nil
}

func (p *BTPage) IsFull() (bool, error) {
	numRecords, err := p.GetNumRecs()
	if err != nil {
		return false, err
	}
	return p.slotPos(numRecords+1) >= p.tx.BlockSize(), nil
}

func (p *BTPage) Split(splitPos int32, flag int32) (*file.BlockID, error) {
	newBlock, err := p.AppendNew(flag)
	if err != nil {
		return nil, err
	}
	newPage, err := NewBTPage(p.tx, newBlock, p.layout)
	if err != nil {
		return nil, err
	}
	if err := p.transferRecs(splitPos, *newPage); err != nil {
		return nil, err
	}
	if err := newPage.SetFlag(flag); err != nil {
		return nil, err
	}
	newPage.Close()
	return newBlock, nil
}

func (p *BTPage) GetDataVal(slot int32) (*query.Constant, error) {
	return p.getVal(slot, "dataval")
}

func (p *BTPage) GetFlag() (int32, error) {
	return p.tx.GetInt(*p.currentBlock, 0)
}

func (p *BTPage) SetFlag(val int32) error {
	return p.tx.SetInt(*p.currentBlock, 0, val, true)
}

func (p *BTPage) AppendNew(flag int32) (*file.BlockID, error) {
	block, err := p.tx.Append(p.currentBlock.Filename())
	if err != nil {
		return nil, err
	}
	if err := p.tx.Pin(block); err != nil {
		return nil, err
	}
	if err := p.Format(&block, flag); err != nil {
		return nil, err
	}
	return &block, nil
}

func (p *BTPage) Format(block *file.BlockID, flag int32) error {
	if err := p.tx.SetInt(*block, 0, flag, false); err != nil {
		return err
	}
	if err := p.tx.SetInt(*block, file.Int32Bytes, 0, false); err != nil {
		return err
	}
	recordSize := p.layout.SlotSize()
	for pos := 2 * file.Int32Bytes; pos+recordSize < p.tx.BlockSize(); pos += recordSize {
		if err := p.MakeDefaultRecord(block, pos); err != nil {
			return err
		}
	}
	return nil
}

func (p *BTPage) MakeDefaultRecord(block *file.BlockID, pos int32) error {
	for _, fieldName := range p.layout.Schema().Fields() {
		offset := p.layout.Offset(fieldName)
		fieldType := p.layout.Schema().Type(fieldName)
		if fieldType == record.INTEGER {
			if err := p.tx.SetInt(*block, pos+offset, 0, false); err != nil {
				return err
			}
		} else {
			if err := p.tx.SetString(*block, pos+offset, "", false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *BTPage) GetChildNum(slot int32) (int32, error) {
	return p.getInt(slot, "block")
}

func (p *BTPage) InsertDir(slot int32, val *query.Constant, blockNum int32) error {
	if err := p.insert(slot); err != nil {
		return nil
	}
	if err := p.setVal(slot, "dataval", val); err != nil {
		return err
	}
	if err := p.setInt(slot, "block", blockNum); err != nil {
		return err
	}
	return nil
}

func (p *BTPage) GetDataRID(slot int32) (*record.RID, error) {
	blockNum, err := p.getInt(slot, "block")
	if err != nil {
		return nil, err
	}
	id, err := p.getInt(slot, "id")
	if err != nil {
		return nil, err
	}
	return record.NewRID(blockNum, id), nil
}

func (p *BTPage) InsertLeaf(slot int32, val *query.Constant, rid *record.RID) error {
	if err := p.insert(slot); err != nil {
		return err
	}
	if err := p.setVal(slot, "dataval", val); err != nil {
		return err
	}
	if err := p.setInt(slot, "block", rid.BlockNumber()); err != nil {
		return err
	}
	if err := p.setInt(slot, "id", rid.Slot()); err != nil {
		return err
	}
	return nil
}

func (p *BTPage) Delete(slot int32) error {
	numRecords, err := p.GetNumRecs()
	if err != nil {
		return err
	}
	for i := slot + 1; i < numRecords; i++ {
		if err := p.copyRecord(i, i-1); err != nil {
			return err
		}
	}
	if err := p.setNumRecs(numRecords - 1); err != nil {
		return err
	}
	return nil
}

func (p *BTPage) GetNumRecs() (int32, error) {
	return p.tx.GetInt(*p.currentBlock, file.Int32Bytes)
}

func (p *BTPage) getInt(slot int32, fieldName string) (int32, error) {
	pos := p.fieldPos(slot, fieldName)
	return p.tx.GetInt(*p.currentBlock, pos)
}

func (p *BTPage) getString(slot int32, fieldName string) (string, error) {
	pos := p.fieldPos(slot, fieldName)
	return p.tx.GetString(*p.currentBlock, pos)
}

func (p *BTPage) getVal(slot int32, fieldName string) (*query.Constant, error) {
	fieldType := p.layout.Schema().Type(fieldName)
	if fieldType == record.INTEGER {
		ival, err := p.getInt(slot, fieldName)
		if err != nil {
			return nil, err
		}
		return query.NewConstantWithInt(ival), nil
	} else {
		sval, err := p.getString(slot, fieldName)
		if err != nil {
			return nil, err
		}
		return query.NewConstantWithString(sval), nil
	}
}

func (p *BTPage) setInt(slot int32, fieldName string, val int32) error {
	pos := p.fieldPos(slot, fieldName)
	return p.tx.SetInt(*p.currentBlock, pos, val, true)
}

func (p *BTPage) setString(slot int32, fieldName string, val string) error {
	pos := p.fieldPos(slot, fieldName)
	return p.tx.SetString(*p.currentBlock, pos, val, true)
}

func (p *BTPage) setVal(slot int32, fieldName string, val *query.Constant) error {
	filedType := p.layout.Schema().Type(fieldName)
	if filedType == record.INTEGER {
		return p.setInt(slot, fieldName, val.AsInt())
	} else {
		return p.setString(slot, fieldName, val.AsString())
	}
}

func (p *BTPage) setNumRecs(n int32) error {
	return p.tx.SetInt(*p.currentBlock, file.Int32Bytes, n, true)
}

func (p *BTPage) insert(slot int32) error {
	numRecords, err := p.GetNumRecs()
	if err != nil {
		return err
	}
	for i := numRecords; i > slot; i-- {
		if err := p.copyRecord(i-1, i); err != nil {
			return err
		}
	}
	if err := p.setNumRecs(numRecords + 1); err != nil {
		return err
	}
	return nil
}

func (p *BTPage) copyRecord(from int32, to int32) error {
	schema := p.layout.Schema()
	for _, fieldName := range schema.Fields() {
		val, err := p.getVal(from, fieldName)
		if err != nil {
			return err
		}
		if err := p.setVal(to, fieldName, val); err != nil {
			return err
		}
	}
	return nil
}

func (p *BTPage) transferRecs(slot int32, dest BTPage) error {
	destSlot := int32(0)
	for {
		numRecords, err := p.GetNumRecs()
		if err != nil {
			return err
		}
		if slot >= numRecords {
			break
		}
		if err := dest.insert(destSlot); err != nil {
			return err
		}
		for _, fieldName := range p.layout.Schema().Fields() {
			val, err := p.getVal(slot, fieldName)
			if err != nil {
				return err
			}
			if err := dest.setVal(destSlot, fieldName, val); err != nil {
				return err
			}
		}
		if err := p.Delete(slot); err != nil {
			return err
		}
		destSlot++
	}
	return nil
}

func (p *BTPage) fieldPos(slot int32, fieldName string) int32 {
	offset := p.layout.Offset(fieldName)
	return p.slotPos(slot) + offset
}

func (p *BTPage) slotPos(slot int32) int32 {
	slotSize := p.layout.SlotSize()
	return file.Int32Bytes + file.Int32Bytes + slot*slotSize
}
