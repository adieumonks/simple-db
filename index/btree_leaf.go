package index

import (
	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

type BTreeLeaf struct {
	tx          *tx.Transaction
	layout      *record.Layout
	searchKey   *query.Constant
	contents    *BTPage
	currentSlot int32
	fileName    string
}

func NewBTreeLeaf(tx *tx.Transaction, block *file.BlockID, layout *record.Layout, searchKey *query.Constant) (*BTreeLeaf, error) {
	contents, err := NewBTPage(tx, block, layout)
	if err != nil {
		return nil, err
	}
	currentSlot, err := contents.FindSlotBefore(searchKey)
	if err != nil {
		return nil, err
	}
	return &BTreeLeaf{
		tx:          tx,
		layout:      layout,
		searchKey:   searchKey,
		contents:    contents,
		currentSlot: currentSlot,
		fileName:    block.Filename(),
	}, nil
}

func (bl *BTreeLeaf) Close() {
	bl.contents.Close()
}

func (bl *BTreeLeaf) Next() (bool, error) {
	bl.currentSlot++
	numRecords, err := bl.contents.GetNumRecs()
	if err != nil {
		return false, err
	}
	if bl.currentSlot >= numRecords {
		return bl.tryOverFlow()
	} else {
		dataVal, err := bl.contents.GetDataVal(bl.currentSlot)
		if err != nil {
			return false, err
		}
		if bl.searchKey.Equals(dataVal) {
			return true, nil
		} else {
			return bl.tryOverFlow()
		}
	}
}

func (bl *BTreeLeaf) GetDataRID() (*record.RID, error) {
	return bl.contents.GetDataRID(bl.currentSlot)
}

func (bl *BTreeLeaf) Delete(dataRID *record.RID) error {
	for {
		next, err := bl.Next()
		if err != nil {
			return err
		}
		if !next {
			break
		}
		rid, err := bl.GetDataRID()
		if err != nil {
			return err
		}
		if rid.Equals(dataRID) {
			if err := bl.contents.Delete(bl.currentSlot); err != nil {
				return err
			}
		}
	}
	return nil
}

func (bl *BTreeLeaf) Insert(dataRID *record.RID) (*DirEntry, error) {
	flag, err := bl.contents.GetFlag()
	if err != nil {
		return nil, err
	}
	firstKey, err := bl.contents.GetDataVal(0)
	if err != nil {
		return nil, err
	}
	if flag >= 0 && firstKey.CompareTo(bl.searchKey) > 0 {
		newBlock, err := bl.contents.Split(0, flag)
		if err != nil {
			return nil, err
		}
		bl.currentSlot = 0
		if err := bl.contents.SetFlag(-1); err != nil {
			return nil, err
		}
		bl.contents.InsertLeaf(bl.currentSlot, bl.searchKey, dataRID)
		return NewDirEntry(firstKey, newBlock.Number()), nil
	}

	bl.currentSlot++
	if err := bl.contents.InsertLeaf(bl.currentSlot, bl.searchKey, dataRID); err != nil {
		return nil, err
	}
	isFull, err := bl.contents.IsFull()
	if err != nil {
		return nil, err
	}
	if !isFull {
		return nil, nil
	}

	// page is full, so split it
	numRecords, err := bl.contents.GetNumRecs()
	if err != nil {
		return nil, err
	}
	lastKey, err := bl.contents.GetDataVal(numRecords - 1)
	if err != nil {
		return nil, err
	}
	if lastKey.Equals(firstKey) {
		// create an overflow block to hold all but the first record
		newBlock, err := bl.contents.Split(1, flag)
		if err != nil {
			return nil, err
		}
		if err := bl.contents.SetFlag(newBlock.Number()); err != nil {
			return nil, err
		}
		return nil, nil
	} else {
		splitPos := numRecords / 2
		splitKey, err := bl.contents.GetDataVal(splitPos)
		if err != nil {
			return nil, err
		}
		if splitKey.Equals(firstKey) {
			// move right, looking for the next key
			for {
				dataVal, err := bl.contents.GetDataVal(splitPos)
				if err != nil {
					return nil, err
				}
				if !dataVal.Equals(splitKey) {
					break
				}
				splitPos++
			}
			splitKey, err = bl.contents.GetDataVal(splitPos)
			if err != nil {
				return nil, err
			}
		} else {
			// move left, looking for first entry having that key
			for {
				dataVal, err := bl.contents.GetDataVal(splitPos - 1)
				if err != nil {
					return nil, err
				}
				if !dataVal.Equals(splitKey) {
					break
				}
				splitPos--
			}
		}
		newBlock, err := bl.contents.Split(splitPos, -1)
		if err != nil {
			return nil, err
		}
		return NewDirEntry(splitKey, newBlock.Number()), nil
	}
}

func (bl *BTreeLeaf) tryOverFlow() (bool, error) {
	firstKey, err := bl.contents.GetDataVal(0)
	if err != nil {
		return false, err
	}
	flag, err := bl.contents.GetFlag()
	if err != nil {
		return false, err
	}
	if !bl.searchKey.Equals(firstKey) || flag < 0 {
		return false, nil
	}
	bl.contents.Close()
	newBlock := file.NewBlockID(bl.fileName, flag)
	newContents, err := NewBTPage(bl.tx, &newBlock, bl.layout)
	if err != nil {
		return false, err
	}
	bl.contents = newContents
	bl.currentSlot = 0
	return true, nil
}
