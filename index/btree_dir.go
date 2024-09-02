package index

import (
	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

type BTreeDir struct {
	tx       *tx.Transaction
	layout   *record.Layout
	contents *BTPage
	filename string
}

func NewBTreeDir(tx *tx.Transaction, block *file.BlockID, layout *record.Layout) (*BTreeDir, error) {
	contents, err := NewBTPage(tx, block, layout)
	if err != nil {
		return nil, err
	}
	return &BTreeDir{
		tx:       tx,
		layout:   layout,
		contents: contents,
		filename: block.Filename(),
	}, nil
}

func (bd *BTreeDir) Close() {
	bd.contents.Close()
}

func (bd *BTreeDir) Search(searchKey *query.Constant) (int32, error) {
	childBlock, err := bd.findChildBlock(searchKey)
	if err != nil {
		return 0, err
	}
	for {
		flag, err := bd.contents.GetFlag()
		if err != nil {
			return 0, err
		}
		if flag <= 0 {
			break
		}
		bd.contents.Close()
		bd.contents, err = NewBTPage(bd.tx, childBlock, bd.layout)
		if err != nil {
			return 0, err
		}
		childBlock, err = bd.findChildBlock(searchKey)
		if err != nil {
			return 0, err
		}
	}
	return childBlock.Number(), nil
}

func (bd *BTreeDir) MakeNewRoot(e *DirEntry) error {
	firstVal, err := bd.contents.GetDataVal(0)
	if err != nil {
		return err
	}
	level, err := bd.contents.GetFlag()
	if err != nil {
		return err
	}
	newBlock, err := bd.contents.Split(0, level)
	if err != nil {
		return err
	}
	oldRoot := NewDirEntry(firstVal, newBlock.Number())
	if _, err = bd.insertEntry(oldRoot); err != nil {
		return err
	}
	if _, err = bd.insertEntry(e); err != nil {
		return err
	}
	if err := bd.contents.SetFlag(level + 1); err != nil {
		return err
	}
	return nil
}

func (bd *BTreeDir) Insert(e *DirEntry) (*DirEntry, error) {
	flag, err := bd.contents.GetFlag()
	if err != nil {
		return nil, err
	}
	if flag == 0 {
		return bd.insertEntry(e)
	}
	childBlock, err := bd.findChildBlock(e.DataVal())
	if err != nil {
		return nil, err
	}
	childDir, err := NewBTreeDir(bd.tx, childBlock, bd.layout)
	if err != nil {
		return nil, err
	}
	entry, err := childDir.Insert(e)
	if err != nil {
		return nil, err
	}
	childDir.Close()
	if entry != nil {
		return bd.insertEntry(entry)
	} else {
		return nil, nil
	}
}

func (bd *BTreeDir) insertEntry(e *DirEntry) (*DirEntry, error) {
	slot, err := bd.contents.FindSlotBefore(e.DataVal())
	if err != nil {
		return nil, err
	}
	if err := bd.contents.InsertDir(slot+1, e.DataVal(), e.BlockNumber()); err != nil {
		return nil, err
	}
	isFull, err := bd.contents.IsFull()
	if err != nil {
		return nil, err
	}
	if !isFull {
		return nil, nil
	}
	// page is full, so split it
	level, err := bd.contents.GetFlag()
	if err != nil {
		return nil, err
	}
	numRecords, err := bd.contents.GetNumRecs()
	if err != nil {
		return nil, err
	}
	splitPos := numRecords / 2
	splitVal, err := bd.contents.GetDataVal(splitPos)
	if err != nil {
		return nil, err
	}
	newBlock, err := bd.contents.Split(splitPos, level)
	if err != nil {
		return nil, err
	}
	return NewDirEntry(splitVal, newBlock.Number()), nil
}

func (bd *BTreeDir) findChildBlock(searchKey *query.Constant) (*file.BlockID, error) {
	slot, err := bd.contents.FindSlotBefore(searchKey)
	if err != nil {
		return nil, err
	}
	dataVal, err := bd.contents.GetDataVal(slot + 1)
	if err != nil {
		return nil, err
	}
	if dataVal.Equals(searchKey) {
		slot++
	}
	blockNum, err := bd.contents.GetChildNum(slot)
	if err != nil {
		return nil, err
	}
	block := file.NewBlockID(bd.filename, blockNum)
	return &block, nil
}
