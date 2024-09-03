package index

import (
	"math"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

var _ Index = (*BTreeIndex)(nil)

type BTreeIndex struct {
	tx         *tx.Transaction
	dirLayout  *record.Layout
	leafLayout *record.Layout
	leafTable  string
	leaf       *BTreeLeaf
	rootBlock  *file.BlockID
}

func NewBTreeIndex(tx *tx.Transaction, indexName string, leafLayout *record.Layout) (*BTreeIndex, error) {
	// deal with the leaves
	leafTable := indexName + "leaf"
	leafTableSize, err := tx.Size(leafTable)
	if err != nil {
		return nil, err
	}
	if leafTableSize == 0 {
		block, err := tx.Append(leafTable)
		if err != nil {
			return nil, err
		}
		node, err := NewBTPage(tx, &block, leafLayout)
		if err != nil {
			return nil, err
		}
		if err := node.Format(&block, -1); err != nil {
			return nil, err
		}
	}

	// deal with the directory
	dirSchema := record.NewSchema()
	dirSchema.Add("block", leafLayout.Schema())
	dirSchema.Add("dataval", leafLayout.Schema())
	dirTable := indexName + "dir"
	dirLayout := record.NewLayoutFromSchema(dirSchema)
	rootBlock := file.NewBlockID(dirTable, 0)
	dirTableSize, err := tx.Size(dirTable)
	if err != nil {
		return nil, err
	}
	if dirTableSize == 0 {
		// create new root block
		if _, err := tx.Append(dirTable); err != nil {
			return nil, err
		}
		node, err := NewBTPage(tx, &rootBlock, dirLayout)
		if err != nil {
			return nil, err
		}
		if err := node.Format(&rootBlock, 0); err != nil {
			return nil, err
		}

		// insert initial directory entry
		fieldType := dirSchema.Type("dataval")

		var minVal *query.Constant

		if fieldType == record.INTEGER {
			minVal = query.NewConstantWithInt(0)
		} else {
			minVal = query.NewConstantWithString("")
		}

		if err := node.InsertDir(0, minVal, 0); err != nil {
			return nil, err
		}
		node.Close()
	}

	return &BTreeIndex{
		tx:         tx,
		dirLayout:  dirLayout,
		leafLayout: leafLayout,
		leafTable:  leafTable,
		leaf:       nil,
		rootBlock:  &rootBlock,
	}, nil
}

func (bi *BTreeIndex) BeforeFirst(searchkey *query.Constant) error {
	bi.Close()

	root, err := NewBTreeDir(bi.tx, bi.rootBlock, bi.dirLayout)
	if err != nil {
		return err
	}
	blockNum, err := root.Search(searchkey)
	if err != nil {
		return err
	}
	root.Close()

	leafBlock := file.NewBlockID(bi.leafTable, blockNum)
	leaf, err := NewBTreeLeaf(bi.tx, &leafBlock, bi.leafLayout, searchkey)
	if err != nil {
		return err
	}
	bi.leaf = leaf
	return nil
}

func (bi *BTreeIndex) Next() (bool, error) {
	return bi.leaf.Next()
}

func (bi *BTreeIndex) GetDataRID() (*record.RID, error) {
	return bi.leaf.GetDataRID()
}

func (bi *BTreeIndex) Insert(dataval *query.Constant, dataRID *record.RID) error {
	if err := bi.BeforeFirst(dataval); err != nil {
		return err
	}
	e, err := bi.leaf.Insert(dataRID)
	if err != nil {
		return err
	}
	if e == nil {
		return nil
	}

	root, err := NewBTreeDir(bi.tx, bi.rootBlock, bi.dirLayout)
	if err != nil {
		return err
	}
	defer root.Close()

	e2, err := root.Insert(e)
	if err != nil {
		return err
	}
	if e2 != nil {
		if err := root.MakeNewRoot(e2); err != nil {
			return err
		}
	}

	return nil
}

func (bi *BTreeIndex) Delete(dataval *query.Constant, dataRID *record.RID) error {
	if err := bi.BeforeFirst(dataval); err != nil {
		return err
	}
	if err := bi.leaf.Delete(dataRID); err != nil {
		return err
	}
	bi.leaf.Close()
	return nil
}

func (bi *BTreeIndex) Close() {
	if bi.leaf != nil {
		bi.leaf.Close()
	}
}

func (bi *BTreeIndex) SearchCost(numBlocks int32, rpb int32) int32 {
	return 1 + int32(math.Log(float64(numBlocks))/math.Log(float64(rpb)))
}
