package index

import (
	"fmt"

	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

var NUM_BUCKETS int32 = 100

var _ Index = (*HashIndex)(nil)

type HashIndex struct {
	tx        *tx.Transaction
	indexName string
	layout    *record.Layout
	searchKey *query.Constant
	ts        *query.TableScan
}

func NewHashIndex(tx *tx.Transaction, indexName string, layout *record.Layout) *HashIndex {
	return &HashIndex{tx: tx, indexName: indexName, layout: layout}
}

func (hi *HashIndex) BeforeFirst(searchKey *query.Constant) error {
	hi.Close()
	hi.searchKey = searchKey
	bucket := searchKey.HashCode() % NUM_BUCKETS
	tableName := fmt.Sprintf("%s%d", hi.indexName, bucket)
	ts, err := query.NewTableScan(hi.tx, tableName, hi.layout)
	if err != nil {
		return err
	}
	hi.ts = ts
	return nil
}

func (hi *HashIndex) Next() (bool, error) {
	for {
		next, err := hi.ts.Next()
		if err != nil {
			return false, err
		}
		if !next {
			return false, nil
		}
		dataVal, err := hi.ts.GetVal("dataval")
		if err != nil {
			return false, err
		}
		if dataVal.Equals(hi.searchKey) {
			return true, nil
		}
	}
}

func (hi *HashIndex) GetDataRID() (*record.RID, error) {
	blockNum, err := hi.ts.GetInt("block")
	if err != nil {
		return nil, err
	}
	id, err := hi.ts.GetInt("id")
	if err != nil {
		return nil, err
	}
	return record.NewRID(blockNum, id), nil
}

func (hi *HashIndex) Insert(dataVal *query.Constant, dataRID *record.RID) error {
	if err := hi.BeforeFirst(dataVal); err != nil {
		return err
	}
	if err := hi.ts.Insert(); err != nil {
		return err
	}
	if err := hi.ts.SetInt("block", dataRID.BlockNumber()); err != nil {
		return err
	}
	if err := hi.ts.SetInt("id", dataRID.Slot()); err != nil {
		return err
	}
	if err := hi.ts.SetVal("dataval", dataVal); err != nil {
		return err
	}
	return nil
}

func (hi *HashIndex) Delete(dataVal *query.Constant, dataRID *record.RID) error {
	if err := hi.BeforeFirst(dataVal); err != nil {
		return err
	}
	for {
		next, err := hi.ts.Next()
		if err != nil {
			return err
		}
		if !next {
			return nil
		}
		rid, err := hi.GetDataRID()
		if err != nil {
			return err
		}
		if rid.Equals(dataRID) {
			if err := hi.ts.Delete(); err != nil {
				return err
			}
			return nil
		}
	}
}

func (hi *HashIndex) Close() {
	if hi.ts != nil {
		hi.ts.Close()
	}
}

func (hi *HashIndex) SearchCost(numBlocks int32, rpb int32) int32 {
	return numBlocks / NUM_BUCKETS
}
