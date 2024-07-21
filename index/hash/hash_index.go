package hash

import (
	"fmt"

	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

const (
	NUM_BUCKETS = 100
)

type HashIndex struct {
	tx        *tx.Transaction
	indexName string
	layout    *record.Layout
	searchKey *record.Constant
	ts        *record.TableScan
}

func NewHashIndex(tx *tx.Transaction, indexName string, layout *record.Layout) *HashIndex {
	return &HashIndex{
		tx:        tx,
		indexName: indexName,
		layout:    layout,
	}
}

func (hi *HashIndex) BeforeFirst(searchKey *record.Constant) error {
	hi.Close()
	hi.searchKey = searchKey
	bucket := searchKey.HashCode() % NUM_BUCKETS
	tableName := fmt.Sprintf("%s%d", hi.indexName, bucket)
	ts, err := record.NewTableScan(hi.tx, tableName, hi.layout)
	if err != nil {
		return fmt.Errorf("failed to create table scan: %w", err)
	}
	hi.ts = ts
	return nil
}

func (hi *HashIndex) Next() (bool, error) {
	next, err := hi.ts.Next()
	if err != nil {
		return false, fmt.Errorf("failed to get next record: %w", err)
	}
	for next {
		constant, err := hi.ts.GetVal("dataval")
		if err != nil {
			return false, fmt.Errorf("failed to get dataval: %w", err)
		}
		if constant.Equals(hi.searchKey) {
			return true, nil
		}
	}
	return false, nil
}

func (hi *HashIndex) GetDataRID() (*record.RID, error) {
	blockNum, err := hi.ts.GetInt("block")
	if err != nil {
		return nil, fmt.Errorf("failed to get block number: %w", err)
	}
	id, err := hi.ts.GetInt("id")
	if err != nil {
		return nil, fmt.Errorf("failed to get id: %w", err)
	}
	return record.NewRID(blockNum, id), nil
}

func (hi *HashIndex) Insert(dataval *record.Constant, dataRID *record.RID) error {
	err := hi.BeforeFirst(dataval)
	if err != nil {
		return fmt.Errorf("failed to search for dataval: %w", err)
	}
	err = hi.ts.Insert()
	if err != nil {
		return fmt.Errorf("failed to insert record: %w", err)
	}
	err = hi.ts.SetInt("block", dataRID.BlockNumber())
	if err != nil {
		return fmt.Errorf("failed to set block number: %w", err)
	}
	err = hi.ts.SetInt("id", dataRID.Slot())
	if err != nil {
		return fmt.Errorf("failed to set slot: %w", err)
	}
	err = hi.ts.SetVal("dataval", dataval)
	if err != nil {
		return fmt.Errorf("failed to set dataval: %w", err)
	}
	return nil
}

func (hi *HashIndex) Delete(dataval *record.Constant, dataRID *record.RID) error {
	err := hi.BeforeFirst(dataval)
	if err != nil {
		return fmt.Errorf("failed to search for dataval: %w", err)
	}
	next, err := hi.Next()
	if err != nil {
		return fmt.Errorf("failed to search for dataval: %w", err)
	}
	for next {
		recordID, err := hi.GetDataRID()
		if err != nil {
			return fmt.Errorf("failed to get data RID: %w", err)
		}
		if recordID == dataRID {
			err = hi.ts.Delete()
			if err != nil {
				return fmt.Errorf("failed to delete record: %w", err)
			}
			return nil
		}
	}
	return nil
}

func (hi *HashIndex) Close() {
	if hi.ts != nil {
		hi.ts.Close()
	}
}
func (hi *HashIndex) SearchCost(numBlocks int32, rpb int32) int32 {
	return numBlocks / NUM_BUCKETS
}
