package metadata

import (
	"github.com/adieumonks/simple-db/index"
	"github.com/adieumonks/simple-db/index/hash"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

type IndexInfo struct {
	indexName   string
	fieldName   string
	tx          *tx.Transaction
	tableSchema *record.Schema
	indexLayout *record.Layout
	si          *StatInfo
}

func NewIndexInfo(indexName string, fieldName string, tableSchema *record.Schema, tx *tx.Transaction, si *StatInfo) *IndexInfo {
	ii := &IndexInfo{
		indexName:   indexName,
		fieldName:   fieldName,
		tx:          tx,
		tableSchema: tableSchema,
		si:          si,
	}
	ii.createIndexLayout()
	return ii
}

func (ii *IndexInfo) Open() index.Index {
	return hash.NewHashIndex(ii.tx, ii.indexName, ii.indexLayout)
}

func (ii *IndexInfo) BlocksAccessed() int32 {
	rpb := ii.tx.BlockSize() / ii.indexLayout.SlotSize()
	numBlocks := ii.si.RecordsOutput() / rpb
	return hash.NewHashIndex(ii.tx, ii.indexName, ii.indexLayout).SearchCost(numBlocks, rpb)
}

func (ii *IndexInfo) RecordsOutput() int32 {
	return ii.si.RecordsOutput() / ii.si.DistinctValues(ii.fieldName)
}

func (ii *IndexInfo) DistinctValues(fieldName string) int32 {
	if fieldName == ii.fieldName {
		return 1
	}
	return ii.si.DistinctValues(fieldName)
}

func (ii *IndexInfo) createIndexLayout() *record.Layout {
	schema := record.NewSchema()
	schema.AddIntField("block")
	schema.AddIntField("id")
	if ii.tableSchema.Type(ii.fieldName) == record.INTEGER {
		schema.AddIntField("dataval")
	} else {
		fieldLength := ii.tableSchema.Length(ii.fieldName)
		schema.AddStringField("dataval", fieldLength)
	}
	return record.NewLayoutFromSchema(schema)
}
