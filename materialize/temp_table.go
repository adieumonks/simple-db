package materialize

import (
	"fmt"
	"sync"

	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

var nextTableNum = 0

var mu sync.Mutex

type TempTable struct {
	tx        *tx.Transaction
	tableName string
	layout    *record.Layout
}

func NewTempTable(tx *tx.Transaction, sch *record.Schema) *TempTable {
	tableName := nextTableName()
	layout := record.NewLayoutFromSchema(sch)
	return &TempTable{tx, tableName, layout}
}

func (tt *TempTable) Open() (query.UpdateScan, error) {
	ts, err := query.NewTableScan(tt.tx, tt.tableName, tt.layout)
	if err != nil {
		return nil, err
	}
	return ts, nil
}

func (tt *TempTable) TableName() string {
	return tt.tableName
}

func (tt *TempTable) Layout() *record.Layout {
	return tt.layout
}

func nextTableName() string {
	mu.Lock()
	defer mu.Unlock()
	nextTableNum++
	return fmt.Sprintf("temp%d", nextTableNum)
}
