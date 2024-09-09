package metadata

import (
	"fmt"

	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

type IndexManager struct {
	layout       *record.Layout
	tableManager *TableManager
	statManager  *StatManager
}

func NewIndexManager(isNew bool, tableManager *TableManager, statManager *StatManager, tx *tx.Transaction) (*IndexManager, error) {
	if isNew {
		schema := record.NewSchema()
		schema.AddStringField("indexname", MAX_NAME)
		schema.AddStringField("tablename", MAX_NAME)
		schema.AddStringField("fieldname", MAX_NAME)
		err := tableManager.CreateTable("idxcat", schema, tx)
		if err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}
	layout, err := tableManager.GetLayout("idxcat", tx)
	if err != nil {
		return nil, fmt.Errorf("failed to get layout: %w", err)
	}
	return &IndexManager{
		layout:       layout,
		tableManager: tableManager,
		statManager:  statManager,
	}, nil
}

func (im *IndexManager) CreateIndex(indexName string, tableName string, fieldName string, tx *tx.Transaction) error {
	ts, err := query.NewTableScan(tx, "idxcat", im.layout)
	if err != nil {
		return fmt.Errorf("failed to create table scan: %w", err)
	}
	err = ts.Insert()
	if err != nil {
		return fmt.Errorf("failed to insert: %w", err)
	}
	err = ts.SetString("indexname", indexName)
	if err != nil {
		return fmt.Errorf("failed to set string: %w", err)
	}
	err = ts.SetString("tablename", tableName)
	if err != nil {
		return fmt.Errorf("failed to set string: %w", err)
	}
	err = ts.SetString("fieldname", fieldName)
	if err != nil {
		return fmt.Errorf("failed to set string: %w", err)
	}
	ts.Close()
	return nil
}

func (im *IndexManager) GetIndexInfo(tableName string, tx *tx.Transaction) (map[string]*IndexInfo, error) {
	result := make(map[string]*IndexInfo)
	ts, err := query.NewTableScan(tx, "idxcat", im.layout)
	if err != nil {
		return nil, fmt.Errorf("failed to create table scan: %w", err)
	}
	next, err := ts.Next()
	if err != nil {
		return nil, fmt.Errorf("failed to get next: %w", err)
	}
	for next {
		tableNameAtRecord, err := ts.GetString("tablename")
		if err != nil {
			return nil, fmt.Errorf("failed to get string: %w", err)
		}
		if tableNameAtRecord == tableName {
			indexName, err := ts.GetString("indexname")
			if err != nil {
				return nil, fmt.Errorf("failed to get string: %w", err)
			}
			fieldName, err := ts.GetString("fieldname")
			if err != nil {
				return nil, fmt.Errorf("failed to get string: %w", err)
			}
			layout, err := im.tableManager.GetLayout(tableName, tx)
			if err != nil {
				return nil, fmt.Errorf("failed to get layout: %w", err)
			}
			si, err := im.statManager.GetStatInfo(tableName, layout, tx)
			if err != nil {
				return nil, fmt.Errorf("failed to get stat info: %w", err)
			}
			ii := NewIndexInfo(indexName, fieldName, layout.Schema(), tx, si)
			result[fieldName] = ii
		}
		next, err = ts.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to get next: %w", err)
		}
	}
	ts.Close()
	return result, nil
}
