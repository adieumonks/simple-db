package metadata

import (
	"fmt"

	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

type MetadataManager struct {
	tableManager *TableManager
	viewManager  *ViewManager
	statManager  *StatManager
	indexManager *IndexManager
}

func NewMetadataManager(isNew bool, tx *tx.Transaction) (*MetadataManager, error) {
	tableManager, err := NewTableManager(isNew, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to create table manager: %w", err)
	}
	viewManager, err := NewViewManager(isNew, tableManager, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to create view manager: %w", err)
	}
	statManager, err := NewStatManager(tableManager, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to create stat manager: %w", err)
	}
	indexManager, err := NewIndexManager(isNew, tableManager, statManager, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to create index manager: %w", err)
	}
	return &MetadataManager{
		tableManager: tableManager,
		viewManager:  viewManager,
		statManager:  statManager,
		indexManager: indexManager,
	}, nil
}

func (mm *MetadataManager) CreateTable(tableName string, schema *record.Schema, tx *tx.Transaction) error {
	return mm.tableManager.CreateTable(tableName, schema, tx)
}

func (mm *MetadataManager) GetLayout(tableName string, tx *tx.Transaction) (*record.Layout, error) {
	return mm.tableManager.GetLayout(tableName, tx)
}

func (mm *MetadataManager) CreateView(viewName string, viewDef string, tx *tx.Transaction) error {
	return mm.viewManager.CreateView(viewName, viewDef, tx)
}

func (mm *MetadataManager) GetViewDef(viewName string, tx *tx.Transaction) (string, error) {
	return mm.viewManager.GetViewDef(viewName, tx)
}

func (mm *MetadataManager) CreateIndex(indexName string, tableName string, fieldName string, tx *tx.Transaction) error {
	return mm.indexManager.CreateIndex(indexName, tableName, fieldName, tx)
}

func (mm *MetadataManager) GetIndexInfo(tableName string, tx *tx.Transaction) (map[string]*IndexInfo, error) {
	return mm.indexManager.GetIndexInfo(tableName, tx)
}

func (mm *MetadataManager) GetStatInfo(tableName string, layout *record.Layout, tx *tx.Transaction) (*StatInfo, error) {
	return mm.statManager.GetStatInfo(tableName, layout, tx)
}
