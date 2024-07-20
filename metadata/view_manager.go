package metadata

import (
	"fmt"

	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

const (
	MAX_VIEWDEF = 100
)

type ViewManager struct {
	tableManager *TableManager
}

func NewViewManager(isNew bool, tableManager *TableManager, tx *tx.Transaction) (*ViewManager, error) {
	if isNew {
		schema := record.NewSchema()
		schema.AddStringField("viewname", MAX_NAME)
		schema.AddStringField("viewdef", MAX_VIEWDEF)
		err := tableManager.CreateTable("viewcat", schema, tx)
		if err != nil {
			return nil, fmt.Errorf("failt to create viewcat table: %w", err)
		}
	}
	return &ViewManager{tableManager: tableManager}, nil
}

func (vm *ViewManager) CreateView(viewName string, viewDef string, tx *tx.Transaction) error {
	layout, err := vm.tableManager.GetLayout("viewcat", tx)
	if err != nil {
		return fmt.Errorf("failed to get layout: %w", err)
	}

	ts, err := record.NewTableScan(tx, "viewcat", layout)
	if err != nil {
		return fmt.Errorf("failed to create table scan: %w", err)
	}

	err = ts.Insert()
	if err != nil {
		return fmt.Errorf("failed to insert: %w", err)
	}
	err = ts.SetString("viewname", viewName)
	if err != nil {
		return fmt.Errorf("failed to set string: %w", err)
	}
	err = ts.SetString("viewdef", viewDef)
	if err != nil {
		return fmt.Errorf("failed to set string: %w", err)
	}
	ts.Close()
	return nil
}
