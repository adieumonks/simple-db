package metadata

import (
	"fmt"
	"sync"

	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

type StatManager struct {
	tableManager *TableManager
	tableStats   map[string]*StatInfo
	numCalls     int32
	mu           sync.Mutex
}

func NewStatManager(tm *TableManager, tx *tx.Transaction) *StatManager {
	sm := &StatManager{
		tableManager: tm,
	}
	sm.refreshStatics(tx)
	return sm
}

func (sm *StatManager) GetStatInfo(tableName string, layout *record.Layout, tx *tx.Transaction) (*StatInfo, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.numCalls++
	if sm.numCalls > 100 {
		sm.refreshStatics(tx)
	}

	si, ok := sm.tableStats[tableName]
	if !ok {
		si, err := sm.calcTableStats(tableName, layout, tx)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate table stats: %w", err)
		}
		sm.tableStats[tableName] = si
	}
	return si, nil
}

func (sm *StatManager) refreshStatics(tx *tx.Transaction) error {
	tcatLayout, err := sm.tableManager.GetLayout("tblcat", tx)
	if err != nil {
		return fmt.Errorf("failed to get layout: %v", err)
	}
	tcat, err := record.NewTableScan(tx, "tblcat", tcatLayout)
	if err != nil {
		return fmt.Errorf("failed to create table scan: %v", err)
	}
	next, err := tcat.Next()
	if err != nil {
		return fmt.Errorf("failed to get next: %v", err)
	}
	for next {
		tableName, err := tcat.GetString("tblname")
		if err != nil {
			return fmt.Errorf("failed to get string: %v", err)
		}
		layout, err := sm.tableManager.GetLayout(tableName, tx)
		if err != nil {
			return fmt.Errorf("failed to get layout: %v", err)
		}
		si, err := sm.calcTableStats(tableName, layout, tx)
		if err != nil {
			return fmt.Errorf("failed to calculate table stats: %v", err)
		}
		sm.tableStats[tableName] = si
	}
	tcat.Close()
	return nil
}

func (sm *StatManager) calcTableStats(tableName string, layout *record.Layout, tx *tx.Transaction) (*StatInfo, error) {
	numRecords := int32(0)
	numBlocks := int32(0)
	ts, err := record.NewTableScan(tx, tableName, layout)
	if err != nil {
		return nil, fmt.Errorf("failed to create table scan: %v", err)
	}
	next, err := ts.Next()
	if err != nil {
		return nil, fmt.Errorf("failed to get next: %v", err)
	}
	for next {
		numRecords++
		numBlocks = ts.GetRID().BlockNumber() + 1
	}
	ts.Close()
	return &StatInfo{
		numRecords: numRecords,
		numBlocks:  numBlocks,
	}, nil
}
