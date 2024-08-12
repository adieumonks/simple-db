package metadata

import (
	"fmt"
	"sync"

	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

type StatManager struct {
	tableManager *TableManager
	tableStats   map[string]*StatInfo
	numCalls     int32
	mu           sync.Mutex
}

func NewStatManager(tm *TableManager, tx *tx.Transaction) (*StatManager, error) {
	sm := &StatManager{
		tableManager: tm,
	}
	err := sm.refreshStatics(tx)
	if err != nil {
		return nil, fmt.Errorf("failed to refresh statics: %w", err)
	}
	return sm, nil
}

func (sm *StatManager) GetStatInfo(tableName string, layout *record.Layout, tx *tx.Transaction) (*StatInfo, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.numCalls++
	if sm.numCalls > 100 {
		if err := sm.refreshStatics(tx); err != nil {
			return nil, err
		}
	}

	var si *StatInfo
	si, ok := sm.tableStats[tableName]
	if !ok {
		var err error
		si, err = sm.calcTableStats(tableName, layout, tx)

		if err != nil {
			return nil, fmt.Errorf("failed to calculate table stats: %w", err)
		}
		sm.tableStats[tableName] = si
	}
	return si, nil
}

func (sm *StatManager) refreshStatics(tx *tx.Transaction) error {
	sm.tableStats = make(map[string]*StatInfo)
	sm.numCalls = 0

	tcatLayout, err := sm.tableManager.GetLayout("tblcat", tx)
	if err != nil {
		return fmt.Errorf("failed to get layout: %v", err)
	}
	tcat, err := query.NewTableScan(tx, "tblcat", tcatLayout)
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

		next, err = tcat.Next()
		if err != nil {
			return fmt.Errorf("failed to get next: %v", err)
		}
	}
	tcat.Close()
	return nil
}

func (sm *StatManager) calcTableStats(tableName string, layout *record.Layout, tx *tx.Transaction) (*StatInfo, error) {
	numRecords := int32(0)
	numBlocks := int32(0)
	ts, err := query.NewTableScan(tx, tableName, layout)
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
		next, err = ts.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to get next: %v", err)
		}
	}
	ts.Close()
	return &StatInfo{
		numRecords: numRecords,
		numBlocks:  numBlocks,
	}, nil
}
