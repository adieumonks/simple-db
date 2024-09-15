package materialize

import (
	"github.com/adieumonks/simple-db/plan"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

var _ plan.Plan = (*SortPlan)(nil)

type SortPlan struct {
	tx   *tx.Transaction
	p    plan.Plan
	sch  *record.Schema
	comp *RecordComparator
}

func NewSortPlan(tx *tx.Transaction, p plan.Plan, sortFields []string) *SortPlan {
	return &SortPlan{
		tx:   tx,
		p:    p,
		sch:  p.Schema(),
		comp: NewRecordComparator(sortFields),
	}
}

func (sp *SortPlan) Open() (query.Scan, error) {
	src, err := sp.p.Open()
	if err != nil {
		return nil, err
	}
	runs, err := sp.splitIntoRuns(src)
	if err != nil {
		return nil, err
	}
	src.Close()
	for len(runs) > 2 {
		runs = sp.doAMergeIteration(runs)
	}
	return NewSortScan(runs, sp.comp)
}

func (sp *SortPlan) BlocksAccessed() int32 {
	mp := NewMaterializePlan(sp.tx, sp.p)
	return mp.BlocksAccessed()
}

func (sp *SortPlan) RecordsOutput() int32 {
	return sp.p.RecordsOutput()
}

func (sp *SortPlan) DistinctValues(fieldName string) int32 {
	return sp.p.DistinctValues(fieldName)
}

func (sp *SortPlan) Schema() *record.Schema {
	return sp.sch
}

func (sp *SortPlan) splitIntoRuns(src query.Scan) ([]*TempTable, error) {
	temps := []*TempTable{}
	if err := src.BeforeFirst(); err != nil {
		return nil, err
	}

	next, err := src.Next()
	if err != nil {
		return nil, err
	}
	if !next {
		return temps, nil
	}

	currentTemp := NewTempTable(sp.tx, sp.sch)
	temps = append(temps, currentTemp)
	currentScan, err := currentTemp.Open()
	if err != nil {
		return nil, err
	}
	for {
		next, err := sp.copy(src, currentScan)
		if err != nil {
			return nil, err
		}
		if !next {
			break
		}

		cmp, err := sp.comp.Compare(src, currentScan)
		if err != nil {
			return nil, err
		}
		if cmp < 0 {
			// start a new run
			currentScan.Close()
			currentTemp = NewTempTable(sp.tx, sp.sch)
			temps = append(temps, currentTemp)
			currentScan, err = currentTemp.Open()
			if err != nil {
				return nil, err
			}
		}
	}
	currentScan.Close()
	return temps, nil
}

func (sp *SortPlan) doAMergeIteration(runs []*TempTable) []*TempTable {
	result := []*TempTable{}
	for len(runs) > 1 {
		p1 := runs[0]
		p2 := runs[1]
		runs = runs[2:]
		merged, err := sp.mergeTwoRuns(p1, p2)
		if err != nil {
			return nil
		}
		result = append(result, merged)
	}
	if len(runs) == 1 {
		result = append(result, runs[0])
	}
	return result
}

func (sp *SortPlan) mergeTwoRuns(p1, p2 *TempTable) (*TempTable, error) {
	src1, err := p1.Open()
	if err != nil {
		return nil, err
	}
	src2, err := p2.Open()
	if err != nil {
		return nil, err
	}

	result := NewTempTable(sp.tx, sp.sch)
	dest, err := result.Open()
	if err != nil {
		return nil, err
	}

	hasMore1, err := src1.Next()
	if err != nil {
		return nil, err
	}
	hasMore2, err := src2.Next()
	if err != nil {
		return nil, err
	}
	for hasMore1 && hasMore2 {
		cmp, err := sp.comp.Compare(src1, src2)
		if err != nil {
			return nil, err
		}
		if cmp < 0 {
			hasMore1, err = sp.copy(src1, dest)
			if err != nil {
				return nil, err
			}
		} else {
			hasMore2, err = sp.copy(src2, dest)
			if err != nil {
				return nil, err
			}
		}
	}

	if hasMore1 {
		for hasMore1 {
			hasMore1, err = sp.copy(src1, dest)
			if err != nil {
				return nil, err
			}
		}
	} else {
		for hasMore2 {
			hasMore2, err = sp.copy(src2, dest)
			if err != nil {
				return nil, err
			}
		}
	}

	src1.Close()
	src2.Close()
	dest.Close()
	return result, nil
}

func (sp *SortPlan) copy(src query.Scan, dest query.UpdateScan) (bool, error) {
	if err := dest.Insert(); err != nil {
		return false, err
	}
	for _, fieldName := range sp.sch.Fields() {
		val, err := src.GetVal(fieldName)
		if err != nil {
			return false, err
		}
		if err := dest.SetVal(fieldName, val); err != nil {
			return false, err
		}
	}
	return src.Next()
}
