package materialize

import (
	"fmt"

	"github.com/adieumonks/simple-db/plan"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

var _ plan.Plan = (*MergeJoinPlan)(nil)

type MergeJoinPlan struct {
	p1         plan.Plan
	p2         plan.Plan
	fieldName1 string
	fieldName2 string
	sch        *record.Schema
}

func NewMergeJoinPlan(tx *tx.Transaction, p1 plan.Plan, p2 plan.Plan, fieldName1 string, fieldName2 string) *MergeJoinPlan {
	p := &MergeJoinPlan{
		p1:         NewSortPlan(tx, p1, []string{fieldName1}),
		p2:         NewSortPlan(tx, p2, []string{fieldName2}),
		fieldName1: fieldName1,
		fieldName2: fieldName2,
		sch:        record.NewSchema(),
	}
	p.sch.AddAll(p1.Schema())
	p.sch.AddAll(p2.Schema())
	return p
}

func (p *MergeJoinPlan) Open() (query.Scan, error) {
	s1, err := p.p1.Open()
	if err != nil {
		return nil, err
	}
	s2, err := p.p2.Open()
	if err != nil {
		return nil, err
	}
	ss2, ok := s2.(*SortScan)
	if !ok {
		return nil, fmt.Errorf("MergeJoinPlan: s2 is not a SortScan")
	}
	return NewMergeJoinScan(s1, ss2, p.fieldName1, p.fieldName2)
}

func (p *MergeJoinPlan) BlocksAccessed() int32 {
	return p.p1.BlocksAccessed() + p.p2.BlocksAccessed()
}

func (p *MergeJoinPlan) RecordsOutput() int32 {
	maxVals := max(p.p1.DistinctValues(p.fieldName1), p.p2.DistinctValues(p.fieldName2))
	return (p.p1.RecordsOutput() * p.p2.RecordsOutput()) / maxVals
}

func (p *MergeJoinPlan) DistinctValues(fieldName string) int32 {
	if p.p1.Schema().HasField(fieldName) {
		return p.p1.DistinctValues(fieldName)
	} else {
		return p.p2.DistinctValues(fieldName)
	}
}

func (p *MergeJoinPlan) Schema() *record.Schema {
	return p.sch
}
