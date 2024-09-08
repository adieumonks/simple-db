package plan

import (
	"fmt"

	"github.com/adieumonks/simple-db/metadata"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
)

var _ Plan = (*IndexJoinPlan)(nil)

type IndexJoinPlan struct {
	p1        Plan
	p2        Plan
	ii        *metadata.IndexInfo
	joinField string
	sch       *record.Schema
}

func NewIndexJoinPlan(p1 Plan, p2 Plan, ii *metadata.IndexInfo, joinField string) *IndexJoinPlan {
	sch := record.NewSchema()
	sch.AddAll(p1.Schema())
	sch.AddAll(p2.Schema())
	return &IndexJoinPlan{p1: p1, p2: p2, ii: ii, joinField: joinField, sch: sch}
}

func (p *IndexJoinPlan) Open() (query.Scan, error) {
	s1, err := p.p1.Open()
	if err != nil {
		return nil, err
	}
	s2, err := p.p2.Open()
	if err != nil {
		return nil, err
	}
	ts, ok := s2.(*query.TableScan)
	if !ok {
		return nil, fmt.Errorf("IndexJoinPlan: unexpected underlying scan type")
	}
	idx := p.ii.Open()
	return query.NewIndexJoinScan(s1, idx, p.joinField, ts)
}

func (p *IndexJoinPlan) BlocksAccessed() int32 {
	return p.p1.BlocksAccessed() + (p.p1.RecordsOutput() * p.ii.BlocksAccessed()) + p.RecordsOutput()
}

func (p *IndexJoinPlan) RecordsOutput() int32 {
	return p.p1.RecordsOutput() * p.ii.RecordsOutput()
}

func (p *IndexJoinPlan) DistinctValues(fieldName string) int32 {
	if p.p1.Schema().HasField(fieldName) {
		return p.p1.DistinctValues(fieldName)
	} else {
		return p.p2.DistinctValues(fieldName)
	}
}

func (p *IndexJoinPlan) Schema() *record.Schema {
	return p.sch
}
