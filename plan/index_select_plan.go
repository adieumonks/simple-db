package plan

import (
	"fmt"

	"github.com/adieumonks/simple-db/metadata"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
)

var _ Plan = (*IndexSelectPlan)(nil)

type IndexSelectPlan struct {
	p   Plan
	ii  *metadata.IndexInfo
	val *query.Constant
}

func NewIndexSelectPlan(p Plan, ii *metadata.IndexInfo, val *query.Constant) *IndexSelectPlan {
	return &IndexSelectPlan{p: p, ii: ii, val: val}
}

func (p *IndexSelectPlan) Open() (query.Scan, error) {
	s, err := p.p.Open()
	if err != nil {
		return nil, err
	}
	ts, ok := s.(*query.TableScan)
	if !ok {
		return nil, fmt.Errorf("IndexSelectPlan: unexpected underlying scan type")
	}
	idx := p.ii.Open()
	iss, err := query.NewIndexSelectScan(ts, idx, p.val)
	if err != nil {
		return nil, err
	}
	return iss, nil
}

func (p *IndexSelectPlan) BlocksAccessed() int32 {
	return p.ii.BlocksAccessed() + p.RecordsOutput()
}

func (p *IndexSelectPlan) RecordsOutput() int32 {
	return p.ii.RecordsOutput()
}

func (p *IndexSelectPlan) DistinctValues(fieldName string) int32 {
	return p.ii.DistinctValues(fieldName)
}

func (p *IndexSelectPlan) Schema() *record.Schema {
	return p.p.Schema()
}
