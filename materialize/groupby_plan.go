package materialize

import (
	"github.com/adieumonks/simple-db/plan"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

var _ plan.Plan = (*GroupByPlan)(nil)

type GroupByPlan struct {
	p           plan.Plan
	groupFields []string
	aggFns      []AggregationFn
	sch         *record.Schema
}

func NewGroupByPlan(tx *tx.Transaction, p plan.Plan, groupFields []string, aggFns []AggregationFn) (*GroupByPlan, error) {
	sch := record.NewSchema()
	for _, fieldName := range groupFields {
		sch.Add(fieldName, p.Schema())
	}
	for _, fn := range aggFns {
		sch.AddIntField(fn.FieldName())
	}
	return &GroupByPlan{
		p:           NewSortPlan(tx, p, groupFields),
		groupFields: groupFields,
		aggFns:      aggFns,
		sch:         sch,
	}, nil
}

func (gp *GroupByPlan) Open() (query.Scan, error) {
	s, err := gp.p.Open()
	if err != nil {
		return nil, err
	}
	return NewGroupByScan(s, gp.groupFields, gp.aggFns)
}

func (gp *GroupByPlan) BlocksAccessed() int32 {
	return gp.p.BlocksAccessed()
}

func (gp *GroupByPlan) RecordsOutput() int32 {
	numGroups := int32(1)
	for _, fieldName := range gp.groupFields {
		numGroups *= gp.p.DistinctValues(fieldName)
	}
	return numGroups
}

func (gp *GroupByPlan) DistinctValues(fieldName string) int32 {
	if gp.p.Schema().HasField(fieldName) {
		return gp.p.DistinctValues(fieldName)
	}
	return gp.p.RecordsOutput()
}

func (gp *GroupByPlan) Schema() *record.Schema {
	return gp.sch
}
