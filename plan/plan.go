package plan

import (
	"github.com/adieumonks/simple-db/metadata"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

type Plan interface {
	Open() (query.Scan, error)
	BlocksAccessed() int32
	RecordsOutput() int32
	DistinctValues(fieldName string) int32
	Schema() *record.Schema
}

var _ Plan = (*TablePlan)(nil)

type TablePlan struct {
	tableName string
	tx        *tx.Transaction
	layout    *record.Layout
	si        *metadata.StatInfo
}

func NewTablePlan(tx *tx.Transaction, tableName string, mdm *metadata.MetadataManager) (*TablePlan, error) {
	layout, err := mdm.GetLayout(tableName, tx)
	if err != nil {
		return nil, err
	}
	si, err := mdm.GetStatInfo(tableName, layout, tx)
	if err != nil {
		return nil, err
	}
	return &TablePlan{
		tableName: tableName,
		tx:        tx,
		layout:    layout,
		si:        si,
	}, nil
}

func (tp *TablePlan) Open() (query.Scan, error) {
	return query.NewTableScan(tp.tx, tp.tableName, tp.layout)
}

func (tp *TablePlan) BlocksAccessed() int32 {
	return tp.si.BlocksAccessed()
}

func (tp *TablePlan) RecordsOutput() int32 {
	return tp.si.RecordsOutput()
}

func (tp *TablePlan) DistinctValues(fieldName string) int32 {
	return tp.si.DistinctValues(fieldName)
}

func (tp *TablePlan) Schema() *record.Schema {
	return tp.layout.Schema()
}

var _ Plan = (*SelectPlan)(nil)

type SelectPlan struct {
	p    Plan
	pred *query.Predicate
}

func NewSelectPlan(p Plan, pred *query.Predicate) *SelectPlan {
	return &SelectPlan{
		p:    p,
		pred: pred,
	}
}

func (sp *SelectPlan) Open() (query.Scan, error) {
	s, err := sp.p.Open()
	if err != nil {
		return nil, err
	}
	return query.NewSelectScan(s, sp.pred), nil
}

func (sp *SelectPlan) BlocksAccessed() int32 {
	return sp.p.BlocksAccessed()
}

func (sp *SelectPlan) RecordsOutput() int32 {
	return sp.p.RecordsOutput() / sp.pred.ReductionFactor(sp.p)
}

func (sp *SelectPlan) DistinctValues(fieldName string) int32 {
	if sp.pred.EquatesWithConstant(fieldName) != nil {
		return 1
	} else {
		otherFieldName := sp.pred.EquatesWithField(fieldName)
		if otherFieldName != "" {
			return min(sp.p.DistinctValues(fieldName), sp.p.DistinctValues(otherFieldName))
		} else {
			return sp.p.DistinctValues(fieldName)
		}
	}
}

func (sp *SelectPlan) Schema() *record.Schema {
	return sp.p.Schema()
}

var _ Plan = (*ProjectPlan)(nil)

type ProjectPlan struct {
	p      Plan
	schema *record.Schema
}

func NewProjectPlan(p Plan, fields []string) *ProjectPlan {
	schema := record.NewSchema()
	for _, field := range fields {
		schema.Add(field, p.Schema())
	}
	return &ProjectPlan{
		p:      p,
		schema: schema,
	}
}

func (pp *ProjectPlan) Open() (query.Scan, error) {
	s, err := pp.p.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProjectScan(s, pp.schema.Fields()), nil
}

func (pp *ProjectPlan) BlocksAccessed() int32 {
	return pp.p.BlocksAccessed()
}

func (pp *ProjectPlan) RecordsOutput() int32 {
	return pp.p.RecordsOutput()
}

func (pp *ProjectPlan) DistinctValues(fieldName string) int32 {
	return pp.p.DistinctValues(fieldName)
}

func (pp *ProjectPlan) Schema() *record.Schema {
	return pp.schema
}

var _ Plan = (*ProductPlan)(nil)

type ProductPlan struct {
	p1     Plan
	p2     Plan
	schema *record.Schema
}

func NewProductPlan(p1 Plan, p2 Plan) *ProductPlan {
	schema := record.NewSchema()
	schema.AddAll(p1.Schema())
	schema.AddAll(p2.Schema())
	return &ProductPlan{
		p1:     p1,
		p2:     p2,
		schema: schema,
	}
}

func (pp *ProductPlan) Open() (query.Scan, error) {
	s1, err := pp.p1.Open()
	if err != nil {
		return nil, err
	}
	s2, err := pp.p2.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProductScan(s1, s2)
}

func (pp *ProductPlan) BlocksAccessed() int32 {
	return pp.p1.BlocksAccessed() + pp.p1.RecordsOutput()*pp.p2.BlocksAccessed()
}

func (pp *ProductPlan) RecordsOutput() int32 {
	return pp.p1.RecordsOutput() * pp.p2.RecordsOutput()
}

func (pp *ProductPlan) DistinctValues(fieldName string) int32 {
	if pp.p1.Schema().HasField(fieldName) {
		return pp.p1.DistinctValues(fieldName)
	} else {
		return pp.p2.DistinctValues(fieldName)
	}
}

func (pp *ProductPlan) Schema() *record.Schema {
	return pp.schema
}
