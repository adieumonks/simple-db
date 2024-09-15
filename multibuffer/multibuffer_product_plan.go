package multibuffer

import (
	"github.com/adieumonks/simple-db/materialize"
	"github.com/adieumonks/simple-db/plan"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

var _ plan.Plan = (*MultibufferProductPlan)(nil)

type MultibufferProductPlan struct {
	tx     *tx.Transaction
	lhs    plan.Plan
	rhs    plan.Plan
	schema *record.Schema
}

func NewMultibufferProductPlan(tx *tx.Transaction, lhs plan.Plan, rhs plan.Plan) *MultibufferProductPlan {
	p := &MultibufferProductPlan{
		tx:     tx,
		lhs:    lhs,
		rhs:    rhs,
		schema: record.NewSchema(),
	}
	p.schema.AddAll(lhs.Schema())
	p.schema.AddAll(rhs.Schema())
	return p
}

func (mp *MultibufferProductPlan) Open() (query.Scan, error) {
	leftScan, err := mp.lhs.Open()
	if err != nil {
		return nil, err
	}
	tt, err := mp.copyRecordFrom(mp.rhs)
	if err != nil {
		return nil, err
	}
	return NewMultibufferProductScan(mp.tx, leftScan, tt.TableName(), tt.GetLayout())
}

func (mp *MultibufferProductPlan) BlocksAccessed() int32 {
	avail := mp.tx.AvailableBuffers()
	size := materialize.NewMaterializePlan(mp.tx, mp.rhs).BlocksAccessed()
	numChunks := size / avail
	return mp.rhs.BlocksAccessed() + (mp.lhs.BlocksAccessed() * numChunks)
}

func (mp *MultibufferProductPlan) RecordsOutput() int32 {
	return mp.lhs.RecordsOutput() * mp.rhs.RecordsOutput()
}

func (mp *MultibufferProductPlan) DistinctValues(fieldName string) int32 {
	if mp.lhs.Schema().HasField(fieldName) {
		return mp.lhs.DistinctValues(fieldName)
	} else {
		return mp.rhs.DistinctValues(fieldName)
	}
}

func (mp *MultibufferProductPlan) Schema() *record.Schema {
	return mp.schema
}

func (mp *MultibufferProductPlan) copyRecordFrom(p plan.Plan) (*materialize.TempTable, error) {
	src, err := p.Open()
	if err != nil {
		return nil, err
	}
	sch := p.Schema()
	t := materialize.NewTempTable(mp.tx, sch)
	dest, err := t.Open()
	if err != nil {
		return nil, err
	}
	for {
		next, err := src.Next()
		if err != nil {
			return nil, err
		}
		if !next {
			break
		}
		dest.Insert()
		for _, fldName := range sch.Fields() {
			val, err := src.GetVal(fldName)
			if err != nil {
				return nil, err
			}
			if err := dest.SetVal(fldName, val); err != nil {
				return nil, err
			}
		}
	}
	src.Close()
	dest.Close()
	return t, nil
}
