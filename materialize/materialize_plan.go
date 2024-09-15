package materialize

import (
	"math"

	"github.com/adieumonks/simple-db/plan"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

var _ query.Plan = (*MaterializePlan)(nil)

type MaterializePlan struct {
	srcPlan plan.Plan
	tx      *tx.Transaction
}

func NewMaterializePlan(tx *tx.Transaction, srcPlan plan.Plan) *MaterializePlan {
	return &MaterializePlan{
		srcPlan: srcPlan,
		tx:      tx,
	}
}

func (mp *MaterializePlan) Open() (query.Scan, error) {
	sch := mp.srcPlan.Schema()
	temp := NewTempTable(mp.tx, sch)
	src, err := mp.srcPlan.Open()
	if err != nil {
		return nil, err
	}
	dest, err := temp.Open()
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

		if err := dest.Insert(); err != nil {
			return nil, err
		}
		for _, fieldName := range sch.Fields() {
			val, err := src.GetVal(fieldName)
			if err != nil {
				return nil, err
			}
			if err := dest.SetVal(fieldName, val); err != nil {
				return nil, err
			}
		}
	}

	src.Close()
	if err := dest.BeforeFirst(); err != nil {
		return nil, err
	}
	return dest, nil
}

func (mp *MaterializePlan) BlocksAccessed() int32 {
	layout := record.NewLayoutFromSchema(mp.srcPlan.Schema())
	rpb := float64(mp.tx.BlockSize()) / float64(layout.SlotSize())
	return int32(math.Ceil(float64(mp.srcPlan.RecordsOutput()) / rpb))
}

func (mp *MaterializePlan) RecordsOutput() int32 {
	return mp.srcPlan.RecordsOutput()
}

func (mp *MaterializePlan) DistinctValues(fieldName string) int32 {
	return mp.srcPlan.DistinctValues(fieldName)
}

func (mp *MaterializePlan) Schema() *record.Schema {
	return mp.srcPlan.Schema()
}
