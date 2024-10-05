package multibuffer

import (
	"github.com/adieumonks/simple-db/materialize"
	"github.com/adieumonks/simple-db/plan"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

var _ plan.Plan = (*HashJoinPlan)(nil)

type HashJoinPlan struct {
	tx                     *tx.Transaction
	p1, p2                 plan.Plan
	fieldName1, fieldName2 string
	schema                 *record.Schema
}

func NewHashJoinPlan(tx *tx.Transaction, p1, p2 plan.Plan, fieldName1, fieldName2 string) *HashJoinPlan {
	schema := record.NewSchema()
	schema.AddAll(p1.Schema())
	schema.AddAll(p2.Schema())

	return &HashJoinPlan{
		tx:         tx,
		p1:         p1,
		p2:         p2,
		fieldName1: fieldName1,
		fieldName2: fieldName2,
		schema:     schema,
	}
}

func (hjp *HashJoinPlan) Open() (query.Scan, error) {
	available := hjp.tx.AvailableBuffers()
	numBuffers := BestFactor(available, hjp.p2.BlocksAccessed())

	t1, err := hjp.copyToTemp(hjp.p1)
	if err != nil {
		return nil, err
	}
	t2, err := hjp.copyToTemp(hjp.p2)
	if err != nil {
		return nil, err
	}

	buckets1, buckets2, err := hjp.recursiveSplitIntoBuckets(t1, t2, numBuffers, 100)
	if err != nil {
		return nil, err
	}

	return query.NewSelectScan(
		NewHashJoinScan(hjp.tx, buckets1, buckets2),
		query.NewPredicateFromTerm(
			query.NewTerm(
				query.NewExpressionFromField(hjp.fieldName1),
				query.NewExpressionFromField(hjp.fieldName2),
			),
		),
	), nil
}

func (hjp *HashJoinPlan) BlocksAccessed() int32 {
	return -1 // TODO
}

func (hjp *HashJoinPlan) RecordsOutput() int32 {
	return -1 // TODO
}

func (hjp *HashJoinPlan) DistinctValues(fieldName string) int32 {
	if hjp.p1.Schema().HasField(fieldName) {
		return hjp.p1.DistinctValues(fieldName)
	}
	return hjp.p2.DistinctValues(fieldName)
}

func (hjp *HashJoinPlan) Schema() *record.Schema {
	return hjp.schema
}

func (hjp *HashJoinPlan) copyToTemp(p plan.Plan) (*materialize.TempTable, error) {
	src, err := p.Open()
	if err != nil {
		return nil, err
	}
	defer src.Close()

	sch := p.Schema()
	t := materialize.NewTempTable(hjp.tx, sch)
	dest, err := t.Open()
	if err != nil {
		return nil, err
	}
	defer dest.Close()

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

	return t, nil
}

func (hjp *HashJoinPlan) recursiveSplitIntoBuckets(t1, t2 *materialize.TempTable, numBuffers int32, depth int32) ([]*materialize.TempTable, []*materialize.TempTable, error) {
	if depth == 0 {
		return []*materialize.TempTable{t1}, []*materialize.TempTable{t2}, nil
	}

	blockNum2, err := hjp.tx.Size(t2.TableName() + ".tbl")
	if err != nil {
		return nil, nil, err
	}

	if blockNum2 <= numBuffers {
		return []*materialize.TempTable{t1}, []*materialize.TempTable{t2}, nil
	}

	buckets1, err := hjp.splitIntoBuckets(t1, numBuffers, numBuffers, hjp.fieldName1)
	if err != nil {
		return nil, nil, err
	}

	buckets2, err := hjp.splitIntoBuckets(t2, numBuffers, numBuffers, hjp.fieldName2)
	if err != nil {
		return nil, nil, err
	}

	subBuckets1 := make([]*materialize.TempTable, 0)
	subBuckets2 := make([]*materialize.TempTable, 0)
	for i := range numBuffers {
		sub1, sub2, err := hjp.recursiveSplitIntoBuckets(buckets1[i], buckets2[i], numBuffers, depth-1)
		if err != nil {
			return nil, nil, err
		}
		subBuckets1 = append(subBuckets1, sub1...)
		subBuckets2 = append(subBuckets2, sub2...)
	}
	return subBuckets1, subBuckets2, nil
}

func (hjp *HashJoinPlan) splitIntoBuckets(t *materialize.TempTable, numBuffers, mod int32, fieldName string) ([]*materialize.TempTable, error) {
	buckets := make([]*materialize.TempTable, numBuffers)
	scans := make([]query.UpdateScan, numBuffers)
	for i := range buckets {
		temp := materialize.NewTempTable(hjp.tx, t.GetLayout().Schema())
		scan, err := temp.Open()
		if err != nil {
			return nil, err
		}
		scans[i] = scan
		buckets[i] = temp
	}

	src, err := t.Open()
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

		val, err := src.GetVal(fieldName)
		if err != nil {
			return nil, err
		}

		hash := val.HashCode()
		bucket := hash % mod

		if err := scans[bucket].Insert(); err != nil {
			return nil, err
		}

		for _, fieldName := range t.GetLayout().Schema().Fields() {
			val, err := src.GetVal(fieldName)
			if err != nil {
				return nil, err
			}

			if err := scans[bucket].SetVal(fieldName, val); err != nil {
				return nil, err
			}
		}
	}
	for _, scan := range scans {
		scan.Close()
	}

	return buckets, nil
}
