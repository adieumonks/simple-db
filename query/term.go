package query

import (
	"fmt"
	"math"

	"github.com/adieumonks/simple-db/record"
)

type Term struct {
	lhs *Expression
	rhs *Expression
}

func NewTerm(lhs *Expression, rhs *Expression) *Term {
	return &Term{lhs: lhs, rhs: rhs}
}

type Plan interface {
	DistinctValues(fieldName string) int32
}

func (t *Term) ReductionFactor(p Plan) int32 {
	var lhsName, rhsName string
	if t.lhs.IsFieldName() && t.rhs.IsFieldName() {
		lhsName = t.lhs.AsFieldName()
		rhsName = t.rhs.AsFieldName()
		return max(p.DistinctValues(lhsName), p.DistinctValues(rhsName))
	}
	if t.lhs.IsFieldName() {
		lhsName = t.lhs.AsFieldName()
		return p.DistinctValues(lhsName)
	}
	if t.rhs.IsFieldName() {
		rhsName = t.rhs.AsFieldName()
		return p.DistinctValues(rhsName)
	}
	if t.lhs.AsConstant().Equals(t.rhs.AsConstant()) {
		return 1
	}
	return math.MaxInt32
}

func (t *Term) EquatesWithConstant(fieldName string) *Constant {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && !t.rhs.IsFieldName() {
		return t.rhs.AsConstant()
	} else if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && !t.lhs.IsFieldName() {
		return t.lhs.AsConstant()
	} else {
		return nil
	}
}

func (t *Term) EquatesWithField(fieldName string) string {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && t.rhs.IsFieldName() {
		return t.rhs.AsFieldName()
	} else if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && t.lhs.IsFieldName() {
		return t.lhs.AsFieldName()
	} else {
		return ""
	}
}

func (t *Term) IsSatisfied(scan Scan) (bool, error) {
	lhsVal, err := t.lhs.Evaluate(scan)
	if err != nil {
		return false, err
	}
	rhsVal, err := t.rhs.Evaluate(scan)
	if err != nil {
		return false, err
	}
	return rhsVal.Equals(lhsVal), nil
}

func (t *Term) AppliesTo(schema *record.Schema) bool {
	return t.lhs.AppliesTo(schema) && t.rhs.AppliesTo(schema)
}

func (t *Term) String() string {
	return fmt.Sprintf("%s = %s", t.lhs.String(), t.rhs.String())
}
