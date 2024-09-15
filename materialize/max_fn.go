package materialize

import (
	"fmt"

	"github.com/adieumonks/simple-db/query"
)

var _ AggregationFn = (*MaxFn)(nil)

type MaxFn struct {
	fieldName string
	val       *query.Constant
}

func NewMaxFn(fieldName string) *MaxFn {
	return &MaxFn{fieldName: fieldName}
}

func (mf *MaxFn) ProcessFirst(s query.Scan) error {
	val, err := s.GetVal(mf.fieldName)
	if err != nil {
		return err
	}
	mf.val = val
	return nil
}

func (mf *MaxFn) ProcessNext(s query.Scan) error {
	newVal, err := s.GetVal(mf.fieldName)
	if err != nil {
		return err
	}
	if newVal.CompareTo(mf.val) > 0 {
		mf.val = newVal
	}
	return nil
}

func (mf *MaxFn) FieldName() string {
	return fmt.Sprintf("maxof%s", mf.fieldName)
}

func (mf *MaxFn) Value() *query.Constant {
	return mf.val
}
