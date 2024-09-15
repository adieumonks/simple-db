package materialize

import (
	"fmt"

	"github.com/adieumonks/simple-db/query"
)

var _ AggregationFn = (*CountFn)(nil)

type CountFn struct {
	fieldName string
	count     int32
}

func NewCountFn(fieldName string) *CountFn {
	return &CountFn{fieldName: fieldName}
}

func (cf *CountFn) ProcessFirst(s query.Scan) error {
	cf.count = 1
	return nil
}

func (cf *CountFn) ProcessNext(s query.Scan) error {
	cf.count++
	return nil
}

func (cf *CountFn) FieldName() string {
	return fmt.Sprintf("countof%s", cf.fieldName)
}

func (cf *CountFn) Value() *query.Constant {
	return query.NewConstantWithInt(cf.count)
}
