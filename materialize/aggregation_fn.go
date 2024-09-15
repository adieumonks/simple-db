package materialize

import "github.com/adieumonks/simple-db/query"

type AggregationFn interface {
	ProcessFirst(s query.Scan) error
	ProcessNext(s query.Scan) error
	FieldName() string
	Value() *query.Constant
}
