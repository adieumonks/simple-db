package query

import (
	"github.com/adieumonks/simple-db/record"
)

type Scan interface {
	BeforeFirst()
	Next() (bool, error)
	GetInt(fieldName string) (int32, error)
	GetString(fieldName string) (string, error)
	GetVal(fieldName string) (*record.Constant, error)
	HasField(fieldName string) bool
	Close()
}
