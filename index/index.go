package index

import (
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
)

type Index interface {
	BeforeFirst(searchKey *query.Constant) error
	Next() (bool, error)
	GetDataRID() (*record.RID, error)
	Insert(dataval *query.Constant, dataRID *record.RID) error
	Delete(dataval *query.Constant, dataRID *record.RID) error
	Close()
}
