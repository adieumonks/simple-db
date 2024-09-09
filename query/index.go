package query

import (
	"github.com/adieumonks/simple-db/record"
)

type Index interface {
	BeforeFirst(searchKey *Constant) error
	Next() (bool, error)
	GetDataRID() (*record.RID, error)
	Insert(dataval *Constant, dataRID *record.RID) error
	Delete(dataval *Constant, dataRID *record.RID) error
	Close()
}
