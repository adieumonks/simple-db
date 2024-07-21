package index

import (
	"github.com/adieumonks/simple-db/record"
)

type Index interface {
	BeforeFirst(searchKey *record.Constant) error
	Next() (bool, error)
	GetDataRID() (*record.RID, error)
	Insert(dataval *record.Constant, dataRID *record.RID) error
	Delete(dataval *record.Constant, dataRID *record.RID) error
	Close()
}
