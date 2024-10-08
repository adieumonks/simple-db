package query

import "github.com/adieumonks/simple-db/record"

type UpdateScan interface {
	Scan
	SetVal(fieldName string, val *Constant) error
	SetInt(fieldName string, val int32) error
	SetString(fieldName string, val string) error
	Insert() error
	Delete() error
	GetRID() *record.RID
	MoveToRID(rid *record.RID) error
}
