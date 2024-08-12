package query

type Scan interface {
	BeforeFirst() error
	Next() (bool, error)
	GetInt(fieldName string) (int32, error)
	GetString(fieldName string) (string, error)
	GetVal(fieldName string) (*Constant, error)
	HasField(fieldName string) bool
	Close()
}
