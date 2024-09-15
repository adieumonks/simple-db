package materialize

import "github.com/adieumonks/simple-db/query"

type RecordComparator struct {
	fields []string
}

func NewRecordComparator(fields []string) *RecordComparator {
	return &RecordComparator{fields: fields}
}

func (rc *RecordComparator) Compare(s1 query.Scan, s2 query.Scan) (int32, error) {
	for _, fieldName := range rc.fields {
		val1, err := s1.GetVal(fieldName)
		if err != nil {
			return 0, err
		}
		val2, err := s2.GetVal(fieldName)
		if err != nil {
			return 0, err
		}
		result := val1.CompareTo(val2)
		if result != 0 {
			return result, nil
		}
	}
	return 0, nil
}
