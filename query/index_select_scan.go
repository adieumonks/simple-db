package query

var _ Scan = (*IndexSelectScan)(nil)

type IndexSelectScan struct {
	ts  *TableScan
	idx Index
	val *Constant
}

func NewIndexSelectScan(ts *TableScan, idx Index, val *Constant) (*IndexSelectScan, error) {
	s := &IndexSelectScan{
		ts:  ts,
		idx: idx,
		val: val,
	}
	if err := s.BeforeFirst(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *IndexSelectScan) BeforeFirst() error {
	return s.idx.BeforeFirst(s.val)
}

func (s *IndexSelectScan) Next() (bool, error) {
	next, err := s.idx.Next()
	if err != nil {
		return false, err
	}
	if next {
		rid, err := s.idx.GetDataRID()
		if err != nil {
			return false, err
		}
		if err := s.ts.MoveToRID(rid); err != nil {
			return false, err
		}
	}
	return next, nil
}

func (s *IndexSelectScan) GetInt(fieldName string) (int32, error) {
	return s.ts.GetInt(fieldName)
}

func (s *IndexSelectScan) GetString(fieldName string) (string, error) {
	return s.ts.GetString(fieldName)
}

func (s *IndexSelectScan) GetVal(fieldName string) (*Constant, error) {
	return s.ts.GetVal(fieldName)
}

func (s *IndexSelectScan) HasField(fieldName string) bool {
	return s.ts.HasField(fieldName)
}

func (s *IndexSelectScan) Close() {
	s.idx.Close()
	s.ts.Close()
}
