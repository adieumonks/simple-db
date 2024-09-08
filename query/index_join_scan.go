package query

var _ Scan = (*IndexJoinScan)(nil)

type IndexJoinScan struct {
	lhs       Scan
	idx       Index
	joinField string
	rhs       *TableScan
}

func NewIndexJoinScan(lhs Scan, idx Index, joinField string, rhs *TableScan) (*IndexJoinScan, error) {
	s := &IndexJoinScan{
		lhs:       lhs,
		idx:       idx,
		joinField: joinField,
		rhs:       rhs,
	}
	if err := s.BeforeFirst(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *IndexJoinScan) BeforeFirst() error {
	if err := s.lhs.BeforeFirst(); err != nil {
		return err
	}
	_, err := s.lhs.Next()
	if err != nil {
		return err
	}
	return s.resetIndex()
}

func (s *IndexJoinScan) Next() (bool, error) {
	for {
		next2, err := s.idx.Next()
		if err != nil {
			return false, err
		}
		if next2 {
			rid, err := s.idx.GetDataRID()
			if err != nil {
				return false, err
			}
			if err := s.rhs.MoveToRID(rid); err != nil {
				return false, err
			}
			return true, nil
		}
		next1, err := s.lhs.Next()
		if err != nil {
			return false, err
		}
		if !next1 {
			return false, nil
		}
		if err := s.resetIndex(); err != nil {
			return false, err
		}
	}
}

func (s *IndexJoinScan) GetInt(fieldName string) (int32, error) {
	if s.rhs.HasField(fieldName) {
		return s.rhs.GetInt(fieldName)
	} else {
		return s.lhs.GetInt(fieldName)
	}
}

func (s *IndexJoinScan) GetString(fieldName string) (string, error) {
	if s.rhs.HasField(fieldName) {
		return s.rhs.GetString(fieldName)
	} else {
		return s.lhs.GetString(fieldName)
	}
}

func (s *IndexJoinScan) GetVal(fieldName string) (*Constant, error) {
	if s.rhs.HasField(fieldName) {
		return s.rhs.GetVal(fieldName)
	} else {
		return s.lhs.GetVal(fieldName)
	}
}

func (s *IndexJoinScan) HasField(fieldName string) bool {
	return s.rhs.HasField(fieldName) || s.lhs.HasField(fieldName)
}

func (s *IndexJoinScan) Close() {
	s.lhs.Close()
	s.idx.Close()
	s.rhs.Close()
}

func (s *IndexJoinScan) resetIndex() error {
	searchKey, err := s.lhs.GetVal(s.joinField)
	if err != nil {
		return err
	}
	return s.idx.BeforeFirst(searchKey)
}
