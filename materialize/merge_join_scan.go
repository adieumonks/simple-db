package materialize

import (
	"github.com/adieumonks/simple-db/query"
)

var _ query.Scan = (*MergeJoinScan)(nil)

type MergeJoinScan struct {
	s1         query.Scan
	s2         *SortScan
	fieldName1 string
	fieldName2 string
	joinVal    *query.Constant
}

func NewMergeJoinScan(s1 query.Scan, s2 *SortScan, fieldName1 string, fieldName2 string) (*MergeJoinScan, error) {
	s := &MergeJoinScan{
		s1:         s1,
		s2:         s2,
		fieldName1: fieldName1,
		fieldName2: fieldName2,
	}
	if err := s.BeforeFirst(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *MergeJoinScan) Close() {
	s.s1.Close()
	s.s2.Close()
}

func (s *MergeJoinScan) BeforeFirst() error {
	if err := s.s1.BeforeFirst(); err != nil {
		return err
	}
	if err := s.s2.BeforeFirst(); err != nil {
		return err
	}
	return nil
}

func (s *MergeJoinScan) Next() (bool, error) {
	hasMore2, err := s.s2.Next()
	if err != nil {
		return false, err
	}
	if hasMore2 {
		val2, err := s.s2.GetVal(s.fieldName2)
		if err != nil {
			return false, err
		}
		if s.joinVal != nil && val2.Equals(s.joinVal) {
			return true, nil
		}
	}

	hasMore1, err := s.s1.Next()
	if err != nil {
		return false, err
	}
	if hasMore1 {
		val1, err := s.s1.GetVal(s.fieldName1)
		if err != nil {
			return false, err
		}
		if s.joinVal != nil && val1.Equals(s.joinVal) {
			if err := s.s2.RestorePosition(); err != nil {
				return false, err
			}
			return true, nil
		}
	}

	for hasMore1 && hasMore2 {
		v1, err := s.s1.GetVal(s.fieldName1)
		if err != nil {
			return false, err
		}
		v2, err := s.s2.GetVal(s.fieldName2)
		if err != nil {
			return false, err
		}
		if v1.CompareTo(v2) < 0 {
			hasMore1, err = s.s1.Next()
			if err != nil {
				return false, err
			}
		} else if v1.CompareTo(v2) > 0 {
			hasMore2, err = s.s2.Next()
			if err != nil {
				return false, err
			}
		} else {
			s.s2.SavePosition()
			joinVal, err := s.s2.GetVal(s.fieldName2)
			if err != nil {
				return false, err
			}
			s.joinVal = joinVal
			return true, nil
		}
	}
	return false, nil
}

func (s *MergeJoinScan) GetInt(fieldName string) (int32, error) {
	if s.s1.HasField(fieldName) {
		return s.s1.GetInt(fieldName)
	} else {
		return s.s2.GetInt(fieldName)
	}
}

func (s *MergeJoinScan) GetString(fieldName string) (string, error) {
	if s.s1.HasField(fieldName) {
		return s.s1.GetString(fieldName)
	} else {
		return s.s2.GetString(fieldName)
	}
}

func (s *MergeJoinScan) GetVal(fieldName string) (*query.Constant, error) {
	if s.s1.HasField(fieldName) {
		return s.s1.GetVal(fieldName)
	} else {
		return s.s2.GetVal(fieldName)
	}
}

func (s *MergeJoinScan) HasField(fieldName string) bool {
	return s.s1.HasField(fieldName) || s.s2.HasField(fieldName)
}
