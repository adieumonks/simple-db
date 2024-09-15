package materialize

import (
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
)

var _ query.Scan = (*SortScan)(nil)

type SortScan struct {
	s1            query.UpdateScan
	s2            query.UpdateScan
	currentScan   query.UpdateScan
	comp          *RecordComparator
	hasMore1      bool
	hasMore2      bool
	savedPosition []*record.RID
	savedScan     query.UpdateScan
}

func NewSortScan(runs []*TempTable, comp *RecordComparator) (*SortScan, error) {
	s1, err := runs[0].Open()
	if err != nil {
		return nil, err
	}
	hasMore1, err := s1.Next()
	if err != nil {
		return nil, err
	}

	var s2 query.UpdateScan
	var hasMore2 bool
	if len(runs) > 1 {
		s2, err = runs[1].Open()
		if err != nil {
			return nil, err
		}
		hasMore2, err = s2.Next()
		if err != nil {
			return nil, err
		}
	}
	return &SortScan{
		s1:       s1,
		s2:       s2,
		comp:     comp,
		hasMore1: hasMore1,
		hasMore2: hasMore2,
	}, nil
}

func (ss *SortScan) BeforeFirst() error {
	ss.currentScan = nil
	if err := ss.s1.BeforeFirst(); err != nil {
		return err
	}
	hasMore1, err := ss.s1.Next()
	if err != nil {
		return err
	}
	ss.hasMore1 = hasMore1
	if ss.s2 != nil {
		if err := ss.s2.BeforeFirst(); err != nil {
			return err
		}
		hasMore2, err := ss.s2.Next()
		if err != nil {
			return err
		}
		ss.hasMore2 = hasMore2
	}
	return nil
}

func (ss *SortScan) Next() (bool, error) {
	if ss.currentScan != nil {
		if ss.currentScan == ss.s1 {
			hasMore1, err := ss.s1.Next()
			if err != nil {
				return false, err
			}
			ss.hasMore1 = hasMore1
		} else if ss.currentScan == ss.s2 {
			hasMore2, err := ss.s2.Next()
			if err != nil {
				return false, err
			}
			ss.hasMore2 = hasMore2
		}
	}

	if !ss.hasMore1 && !ss.hasMore2 {
		return false, nil
	} else if ss.hasMore1 && ss.hasMore2 {
		cmp, err := ss.comp.Compare(ss.s1, ss.s2)
		if err != nil {
			return false, err
		}
		if cmp < 0 {
			ss.currentScan = ss.s1
		} else {
			ss.currentScan = ss.s2
		}
	} else if ss.hasMore1 {
		ss.currentScan = ss.s1
	} else if ss.hasMore2 {
		ss.currentScan = ss.s2
	}
	return true, nil
}

func (ss *SortScan) GetInt(fieldName string) (int32, error) {
	return ss.currentScan.GetInt(fieldName)
}

func (ss *SortScan) GetString(fieldName string) (string, error) {
	return ss.currentScan.GetString(fieldName)
}

func (ss *SortScan) GetVal(fieldName string) (*query.Constant, error) {
	return ss.currentScan.GetVal(fieldName)
}

func (ss *SortScan) HasField(fieldName string) bool {
	return ss.currentScan.HasField(fieldName)
}

func (ss *SortScan) Close() {
	ss.s1.Close()
	if ss.s2 != nil {
		ss.s2.Close()
	}
}

func (ss *SortScan) SavePosition() {
	rid1 := ss.s1.GetRID()
	var rid2 *record.RID
	if ss.s2 != nil {
		rid2 = ss.s2.GetRID()
	}
	ss.savedPosition = []*record.RID{rid1, rid2}
	ss.savedScan = ss.currentScan
}

func (ss *SortScan) RestorePosition() error {
	rid1 := ss.savedPosition[0]
	rid2 := ss.savedPosition[1]
	if err := ss.s1.MoveToRID(rid1); err != nil {
		return err
	}
	if rid2 != nil {
		if err := ss.s2.MoveToRID(rid2); err != nil {
			return err
		}
	}
	ss.currentScan = ss.savedScan
	return nil
}
