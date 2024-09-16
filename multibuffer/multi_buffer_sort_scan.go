package multibuffer

import (
	"github.com/adieumonks/simple-db/materialize"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
)

var _ query.Scan = (*MultiBufferSortScan)(nil)

type MultiBufferSortScan struct {
	srcs          []query.UpdateScan
	currentScan   query.UpdateScan
	comp          *materialize.RecordComparator
	hasMores      []bool
	savedPosition []*record.RID
	savedScan     query.UpdateScan
}

func NewMultiBufferSortScan(runs []*materialize.TempTable, comp *materialize.RecordComparator) (*MultiBufferSortScan, error) {
	srcs := make([]query.UpdateScan, len(runs))
	hasMores := make([]bool, len(runs))
	for i, run := range runs {
		src, err := run.Open()
		if err != nil {
			return nil, err
		}
		hasMore, err := src.Next()
		if err != nil {
			return nil, err
		}
		srcs[i] = src
		hasMores[i] = hasMore
	}

	return &MultiBufferSortScan{
		srcs:     srcs,
		comp:     comp,
		hasMores: hasMores,
	}, nil
}

func (ss *MultiBufferSortScan) BeforeFirst() error {
	ss.currentScan = nil
	for i, src := range ss.srcs {
		if err := src.BeforeFirst(); err != nil {
			return err
		}
		hasMore, err := src.Next()
		if err != nil {
			return err
		}
		ss.hasMores[i] = hasMore
	}
	return nil
}

func (ss *MultiBufferSortScan) Next() (bool, error) {
	if ss.currentScan != nil {
		for i, src := range ss.srcs {
			if src == ss.currentScan {
				hasMore, err := src.Next()
				if err != nil {
					return false, err
				}
				ss.hasMores[i] = hasMore
			}
		}
	}
	min := -1
	for i, src := range ss.srcs {
		if !ss.hasMores[i] {
			continue
		}
		if min == -1 {
			min = i
		} else {
			cmp, err := ss.comp.Compare(ss.srcs[min], src)
			if err != nil {
				return false, err
			}
			if cmp > 0 {
				min = i
			}
		}
	}
	if min == -1 {
		return false, nil
	}
	ss.currentScan = ss.srcs[min]
	return true, nil
}

func (ss *MultiBufferSortScan) GetInt(fieldName string) (int32, error) {
	return ss.currentScan.GetInt(fieldName)
}

func (ss *MultiBufferSortScan) GetString(fieldName string) (string, error) {
	return ss.currentScan.GetString(fieldName)
}

func (ss *MultiBufferSortScan) GetVal(fieldName string) (*query.Constant, error) {
	return ss.currentScan.GetVal(fieldName)
}

func (ss *MultiBufferSortScan) HasField(fieldName string) bool {
	return ss.currentScan.HasField(fieldName)
}

func (ss *MultiBufferSortScan) Close() {
	for _, src := range ss.srcs {
		src.Close()
	}
}

func (ss *MultiBufferSortScan) SavePosition() {
	rids := make([]*record.RID, len(ss.srcs))
	for i, src := range ss.srcs {
		rids[i] = src.GetRID()
	}
	ss.savedPosition = rids
	ss.savedScan = ss.currentScan
}

func (ss *MultiBufferSortScan) RestorePosition() error {
	for i, src := range ss.srcs {
		if err := src.MoveToRID(ss.savedPosition[i]); err != nil {
			return err
		}
	}
	ss.currentScan = ss.savedScan
	return nil
}
