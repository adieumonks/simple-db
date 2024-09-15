package materialize

import (
	"fmt"

	"github.com/adieumonks/simple-db/query"
)

type GroupValue struct {
	vals map[string]*query.Constant
}

func NewGroupValue(s query.Scan, fields []string) (*GroupValue, error) {
	vals := make(map[string]*query.Constant)
	for _, field := range fields {
		val, err := s.GetVal(field)
		if err != nil {
			return nil, err
		}
		vals[field] = val
	}
	return &GroupValue{vals: vals}, nil
}

func (gv *GroupValue) GetVal(fieldName string) *query.Constant {
	return gv.vals[fieldName]
}

func (gv *GroupValue) Equals(other *GroupValue) bool {
	for fieldName := range gv.vals {
		v1 := gv.vals[fieldName]
		v2 := other.GetVal(fieldName)
		if !v1.Equals(v2) {
			return false
		}
	}
	return true
}

func (gv *GroupValue) HashCode() int32 {
	hashVal := int32(0)
	for _, val := range gv.vals {
		hashVal += val.HashCode()
	}
	return hashVal
}

var _ query.Scan = (*GroupByScan)(nil)

type GroupByScan struct {
	s           query.Scan
	groupFiedls []string
	aggFns      []AggregationFn
	groupVal    *GroupValue
	moreGroups  bool
}

func NewGroupByScan(s query.Scan, groupFields []string, aggFns []AggregationFn) (*GroupByScan, error) {
	gs := &GroupByScan{
		s:           s,
		groupFiedls: groupFields,
		aggFns:      aggFns,
	}
	if err := gs.BeforeFirst(); err != nil {
		return nil, err
	}
	return gs, nil
}

func (gs *GroupByScan) BeforeFirst() error {
	if err := gs.s.BeforeFirst(); err != nil {
		return err
	}
	moreGroups, err := gs.s.Next()
	if err != nil {
		return err
	}
	gs.moreGroups = moreGroups
	return nil
}

func (gs *GroupByScan) Next() (bool, error) {
	if !gs.moreGroups {
		return false, nil
	}
	for _, fn := range gs.aggFns {
		if err := fn.ProcessFirst(gs.s); err != nil {
			return false, err
		}
	}
	groupVal, err := NewGroupValue(gs.s, gs.groupFiedls)
	if err != nil {
		return false, err
	}
	gs.groupVal = groupVal
	for {
		moreGroups, err := gs.s.Next()
		if err != nil {
			return false, err
		}
		gs.moreGroups = moreGroups
		if !moreGroups {
			break
		}
		gv, err := NewGroupValue(gs.s, gs.groupFiedls)
		if err != nil {
			return false, err
		}
		if !groupVal.Equals(gv) {
			break
		}
		for _, fn := range gs.aggFns {
			if err := fn.ProcessNext(gs.s); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}
func (gs *GroupByScan) Close() {
	gs.s.Close()
}

func (gs *GroupByScan) GetVal(fieldName string) (*query.Constant, error) {
	for _, field := range gs.groupFiedls {
		if field == fieldName {
			return gs.groupVal.GetVal(fieldName), nil
		}
	}
	for _, fn := range gs.aggFns {
		if fn.FieldName() == fieldName {
			return fn.Value(), nil
		}
	}
	return nil, fmt.Errorf("field %s not found", fieldName)
}

func (gs *GroupByScan) GetInt(fieldName string) (int32, error) {
	val, err := gs.GetVal(fieldName)
	if err != nil {
		return 0, err
	}
	return val.AsInt(), nil
}

func (gs *GroupByScan) GetString(fieldName string) (string, error) {
	val, err := gs.GetVal(fieldName)
	if err != nil {
		return "", err
	}
	return val.AsString(), nil
}

func (gs *GroupByScan) HasField(fieldName string) bool {
	for _, field := range gs.groupFiedls {
		if field == fieldName {
			return true
		}
	}
	for _, fn := range gs.aggFns {
		if fn.FieldName() == fieldName {
			return true
		}
	}
	return false
}
