package plan

import (
	"fmt"

	"github.com/adieumonks/simple-db/metadata"
	"github.com/adieumonks/simple-db/parse"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/tx"
)

var _ UpdatePlanner = (*IndexUpdatePlanner)(nil)

type IndexUpdatePlanner struct {
	mdm *metadata.MetadataManager
}

func (up *IndexUpdatePlanner) ExecuteInsert(data *parse.InsertData, tx *tx.Transaction) (int32, error) {
	tableName := data.TableName
	p, err := NewTablePlan(tx, tableName, up.mdm)
	if err != nil {
		return 0, err
	}

	s, err := p.Open()
	if err != nil {
		return 0, err
	}
	us, ok := s.(query.UpdateScan)
	if !ok {
		return 0, fmt.Errorf("insert: invalid scan type")
	}
	if err := us.Insert(); err != nil {
		return 0, err
	}
	rid := us.GetRID()

	indexes, err := up.mdm.GetIndexInfo(tableName, tx)
	if err != nil {
		return 0, err
	}
	for i := 0; i < len(data.Fields); i++ {
		fieldName := data.Fields[i]
		val := data.Values[i]
		if err := us.SetVal(fieldName, val); err != nil {
			return 0, err
		}

		ii, ok := indexes[fieldName]
		if ok {
			idx := ii.Open()
			if err := idx.Insert(val, rid); err != nil {
				return 0, err
			}
			idx.Close()
		}
	}
	us.Close()
	return 1, nil
}

func (up *IndexUpdatePlanner) ExecuteDelete(data *parse.DeleteData, tx *tx.Transaction) (int32, error) {
	tableName := data.TableName
	tp, err := NewTablePlan(tx, tableName, up.mdm)
	if err != nil {
		return 0, err
	}
	sp := NewSelectPlan(tp, data.Pred)
	indexes, err := up.mdm.GetIndexInfo(tableName, tx)
	if err != nil {
		return 0, err
	}

	s, err := sp.Open()
	if err != nil {
		return 0, err
	}
	us, ok := s.(query.UpdateScan)
	if !ok {
		return 0, fmt.Errorf("delete: invalid scan type")
	}

	count := int32(0)
	for {
		next, err := us.Next()
		if err != nil {
			return 0, err
		}
		if !next {
			break
		}

		rid := us.GetRID()
		for fieldName := range indexes {
			val, err := us.GetVal(fieldName)
			if err != nil {
				return 0, err
			}
			ii := indexes[fieldName]
			idx := ii.Open()
			if err := idx.Delete(val, rid); err != nil {
				return 0, err
			}
			idx.Close()
		}

		if err := us.Delete(); err != nil {
			return 0, err
		}
		count++
	}
	us.Close()
	return count, nil
}

func (up *IndexUpdatePlanner) ExecuteModify(data *parse.ModifyData, tx *tx.Transaction) (int32, error) {
	tableName := data.TableName
	fieldName := data.FieldName
	tp, err := NewTablePlan(tx, tableName, up.mdm)
	if err != nil {
		return 0, err
	}
	sp := NewSelectPlan(tp, data.Pred)

	indexes, err := up.mdm.GetIndexInfo(tableName, tx)
	if err != nil {
		return 0, err
	}
	ii := indexes[fieldName]
	var idx query.Index
	if ii != nil {
		idx = ii.Open()
	}

	s, err := sp.Open()
	if err != nil {
		return 0, err
	}
	us, ok := s.(query.UpdateScan)
	if !ok {
		return 0, fmt.Errorf("modify: invalid scan type")
	}

	count := int32(0)
	for {
		next, err := us.Next()
		if err != nil {
			return 0, err
		}
		if !next {
			break
		}

		newVal, err := data.NewValue.Evaluate(us)
		if err != nil {
			return 0, err
		}
		oldVal, err := us.GetVal(fieldName)
		if err != nil {
			return 0, err
		}
		if err := us.SetVal(fieldName, newVal); err != nil {
			return 0, err
		}

		if idx != nil {
			rid := us.GetRID()
			if err := idx.Delete(oldVal, rid); err != nil {
				return 0, err
			}
			if err := idx.Insert(newVal, rid); err != nil {
				return 0, err
			}
		}

		count++
	}

	if idx != nil {
		idx.Close()
	}
	us.Close()
	return count, nil
}

func (up *IndexUpdatePlanner) ExecuteCreateTable(data *parse.CreateTableData, tx *tx.Transaction) (int32, error) {
	if err := up.mdm.CreateTable(data.TableName, data.Schema, tx); err != nil {
		return 0, err
	}
	return 0, nil
}

func (up *IndexUpdatePlanner) ExecuteCreateView(data *parse.CreateViewData, tx *tx.Transaction) (int32, error) {
	if err := up.mdm.CreateView(data.ViewName, data.ViewDef(), tx); err != nil {
		return 0, err
	}
	return 0, nil
}

func (up *IndexUpdatePlanner) ExecuteCreateIndex(data *parse.CreateIndexData, tx *tx.Transaction) (int32, error) {
	if err := up.mdm.CreateIndex(data.IndexName, data.TableName, data.FieldName, tx); err != nil {
		return 0, err
	}
	return 0, nil
}
