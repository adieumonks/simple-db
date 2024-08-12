package plan

import (
	"fmt"

	"github.com/adieumonks/simple-db/metadata"
	"github.com/adieumonks/simple-db/parse"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/tx"
)

type QueryPlanner interface {
	CreatePlan(data *parse.QueryData, tx *tx.Transaction) (Plan, error)
}

type UpdatePlanner interface {
	ExecuteInsert(data *parse.InsertData, tx *tx.Transaction) (int32, error)
	ExecuteDelete(data *parse.DeleteData, tx *tx.Transaction) (int32, error)
	ExecuteModify(data *parse.ModifyData, tx *tx.Transaction) (int32, error)
	ExecuteCreateTable(data *parse.CreateTableData, tx *tx.Transaction) (int32, error)
	ExecuteCreateView(data *parse.CreateViewData, tx *tx.Transaction) (int32, error)
	ExecuteCreateIndex(data *parse.CreateIndexData, tx *tx.Transaction) (int32, error)
}

type BasicQueryPlanner struct {
	mdm *metadata.MetadataManager
}

func NewBasicQueryPlanner(mdm *metadata.MetadataManager) *BasicQueryPlanner {
	return &BasicQueryPlanner{mdm: mdm}
}

func (qp *BasicQueryPlanner) CreatePlan(data *parse.QueryData, tx *tx.Transaction) (Plan, error) {
	plans := make([]Plan, 0)
	for _, table := range data.Tables {
		viewDef, err := qp.mdm.GetViewDef(table, tx)
		if err != nil {
			return nil, err
		}
		if viewDef != "" {
			parser, err := parse.NewParser(viewDef)
			if err != nil {
				return nil, err
			}
			viewData, err := parser.Query()
			if err != nil {
				return nil, err
			}
			viewPlan, err := qp.CreatePlan(viewData, tx)
			if err != nil {
				return nil, err
			}
			plans = append(plans, viewPlan)
		} else {
			tablePlan, err := NewTablePlan(tx, table, qp.mdm)
			if err != nil {
				return nil, err
			}
			plans = append(plans, tablePlan)
		}
	}

	plan := plans[0]
	for i := 1; i < len(plans); i++ {
		choice1 := NewProductPlan(plans[i], plan)
		choice2 := NewProductPlan(plan, plans[i])
		if choice1.BlocksAccessed() < choice2.BlocksAccessed() {
			plan = choice1
		} else {
			plan = choice2
		}
	}

	plan = NewSelectPlan(plan, data.Pred)

	plan = NewProjectPlan(plan, data.Fields)

	return plan, nil
}

type BasicUpdatePlanner struct {
	mdm *metadata.MetadataManager
}

func NewBasicUpdatePlanner(mdm *metadata.MetadataManager) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{mdm: mdm}
}

func (up *BasicUpdatePlanner) ExecuteDelete(data *parse.DeleteData, tx *tx.Transaction) (int32, error) {
	var plan Plan
	plan, err := NewTablePlan(tx, data.TableName, up.mdm)
	if err != nil {
		return 0, err
	}
	plan = NewSelectPlan(plan, data.Pred)

	scan, err := plan.Open()
	if err != nil {
		return 0, err
	}
	defer scan.Close()
	us, ok := scan.(query.UpdateScan)
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
		if err := us.Delete(); err != nil {
			return 0, err
		}
		count++
	}

	return count, nil
}

func (up *BasicUpdatePlanner) ExecuteModify(data *parse.ModifyData, tx *tx.Transaction) (int32, error) {
	var plan Plan
	plan, err := NewTablePlan(tx, data.TableName, up.mdm)
	if err != nil {
		return 0, err
	}
	plan = NewSelectPlan(plan, data.Pred)

	scan, err := plan.Open()
	if err != nil {
		return 0, err
	}
	defer scan.Close()
	us, ok := scan.(query.UpdateScan)
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
		val, err := data.NewValue.Evaluate(us)
		if err != nil {
			return 0, err
		}
		if err := us.SetVal(data.FieldName, val); err != nil {
			return 0, err
		}
		count++
	}

	return count, nil
}

func (up *BasicUpdatePlanner) ExecuteInsert(data *parse.InsertData, tx *tx.Transaction) (int32, error) {
	var plan Plan
	plan, err := NewTablePlan(tx, data.TableName, up.mdm)
	if err != nil {
		return 0, err
	}

	scan, err := plan.Open()
	if err != nil {
		return 0, err
	}
	defer scan.Close()
	us, ok := scan.(query.UpdateScan)
	if !ok {
		return 0, fmt.Errorf("insert: invalid scan type")
	}

	if err := us.Insert(); err != nil {
		return 0, err
	}

	for i := 0; i < len(data.Fields); i++ {
		field := data.Fields[i]
		val := data.Values[i]
		if err := us.SetVal(field, val); err != nil {
			return 0, err
		}
	}

	return 1, nil
}

func (up *BasicUpdatePlanner) ExecuteCreateTable(data *parse.CreateTableData, tx *tx.Transaction) (int32, error) {
	if err := up.mdm.CreateTable(data.TableName, data.Schema, tx); err != nil {
		return 0, err
	}
	return 0, nil
}

func (up *BasicUpdatePlanner) ExecuteCreateView(data *parse.CreateViewData, tx *tx.Transaction) (int32, error) {
	if err := up.mdm.CreateView(data.ViewName, data.QueryData.String(), tx); err != nil {
		return 0, err
	}
	return 0, nil
}

func (up *BasicUpdatePlanner) ExecuteCreateIndex(data *parse.CreateIndexData, tx *tx.Transaction) (int32, error) {
	if err := up.mdm.CreateIndex(data.IndexName, data.TableName, data.FieldName, tx); err != nil {
		return 0, err
	}
	return 0, nil
}

type Planner struct {
	qp QueryPlanner
	up UpdatePlanner
}

func NewPlanner(qp QueryPlanner, up UpdatePlanner) *Planner {
	return &Planner{qp: qp, up: up}
}

func (p *Planner) CreateQueryPlan(query string, tx *tx.Transaction) (Plan, error) {
	parser, err := parse.NewParser(query)
	if err != nil {
		return nil, err
	}
	data, err := parser.Query()
	if err != nil {
		return nil, err
	}
	return p.qp.CreatePlan(data, tx)
}

func (p *Planner) ExecuteUpdate(command string, tx *tx.Transaction) (int32, error) {
	parser, err := parse.NewParser(command)
	if err != nil {
		return 0, err
	}
	data, err := parser.UpdateCommand()
	if err != nil {
		return 0, err
	}
	switch data.CommandType() {
	case parse.Insert:
		return p.up.ExecuteInsert(data.(*parse.InsertData), tx)
	case parse.Delete:
		return p.up.ExecuteDelete(data.(*parse.DeleteData), tx)
	case parse.Modify:
		return p.up.ExecuteModify(data.(*parse.ModifyData), tx)
	case parse.CreateTable:
		return p.up.ExecuteCreateTable(data.(*parse.CreateTableData), tx)
	case parse.CreateView:
		return p.up.ExecuteCreateView(data.(*parse.CreateViewData), tx)
	case parse.CreateIndex:
		return p.up.ExecuteCreateIndex(data.(*parse.CreateIndexData), tx)
	default:
		return 0, fmt.Errorf("invalid command type")
	}
}
