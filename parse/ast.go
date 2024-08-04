package parse

import (
	"fmt"
	"strings"

	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
)

type QueryData struct {
	Fields  []string
	Talbles []string
	Pred    *query.Predicate
}

func NewQueryData(fields, tables []string, pred *query.Predicate) *QueryData {
	return &QueryData{Fields: fields, Talbles: tables, Pred: pred}
}

func (q *QueryData) String() string {
	var sb strings.Builder

	fmt.Fprintf(&sb, "select %s from %s", strings.Join(q.Fields, ", "), strings.Join(q.Talbles, ", "))

	fmt.Println(q.Pred)

	if pred := q.Pred.String(); pred != "" {
		fmt.Fprintf(&sb, " where %s", pred)
	}

	return sb.String()
}

type UpdateCommand interface {
	updateCommand()
}

func (*InsertData) updateCommand()      {}
func (*ModifyData) updateCommand()      {}
func (*DeleteData) updateCommand()      {}
func (*CreateTableData) updateCommand() {}
func (*CreateViewData) updateCommand()  {}
func (*CreateIndexData) updateCommand() {}

type InsertData struct {
	TableName string
	Fields    []string
	Values    []*record.Constant
}

func NewInsertData(tableName string, fields []string, values []*record.Constant) *InsertData {
	return &InsertData{TableName: tableName, Fields: fields, Values: values}
}

type ModifyData struct {
	TableName string
	FieldName string
	NewValue  *query.Expression
	Pred      *query.Predicate
}

func NewModifyData(tableName, fieldName string, newValue *query.Expression, pred *query.Predicate) *ModifyData {
	return &ModifyData{TableName: tableName, FieldName: fieldName, NewValue: newValue, Pred: pred}
}

type DeleteData struct {
	TableName string
	Pred      *query.Predicate
}

func NewDeleteData(tableName string, pred *query.Predicate) *DeleteData {
	return &DeleteData{TableName: tableName, Pred: pred}
}

type CreateTableData struct {
	TableName string
	Schema    *record.Schema
}

func NewCreateTableData(tableName string, schema *record.Schema) *CreateTableData {
	return &CreateTableData{TableName: tableName, Schema: schema}
}

type CreateViewData struct {
	ViewName  string
	QueryData *QueryData
}

func NewCreateViewData(viewName string, queryData *QueryData) *CreateViewData {
	return &CreateViewData{ViewName: viewName, QueryData: queryData}
}

type CreateIndexData struct {
	IndexName string
	TableName string
	FieldName string
}

func NewCreateIndexData(indexName, tableName, fieldName string) *CreateIndexData {
	return &CreateIndexData{IndexName: indexName, TableName: tableName, FieldName: fieldName}
}
