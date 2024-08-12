package query

import "github.com/adieumonks/simple-db/record"

type Expression struct {
	val       *Constant
	fieldName *string
}

func NewExpressionFromConstant(val *Constant) *Expression {
	return &Expression{val: val}
}

func NewExpressionFromField(fieldName string) *Expression {
	return &Expression{fieldName: &fieldName}
}

func (e *Expression) Evaluate(scan Scan) (*Constant, error) {
	if e.val != nil {
		return e.val, nil
	}
	return scan.GetVal(*e.fieldName)
}

func (e *Expression) IsFieldName() bool {
	return e.fieldName != nil
}

func (e *Expression) AsConstant() *Constant {
	return e.val
}

func (e *Expression) AsFieldName() string {
	return *e.fieldName
}

func (e *Expression) AppliesTo(schema *record.Schema) bool {
	if e.val != nil {
		return true
	}
	return schema.HasField(*e.fieldName)
}

func (e *Expression) String() string {
	if e.val != nil {
		return e.val.String()
	}
	return *e.fieldName
}
