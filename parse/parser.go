package parse

import (
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
)

type Parser struct {
	lex *Lexer
}

func NewParser(input string) (*Parser, error) {
	lex, err := NewLexer(input)
	if err != nil {
		return nil, err
	}

	return &Parser{lex: lex}, nil
}

func (p *Parser) Field() (string, error) {
	return p.lex.EatIdentifier()
}

func (p *Parser) Constant() (*query.Constant, error) {
	if p.lex.MatchStringConstant() {
		value, err := p.lex.EatStringConstant()
		if err != nil {
			return nil, err
		}
		return query.NewConstantWithString(value), nil
	} else {
		value, err := p.lex.EatIntConstant()
		if err != nil {
			return nil, err
		}
		return query.NewConstantWithInt(value), nil
	}
}

func (p *Parser) Expression() (*query.Expression, error) {
	if p.lex.MatchIdentifier() {
		field, err := p.lex.EatIdentifier()
		if err != nil {
			return nil, err
		}
		return query.NewExpressionFromField(field), nil
	} else {
		constant, err := p.Constant()
		if err != nil {
			return nil, err
		}
		return query.NewExpressionFromConstant(constant), nil
	}
}

func (p *Parser) Term() (*query.Term, error) {
	lhs, err := p.Expression()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim('='); err != nil {
		return nil, err
	}
	rhs, err := p.Expression()
	if err != nil {
		return nil, err
	}
	return query.NewTerm(lhs, rhs), nil
}

func (p *Parser) Predicate() (*query.Predicate, error) {
	term, err := p.Term()
	if err != nil {
		return nil, err
	}

	pred := query.NewPredicateFromTerm(term)

	if p.lex.MatchKeyword("and") {
		if err := p.lex.EatKeyword("and"); err != nil {
			return nil, err
		}
		rhs, err := p.Predicate()
		if err != nil {
			return nil, err
		}

		pred.ConjoinWith(rhs)
	}

	return pred, nil
}

func (p *Parser) Query() (*QueryData, error) {
	if err := p.lex.EatKeyword("select"); err != nil {
		return nil, err
	}

	fields, err := p.selectList()
	if err != nil {
		return nil, err
	}

	if err := p.lex.EatKeyword("from"); err != nil {
		return nil, err
	}

	tables, err := p.tableList()
	if err != nil {
		return nil, err
	}

	pred := query.NewPredicate()
	if p.lex.MatchKeyword("where") {
		if err := p.lex.EatKeyword("where"); err != nil {
			return nil, err
		}

		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}

	return NewQueryData(fields, tables, pred), nil
}

func (p *Parser) selectList() ([]string, error) {
	fields := []string{}

	field, err := p.Field()
	if err != nil {
		return nil, err
	}

	fields = append(fields, field)

	if p.lex.MatchDelim(',') {
		if err := p.lex.EatDelim(','); err != nil {
			return nil, err
		}

		rest, err := p.selectList()
		if err != nil {
			return nil, err
		}

		fields = append(fields, rest...)
	}

	return fields, nil
}

func (p *Parser) tableList() ([]string, error) {
	tables := []string{}

	table, err := p.Field()
	if err != nil {
		return nil, err
	}

	tables = append(tables, table)

	if p.lex.MatchDelim(',') {
		if err := p.lex.EatDelim(','); err != nil {
			return nil, err
		}

		rest, err := p.tableList()
		if err != nil {
			return nil, err
		}

		tables = append(tables, rest...)
	}
	return tables, nil
}

func (p *Parser) UpdateCommand() (UpdateCommand, error) {
	if p.lex.MatchKeyword("insert") {
		return p.Insert()
	} else if p.lex.MatchKeyword("delete") {
		return p.Delete()
	} else if p.lex.MatchKeyword("update") {
		return p.Modify()
	} else {
		return p.Create()
	}
}

func (p *Parser) Create() (UpdateCommand, error) {
	if err := p.lex.EatKeyword("create"); err != nil {
		return nil, err
	}

	if p.lex.MatchKeyword("table") {
		return p.CreateTable()
	} else if p.lex.MatchKeyword("view") {
		return p.CreateView()
	} else {
		return p.CreateIndex()
	}
}

func (p *Parser) Delete() (*DeleteData, error) {
	if err := p.lex.EatKeyword("delete"); err != nil {
		return nil, err
	}

	if err := p.lex.EatKeyword("from"); err != nil {
		return nil, err
	}

	table, err := p.Field()
	if err != nil {
		return nil, err
	}

	pred := query.NewPredicate()
	if p.lex.MatchKeyword("where") {
		if err := p.lex.EatKeyword("where"); err != nil {
			return nil, err
		}

		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}

	return NewDeleteData(table, pred), nil
}

func (p *Parser) Insert() (*InsertData, error) {
	if err := p.lex.EatKeyword("insert"); err != nil {
		return nil, err
	}

	if err := p.lex.EatKeyword("into"); err != nil {
		return nil, err
	}

	table, err := p.Field()
	if err != nil {
		return nil, err
	}

	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}

	fields, err := p.fieldList()
	if err != nil {
		return nil, err
	}

	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}

	if err := p.lex.EatKeyword("values"); err != nil {
		return nil, err
	}

	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}

	values, err := p.constList()
	if err != nil {
		return nil, err
	}

	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}

	return NewInsertData(table, fields, values), nil
}

func (p *Parser) fieldList() ([]string, error) {
	fields := []string{}

	field, err := p.Field()
	if err != nil {
		return nil, err
	}

	fields = append(fields, field)

	if p.lex.MatchDelim(',') {
		if err := p.lex.EatDelim(','); err != nil {
			return nil, err
		}

		rest, err := p.fieldList()
		if err != nil {
			return nil, err
		}

		fields = append(fields, rest...)
	}

	return fields, nil
}

func (p *Parser) constList() ([]*query.Constant, error) {
	values := []*query.Constant{}

	value, err := p.Constant()
	if err != nil {
		return nil, err
	}

	values = append(values, value)

	if p.lex.MatchDelim(',') {
		if err := p.lex.EatDelim(','); err != nil {
			return nil, err
		}

		rest, err := p.constList()
		if err != nil {
			return nil, err
		}

		values = append(values, rest...)
	}

	return values, nil
}

func (p *Parser) Modify() (*ModifyData, error) {
	if err := p.lex.EatKeyword("update"); err != nil {
		return nil, err
	}

	table, err := p.Field()
	if err != nil {
		return nil, err
	}

	if err := p.lex.EatKeyword("set"); err != nil {
		return nil, err
	}

	field, err := p.Field()
	if err != nil {
		return nil, err
	}

	if err := p.lex.EatDelim('='); err != nil {
		return nil, err
	}

	newValue, err := p.Expression()
	if err != nil {
		return nil, err
	}

	pred := query.NewPredicate()
	if p.lex.MatchKeyword("where") {
		if err := p.lex.EatKeyword("where"); err != nil {
			return nil, err
		}

		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}

	return NewModifyData(table, field, newValue, pred), nil
}

func (p *Parser) CreateTable() (*CreateTableData, error) {
	if err := p.lex.EatKeyword("table"); err != nil {
		return nil, err
	}

	table, err := p.Field()
	if err != nil {
		return nil, err
	}

	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}

	schema, err := p.fieldDefs()
	if err != nil {
		return nil, err
	}

	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}

	return NewCreateTableData(table, schema), nil
}

func (p *Parser) fieldDefs() (*record.Schema, error) {
	schema, err := p.fieldDef()
	if err != nil {
		return nil, err
	}

	if p.lex.MatchDelim(',') {
		if err := p.lex.EatDelim(','); err != nil {
			return nil, err
		}

		rest, err := p.fieldDefs()
		if err != nil {
			return nil, err
		}

		schema.AddAll(rest)
	}

	return schema, nil
}

func (p *Parser) fieldDef() (*record.Schema, error) {
	field, err := p.Field()
	if err != nil {
		return nil, err
	}

	schema, err := p.fieldType(field)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

func (p *Parser) fieldType(field string) (*record.Schema, error) {
	schema := record.NewSchema()

	if p.lex.MatchKeyword("int") {
		if err := p.lex.EatKeyword("int"); err != nil {
			return nil, err
		}
		schema.AddIntField(field)
	} else {
		if err := p.lex.EatKeyword("varchar"); err != nil {
			return nil, err
		}

		if err := p.lex.EatDelim('('); err != nil {
			return nil, err
		}

		length, err := p.lex.EatIntConstant()
		if err != nil {
			return nil, err
		}

		if err := p.lex.EatDelim(')'); err != nil {
			return nil, err
		}

		schema.AddStringField(field, length)
	}

	return schema, nil
}

func (p *Parser) CreateView() (*CreateViewData, error) {
	if err := p.lex.EatKeyword("view"); err != nil {
		return nil, err
	}

	view, err := p.Field()
	if err != nil {
		return nil, err
	}

	if err := p.lex.EatKeyword("as"); err != nil {
		return nil, err
	}

	query, err := p.Query()
	if err != nil {
		return nil, err
	}

	return NewCreateViewData(view, query), nil
}

func (p *Parser) CreateIndex() (*CreateIndexData, error) {
	if err := p.lex.EatKeyword("index"); err != nil {
		return nil, err
	}

	index, err := p.Field()
	if err != nil {
		return nil, err
	}

	if err := p.lex.EatKeyword("on"); err != nil {
		return nil, err
	}

	table, err := p.Field()
	if err != nil {
		return nil, err
	}

	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}

	field, err := p.Field()
	if err != nil {
		return nil, err
	}

	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}

	return NewCreateIndexData(index, table, field), nil
}
