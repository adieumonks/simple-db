package parse_test

import (
	"testing"

	"github.com/adieumonks/simple-db/parse"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParserQuery(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		input     string
		wantQuery string
		wantError bool
	}{
		{
			input:     "SELECT sid, SName, age FROM STUDENT",
			wantQuery: "select sid, sname, age from student",
			wantError: false,
		},
		{
			input:     "SELECT sname FROM STUDENT WHERE age = 20",
			wantQuery: "select sname from student where age = 20",
			wantError: false,
		},
		{
			input:     "SELECT sname FROM student WHERE age = 20 AND did = 3",
			wantQuery: "select sname from student where age = 20 and did = 3",
			wantError: false,
		},
		{
			input:     "select sid, sname, did, dname FROM student, dept WHERE sname = 'John'",
			wantQuery: "select sid, sname, did, dname from student, dept where sname = John",
			wantError: false,
		},
		{
			input:     "SELECT * FROM STUDENT",
			wantError: true,
		},
		{
			input:     "SELECT sid, FROM STUDENT",
			wantError: true,
		},
		{
			input:     "SELECT sid STUDENT",
			wantError: true,
		},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			p, err := parse.NewParser(tt.input)
			assert.NoError(t, err)

			query, err := p.Query()

			if tt.wantError {
				var errBadSyntax *parse.BadSyntaxError
				assert.ErrorAs(t, err, &errBadSyntax)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantQuery, query.String())
			}
		})
	}
}

func TestParserUpdateCmd(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		input     string
		wantCmd   parse.UpdateCommand
		wantError bool
	}{
		{
			input: "UPDATE STUDENT SET age = 20 WHERE sid = 1",
			wantCmd: parse.NewModifyData(
				"student",
				"age",
				query.NewExpressionFromConstant(record.NewConstantWithInt(20)),
				query.NewPredicateFromTerm(
					query.NewTerm(
						query.NewExpressionFromField("sid"),
						query.NewExpressionFromConstant(record.NewConstantWithInt(1)),
					),
				),
			),
			wantError: false,
		},
		{
			input: "DELETE FROM STUDENT",
			wantCmd: parse.NewDeleteData(
				"student",
				query.NewPredicate(),
			),
			wantError: false,
		},
		{
			input: "INSERT INTO STUDENT(sid, sname, age) VALUES (1, 'John', 20)",
			wantCmd: parse.NewInsertData(
				"student",
				[]string{"sid", "sname", "age"},
				[]*record.Constant{
					record.NewConstantWithInt(1),
					record.NewConstantWithString("John"),
					record.NewConstantWithInt(20),
				},
			),
			wantError: false,
		},
		{
			input: "CREATE TABLE STUDENT(sid INT, sname VARCHAR(20), age INT)",
			wantCmd: parse.NewCreateTableData(
				"student",
				func() *record.Schema {
					schema := record.NewSchema()
					schema.AddIntField("sid")
					schema.AddStringField("sname", 20)
					schema.AddIntField("age")
					return schema
				}(),
			),
			wantError: false,
		},
		{
			input:     "CREATE TABLE STUDENT(sid INT, sname VARCHAR, age INT)", // VARCHARは長さ指定が必要
			wantError: true,
		},
		{
			input: "CREATE VIEW tmp AS SELECT sname, age FROM STUDENT",
			wantCmd: parse.NewCreateViewData(
				"tmp",
				parse.NewQueryData(
					[]string{"sname", "age"},
					[]string{"student"},
					query.NewPredicate(),
				),
			),
			wantError: false,
		},
		{
			input: "CREATE INDEX student_sname_idx ON STUDENT(sname)",
			wantCmd: parse.NewCreateIndexData(
				"student_sname_idx",
				"student",
				"sname",
			),
			wantError: false,
		},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			p, err := parse.NewParser(tt.input)
			assert.NoError(t, err)

			cmd, err := p.UpdateCommand()

			if tt.wantError {
				var errBadSyntax *parse.BadSyntaxError
				assert.ErrorAs(t, err, &errBadSyntax)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantCmd, cmd)
			}
		})
	}
}
