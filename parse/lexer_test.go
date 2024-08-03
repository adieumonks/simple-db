package parse_test

import (
	"testing"

	"github.com/adieumonks/simple-db/parse"
	"github.com/stretchr/testify/assert"
)

func TestLexer(t *testing.T) {
	t.Parallel()

	lexer, err := parse.NewLexer("SELECT * from Table_1 where age = 20 and country = 'United States';")
	assert.NoError(t, err)

	{
		assert.True(t, lexer.MatchKeyword("select"))
		err := lexer.EatKeyword("select")
		assert.NoError(t, err)
	}
	{
		assert.True(t, lexer.MatchDelim('*'))
		err := lexer.EatDelim('*')
		assert.NoError(t, err)
	}
	{
		assert.True(t, lexer.MatchKeyword("from"))
		err := lexer.EatKeyword("from")
		assert.NoError(t, err)
	}
	{
		assert.True(t, lexer.MatchIdentifier())
		ident, err := lexer.EatIdentifier()
		assert.NoError(t, err)
		assert.Equal(t, "table_1", ident)
	}
	{
		assert.True(t, lexer.MatchKeyword("where"))
		err := lexer.EatKeyword("where")
		assert.NoError(t, err)
	}
	{
		assert.True(t, lexer.MatchIdentifier())
		ident, err := lexer.EatIdentifier()
		assert.NoError(t, err)
		assert.Equal(t, "age", ident)
	}
	{
		assert.True(t, lexer.MatchDelim('='))
		err := lexer.EatDelim('=')
		assert.NoError(t, err)
	}
	{
		assert.True(t, lexer.MatchIntConstant())
		value, err := lexer.EatIntConstant()
		assert.NoError(t, err)
		assert.Equal(t, int32(20), value)
	}
	{
		assert.True(t, lexer.MatchKeyword("and"))
		err := lexer.EatKeyword("and")
		assert.NoError(t, err)
	}
	{
		assert.True(t, lexer.MatchIdentifier())
		ident, err := lexer.EatIdentifier()
		assert.NoError(t, err)
		assert.Equal(t, "country", ident)
	}
	{
		assert.True(t, lexer.MatchDelim('='))
		err := lexer.EatDelim('=')
		assert.NoError(t, err)
	}
	{
		assert.True(t, lexer.MatchStringConstant())
		value, err := lexer.EatStringConstant()
		assert.NoError(t, err)
		assert.Equal(t, "United States", value)
	}
	{
		assert.True(t, lexer.MatchDelim(';'))
		err := lexer.EatDelim(';')
		assert.NoError(t, err)
	}
	{
		_, err := lexer.EatIdentifier()
		var errBadSyntax *parse.BadSyntaxError
		assert.ErrorAs(t, err, &errBadSyntax)
	}
}
