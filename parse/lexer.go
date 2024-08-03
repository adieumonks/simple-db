package parse

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

type tokenKind int

const (
	tokenKindEOF tokenKind = iota
	tokenKindDelimiter
	tokenKindInteger
	tokenKindString
	tokenKindKeyword
	tokenKindIdentifier
)

const whiteSpaces = " \t\n\r"

var keywords = map[string]struct{}{
	"select":  {},
	"from":    {},
	"where":   {},
	"and":     {},
	"insert":  {},
	"into":    {},
	"values":  {},
	"delete":  {},
	"update":  {},
	"set":     {},
	"create":  {},
	"table":   {},
	"int":     {},
	"varchar": {},
	"view":    {},
	"as":      {},
	"index":   {},
	"on":      {},
}

type token struct {
	kind  tokenKind
	value string
}

type Lexer struct {
	input       string
	token       *token
	whiteSpaces string
	keywords    map[string]struct{}
}

func NewLexer(input string) (*Lexer, error) {
	l := &Lexer{
		input:       input,
		token:       nil,
		whiteSpaces: whiteSpaces,
		keywords:    keywords,
	}

	if err := l.nextToken(); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *Lexer) MatchDelim(d rune) bool {
	return l.token.kind == tokenKindDelimiter && l.token.value == string(d)
}

func (l *Lexer) MatchIntConstant() bool {
	return l.token.kind == tokenKindInteger
}

func (l *Lexer) MatchStringConstant() bool {
	return l.token.kind == tokenKindString
}

func (l *Lexer) MatchKeyword(w string) bool {
	return l.token.kind == tokenKindKeyword && l.token.value == w
}

func (l *Lexer) MatchIdentifier() bool {
	return l.token.kind == tokenKindIdentifier
}

func (l *Lexer) EatDelim(d rune) error {
	if !l.MatchDelim(d) {
		return NewBadSyntaxError(fmt.Sprintf("expected %q, but got %q", d, l.token.value))
	}

	if err := l.nextToken(); err != nil {
		return err
	}

	return nil
}

func (l *Lexer) EatIntConstant() (int32, error) {
	if !l.MatchIntConstant() {
		return 0, NewBadSyntaxError(fmt.Sprintf("expected integer, but got %q", l.token.value))
	}

	value, err := strconv.Atoi(l.token.value)
	if err != nil {
		return 0, err
	}

	if err := l.nextToken(); err != nil {
		return 0, err
	}

	return int32(value), nil
}

func (l *Lexer) EatStringConstant() (string, error) {
	if !l.MatchStringConstant() {
		return "", NewBadSyntaxError(fmt.Sprintf("expected string, but got %q", l.token.value))
	}

	value := l.token.value

	if err := l.nextToken(); err != nil {
		return "", err
	}

	return value, nil
}

func (l *Lexer) EatKeyword(w string) error {
	if !l.MatchKeyword(w) {
		return NewBadSyntaxError(fmt.Sprintf("expected %q, but got %q", w, l.token.value))
	}

	if err := l.nextToken(); err != nil {
		return err
	}

	return nil
}

func (l *Lexer) EatIdentifier() (string, error) {
	if !l.MatchIdentifier() {
		return "", NewBadSyntaxError(fmt.Sprintf("expected identifier, but got %q", l.token.value))
	}

	value := l.token.value

	if err := l.nextToken(); err != nil {
		return "", err
	}

	return value, nil
}

func (l *Lexer) nextToken() error {
	l.input = strings.TrimLeft(l.input, l.whiteSpaces)

	if len(l.input) == 0 {
		l.token = &token{
			kind:  tokenKindEOF,
			value: "",
		}
		return nil
	}

	switch r := l.input[0]; {
	case isDigit(r):
		return l.readInteger()
	case r == '\'':
		return l.readString()
	case isIdentifierStart(r):
		return l.readIdentifier()
	default:
		return l.readDelimiter()
	}
}

func (l *Lexer) readInteger() error {
	pos := 1
	for ; pos < len(l.input) && isDigit(l.input[pos]); pos++ {
	}

	l.token = &token{
		kind:  tokenKindInteger,
		value: l.input[:pos],
	}

	l.input = l.input[pos:]
	return nil
}

func (l *Lexer) readString() error {
	pos := 1
	for ; pos < len(l.input) && l.input[pos] != '\''; pos++ {
	}

	if pos == len(l.input) {
		return NewBadSyntaxError("unterminated string")
	}

	l.token = &token{
		kind:  tokenKindString,
		value: l.input[1:pos],
	}

	l.input = l.input[pos+1:]
	return nil
}

func (l *Lexer) readIdentifier() error {
	pos := 1
	for ; pos < len(l.input) && isIdentifierBody(l.input[pos]); pos++ {
	}

	word := strings.ToLower(l.input[:pos])

	kind := tokenKindIdentifier
	if _, ok := l.keywords[word]; ok {
		kind = tokenKindKeyword
	}

	l.token = &token{
		kind:  kind,
		value: word,
	}

	l.input = l.input[pos:]
	return nil
}

func (l *Lexer) readDelimiter() error {
	_, size := utf8.DecodeRuneInString(l.input)

	l.token = &token{
		kind:  tokenKindDelimiter,
		value: l.input[:size],
	}
	l.input = l.input[size:]
	return nil
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}

func isIdentifierStart(c byte) bool {
	return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || c == '_'
}

func isIdentifierBody(c byte) bool {
	return isIdentifierStart(c) || isDigit(c)
}
