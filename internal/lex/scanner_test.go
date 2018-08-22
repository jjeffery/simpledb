package lex

import (
	"strings"
	"testing"
)

func TestScan(t *testing.T) {
	type tokenLexeme struct {
		token  Token
		lexeme string
	}
	testCases := []struct {
		sql                    string
		tokens                 []tokenLexeme
		ignoreWhiteSpaceTokens []tokenLexeme
		errText                string
	}{
		{
			sql: "select * from [from] t where t.id = 'one'",
			tokens: []tokenLexeme{
				{TokenKeyword, "select"},
				{TokenWhiteSpace, " "},
				{TokenOperator, "*"},
				{TokenWhiteSpace, " "},
				{TokenKeyword, "from"},
				{TokenWhiteSpace, " "},
				{TokenIdent, "[from]"},
				{TokenWhiteSpace, " "},
				{TokenIdent, "t"},
				{TokenWhiteSpace, " "},
				{TokenKeyword, "where"},
				{TokenWhiteSpace, " "},
				{TokenIdent, "t"},
				{TokenOperator, "."},
				{TokenIdent, "id"},
				{TokenWhiteSpace, " "},
				{TokenOperator, "="},
				{TokenWhiteSpace, " "},
				{TokenLiteral, "'one'"},
				{TokenEOF, ""},
			},
			ignoreWhiteSpaceTokens: []tokenLexeme{
				{TokenKeyword, "select"},
				{TokenOperator, "*"},
				{TokenKeyword, "from"},
				{TokenIdent, "[from]"},
				{TokenIdent, "t"},
				{TokenKeyword, "where"},
				{TokenIdent, "t"},
				{TokenOperator, "."},
				{TokenIdent, "id"},
				{TokenOperator, "="},
				{TokenLiteral, "'one'"},
				{TokenEOF, ""},
			},
		},
		{ // identifiers
			sql: "a _aa BB xyz_abc-d",
			tokens: []tokenLexeme{
				{TokenIdent, "a"},
				{TokenWhiteSpace, " "},
				{TokenIdent, "_aa"},
				{TokenWhiteSpace, " "},
				{TokenIdent, "BB"},
				{TokenWhiteSpace, " "},
				{TokenIdent, "xyz_abc"},
				{TokenOperator, "-"},
				{TokenIdent, "d"},
				{TokenEOF, ""},
			},
		},
		{ // delimited identifiers
			sql: "[table_]]name]]] `column_[]_n``ame`, \"another \"\"name\"\"\"",
			tokens: []tokenLexeme{
				{TokenIdent, "[table_]]name]]]"},
				{TokenWhiteSpace, " "},
				{TokenIdent, "`column_[]_n``ame`"},
				{TokenOperator, ","},
				{TokenWhiteSpace, " "},
				{TokenIdent, "\"another \"\"name\"\"\""},
				{TokenEOF, ""},
			},
		},
		{ // unfinished delimited identifier
			sql: "[table_]]name]]",
			tokens: []tokenLexeme{
				{TokenIllegal, "[table_]]name]]"},
				{TokenEOF, ""},
			},
			errText: `unrecognised input near "[table_]]name]]"`,
		},
		{ // placeholders
			sql: "? ? ? ? ?",
			tokens: []tokenLexeme{
				{TokenPlaceholder, "?"},
				{TokenWhiteSpace, " "},
				{TokenPlaceholder, "?"},
				{TokenWhiteSpace, " "},
				{TokenPlaceholder, "?"},
				{TokenWhiteSpace, " "},
				{TokenPlaceholder, "?"},
				{TokenWhiteSpace, " "},
				{TokenPlaceholder, "?"},
				{TokenEOF, ""},
			},
		},
		{ // comments
			sql: "select -- this is a comment\n5-2-- another comment",
			tokens: []tokenLexeme{
				{TokenKeyword, "select"},
				{TokenWhiteSpace, " "},
				{TokenComment, "-- this is a comment\n"},
				{TokenLiteral, "5"},
				{TokenOperator, "-"},
				{TokenLiteral, "2"},
				{TokenComment, "-- another comment"},
				{TokenEOF, ""},
			},
		},
		{ // literals
			sql: "'literal ''string''',x'1010',X'1010',n'abc',N'abc',xy,X,nm,N,",
			tokens: []tokenLexeme{
				{TokenLiteral, "'literal ''string'''"},
				{TokenOperator, ","},
				{TokenLiteral, "x'1010'"},
				{TokenOperator, ","},
				{TokenLiteral, "X'1010'"},
				{TokenOperator, ","},
				{TokenLiteral, "n'abc'"},
				{TokenOperator, ","},
				{TokenLiteral, "N'abc'"},
				{TokenOperator, ","},
				{TokenIdent, "xy"},
				{TokenOperator, ","},
				{TokenIdent, "X"},
				{TokenOperator, ","},
				{TokenIdent, "nm"},
				{TokenOperator, ","},
				{TokenIdent, "N"},
				{TokenOperator, ","},
				{TokenEOF, ""},
			},
		},
		{ // illegal quoted literal
			sql: "'missing quote",
			tokens: []tokenLexeme{
				{TokenIllegal, "'missing quote"},
				{TokenEOF, ""},
			},
			errText: `unrecognised input near "'missing quote"`,
		},
		{ // numbers
			sql: "123,123.456,.123,5",
			tokens: []tokenLexeme{
				{TokenLiteral, "123"},
				{TokenOperator, ","},
				{TokenLiteral, "123.456"},
				{TokenOperator, ","},
				{TokenLiteral, ".123"},
				{TokenOperator, ","},
				{TokenLiteral, "5"},
				{TokenEOF, ""},
			},
		},
		{ // not-equals, gt, lt operators
			sql: "<<>>",
			tokens: []tokenLexeme{
				{TokenOperator, "<"},
				{TokenOperator, "<>"},
				{TokenOperator, ">"},
				{TokenEOF, ""},
			},
		},
		{ // illegal token
			sql: "\x03",
			tokens: []tokenLexeme{
				{TokenIllegal, "\x03"},
				{TokenEOF, ""},
			},
			errText: `unrecognised input near "\x03"`,
		},
		{ // white space
			sql: " a  b\r\nc\td \v\t\r\n  e\n\n",
			tokens: []tokenLexeme{
				{TokenWhiteSpace, " "},
				{TokenIdent, "a"},
				{TokenWhiteSpace, "  "},
				{TokenIdent, "b"},
				{TokenWhiteSpace, "\r\n"},
				{TokenIdent, "c"},
				{TokenWhiteSpace, "\t"},
				{TokenIdent, "d"},
				{TokenWhiteSpace, " \v\t\r\n  "},
				{TokenIdent, "e"},
				{TokenWhiteSpace, "\n\n"},
				{TokenEOF, ""},
			},
		},
		// placeholder
		{
			sql: "select * from [tbl] t where t.id = ? and t.version = ?",
			tokens: []tokenLexeme{
				{TokenKeyword, "select"},
				{TokenWhiteSpace, " "},
				{TokenOperator, "*"},
				{TokenWhiteSpace, " "},
				{TokenKeyword, "from"},
				{TokenWhiteSpace, " "},
				{TokenIdent, "[tbl]"},
				{TokenWhiteSpace, " "},
				{TokenIdent, "t"},
				{TokenWhiteSpace, " "},
				{TokenKeyword, "where"},
				{TokenWhiteSpace, " "},
				{TokenIdent, "t"},
				{TokenOperator, "."},
				{TokenIdent, "id"},
				{TokenWhiteSpace, " "},
				{TokenOperator, "="},
				{TokenWhiteSpace, " "},
				{TokenPlaceholder, "?"},
				{TokenWhiteSpace, " "},
				{TokenKeyword, "and"},
				{TokenWhiteSpace, " "},
				{TokenIdent, "t"},
				{TokenOperator, "."},
				{TokenIdent, "version"},
				{TokenWhiteSpace, " "},
				{TokenOperator, "="},
				{TokenWhiteSpace, " "},
				{TokenPlaceholder, "?"},
				{TokenEOF, ""},
			},
		},
		// placeholder
		{
			sql: "select {whatever} from [tbl]",
			tokens: []tokenLexeme{
				{TokenKeyword, "select"},
				{TokenWhiteSpace, " "},
				{TokenIdent, "{whatever}"},
				{TokenWhiteSpace, " "},
				{TokenKeyword, "from"},
				{TokenWhiteSpace, " "},
				{TokenIdent, "[tbl]"},
				{TokenEOF, ""},
			},
		},
	}

	check := func(tn int, scan *Scanner, tokens []tokenLexeme, sql string, errText string) {
		if len(tokens) == 0 {
			return
		}
		for i, expected := range tokens {
			if !scan.Scan() {
				if scan.Token() != TokenEOF && scan.Token() != TokenIllegal {
					t.Errorf("%d: %d: premature end of input: tok=%v, lit=%q, sql=%q",
						tn, i, scan.Token(), scan.Text(), sql)
				}
				continue
			}
			tok, lit := scan.Token(), scan.Text()
			if tok != expected.token || lit != expected.lexeme {
				t.Errorf("%d: %d: %q, expected (%v,%s), got (%v,%s)",
					tn, i, sql, expected.token, expected.lexeme, tok, lit)
			}
		}
		if errText == "" {
			if scan.Err() != nil {
				t.Errorf("%d: expected no error, actual=%v", tn, scan.Err())
			}
		} else {
			if scan.Err() == nil {
				t.Errorf("%d: expected error %q, actual=nil", tn, errText)
			} else if scan.Err().Error() != errText {
				t.Errorf("%d: expected error %q, actual=%v", tn, errText, scan.Err())
			}
		}
	}

	for tn, tc := range testCases {
		scanner := New(strings.NewReader(tc.sql))
		check(tn, scanner, tc.tokens, tc.sql, tc.errText)
		scanner = New(strings.NewReader(tc.sql))
		scanner.IgnoreWhiteSpace = true
		check(tn, scanner, tc.ignoreWhiteSpaceTokens, tc.sql, tc.errText)
	}
}
