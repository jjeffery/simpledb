// Package lex implements a simple lexical scanner
// for SQL statements.
//
// This package was adapted from github.com/jjeffery/sqlr/private/scanner.
package lex

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
	"unicode"
)

// Token is a lexical token for SQL.
type Token int

// Tokens
const (
	TokenIllegal     Token = iota // unexpected character
	TokenEOF                      // End of input
	TokenWhiteSpace               // White space
	TokenComment                  // SQL comment
	TokenIdent                    // identifer, which may be quoted
	TokenKeyword                  // keyword
	TokenLiteral                  // string or numeric literal
	TokenOperator                 // operator
	TokenPlaceholder              // prepared statement placeholder
)

const (
	eof       = rune(0)
	operators = "%&()*+,-./:;<=>?^|{}"
)

var (
	keywords = map[string]bool{
		// list obtained from
		// https://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/QuotingRulesSelect.html
		"or":           true,
		"and":          true,
		"not":          true,
		"from":         true,
		"where":        true,
		"select":       true,
		"like":         true,
		"null":         true,
		"is":           true,
		"order":        true,
		"by":           true,
		"asc":          true,
		"desc":         true,
		"in":           true,
		"between":      true,
		"intersection": true,
		"limit":        true,
		"every":        true,

		// not simpledb keywords, but SQL keywords
		"update": true,
		"upsert": true,
		"insert": true,
		"delete": true,
		"create": true,
		"drop":   true,
		"table":  true,
		"values": true,
		"set":    true,
	}
)

// Scanner is a simple lexical scanner for SQL statements.
type Scanner struct {
	IgnoreWhiteSpace bool

	r        *bufio.Reader
	keywords map[string]bool
	err      error
	token    Token
	lexeme   string
}

// New returns a new scanner that takes its input from r.
func New(r io.Reader) *Scanner {
	return &Scanner{
		r:        bufio.NewReader(r),
		keywords: keywords,
	}
}

func (s *Scanner) isKeyword(lit string) bool {
	return s.keywords[strings.ToLower(lit)]
}

// Token returns the token from the last scan.
func (s *Scanner) Token() Token {
	return s.token
}

// Text returns the token's text from the last scan.
func (s *Scanner) Text() string {
	return s.lexeme
}

// Err returns the first non-EOF error that was
// encountered by the Scanner.
func (s *Scanner) Err() error {
	return s.err
}

// Scan the next SQL token.
func (s *Scanner) Scan() bool {
	ch := s.read()
	for s.IgnoreWhiteSpace && isWhitespace(ch) {
		ch = s.read()
	}
	if ch == eof {
		return s.setToken(TokenEOF, "")
	}
	if isWhitespace(ch) {
		s.unread(ch)
		return s.scanWhitespace()
	}
	if ch == '-' {
		ch2 := s.read()
		if ch2 == '-' {
			return s.scanComment("--")
		}
		s.unread(ch2)
		return s.setToken(TokenOperator, runeToString(ch))
	}
	if ch == '[' {
		return s.scanDelimitedIdentifier('[', ']')
	}
	if ch == '`' {
		return s.scanDelimitedIdentifier('`', '`')
	}
	if ch == '"' {
		return s.scanDelimitedIdentifier('"', '"')
	}
	if ch == '\'' {
		return s.scanQuote(ch)
	}
	if ch == '{' {
		return s.scanDelimitedIdentifier('{', '}')
	}
	if strings.ContainsRune("NnXx", ch) {
		ch2 := s.read()
		if ch2 == '\'' {
			return s.scanQuote(ch, ch2)
		}
		s.unread(ch2)
		return s.scanIdentifier(ch)
	}
	if isStartIdent(ch) {
		return s.scanIdentifier(ch)
	}
	if isDigit(ch) {
		return s.scanNumber(ch)
	}
	if ch == '.' {
		ch2 := s.read()
		s.unread(ch2)
		if isDigit(ch2) {
			return s.scanNumber(ch)
		}
		return s.setToken(TokenOperator, runeToString(ch))
	}
	if ch == '<' {
		ch2 := s.read()
		if ch2 == '>' {
			return s.setToken(TokenOperator, "<>")
		}
		s.unread(ch2)
		return s.setToken(TokenOperator, runeToString(ch))
	}
	if ch == '?' {
		return s.scanPlaceholder(ch)
	}
	if strings.ContainsRune(operators, ch) {
		return s.setToken(TokenOperator, runeToString(ch))
	}

	return s.setToken(TokenIllegal, runeToString(ch))
}

func (s *Scanner) setToken(tok Token, text string) bool {
	s.token = tok
	s.lexeme = text
	if tok == TokenIllegal {
		s.err = fmt.Errorf("unrecognised input near %q", text)
		return false
	}
	return tok != TokenEOF
}

func (s *Scanner) scanWhitespace() bool {
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isWhitespace(ch) {
			s.unread(ch)
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	return s.setToken(TokenWhiteSpace, buf.String())
}

func (s *Scanner) scanComment(prefix string) bool {
	var buf bytes.Buffer
	buf.WriteString(prefix)
	for {
		if ch := s.read(); ch == eof {
			break
		} else {
			buf.WriteRune(ch)
			if ch == '\n' {
				break
			}
		}
	}
	return s.setToken(TokenComment, buf.String())
}

func (s *Scanner) scanDelimitedIdentifier(startCh rune, endCh rune) bool {
	var buf bytes.Buffer
	buf.WriteRune(startCh)
	for {
		ch := s.read()
		if ch == eof {
			return s.setToken(TokenIllegal, buf.String())
		}
		buf.WriteRune(ch)
		if ch == endCh {
			// double endCh is an escape
			ch2 := s.read()
			if ch2 != endCh {
				s.unread(ch2)
				break
			}
			buf.WriteRune(ch2)
		}
	}
	return s.setToken(TokenIdent, buf.String())
}

func (s *Scanner) scanIdentifier(startCh rune) bool {
	var buf bytes.Buffer
	buf.WriteRune(startCh)
	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isIdent(ch) {
			s.unread(ch)
			break
		} else {
			buf.WriteRune(ch)
		}
	}
	lexeme := buf.String()
	if s.isKeyword(lexeme) {
		return s.setToken(TokenKeyword, strings.ToLower(lexeme))
	}

	return s.setToken(TokenIdent, lexeme)
}

func (s *Scanner) scanNumber(startCh rune) bool {
	var buf bytes.Buffer

	// comparison function changes after first period encountered
	var cmp = func(ch rune) bool {
		return isDigit(ch) || ch == '.'
	}

	// add to buffer and change comparison function if period encountered
	var add = func(ch rune) {
		buf.WriteRune(ch)
		if ch == '.' {
			cmp = isDigit
		}
	}

	add(startCh)
	for {
		if ch := s.read(); ch == eof {
			break
		} else if cmp(ch) {
			add(ch)
		} else {
			s.unread(ch)
			break
		}
	}

	return s.setToken(TokenLiteral, buf.String())
}

func (s *Scanner) scanQuote(startChs ...rune) bool {
	var buf bytes.Buffer
	var endCh rune
	for _, ch := range startChs {
		endCh = ch
		buf.WriteRune(ch)
	}
	for {
		ch := s.read()
		if ch == eof {
			return s.setToken(TokenIllegal, buf.String())
		}
		buf.WriteRune(ch)
		if ch == endCh {
			if ch2 := s.read(); ch2 == endCh {
				buf.WriteRune(ch2)
			} else {
				s.unread(ch2)
				break
			}
		}
	}
	return s.setToken(TokenLiteral, buf.String())
}

func (s *Scanner) scanPlaceholder(startCh rune) bool {
	var buf bytes.Buffer
	buf.WriteRune(startCh)
	return s.setToken(TokenPlaceholder, buf.String())
}

func (s *Scanner) read() rune {
	ch, _, err := s.r.ReadRune()
	if err != nil {
		if err != io.EOF {
			s.err = err
		}
		return eof
	}
	return ch
}

func (s *Scanner) unread(ch rune) {
	if ch != eof {
		err := s.r.UnreadRune()
		if err != nil {
			s.err = err
		}
	}
}

func isWhitespace(ch rune) bool {
	return unicode.IsSpace(ch)
}

func isDigit(ch rune) bool {
	return unicode.IsDigit(ch)
}

func isStartIdent(ch rune) bool {
	return ch == '_' || unicode.IsLetter(ch)
}

func isIdent(ch rune) bool {
	return isStartIdent(ch) || unicode.IsDigit(ch)
}

func runeToString(ch rune) string {
	return fmt.Sprintf("%c", ch)
}
