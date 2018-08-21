// Package parse parses SQL statements for the SimpleDB driver.
package parse

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"

	// TODO(jpj): copy this package and simplify
	"github.com/jjeffery/sqlr/private/scanner"
)

var (
	// TODO(jpj): these keywords are not particularly useful.
	// Better to keep the number small and adjust as the parsing
	// progresses. Still, the code works so leave it for now.
	keywords = []string{
		// list obtained from
		// https://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/QuotingRulesSelect.html
		"or",
		"and",
		"not",
		"from",
		"where",
		"select",
		"like",
		"null",
		"is",
		"order",
		"by",
		"asc",
		"desc",
		"in",
		"between",
		"intersection",
		"limit",
		"every",

		// not recognized by simpledb
		"update",
		"insert",
		"delete",
		"create",
		"drop",
		"consistent",
	}
)

// Query is the representation of a single parsed query.
type Query struct {
	Select      *SelectQuery
	Insert      *InsertQuery
	Update      *UpdateQuery
	Delete      *DeleteQuery
	CreateTable *CreateTableQuery
	DropTable   *DropTableQuery
}

// SelectQuery is the representation of a select query.
type SelectQuery struct {
	ConsistentRead bool
	ColumnNames    []string

	// Segments consists of text that surrounds
	// the placeholders. There is always one more
	// segment than there are placeholders.
	Segments []string
}

// InsertQuery is the representation of an insert query.
type InsertQuery struct {
	TableName string
	Columns   []Column
	Key       Key
}

// UpdateQuery is the representation of an update query.
type UpdateQuery struct {
	TableName string
	Columns   []Column
	Key       Key
}

// DeleteQuery is the representation of a delete query.
type DeleteQuery struct {
	TableName string
	Key       Key
}

// CreateTableQuery is the representation of a create table query.
type CreateTableQuery struct {
	TableName string
}

// DropTableQuery is the representation of a drop table query.
type DropTableQuery struct {
	TableName string
}

// Column represents a column in the query
// and the placeholder or value it is associated with.
type Column struct {
	ColumnName string  // name of associated column
	Ordinal    int     // zero-based placeholder ordinal
	Value      *string // if non-nil, then a literal value
}

// GetValue gets the value for a column, either from the placeholder
// value or the literal value.
func (col *Column) GetValue(values []driver.Value) (driver.Value, error) {
	if col.Value != nil {
		return *col.Value, nil
	}
	if col.Ordinal < 0 || col.Ordinal >= len(values) {
		return nil, fmt.Errorf("internal error: ordinal=%d, value len=%d", col.Ordinal, len(values))
	}
	return values[col.Ordinal], nil
}

// Key represents the primary key of the record
// being inserted/updated/deleted.
type Key struct {
	Ordinal int     // zero-based placeholder ordinal
	Value   *string // if non-nil, then a literal value
}

// String returns the string for the primary key, either from the
// placeholder values or the literal value.
func (key *Key) String(values []driver.Value) (string, error) {
	if key.Value != nil {
		return *key.Value, nil
	}
	if key.Ordinal < 0 || key.Ordinal >= len(values) {
		return "", fmt.Errorf("internal error: key ordinal=%d, value len=%d", key.Ordinal, len(values))
	}
	// TODO(jpj): verify that key.Ordinal can never be out of range
	// for the values slice
	v := values[key.Ordinal]
	if s, ok := v.(string); ok {
		return s, nil
	}
	vv := reflect.ValueOf(v)
	if vv.Kind() == reflect.String {
		return vv.String(), nil
	}

	return "", fmt.Errorf("invalid type for item name: %q", vv.Type())
}

// Parse a query.
func Parse(query string) (*Query, error) {
	var p parser
	return p.parse(query)
}

type parser struct {
	lexer            *scanner.Scanner
	query            Query
	sb               strings.Builder
	placeholderIndex int
}

func (p *parser) next() bool {
	if p.token() == scanner.PLACEHOLDER {
		// keep a track of how many placeholders
		// are behind us, so when the curent token
		// is a placeholder, then placeholderIndex
		// is its index.
		p.placeholderIndex++
	}
	p.lexer.Scan()
	for {
		if p.token() == scanner.COMMENT {
			// ignore all comments
			p.lexer.Scan()
			continue
		}
		if p.token() == scanner.WS {
			// if not ignoring white space then we are copying
			// the query, which happens for select queries --
			// copy a single ws to the string builder
			p.sb.WriteRune(' ')
			p.lexer.Scan()
			continue
		}
		break
	}
	return p.token() != scanner.EOF
}

func (p *parser) token() scanner.Token {
	return p.lexer.Token()
}

func (p *parser) text() string {
	return p.lexer.Text()
}

func (p *parser) copyText() {
	p.sb.WriteString(p.text())
}

func (p *parser) expect(toks ...scanner.Token) {
	current := p.token()
	for _, tok := range toks {
		if current == tok {
			return
		}
	}
	p.errorf("unexpected %q", p.text())
}

func (p *parser) expectText(text string) {
	if !strings.EqualFold(p.text(), text) {
		p.errorf("expected %q, found %q", text, p.text())
	}
}

func (p *parser) expectEOF() {
	if p.token() != scanner.EOF {
		p.errorf("expected end of query, found %q", p.text())
	}
}

func (p *parser) errorf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	panic(msg)
}

func (p *parser) parse(query string) (q *Query, err error) {
	reader := strings.NewReader(query)
	p.lexer = scanner.New(reader)
	p.lexer.AddKeywords(keywords...)
	p.lexer.IgnoreWhiteSpace = true

	defer func() {
		if e := recover(); e != nil {
			msg, ok := e.(string)
			if !ok {
				panic(e)
			}
			err = errors.New(msg)
		}
	}()

	p.next()
	tok, text := p.token(), p.text()
	if tok != scanner.KEYWORD {
		p.errorf("unrecognized query %q", text)
	}
	switch strings.ToLower(text) {
	case "select", "consistent":
		p.parseSelect()
	case "update":
		p.parseUpdate()
	case "insert":
		p.parseInsert()
	case "delete":
		p.parseDelete()
	case "create":
		p.parseCreateTable()
	case "drop":
		p.parseDropTable()
	default:
		p.errorf("unexpected keyword %q", text)
	}

	return &p.query, nil
}

func (p *parser) parseSelect() {
	p.query.Select = &SelectQuery{}
	if p.text() == "consistent" {
		p.query.Select.ConsistentRead = true
		p.next()
		p.expectText("select")
	}
	p.copyText()
	p.next()
	p.parseSelectColumnList()
	p.parseSegments()
}

// IsID returns true if name corresponds to the special
// name of the item name column ("id").
func IsID(name string) bool {
	name = scanner.Unquote(name)
	return strings.EqualFold(name, "id")
}

func (p *parser) parseSelectColumnList() {
	var columnNames []string

	expectIdent := func() {
		p.expect(scanner.IDENT)
		name := scanner.Unquote(p.text())
		p.query.Select.ColumnNames = append(p.query.Select.ColumnNames, name)
		if !IsID(name) {
			// don't copy the special id column to the sql output,
			// simpledb always incluldes the item name
			columnNames = append(columnNames, p.text())
			typeName := "sql:" + scanner.Unquote(p.text()) // TODO(jpj): need to have common fn for this
			columnNames = append(columnNames, scanner.Quote(typeName, "`", "`"))
		}
		p.next()
	}
	expectIdent()
	for p.text() == "," {
		p.next()
		expectIdent()
	}
	p.sb.WriteRune(' ')
	if len(columnNames) > 0 {
		p.sb.WriteString(strings.Join(columnNames, ", "))
	} else {
		// this happens for 'select id from ...', so there are no columns
		p.sb.WriteString("itemName()")
	}
	p.sb.WriteRune(' ')
}

func (p *parser) parseSegments() {
	// need white space when copying segments
	p.lexer.IgnoreWhiteSpace = false
	for p.token() != scanner.EOF {
		if p.token() == scanner.PLACEHOLDER {
			segment := p.sb.String()
			p.sb = strings.Builder{}
			p.query.Select.Segments = append(p.query.Select.Segments, segment)
		} else {
			if p.token() == scanner.IDENT && IsID(p.text()) {
				p.sb.WriteString("itemName()")
			} else {
				p.copyText()
			}
		}
		p.next()
	}
	p.query.Select.Segments = append(p.query.Select.Segments, p.sb.String())
	p.sb = strings.Builder{}
}

func (p *parser) parseUpdate() {
	p.query.Update = &UpdateQuery{}
	p.next()
	p.expect(scanner.IDENT)
	p.query.Update.TableName = scanner.Unquote(p.text())
	p.next()
	p.expectText("set")
	p.next()
	p.parseUpdateColumns()
	p.parseUpdateWhere()
	p.expectEOF()
}

func (p *parser) parseUpdateColumns() {
	p.parseUpdateColumn()
	for p.text() == "," {
		p.next()
		p.parseUpdateColumn()
	}
}

func (p *parser) parseUpdateColumn() {
	p.expect(scanner.IDENT)
	col := Column{
		ColumnName: scanner.Unquote(p.text()),
	}
	p.next()
	p.expectText("=")
	p.next()
	p.expect(scanner.PLACEHOLDER, scanner.LITERAL)
	if p.token() == scanner.PLACEHOLDER {
		col.Ordinal = p.placeholderIndex
	} else {
		value := scanner.Unquote(p.text())
		col.Value = &value
	}
	p.query.Update.Columns = append(p.query.Update.Columns, col)
	p.next()
}

func (p *parser) parseUpdateWhere() {
	p.expectText("where")
	p.next()
	p.expectText("id")
	p.next()
	p.expectText("=")
	p.next()
	p.expect(scanner.PLACEHOLDER, scanner.LITERAL)
	if p.token() == scanner.PLACEHOLDER {
		p.query.Update.Key = Key{
			Ordinal: p.placeholderIndex,
		}
	} else {
		value := scanner.Unquote(p.text())
		p.query.Update.Key = Key{
			Value: &value,
		}
	}
	p.next()
}

func (p *parser) parseInsert() {
	p.query.Insert = &InsertQuery{}
	p.next()
	if strings.EqualFold(p.text(), "into") {
		p.next()
	}
	p.expect(scanner.IDENT)
	p.query.Insert.TableName = scanner.Unquote(p.text())
	p.next()
	p.expectText("(")
	p.next()
	p.parseInsertColumnList()
	p.expectText(")")
	p.next()
	p.expectText("values")
	p.next()
	p.expectText("(")
	p.next()
	p.parseInsertValueList()
	p.expectText(")")
	p.next()
	p.expectEOF()
}

func (p *parser) parseInsertColumnList() {
	var columns []Column
	expectIdent := func() {
		p.expect(scanner.IDENT)
		col := Column{
			ColumnName: scanner.Unquote(p.text()),
		}
		columns = append(columns, col)
		p.next()
	}
	expectIdent()
	for p.text() == "," {
		p.next()
		expectIdent()
	}
	// the id column will be removed
	// from this list once the value list
	// has been parsed
	p.query.Insert.Columns = columns
}

func (p *parser) parseInsertValueList() {
	// we know how any items in the list we
	// are expecting -- it has to match the
	// column list
	for i := range p.query.Insert.Columns {
		if i > 0 {
			p.expectText(",")
			p.next()
		}
		col := &p.query.Insert.Columns[i]
		p.expect(scanner.PLACEHOLDER, scanner.LITERAL)
		if p.token() == scanner.PLACEHOLDER {
			col.Ordinal = p.placeholderIndex
		} else {
			value := scanner.Unquote(p.text())
			col.Value = &value
		}
		p.next()
	}

	// strip out the id column in the insert statement
	// and put it in the key field
	var haveKey bool
	columns := make([]Column, 0, len(p.query.Insert.Columns))
	for _, col := range p.query.Insert.Columns {
		if IsID(col.ColumnName) {
			if haveKey {
				p.errorf("duplicate id column in insert statement")
			}
			p.query.Insert.Key = Key{
				Ordinal: col.Ordinal,
				Value:   col.Value,
			}
			haveKey = true
		} else {
			columns = append(columns, col)
		}
	}
	if !haveKey {
		p.errorf("missing id column in insert statement")
	}
	p.query.Insert.Columns = columns
}

func (p *parser) parseDelete() {
	p.query.Delete = &DeleteQuery{}
	p.next()
	if strings.ToLower(p.text()) == "from" {
		p.next()
	}
	p.expect(scanner.IDENT)
	p.query.Delete.TableName = scanner.Unquote(p.text())
	p.next()
	p.parseDeleteWhere()
	p.expectEOF()
}

func (p *parser) parseDeleteWhere() {
	p.expectText("where")
	p.next()
	p.expectText("id")
	p.next()
	p.expectText("=")
	p.next()
	p.expect(scanner.PLACEHOLDER, scanner.LITERAL)
	if p.token() == scanner.PLACEHOLDER {
		p.query.Delete.Key = Key{
			Ordinal: p.placeholderIndex,
		}
	} else {
		value := scanner.Unquote(p.text())
		p.query.Delete.Key = Key{
			Value: &value,
		}
	}
	p.next()
}

func (p *parser) parseCreateTable() {
	p.query.CreateTable = &CreateTableQuery{}
	p.next()
	p.expectText("table")
	p.next()
	p.expect(scanner.IDENT)
	p.query.CreateTable.TableName = scanner.Unquote(p.text())
	p.next()
	p.expectEOF()
}

func (p *parser) parseDropTable() {
	p.query.DropTable = &DropTableQuery{}
	p.next()
	p.expectText("table")
	p.next()
	p.expect(scanner.IDENT)
	p.query.DropTable.TableName = scanner.Unquote(p.text())
	p.next()
	p.expectEOF()
}
