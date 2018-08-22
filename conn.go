package simpledbsql

import (
	"context"
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/simpledb"
	"github.com/aws/aws-sdk-go/service/simpledb/simpledbiface"
	"github.com/jjeffery/errors"
	"github.com/jjeffery/simpledbsql/internal/parse"
	"golang.org/x/sync/errgroup"
)

// SimpleDB error codes
const (
	// conditionalCheckFailed is the error code returned by the AWS SimpleDB API
	// when an expected condition is not met.
	conditionalCheckFailed = "ConditionalCheckFailed"

	// attributeDoesNotExist is the error code returned by the AWS SimpleDB API
	// when an expected condition specifies a value for an attribute, but the
	// attribute does not exist
	attributeDoesNotExist = "AttributeDoesNotExist"
)

// checks that conn implements the various driver interfaces
var (
	_ driver.Queryer           = (*conn)(nil)
	_ driver.Execer            = (*conn)(nil)
	_ driver.QueryerContext    = (*conn)(nil)
	_ driver.ExecerContext     = (*conn)(nil)
	_ driver.NamedValueChecker = (*conn)(nil)
)

type conn struct {
	SimpleDB simpledbiface.SimpleDBAPI
	Schema   string
	Synonyms map[string]string
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c *conn) Close() error {
	return nil
}

func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	// see https://github.com/golang/go/issues/22980
	// this should be fixed in go1.10, so remove Query method when
	// go1.9 is not supported
	return nil, errors.New("not implemented: use QueryContext instead")
}

func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	// see https://github.com/golang/go/issues/22980
	// this should be fixed in go1.10, so remove Exec method when
	// go1.9 is not supported
	return nil, errors.New("not implemented: use ExecContext instead")
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	q, err := parse.Parse(query)
	if err != nil {
		return nil, err
	}
	if q.Select == nil {
		return nil, errors.New("expect select query for QueryContext")
	}

	if q.Select.Key == nil {
		return c.selectQuery(ctx, q.Select, getArgs(args))
	}

	return c.getAttributes(ctx, q.Select, getArgs(args))
}

func (c *conn) getAttributes(ctx context.Context, q *parse.SelectQuery, args []driver.Value) (driver.Rows, error) {
	itemName, err := q.Key.String(args)
	if err != nil {
		return nil, err
	}
	domainName := c.getDomainName(q.TableName)

	getAttributesInput := simpledb.GetAttributesInput{
		ConsistentRead: aws.Bool(q.ConsistentRead),
		DomainName:     aws.String(domainName),
		ItemName:       aws.String(itemName),
		AttributeNames: make([]*string, 0, len(q.ColumnNames)*2+1),
	}

	for _, columnName := range q.ColumnNames {
		getAttributesInput.AttributeNames = append(getAttributesInput.AttributeNames,
			aws.String(columnName),
			aws.String("sql:"+columnName),
		)
	}
	getAttributesInput.AttributeNames = append(getAttributesInput.AttributeNames, aws.String("sql:id"))

	getAttributesOutput, err := c.SimpleDB.GetAttributesWithContext(ctx, &getAttributesInput)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get item").With(
			"itemName", itemName,
			"table", q.TableName,
			"domain", domainName,
		)
	}
	rows := newGetAttributeRows(q.ColumnNames)
	if len(getAttributesOutput.Attributes) > 0 {
		rows.item = &simpledb.Item{
			Name:       aws.String(itemName),
			Attributes: getAttributesOutput.Attributes,
		}
	}
	return rows, nil
}

func (c *conn) selectQuery(ctx context.Context, q *parse.SelectQuery, args []driver.Value) (driver.Rows, error) {
	selectExpression, err := c.makeSelectExpression(q, args)
	if err != nil {
		return nil, err
	}

	selectInput := &simpledb.SelectInput{
		ConsistentRead:   aws.Bool(q.ConsistentRead),
		SelectExpression: aws.String(selectExpression),
	}

	rows := newRows(ctx, c.SimpleDB, q.ColumnNames, selectInput)
	if err := rows.selectNext(); err != nil {
		return nil, err
	}

	return rows, nil
}

func (c *conn) getDomainName(tableName string) string {
	if dn, ok := c.Synonyms[tableName]; ok {
		return dn
	}
	if c.Schema != "" {
		return c.Schema + "." + tableName
	}
	return tableName
}

func (c *conn) makeSelectExpression(q *parse.SelectQuery, args []driver.Value) (string, error) {
	quoteIdentifier := func(columnName string) string {
		s := strings.Replace(columnName, "`", "``", -1)
		return "`" + s + "`"
	}
	getArg := func(index int) (string, error) {
		if index >= len(args) {
			return "", errors.New("not enough args for select query")
		}
		v := args[index]
		if s, ok := v.(string); ok {
			return s, nil
		}
		vv := reflect.ValueOf(v)
		if vv.Kind() == reflect.String {
			return vv.String(), nil
		}
		return "", errors.New("all args to a select query must be strings")
	}
	columnNames := make([]string, 0, len(q.ColumnNames)*2+1)
	columnNames = append(columnNames, quoteIdentifier("sql:id"))
	for _, columnName := range q.ColumnNames {
		if !parse.IsID(columnName) {
			columnNames = append(columnNames, quoteIdentifier(columnName))
			columnNames = append(columnNames, quoteIdentifier("sql:"+columnName))
		}
	}

	var sb strings.Builder
	sb.WriteString("select ")
	sb.WriteString(strings.Join(columnNames, ", "))
	sb.WriteString(" from ")
	sb.WriteString(quoteIdentifier(c.getDomainName(q.TableName)))
	sb.WriteString(" ")
	var argIndex int
	for _, lexeme := range q.WhereClause {
		switch lexeme {
		case "id", "`id`":
			sb.WriteString("itemName()")
		case "?":
			arg, err := getArg(argIndex)
			if err != nil {
				return "", err
			}
			sb.WriteString(quoteString(arg))
			argIndex++
		default:
			sb.WriteString(lexeme)
		}
	}
	return sb.String(), nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	q, err := parse.Parse(query)
	if err != nil {
		return nil, err
	}
	if q.Select != nil {
		return nil, errors.New("unexpected select query for ExecContext")
	}
	if q.CreateTable != nil {
		return c.createTable(ctx, q.CreateTable)
	}
	if q.DropTable != nil {
		return c.dropTable(ctx, q.DropTable)
	}
	if q.Insert != nil {
		return c.insertRow(ctx, q.Insert, getArgs(args))
	}
	if q.Update != nil {
		return c.updateRow(ctx, q.Update, getArgs(args))
	}
	if q.Delete != nil {
		return c.deleteRow(ctx, q.Delete, getArgs(args))
	}

	return nil, errors.New("unsupported query")
}

func (c *conn) CheckNamedValue(arg *driver.NamedValue) (err error) {
	if arg.Name != "" {
		return errors.New("named args are not implemented")
	}
	arg.Value, err = driver.DefaultParameterConverter.ConvertValue(arg.Value)
	if err != nil {
		return err
	}
	return nil
}

func (c *conn) createTable(ctx context.Context, q *parse.CreateTableQuery) (driver.Result, error) {
	domainName := c.getDomainName(q.TableName)
	input := simpledb.CreateDomainInput{
		DomainName: aws.String(domainName),
	}
	_, err := c.SimpleDB.CreateDomainWithContext(ctx, &input)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create simpledb domain").With(
			"domain", domainName,
			"table", q.TableName,
		)
	}
	return newResult(1), nil
}

func (c *conn) dropTable(ctx context.Context, q *parse.DropTableQuery) (driver.Result, error) {
	domainName := c.getDomainName(q.TableName)
	input := simpledb.DeleteDomainInput{
		DomainName: aws.String(c.getDomainName(domainName)),
	}
	_, err := c.SimpleDB.DeleteDomainWithContext(ctx, &input)
	if err != nil {
		return nil, errors.Wrap(err, "cannot delete simpledb domain").With(
			"domain", domainName,
			"table", q.TableName,
		)
	}
	return newResult(1), nil
}

func (c *conn) deleteRow(ctx context.Context, q *parse.DeleteQuery, args []driver.Value) (driver.Result, error) {
	itemName, err := q.Key.String(args)
	if err != nil {
		return nil, err
	}
	deleteInput := simpledb.DeleteAttributesInput{
		DomainName: aws.String(c.getDomainName(q.TableName)),
		ItemName:   aws.String(itemName),
	}
	_, err = c.SimpleDB.DeleteAttributesWithContext(ctx, &deleteInput)
	if err != nil {
		return nil, errors.Wrap(err, "cannot delete attributes").With(
			"itemName", itemName,
		)
	}
	// TODO(jpj): would have to perform a get first to know if we deleted something
	return newResult(0), nil
}

func (c *conn) insertRow(ctx context.Context, q *parse.InsertQuery, args []driver.Value) (driver.Result, error) {
	putInput, _, err := c.newPutDeleteInputs(ctx, q.TableName, q.Columns, q.Key, args)
	if err != nil {
		return nil, err
	}
	// Add a condition that the item must not already exist.
	// The `sql:id` attribute is added to every item.
	putInput.Expected = &simpledb.UpdateCondition{
		Exists: aws.Bool(false),
		Name:   aws.String("sql:id"),
	}

	_, err = c.SimpleDB.PutAttributesWithContext(ctx, putInput)
	if err != nil {
		if hasCode(err, conditionalCheckFailed) {
			msg := fmt.Sprintf(
				"cannot insert duplicate key table=%q itemName=%q",
				derefString(putInput.DomainName),
				derefString(putInput.ItemName),
			)
			return nil, duplicateKeyError(msg)
		}
		return nil, errors.Wrap(err, "cannot put attributes").With(
			"itemName", derefString(putInput.ItemName),
		)
	}

	return newResult(1), nil
}

func (c *conn) updateRow(ctx context.Context, q *parse.UpdateQuery, args []driver.Value) (driver.Result, error) {
	putInput, deleteInput, err := c.newPutDeleteInputs(ctx, q.TableName, q.Columns, q.Key, args)
	if err != nil {
		return nil, err
	}
	if !q.Upsert {
		// Add a condition that the item must already exist.
		// The `sql:id` attribute is added to every item.
		putInput.Expected = &simpledb.UpdateCondition{
			Exists: aws.Bool(true),
			Name:   aws.String("sql:id"),
			// TODO(jpj): if/when we allow int64 keys, we need to get the key type from the query
			Value: aws.String("string"),
		}
		deleteInput.Expected = putInput.Expected
	}

	// An update may consist of either a put or a delete, or maybe both.
	// the goroutine for put updates putItemExists, and the goroutine for
	// delete updated delItemExists. If either is true, then the item was
	// updated and the rowcount is 1.
	var putItemExists, delItemExists bool

	group, ctx := errgroup.WithContext(ctx)

	if len(putInput.Attributes) > 0 {
		group.Go(func() error {
			var err error
			_, err = c.SimpleDB.PutAttributesWithContext(ctx, putInput)
			if err != nil {
				if hasCode(err, attributeDoesNotExist) {
					// not an error, it just means the item does not exist
					return nil
				}
				return errors.Wrap(err, "cannot put attributes").With(
					"itemName", derefString(putInput.ItemName),
				)
			}

			// item was updated
			putItemExists = true
			return nil
		})
	}
	if len(deleteInput.Attributes) > 0 {
		group.Go(func() error {
			var err error
			_, err = c.SimpleDB.DeleteAttributesWithContext(ctx, deleteInput)
			if err != nil {
				if hasCode(err, attributeDoesNotExist) {
					// not an error, it just means the item does not exist
					return nil
				}
				return errors.Wrap(err, "cannot delete attributes").With(
					"itemName", derefString(deleteInput.ItemName),
				)
			}
			// item was updated
			delItemExists = true
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	var rowCount int
	if putItemExists || delItemExists {
		rowCount = 1
	}
	return newResult(rowCount), nil

}

// newPutDeleteInputs is common to insert and update. It assembles the attributes for the put item
// and delete item requests. Bear in mind that SimpleDB cannot store blanks, so if a column is updated
// to a blank string, it results in the attribute being deleted.
func (c *conn) newPutDeleteInputs(ctx context.Context, tableName string, columns []parse.Column, key parse.Key, args []driver.Value) (putInput *simpledb.PutAttributesInput, deleteInput *simpledb.DeleteAttributesInput, err error) {
	itemName, err := key.String(args)
	if err != nil {
		return nil, nil, err
	}
	putInput = &simpledb.PutAttributesInput{
		DomainName: aws.String(c.getDomainName(tableName)),
		ItemName:   aws.String(itemName),
	}
	deleteInput = &simpledb.DeleteAttributesInput{
		DomainName: aws.String(c.getDomainName(tableName)),
		ItemName:   aws.String(itemName),
	}
	addPut := func(name, value string) {
		putInput.Attributes = append(putInput.Attributes, &simpledb.ReplaceableAttribute{
			Name:    aws.String(name),
			Replace: aws.Bool(true),
			Value:   aws.String(value),
		})
	}
	addType := func(name, value string) {
		putInput.Attributes = append(putInput.Attributes, &simpledb.ReplaceableAttribute{
			Name:    aws.String(typeColumnName(name)),
			Replace: aws.Bool(true),
			Value:   aws.String(value),
		})
	}
	addDelete := func(name string) {
		deleteInput.Attributes = append(deleteInput.Attributes, &simpledb.DeletableAttribute{
			Name: aws.String(name),
		})
	}

	// Every item has this attribute, which is used in the expected update condition,
	// and forms the difference between an insert and an update.
	addPut("sql:id", "string")

	for _, col := range columns {
		v, err := col.GetValue(args)
		if err != nil {
			return nil, nil, err
		}
		if v == nil {
			addType(col.ColumnName, "null")
			addDelete(col.ColumnName)
		} else {
			switch val := v.(type) {
			case string:
				addType(col.ColumnName, "string")
				if val == "" {
					// cannot store an empty string
					addDelete(col.ColumnName)
				} else {
					addPut(col.ColumnName, val)
				}
			case int64:
				addType(col.ColumnName, "int64")
				addPut(col.ColumnName, strconv.FormatInt(val, 10))
			case float64:
				addType(col.ColumnName, "float64")
				addPut(col.ColumnName, strconv.FormatFloat(val, 'g', -1, 64))
			case time.Time:
				addType(col.ColumnName, "time")
				addPut(col.ColumnName, val.Format(time.RFC3339))
			case bool:
				addType(col.ColumnName, "bool")
				addPut(col.ColumnName, strconv.FormatBool(val))
			case []byte:
				addType(col.ColumnName, "binary")
				// TODO(jpj): handle strings longer than 1024
				addPut(col.ColumnName, base64.StdEncoding.EncodeToString(val))
			default:
				// We should only get one of the above types, because the args were
				// converted in the CheckNamedValue method.
				return nil, nil, fmt.Errorf("unexpected arg type: %v", reflect.TypeOf(v))
			}
		}
	}

	return putInput, deleteInput, nil
}

func typeColumnName(columnName string) string {
	// TODO(jpj): this fn probably needs to be in the parse package,
	// because it needs to inject column names into the select statements
	return "sql:" + columnName
}

func quoteString(s string) string {
	s = strings.Replace(s, "'", "''", -1)
	return "'" + s + "'"
}

func getArgs(args []driver.NamedValue) []driver.Value {
	var max int
	for _, arg := range args {
		if arg.Ordinal > max {
			max = arg.Ordinal
		}
	}
	list := make([]driver.Value, max)
	for _, arg := range args {
		list[arg.Ordinal-1] = arg.Value
	}
	return list
}

func hasCode(err error, code string) bool {
	if coder, ok := err.(interface{ Code() string }); ok {
		return code == coder.Code()
	}
	return false
}

func derefString(sp *string) string {
	if sp == nil {
		return ""
	}
	return *sp
}

type duplicateKeyError string

func (e duplicateKeyError) Error() string {
	return string(e)
}

func (e duplicateKeyError) DuplicateKey() bool {
	return true
}
