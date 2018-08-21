// Package driver provides an AWS SimpleDB driver for database/sql.
package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/simpledb"
	"github.com/aws/aws-sdk-go/service/simpledb/simpledbiface"
	"github.com/jjeffery/errors"
	"github.com/jjeffery/simpledb/internal/parse"
	"golang.org/x/sync/errgroup"
)

const (
	// conditionalCheckFailed is the error code returned by the AWS SimpleDB API
	// when an expected condition is not met.
	conditionalCheckFailed = "ConditionalCheckFailed"

	// attributeDoesNotExist is the error code returned by the AWS SimpleDB API
	// when an expected condition specifies a value for an attribute, but the
	// attribute does not exist
	attributeDoesNotExist = "AttributeDoesNotExist"
)

func init() {
	sql.Register("simpledb", &Driver{})
}

// Driver implements the driver.Driver interface.
type Driver struct {
	sdb simpledbiface.SimpleDBAPI
}

// Open returns a new connection to the database.
// The name is currently ignored and should be a blank
// string, but in future may include parameters like
// region, profile, consistent-read, etc.
func (d *Driver) Open(name string) (driver.Conn, error) {
	if d.sdb == nil {
		sess, err := session.NewSessionWithOptions(session.Options{
			// this option obtains the region setting from the ~/.aws/config file
			// if it is set
			SharedConfigState: session.SharedConfigEnable,
		})
		if err != nil {
			return nil, err
		}
		d.sdb = simpledb.New(sess)
	}
	c := &conn{
		sdb: d.sdb,
	}
	return c, nil
}

var (
	_ driver.Queryer           = (*conn)(nil)
	_ driver.Execer            = (*conn)(nil)
	_ driver.QueryerContext    = (*conn)(nil)
	_ driver.ExecerContext     = (*conn)(nil)
	_ driver.NamedValueChecker = (*conn)(nil)
)

type connector struct {
	sdb simpledbiface.SimpleDBAPI
}

// NewConnector returns a connector that can be used with the
// sql.OpenDB function. Although sess can be any client.ConfigProvider,
// it will typically be a *session.Session.
func NewConnector(sess client.ConfigProvider) driver.Connector {
	return &connector{
		sdb: simpledb.New(sess),
	}
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return &conn{
		sdb: c.sdb,
	}, nil
}

func (c *connector) Driver() driver.Driver {
	return &Driver{
		sdb: c.sdb,
	}
}

type conn struct {
	sdb simpledbiface.SimpleDBAPI
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

	var selectExpression string
	{
		var sb strings.Builder
		sb.WriteString(q.Select.Segments[0])
		for i, arg := range args {
			s := arg.Value.(string)
			sb.WriteString(quoteString(s))
			sb.WriteString(q.Select.Segments[i+1])
		}
		selectExpression = sb.String()
	}

	selectInput := &simpledb.SelectInput{
		ConsistentRead:   aws.Bool(q.Select.ConsistentRead),
		SelectExpression: aws.String(selectExpression),
	}

	rows := newRows(ctx, c.sdb, q.Select.ColumnNames, selectInput)
	if err := rows.selectNext(); err != nil {
		return nil, err
	}

	return rows, nil
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
	input := simpledb.CreateDomainInput{
		DomainName: aws.String(q.TableName),
	}
	_, err := c.sdb.CreateDomainWithContext(ctx, &input)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create simpledb domain").With(
			"domain", q.TableName,
		)
	}
	return newResult(1), nil
}

func (c *conn) dropTable(ctx context.Context, q *parse.DropTableQuery) (driver.Result, error) {
	input := simpledb.DeleteDomainInput{
		DomainName: aws.String(q.TableName),
	}
	_, err := c.sdb.DeleteDomainWithContext(ctx, &input)
	if err != nil {
		return nil, errors.Wrap(err, "cannot delete simpledb domain").With(
			"domain", q.TableName,
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
		DomainName: aws.String(q.TableName),
		ItemName:   aws.String(itemName),
	}
	_, err = c.sdb.DeleteAttributesWithContext(ctx, &deleteInput)
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

	_, err = c.sdb.PutAttributesWithContext(ctx, putInput)
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
	// Add a condition that the item must already exist.
	// The `sql:id` attribute is added to every item.
	putInput.Expected = &simpledb.UpdateCondition{
		Exists: aws.Bool(true),
		Name:   aws.String("sql:id"),
		// TODO(jpj): if/when we allow int64 keys, we need to get the key type from the query
		Value: aws.String("string"),
	}
	deleteInput.Expected = putInput.Expected

	// An update may consist of either a put or a delete, or maybe both.
	// the goroutine for put updates putItemExists, and the goroutine for
	// delete updated delItemExists. If either is true, then the item was
	// updated and the rowcount is 1.
	var putItemExists, delItemExists bool

	group, ctx := errgroup.WithContext(ctx)

	if len(putInput.Attributes) > 0 {
		group.Go(func() error {
			var err error
			_, err = c.sdb.PutAttributesWithContext(ctx, putInput)
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
			_, err = c.sdb.DeleteAttributesWithContext(ctx, deleteInput)
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
		DomainName: aws.String(tableName),
		ItemName:   aws.String(itemName),
	}
	deleteInput = &simpledb.DeleteAttributesInput{
		DomainName: aws.String(tableName),
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

// rowsT implements the sql.Rows interface. It can keep querying the next page of
// results for as long as the calling program wants them. This makes it possible
// for the calling program to initiate queries that return a large number of rows
// without filling up memory.
type rowsT struct {
	columns       []string
	colmap        map[string]int
	itemNameIndex int // index of column corresponding to itemName
	ctx           context.Context
	simpledb      simpledbiface.SimpleDBAPI
	input         *simpledb.SelectInput
	items         []*simpledb.Item
}

func newRows(ctx context.Context, simpledb simpledbiface.SimpleDBAPI, columns []string, input *simpledb.SelectInput) *rowsT {
	rows := &rowsT{
		columns:  columns,
		colmap:   make(map[string]int),
		ctx:      ctx,
		simpledb: simpledb,
		input:    input,
	}
	for i, col := range columns {
		if parse.IsID(col) {
			rows.itemNameIndex = i
		} else {
			rows.colmap[col] = i
		}
	}
	return rows
}

func (rows *rowsT) selectNext() error {
	output, err := rows.simpledb.SelectWithContext(rows.ctx, rows.input)
	if err != nil {
		return err
	}
	rows.input.NextToken = output.NextToken
	rows.items = output.Items
	return nil
}

func (rows *rowsT) Columns() []string {
	return rows.columns
}

func (rows *rowsT) Close() error {
	rows.items = nil
	return nil
}

func (rows *rowsT) Next(dest []driver.Value) error {
	for len(rows.items) == 0 {
		// if input next token is non-nil, that means there are more rows
		if rows.input.NextToken == nil {
			return io.EOF
		}
		if err := rows.selectNext(); err != nil {
			return err
		}
	}
	item := rows.items[0]
	rows.items = rows.items[1:]

	// everything starts as nil
	for i := range dest {
		dest[i] = nil
	}

	dest[rows.itemNameIndex] = derefString(item.Name)
	colTypes := make(map[string]string, len(item.Attributes))

	// collect the column types first
	for _, attr := range item.Attributes {
		name := derefString(attr.Name)
		if strings.HasPrefix(name, "sql:") {
			value := derefString(attr.Value)
			colTypes[name] = value
			colName := strings.TrimPrefix(name, "sql:")
			if index, ok := rows.colmap[colName]; ok {
				switch value {
				case "string":
					dest[index] = ""
				case "int64":
					dest[index] = int64(0)
				case "float64":
					dest[index] = float64(0)
				case "bool":
					dest[index] = false
				case "binary", "null":
					dest[index] = nil
				}
			}
		}
	}

	for _, attr := range item.Attributes {
		name := derefString(attr.Name)
		value := derefString(attr.Value)
		colType := colTypes[typeColumnName(name)]
		if colType == "" {
			colType = "string"
		}
		if index, ok := rows.colmap[name]; ok {
			switch colType {
			case "string":
				dest[index] = value
			case "int64":
				{
					n, _ := strconv.ParseInt(value, 10, 64)
					dest[index] = n
				}
			case "float64":
				{
					n, _ := strconv.ParseFloat(value, 64)
					dest[index] = n
				}

			case "bool":
				{
					b, _ := strconv.ParseBool(value)
					dest[index] = b
				}
			case "time":
				{
					t, _ := time.Parse(time.RFC3339, value)
					dest[index] = t
				}
			case "binary":
				{
					// TODO(jpj): handle strings longer than 1024
					data, _ := base64.StdEncoding.DecodeString(value)
					dest[index] = data
				}
			}
		}
	}
	return nil
}

type resultT struct {
	rowsAffected int64
}

func newResult(rowCount int) *resultT {
	return &resultT{
		rowsAffected: int64(rowCount),
	}
}

func (r *resultT) LastInsertId() (int64, error) {
	return 0, errors.New("not supported: LastInsertId")
}

func (r *resultT) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

func derefString(sp *string) string {
	if sp == nil {
		return ""
	}
	return *sp
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

type duplicateKeyError string

func (e duplicateKeyError) Error() string {
	return string(e)
}

func (e duplicateKeyError) DuplicateKey() bool {
	return true
}
