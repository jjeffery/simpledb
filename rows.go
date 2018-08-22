package simpledbsql

import (
	"context"
	"database/sql/driver"
	"encoding/base64"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/simpledb"
	"github.com/aws/aws-sdk-go/service/simpledb/simpledbiface"
	"github.com/jjeffery/errors"
	"github.com/jjeffery/simpledbsql/internal/parse"
)

type columnMap struct {
	columns       []string
	colmap        map[string]int
	itemNameIndex int // index of column corresponding to itemName
}

func (cm *columnMap) setColumns(columns []string) {
	cm.columns = columns
	cm.colmap = make(map[string]int, len(cm.columns))
	for i, col := range columns {
		if parse.IsID(col) {
			cm.itemNameIndex = i
		} else {
			cm.colmap[col] = i
		}
	}
}

func (cm *columnMap) setValues(item *simpledb.Item, values []driver.Value) {
	// everything starts as nil
	for i := range values {
		values[i] = nil
	}

	values[cm.itemNameIndex] = derefString(item.Name)
	colTypes := make(map[string]string, len(item.Attributes))

	// collect the column types first
	for _, attr := range item.Attributes {
		name := derefString(attr.Name)
		if strings.HasPrefix(name, "sql:") {
			value := derefString(attr.Value)
			colTypes[name] = value
			colName := strings.TrimPrefix(name, "sql:")
			if index, ok := cm.colmap[colName]; ok {
				switch value {
				case "string":
					values[index] = ""
				case "int64":
					values[index] = int64(0)
				case "float64":
					values[index] = float64(0)
				case "bool":
					values[index] = false
				case "binary", "null":
					values[index] = nil
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
		if index, ok := cm.colmap[name]; ok {
			switch colType {
			case "string":
				values[index] = value
			case "int64":
				{
					n, _ := strconv.ParseInt(value, 10, 64)
					values[index] = n
				}
			case "float64":
				{
					n, _ := strconv.ParseFloat(value, 64)
					values[index] = n
				}
			case "bool":
				{
					b, _ := strconv.ParseBool(value)
					values[index] = b
				}
			case "time":
				{
					t, _ := time.Parse(time.RFC3339, value)
					values[index] = t
				}
			case "binary":
				{
					// TODO(jpj): handle strings longer than 1024
					data, _ := base64.StdEncoding.DecodeString(value)
					values[index] = data
				}
			}
		}
	}
}

// getAttributeRows implements the sql.Rows interface. It returns at most one row.
type getAttributesRows struct {
	cm   columnMap
	item *simpledb.Item
}

func newGetAttributeRows(columns []string) *getAttributesRows {
	rows := &getAttributesRows{}
	rows.cm.setColumns(columns)
	return rows
}

func (rows *getAttributesRows) Columns() []string {
	return rows.cm.columns
}

func (rows *getAttributesRows) Close() error {
	rows.item = nil
	return nil
}

func (rows *getAttributesRows) Next(dest []driver.Value) error {
	if rows.item == nil {
		return io.EOF
	}
	rows.cm.setValues(rows.item, dest)
	rows.item = nil
	return nil
}

// selectQueryRows implements the sql.Rows interface. It can keep querying the next page of
// results for as long as the calling program wants them. This makes it possible
// for the calling program to initiate queries that return a large number of rows
// without filling up memory.
type selectQueryRows struct {
	cm       columnMap
	ctx      context.Context
	simpledb simpledbiface.SimpleDBAPI
	input    *simpledb.SelectInput
	items    []*simpledb.Item
}

func newRows(ctx context.Context, simpledb simpledbiface.SimpleDBAPI, columns []string, input *simpledb.SelectInput) *selectQueryRows {
	rows := &selectQueryRows{
		ctx:      ctx,
		simpledb: simpledb,
		input:    input,
	}
	rows.cm.setColumns(columns)
	return rows
}

func (rows *selectQueryRows) selectNext() error {
	output, err := rows.simpledb.SelectWithContext(rows.ctx, rows.input)
	if err != nil {
		return err
	}
	rows.input.NextToken = output.NextToken
	rows.items = output.Items
	return nil
}

func (rows *selectQueryRows) Columns() []string {
	return rows.cm.columns
}

func (rows *selectQueryRows) Close() error {
	rows.items = nil
	return nil
}

func (rows *selectQueryRows) Next(dest []driver.Value) error {
	for len(rows.items) == 0 {
		// if input next token is nil, that means there are no more rows
		if rows.input.NextToken == nil {
			return io.EOF
		}
		if err := rows.selectNext(); err != nil {
			return err
		}
	}
	item := rows.items[0]
	rows.items = rows.items[1:]
	rows.cm.setValues(item, dest)
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
