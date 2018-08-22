package simpledbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/simpledb"
	"github.com/jjeffery/simpledbsql/internal/parse"
)

func TestCreateDropTable(t *testing.T) {
	const tableName = "temp_test_table1"
	createQuery := fmt.Sprintf("create table %s", tableName)
	dropQuery := fmt.Sprintf("drop table %s", tableName)

	ctx := context.Background()
	db := newDB(t)

	r, err := db.ExecContext(ctx, createQuery)
	wantNoError(t, err)
	wantRowsAffected(t, r, 1)

	// should be able to create the table twice
	_, err = db.ExecContext(ctx, createQuery)
	wantNoError(t, err)

	r, err = db.ExecContext(ctx, dropQuery)
	wantNoError(t, err)
	wantRowsAffected(t, r, 1)

	// should be able to drop twice
	_, err = db.ExecContext(ctx, dropQuery)
	wantNoError(t, err)

	// create and drop using no context
	_, err = db.Exec(createQuery)
	wantNoError(t, err)
	_, err = db.Exec(dropQuery)
	wantNoError(t, err)
}

func TestCRUD(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	createTestTable(t, db)

	result, err := db.ExecContext(ctx,
		"insert into temp_test_table1(id, a, b) values(?, ?, ?)",
		"ID1",
		"aaa",
		"bbb",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 1)
	_, err = result.LastInsertId()
	wantNotSupported(t, err)
	waitForConsistency(t)

	var a, b, id string
	queries := []struct {
		query string
		arg   string
	}{
		{
			query: "select id, a, b from temp_test_table1 where id = ?",
			arg:   "ID1",
		},
		{
			query: "select id, a, b from temp_test_table1 where a = ?",
			arg:   "aaa",
		},
		{
			query: "select id, a, b from temp_test_table1 where b = ?",
			arg:   "bbb",
		},
	}
	for _, q := range queries {
		err = db.QueryRowContext(ctx, q.query, q.arg).Scan(&id, &a, &b)
		wantNoError(t, err)
		if got, want := a, "aaa"; got != want {
			t.Errorf("got=%v, want=%v", got, want)
		}
		if got, want := b, "bbb"; got != want {
			t.Errorf("got=%v, want=%v", got, want)
		}
		if got, want := id, "ID1"; got != want {
			t.Errorf("got=%v, want=%v", got, want)
		}
	}

	result, err = db.ExecContext(ctx,
		"update temp_test_table1 set a = ?, b = ? where id = ?",
		"aaaa",
		"",
		"ID1",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 1)
	waitForConsistency(t)

	err = db.QueryRowContext(ctx, "select id, a, b from temp_test_table1 where id = 'ID1'").Scan(&id, &a, &b)
	wantNoError(t, err)
	if got, want := a, "aaaa"; got != want {
		t.Errorf("got=%v, want=%v", got, want)
	}
	if got, want := b, ""; got != want {
		t.Errorf("got=%v, want=%v", got, want)
	}
	if got, want := id, "ID1"; got != want {
		t.Errorf("got=%v, want=%v", got, want)
	}

	result, err = db.ExecContext(ctx,
		"update temp_test_table1 set a = ?, b = ? where id = ?",
		"aaaa5",
		nil,
		"ID1",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 1)
	waitForConsistency(t)

	var b2 sql.NullString
	err = db.QueryRowContext(ctx, "select id, a, b from temp_test_table1 where id = 'ID1'").Scan(&id, &a, &b2)
	wantNoError(t, err)
	if got, want := a, "aaaa5"; got != want {
		t.Errorf("got=%v, want=%v", got, want)
	}
	if got, want := b2.Valid, false; got != want {
		t.Errorf("got=%v, want=%v", got, want)
	}
	if got, want := id, "ID1"; got != want {
		t.Errorf("got=%v, want=%v", got, want)
	}

	//_, err = db.ExecContext(ctx, "delete from temp_test_table1 where id = ?", "ID1")
	wantNoError(t, err)
}

func TestTime(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	createTestTable(t, db)

	tm := time.Date(2099, 12, 31, 23, 59, 59, 0, time.UTC)
	_, err := db.ExecContext(ctx, "insert into temp_test_table1(id, tm) values('ID1', ?)", tm)
	wantNoError(t, err)
	waitForConsistency(t)

	var tm2 time.Time
	err = db.QueryRowContext(ctx, "select tm from temp_test_table1 where id = 'ID1'").Scan(&tm2)
	wantNoError(t, err)
	if !tm2.Equal(tm) {
		t.Errorf("got=%v, want=%v", tm2.Format(time.RFC3339), tm.Format(time.RFC3339))
	}
}

func TestInt64(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	createTestTable(t, db)

	i64 := int64(42)
	_, err := db.ExecContext(ctx, "insert into temp_test_table1(id, i64) values('ID1', ?)", i64)
	wantNoError(t, err)
	waitForConsistency(t)

	var i64a int64
	err = db.QueryRowContext(ctx, "select i64 from temp_test_table1 where id = 'ID1'").Scan(&i64a)
	wantNoError(t, err)
	if i64 != i64a {
		t.Errorf("got=%v, want=%v", i64a, i64)
	}
}

func TestFloat64(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	createTestTable(t, db)

	f64 := float64(42)
	_, err := db.ExecContext(ctx, "insert into temp_test_table1(id, f64) values('ID1', ?)", f64)
	wantNoError(t, err)
	waitForConsistency(t)

	var f64a float64
	err = db.QueryRowContext(ctx, "select f64 from temp_test_table1 where id = 'ID1'").Scan(&f64a)
	wantNoError(t, err)
	if f64 != f64a {
		t.Errorf("got=%v, want=%v", f64a, f64)
	}
}

func TestBool(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	createTestTable(t, db)

	b := true
	_, err := db.ExecContext(ctx, "insert into temp_test_table1(id, b) values('ID1', ?)", b)
	wantNoError(t, err)
	waitForConsistency(t)

	var b2 bool
	err = db.QueryRowContext(ctx, "select b from temp_test_table1 where id = 'ID1'").Scan(&b2)
	wantNoError(t, err)
	if b != b2 {
		t.Errorf("got=%v, want=%v", b2, b)
	}
}

func TestBinary(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	createTestTable(t, db)

	bin := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
	_, err := db.ExecContext(ctx, "insert into temp_test_table1(id, b) values('ID1', ?)", bin)
	wantNoError(t, err)
	waitForConsistency(t)

	var bin2 []byte
	err = db.QueryRowContext(ctx, "select b from temp_test_table1 where id = 'ID1'").Scan(&bin2)
	wantNoError(t, err)
	if !reflect.DeepEqual(bin, bin2) {
		t.Errorf("got=%v, want=%v", bin2, bin)
	}
}

func TestDuplicateInsert(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	createTestTable(t, db)

	result, err := db.ExecContext(ctx,
		"insert into temp_test_table1(id, a, b) values(?, ?, ?)",
		"ID1",
		"aaa",
		"bbb",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 1)
	waitForConsistency(t)

	result, err = db.ExecContext(ctx,
		"insert into temp_test_table1(id, a, b) values(?, ?, ?)",
		"ID1",
		"aaa",
		"bbb",
	)
	wantDuplicateKeyError(t, err)
}

func TestUpdateRowCount(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	createTestTable(t, db)

	result, err := db.ExecContext(ctx,
		"insert into temp_test_table1(id, a, b) values(?, ?, ?)",
		"ID1",
		"aaa",
		"bbb",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 1)
	waitForConsistency(t)

	result, err = db.ExecContext(ctx,
		"update temp_test_table1 set a = 'xx' where id = ?",
		"ID1",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 1)

	// this will put attributes only
	result, err = db.ExecContext(ctx,
		"update temp_test_table1 set a = 'xx' where id = ?",
		"ID2",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 0)

	// this will put and delete attributes
	result, err = db.ExecContext(ctx,
		"update temp_test_table1 set a = '' where id = ?",
		"ID2",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 0)

	result, err = db.ExecContext(ctx,
		"upsert temp_test_table1 set a = '' where id = ?",
		"ID2",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 1)
}

func TestConnector(t *testing.T) {
	ctx := context.Background()
	sess := session.New()
	connector := Connector{}
	conn, err := connector.Connect(ctx)
	wantErrorMessageContaining(t, err, "SimpleDB cannot be nil")
	connector.SimpleDB = simpledb.New(sess)
	conn, err = connector.Connect(ctx)
	wantNoError(t, err)
	if conn == nil {
		t.Errorf("got=nil, want=non-nil")
	}
	err = conn.Close()
	wantNoError(t, err)

	drv := connector.Driver()
	if drv == nil {
		t.Errorf("got=nil, want=non-nil")
	}
}

// TestNotImplemented is not very useful, but it prevents our
// code coverage metrics from being artificially lowered.
func TestNotImplemented(t *testing.T) {
	ctx := context.Background()
	sess := session.New()
	connector := Connector{SimpleDB: simpledb.New(sess)}
	conn, err := connector.Connect(ctx)
	wantNoError(t, err)

	_, err = conn.Prepare("")
	wantNotImplemented(t, err)

	_, err = conn.Begin()
	wantNotImplemented(t, err)

	{
		queryer := conn.(driver.Queryer)
		_, err = queryer.Query("", nil)
		wantNotImplemented(t, err)
	}

	{
		execer := conn.(driver.Execer)
		_, err = execer.Exec("", nil)
		wantNotImplemented(t, err)
	}
}

// Various error conditions
func TestErrors(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)

	_, err := db.QueryContext(ctx, "insert into tbl(id,a,b) values('id','a','b')")
	wantErrorMessageStartingWith(t, err, "expect select query")

	_, err = db.ExecContext(ctx, "select id, a, b from table_name")
	wantErrorMessageStartingWith(t, err, "unexpected select query")

	_, err = db.ExecContext(ctx, "select id, a from tbl where id = :name", sql.Named("name", "xxx"))
	wantErrorMessageContaining(t, err, "named args are not implemented")

	_, err = db.QueryContext(ctx, "select a, b from tbl where id = ?")
	wantErrorMessageContaining(t, err, "not enough args supplied")

	_, err = db.QueryContext(ctx, "select a, b from tbl where id = ? and b = 'x'")
	wantErrorMessageContaining(t, err, "not enough args for select query")
}

type aStringType string

func TestMakeSelectExpression(t *testing.T) {
	tests := []struct {
		query   string
		args    []interface{}
		want    string
		wantErr string
	}{
		{
			query: "select id, a from tbl where a > ?",
			args:  []interface{}{"X"},
			want:  "select `sql:id`, `a`, `sql:a` from `tbl` where a > 'X'",
		},
		{
			query: "select a, b, c from tbl where id = ? and d < ?",
			args:  []interface{}{"X", "zz"},
			want: "select `sql:id`, `a`, `sql:a`, `b`, `sql:b`, `c`, `sql:c`" +
				" from `tbl` where itemName() = 'X' and d < 'zz'",
		},
		{
			query: "select id from tbl where a = ?",
			args:  []interface{}{aStringType("X'X")},
			want:  "select `sql:id` from `tbl` where a = 'X''X'",
		},
		{
			query:   "select id from tbl where a = ?",
			args:    nil,
			wantErr: "not enough args for select query",
		},
	}
	for tn, tt := range tests {
		var args []driver.Value
		for _, arg := range tt.args {
			args = append(args, driver.Value(arg))
		}
		q, err := parse.Parse(tt.query)
		wantNoError(t, err)
		c := conn{}
		got, err := c.makeSelectExpression(q.Select, args)
		if tt.wantErr != "" {
			wantErrorMessageContaining(t, err, tt.wantErr)
			continue
		}
		wantNoError(t, err)
		if got != tt.want {
			t.Errorf("%d:\n got=%v\nwant=%v", tn, got, tt.want)
		}
	}
}

func TestDomainName(t *testing.T) {
	tests := []struct {
		c          conn
		tableName  string
		domainName string
	}{
		{
			c:          conn{},
			tableName:  "tbl",
			domainName: "tbl",
		},
		{
			c: conn{
				Schema: "dev",
			},
			tableName:  "tbl",
			domainName: "dev.tbl",
		},
		{
			c: conn{
				Schema: "dev",
				Synonyms: map[string]string{
					"tbl": "abc",
				},
			},
			tableName:  "tbl",
			domainName: "abc",
		},
	}
	for tn, tt := range tests {
		if got, want := tt.c.getDomainName(tt.tableName), tt.domainName; got != want {
			t.Errorf("%d: got=%q want=%q", tn, got, want)
		}
	}
}

func wantNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("want no error, got %v", err)
	}
}

func wantError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("want an error, got nil")
	}
}

func wantErrorMessageStartingWith(t *testing.T, err error, prefix string) {
	t.Helper()
	wantError(t, err)
	if msg := err.Error(); !strings.HasPrefix(msg, prefix) {
		t.Fatalf(`got=%q, want="%s..."`, msg, prefix)
	}
}

func wantErrorMessageContaining(t *testing.T, err error, part string) {
	t.Helper()
	wantError(t, err)
	if msg := err.Error(); !strings.Contains(msg, part) {
		t.Fatalf(`got=%q, want="...%s..."`, msg, part)
	}
}

func wantNotImplemented(t *testing.T, err error) {
	t.Helper()
	wantErrorMessageStartingWith(t, err, "not implemented")
}

func wantNotSupported(t *testing.T, err error) {
	t.Helper()
	wantErrorMessageStartingWith(t, err, "not supported")
}

func wantDuplicateKeyError(t *testing.T, err error) {
	t.Helper()
	wantErrorMessageStartingWith(t, err, "cannot insert duplicate key")
	duplicateKeyer, ok := err.(interface{ DuplicateKey() bool })
	if !ok {
		t.Fatalf("got=%v, want=duplicate key error", err)
	}
	if got, want := duplicateKeyer.DuplicateKey(), true; got != want {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func wantRowsAffected(t *testing.T, result sql.Result, want int64) {
	t.Helper()
	got, err := result.RowsAffected()
	wantNoError(t, err)
	if got != want {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func waitForConsistency(t *testing.T) {
	// wait for simpledb to become consistent
	time.Sleep(500 * time.Millisecond)
}

func newDB(t *testing.T) *sql.DB {
	db, err := sql.Open("simpledb", "")
	if err != nil {
		t.Fatalf("cannot open db: %v", err)
	}
	return db
}

func createTestTable(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	_, err := db.ExecContext(ctx, "create table temp_test_table1")
	wantNoError(t, err)
	rows, err := db.QueryContext(ctx, "select id from temp_test_table1")
	wantNoError(t, err)
	for rows.Next() {
		var id string
		err = rows.Scan(&id)
		wantNoError(t, err)
		_, err = db.ExecContext(ctx, "delete from temp_test_table1 where id = ?", id)
		wantNoError(t, err)
	}
	waitForConsistency(t)
}

func dropTestTable(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	_, err := db.ExecContext(ctx, "drop table temp_test_table1")
	wantNoError(t, err)
}
