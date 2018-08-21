package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	_ "github.com/jjeffery/simpledb/driver"
)

func TestCreateDropTable(t *testing.T) {
	// something unlikely to conflict
	const tableName = "temp_test_table"
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

func newDB(t *testing.T) *sql.DB {
	db, err := sql.Open("simpledb", "")
	if err != nil {
		t.Fatalf("cannot open db: %v", err)
	}
	return db
}

func TestCRUD(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	createTestTable(t, db)

	result, err := db.ExecContext(ctx,
		"insert into temp_test_table(id, a, b) values(?, ?, ?)",
		"ID1",
		"aaa",
		"bbb",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 1)
	waitForConsistency(t)

	var a, b, id string
	queries := []struct {
		query string
		arg   string
	}{
		{
			query: "select id, a, b from temp_test_table where id = ?",
			arg:   "ID1",
		},
		{
			query: "select id, a, b from temp_test_table where a = ?",
			arg:   "aaa",
		},
		{
			query: "select id, a, b from temp_test_table where b = ?",
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
		"update temp_test_table set a = ?, b = ? where id = ?",
		"aaaa",
		"",
		"ID1",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 1)
	waitForConsistency(t)

	err = db.QueryRowContext(ctx, "select id, a, b from temp_test_table where id = 'ID1'").Scan(&id, &a, &b)
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
		"update temp_test_table set a = ?, b = ? where id = ?",
		"aaaa5",
		nil,
		"ID1",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 1)
	waitForConsistency(t)

	var b2 sql.NullString
	err = db.QueryRowContext(ctx, "select id, a, b from temp_test_table where id = 'ID1'").Scan(&id, &a, &b2)
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

	//_, err = db.ExecContext(ctx, "delete from temp_test_table where id = ?", "ID1")
	wantNoError(t, err)
}

func TestTime(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	createTestTable(t, db)

	tm := time.Date(2099, 12, 31, 23, 59, 59, 0, time.UTC)
	_, err := db.ExecContext(ctx, "insert into temp_test_table(id, tm) values('ID1', ?)", tm)
	wantNoError(t, err)
	waitForConsistency(t)

	var tm2 time.Time
	err = db.QueryRowContext(ctx, "select tm from temp_test_table where id = 'ID1'").Scan(&tm2)
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
	_, err := db.ExecContext(ctx, "insert into temp_test_table(id, i64) values('ID1', ?)", i64)
	wantNoError(t, err)
	waitForConsistency(t)

	var i64a int64
	err = db.QueryRowContext(ctx, "select i64 from temp_test_table where id = 'ID1'").Scan(&i64a)
	wantNoError(t, err)
	if i64 != i64a {
		t.Errorf("got=%v, want=%v", i64a, i64)
	}
}

func TestBool(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	createTestTable(t, db)

	b := true
	_, err := db.ExecContext(ctx, "insert into temp_test_table(id, b) values('ID1', ?)", b)
	wantNoError(t, err)
	waitForConsistency(t)

	var b2 bool
	err = db.QueryRowContext(ctx, "select b from temp_test_table where id = 'ID1'").Scan(&b2)
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
	_, err := db.ExecContext(ctx, "insert into temp_test_table(id, b) values('ID1', ?)", bin)
	wantNoError(t, err)
	waitForConsistency(t)

	var bin2 []byte
	err = db.QueryRowContext(ctx, "select b from temp_test_table where id = 'ID1'").Scan(&bin2)
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
		"insert into temp_test_table(id, a, b) values(?, ?, ?)",
		"ID1",
		"aaa",
		"bbb",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 1)
	waitForConsistency(t)

	result, err = db.ExecContext(ctx,
		"insert into temp_test_table(id, a, b) values(?, ?, ?)",
		"ID1",
		"aaa",
		"bbb",
	)
	wantError(t, err)
	if result != nil {
		t.Errorf("got=%v, want=nil", result)
	}
	if duplicateKeyer, ok := err.(interface{ DuplicateKey() bool }); ok {
		if got, want := duplicateKeyer.DuplicateKey(), true; got != want {
			t.Errorf("got=%v, want=%v", got, want)
		}
	} else {
		t.Errorf("got=%v, want=duplicate key error", err)
	}
}

func TestUpdateRowCount(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	createTestTable(t, db)

	result, err := db.ExecContext(ctx,
		"insert into temp_test_table(id, a, b) values(?, ?, ?)",
		"ID1",
		"aaa",
		"bbb",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 1)
	waitForConsistency(t)

	result, err = db.ExecContext(ctx,
		"update temp_test_table set a = 'xx' where id = ?",
		"ID1",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 1)

	// this will put attributes only
	result, err = db.ExecContext(ctx,
		"update temp_test_table set a = 'xx' where id = ?",
		"ID2",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 0)

	// this will put and delete attributes
	result, err = db.ExecContext(ctx,
		"update temp_test_table set a = '' where id = ?",
		"ID2",
	)
	wantNoError(t, err)
	wantRowsAffected(t, result, 0)
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

func createTestTable(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	_, err := db.ExecContext(ctx, "create table temp_test_table")
	wantNoError(t, err)
	rows, err := db.QueryContext(ctx, "select id from temp_test_table")
	wantNoError(t, err)
	for rows.Next() {
		var id string
		err = rows.Scan(&id)
		wantNoError(t, err)
		_, err = db.ExecContext(ctx, "delete from temp_test_table where id = ?", id)
		wantNoError(t, err)
	}
	waitForConsistency(t)
}

func dropTestTable(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	_, err := db.ExecContext(ctx, "drop table temp_test_table")
	wantNoError(t, err)
}
