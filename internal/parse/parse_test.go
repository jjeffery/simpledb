package parse

import (
	"database/sql/driver"
	"reflect"
	"testing"
)

func TestParseSelect(t *testing.T) {
	tests := []struct {
		query       string
		columnNames []string
		tableName   string
		whereClause []string
		consistent  bool
		key         *Key
	}{
		{
			query:       "select a, b, c from tbl where id = ?",
			columnNames: []string{"a", "b", "c"},
			tableName:   "tbl",
			key:         &Key{},
		},
		{
			query:       "select a, b, c from tbl where id = '11'",
			columnNames: []string{"a", "b", "c"},
			tableName:   "tbl",
			key: &Key{
				Value: stringPtr("11"),
			},
		},
		{
			query:       "select a, b, c from tbl limit 10",
			columnNames: []string{"a", "b", "c"},
			tableName:   "tbl",
			whereClause: []string{
				"limit", " ", "10",
			},
		},
		{
			query:       "select a, b, c from tbl where id > '1000'",
			columnNames: []string{"a", "b", "c"},
			tableName:   "tbl",
			whereClause: []string{
				"where", " ", "id", " ", ">", " ", "'1000'",
			},
		},
		{
			// simpledb won't run it, but it parses correctly
			query:       "select a, b, c from tbl where id = a",
			columnNames: []string{"a", "b", "c"},
			tableName:   "tbl",
			whereClause: []string{
				"where", " ", "id", " ", "=", " ", "a",
			},
		},
		{
			query:       "select a, b, c from tbl where id = ? order by id",
			columnNames: []string{"a", "b", "c"},
			tableName:   "tbl",
			whereClause: []string{
				"where", " ", "id", " ", "=", " ", "?",
				" ", "order", " ", "by", " ", "id",
			},
		},
		{
			query:       "select `a`, `b`, `c` from `tbl` where id = ? and c in (?, ?, ?)",
			columnNames: []string{"a", "b", "c"},
			tableName:   "tbl",
			whereClause: []string{
				"where", " ", "id", " ", "=", " ", "?", " ", "and", " ", "c",
				" ", "in", " ", "(", "?", ",", " ", "?", ",", " ", "?", ")",
			},
		},
		{
			query:       "select `a`, `b`, `c` from `tbl` where id = ? and c in (?, ?, ?)",
			columnNames: []string{"a", "b", "c"},
			tableName:   "tbl",
			whereClause: []string{
				"where", " ", "id", " ", "=", " ", "?", " ", "and", " ", "c", " ", "in", " ",
				"(", "?", ",", " ", "?", ",", " ", "?", ")",
			},
		},
		{
			query:       "consistent select `id` from `tbl` where d in (?)",
			columnNames: []string{"id"},
			tableName:   "tbl",
			whereClause: []string{
				"where", " ", "d", " ", "in", " ", "(", "?", ")",
			},
			consistent: true,
		},
	}

	for tn, tt := range tests {
		q, err := Parse(tt.query)
		if err != nil {
			t.Errorf("%d: got=%v, want=nil", tn, err)
		}
		if q.Select == nil {
			t.Errorf("%d: got=nil, want=non-nil", tn)
		}
		if got, want := q.Select.TableName, tt.tableName; got != want {
			t.Errorf("%d: got=%q, want=%q", tn, got, want)
		}
		compareStringSlices(t, tn, q.Select.ColumnNames, tt.columnNames)
		compareStringSlices(t, tn, q.Select.WhereClause, tt.whereClause)
		if got, want := q.Select.ConsistentRead, tt.consistent; got != want {
			t.Errorf("%d: got=%v, want=%v", tn, got, want)
		}
		if got, want := q.Select.Key, tt.key; !reflect.DeepEqual(got, want) {
			t.Errorf("%d: got=%+v, want=%+v", tn, got, want)
		}
	}
}

func TestParseUpdate(t *testing.T) {
	tests := []struct {
		query string
		upd   *UpdateQuery
	}{
		{
			query: "update tbl set a=?, b = ? where id = ?",
			upd: &UpdateQuery{
				TableName: "tbl",
				Columns: []Column{
					{
						ColumnName: "a",
						Ordinal:    0,
					},
					{
						ColumnName: "b",
						Ordinal:    1,
					},
				},
				Key: Key{
					Ordinal: 2,
				},
			},
		},
		{
			query: "update `tbl` set a=?, b ='done' where id = ?",
			upd: &UpdateQuery{
				TableName: "tbl",
				Columns: []Column{
					{
						ColumnName: "a",
						Ordinal:    0,
					},
					{
						ColumnName: "b",
						Value:      stringPtr("done"),
					},
				},
				Key: Key{
					Ordinal: 1,
				},
			},
		},
		{
			query: "-- a comment\nuPdate `tbl` seT a=?, b ='done' where id = 'xx'",
			upd: &UpdateQuery{
				TableName: "tbl",
				Columns: []Column{
					{
						ColumnName: "a",
						Ordinal:    0,
					},
					{
						ColumnName: "b",
						Value:      stringPtr("done"),
					},
				},
				Key: Key{
					Value: stringPtr("xx"),
				},
			},
		},
	}

	for tn, tt := range tests {
		q, err := Parse(tt.query)
		if err != nil {
			t.Errorf("%d: got=%v, want=nil", tn, err)
			continue
		}
		if q.Update == nil {
			t.Errorf("%d: got=nil, want=non-nil", tn)
			continue
		}
		if !reflect.DeepEqual(q.Update, tt.upd) {
			t.Errorf("%d: got=%v\n  want=%v\n", tn, q.Update, tt.upd)
		}
	}
}

func TestParseInsert(t *testing.T) {
	tests := []struct {
		query string
		ins   *InsertQuery
	}{
		{
			query: "insert into tbl(id, a, b) values(?,?,?)",
			ins: &InsertQuery{
				TableName: "tbl",
				Columns: []Column{
					{
						ColumnName: "a",
						Ordinal:    1,
					},
					{
						ColumnName: "b",
						Ordinal:    2,
					},
				},
				Key: Key{
					Ordinal: 0,
				},
			},
		},
		{
			query: "insert `tbl`(a,b,id) values('a','b','1')",
			ins: &InsertQuery{
				TableName: "tbl",
				Columns: []Column{
					{
						ColumnName: "a",
						Value:      stringPtr("a"),
					},
					{
						ColumnName: "b",
						Value:      stringPtr("b"),
					},
				},
				Key: Key{
					Value: stringPtr("1"),
				},
			},
		},
	}

	for tn, tt := range tests {
		q, err := Parse(tt.query)
		if err != nil {
			t.Errorf("%d: got=%v, want=nil", tn, err)
			continue
		}
		if q.Insert == nil {
			t.Errorf("%d: got=nil, want=non-nil", tn)
			continue
		}
		if !reflect.DeepEqual(q.Insert, tt.ins) {
			t.Errorf("%d: got=%v\n  want=%v\n", tn, q.Insert, tt.ins)
		}
	}
}

func TestParseDelete(t *testing.T) {
	tests := []struct {
		query string
		del   *DeleteQuery
	}{
		{
			query: "delete from tbl where id = ?",
			del: &DeleteQuery{
				TableName: "tbl",
				Key: Key{
					Ordinal: 0,
				},
			},
		},
		{
			query: "delete `tbl` where id = '11'",
			del: &DeleteQuery{
				TableName: "tbl",
				Key: Key{
					Value: stringPtr("11"),
				},
			},
		},
	}

	for tn, tt := range tests {
		q, err := Parse(tt.query)
		if err != nil {
			t.Errorf("%d: got=%v, want=nil", tn, err)
			continue
		}
		if q.Delete == nil {
			t.Errorf("%d: got=nil, want=non-nil", tn)
			continue
		}
		if !reflect.DeepEqual(q.Delete, tt.del) {
			t.Errorf("%d: got=%v\n  want=%v\n", tn, q.Delete, tt.del)
		}
	}
}

func TestParseCreateTable(t *testing.T) {
	tests := []struct {
		query string
		ct    *CreateTableQuery
	}{
		{
			query: "create table tbl",
			ct: &CreateTableQuery{
				TableName: "tbl",
			},
		},
	}

	for tn, tt := range tests {
		q, err := Parse(tt.query)
		if err != nil {
			t.Errorf("%d: got=%v, want=nil", tn, err)
			continue
		}
		if q.CreateTable == nil {
			t.Errorf("%d: got=nil, want=non-nil", tn)
			continue
		}
		if !reflect.DeepEqual(q.CreateTable, tt.ct) {
			t.Errorf("%d: got=%v\n  want=%v\n", tn, q.Delete, tt.ct)
		}
	}
}

func TestParseDropTable(t *testing.T) {
	tests := []struct {
		query string
		ct    *DropTableQuery
	}{
		{
			query: "drop table tbl",
			ct: &DropTableQuery{
				TableName: "tbl",
			},
		},
	}

	for tn, tt := range tests {
		q, err := Parse(tt.query)
		if err != nil {
			t.Errorf("%d: got=%v, want=nil", tn, err)
			continue
		}
		if q.DropTable == nil {
			t.Errorf("%d: got=nil, want=non-nil", tn)
			continue
		}
		if !reflect.DeepEqual(q.DropTable, tt.ct) {
			t.Errorf("%d: got=%v\n  want=%v\n", tn, q.Delete, tt.ct)
		}
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		query   string
		errtext string
	}{
		{
			query:   "backup would be nice",
			errtext: `unrecognized query "backup"`,
		},
		{
			query:   "select from",
			errtext: `unexpected "from"`,
		},
		{
			query:   "from wherever",
			errtext: `unexpected keyword "from"`,
		},
		{
			query:   "insert into tbl(a, b) values(?, ?)",
			errtext: "missing id column in insert statement",
		},
		{
			query:   "insert into tbl(id, a, b, id) values(?,?,?,?)",
			errtext: "duplicate id column in insert statement",
		},
		{
			query:   "update x set y = ? where id = ? robins",
			errtext: `expected end of query, found "robins"`,
		},
		{
			query:   "update x get y = ? where id = ?",
			errtext: `expected "set", found "get"`,
		},
	}

	for tn, tt := range tests {
		_, err := Parse(tt.query)
		if err == nil {
			t.Errorf("%d: got=nil, want=non-nil", tn)
			continue
		}
		if got, want := err.Error(), tt.errtext; got != want {
			t.Errorf("%d: got=%v, want=%v", tn, got, want)
		}
	}
}

type aStringType string

func TestKeyString(t *testing.T) {
	tests := []struct {
		key       Key
		values    []driver.Value
		str       string
		expectErr bool
	}{
		{
			key: Key{
				Ordinal: 1,
			},
			values: []driver.Value{"a", "b", "c"},
			str:    "b",
		},
		{
			key: Key{
				Value: stringPtr("z"),
			},
			values: []driver.Value{"a", "b", "c"},
			str:    "z",
		},
		{
			key: Key{
				Ordinal: 0,
			},
			values: []driver.Value{aStringType("a")},
			str:    "a",
		},
		{
			key: Key{
				Ordinal: 4,
			},
			values:    []driver.Value{"a", "b"},
			expectErr: true,
		},
		{
			key: Key{
				Ordinal: 0,
			},
			values:    []driver.Value{0},
			expectErr: true,
		},
	}
	for tn, tt := range tests {
		s, err := tt.key.String(tt.values)
		if tt.expectErr {
			if err == nil {
				t.Errorf("%d: got=nil want=non-nil", tn)
			}
			continue
		}
		if got, want := s, tt.str; got != want {
			t.Errorf("%d: got=%v, want=%v", tn, got, want)
		}
	}
}

func TestColumnGetValue(t *testing.T) {
	tests := []struct {
		col       Column
		values    []driver.Value
		val       driver.Value
		expectErr bool
	}{
		{
			col: Column{
				Ordinal: 1,
			},
			values: []driver.Value{"a", "b", "c"},
			val:    "b",
		},
		{
			col: Column{
				Ordinal: 1,
			},
			values: []driver.Value{"a", int64(4), "c"},
			val:    int64(4),
		},
		{
			col: Column{
				Value: stringPtr("z"),
			},
			values: []driver.Value{"a", "b", "c"},
			val:    "z",
		},
		{
			col: Column{
				Ordinal: 4,
			},
			values:    []driver.Value{"a", "b"},
			expectErr: true,
		},
	}
	for tn, tt := range tests {
		s, err := tt.col.GetValue(tt.values)
		if tt.expectErr {
			if err == nil {
				t.Errorf("%d: got=nil want=non-nil", tn)
			}
			continue
		}
		if got, want := s, tt.val; got != want {
			t.Errorf("%d: got=%v, want=%v", tn, got, want)
		}
	}
}

func stringPtr(s string) *string {
	return &s
}

func compareStringSlices(t *testing.T, tn int, got []string, want []string) {
	t.Helper()
	if g, w := len(got), len(want); g != w {
		t.Errorf("%d: length: got=%v, want=%v", tn, g, w)
		for i := 0; i < len(got); i++ {
			t.Logf("   got[%d]=%v", i, got[i])
		}
		for i := 0; i < len(want); i++ {
			t.Logf("   want[%d]=%v", i, want[i])
		}
	}
	n := len(got)
	if len(want) < n {
		n = len(want)
	}

	for i := 0; i < n; i++ {
		if g, w := got[i], want[i]; g != w {
			t.Errorf("%d: %d:\n got=%q,\nwant=%q", tn, i, got[i], want[i])
		}
	}
}
