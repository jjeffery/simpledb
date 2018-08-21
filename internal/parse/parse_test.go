package parse

import (
	"reflect"
	"testing"
)

func TestParseSelect(t *testing.T) {
	tests := []struct {
		query       string
		columnNames []string
		segments    []string
	}{
		{
			query:       "select a, b, c from tbl where id = ?",
			columnNames: []string{"a", "b", "c"},
			segments: []string{
				"select a, `sql:a`, b, `sql:b`, c, `sql:c` from tbl where itemName() = ",
				"",
			},
		},
		{
			query:       "select `a`, `b`, `c` from `tbl` where id = ? and c in (?, ?, ?)",
			columnNames: []string{"a", "b", "c"},
			segments: []string{
				"select `a`, `sql:a`, `b`, `sql:b`, `c`, `sql:c` from `tbl` where itemName() = ",
				" and c in (",
				", ",
				", ",
				")",
			},
		},
		{
			query:       "select `a`, `b`, `c` from `tbl` where id = ? and c in (?, ?, ?)",
			columnNames: []string{"a", "b", "c"},
			segments: []string{
				"select `a`, `sql:a`, `b`, `sql:b`, `c`, `sql:c` from `tbl` where itemName() = ",
				" and c in (",
				", ",
				", ",
				")",
			},
		},
		{
			query:       "select `id`, `b`, `c` from `tbl` where d = ? and `Id` in (?, ?, ?)",
			columnNames: []string{"id", "b", "c"},
			segments: []string{
				"select `b`, `sql:b`, `c`, `sql:c` from `tbl` where d = ",
				" and itemName() in (",
				", ",
				", ",
				")",
			},
		},
		{
			query:       "select `a`, `id`, `c` from `tbl` where d = ? and `Id` in (?, ?, ?)",
			columnNames: []string{"a", "id", "c"},
			segments: []string{
				"select `a`, `sql:a`, `c`, `sql:c` from `tbl` where d = ",
				" and itemName() in (",
				", ",
				", ",
				")",
			},
		},
		{
			query:       "select `a`, `b`, `id` from `tbl` where d = ? and `Id` in (?, ?, ?)",
			columnNames: []string{"a", "b", "id"},
			segments: []string{
				"select `a`, `sql:a`, `b`, `sql:b` from `tbl` where d = ",
				" and itemName() in (",
				", ",
				", ",
				")",
			},
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
		compareStringSlices(t, tn, q.Select.ColumnNames, tt.columnNames)
		compareStringSlices(t, tn, q.Select.Segments, tt.segments)
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
		/*
			{
				query: "update `tbl` set a=?, b ='done' where id = 'xx'",
				ins: &InsertQuery{
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
		*/
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

func stringPtr(s string) *string {
	return &s
}

func compareStringSlices(t *testing.T, tn int, got []string, want []string) {
	t.Helper()
	if g, w := len(got), len(want); g != w {
		t.Errorf("%d: length: got=%v, want=%v", tn, g, w)
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
