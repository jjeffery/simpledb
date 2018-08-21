package simpledb_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/jjeffery/simpledb/driver"
)

func Example() {
	db, err := sql.Open("simpledb", "")
	exitIfError(err)
	ctx := context.Background()

	// create a table
	_, err = db.ExecContext(ctx, "create table temp_test_table")
	exitIfError(err)
	waitForConsistency()

	// insert some rows
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("ID%03d", i)
		name := fmt.Sprintf("name-%d", i)
		number := i * i
		_, err = db.ExecContext(ctx,
			"insert into temp_test_table(id, name, number) values(?, ?, ?)",
			id, name, number,
		)
		exitIfError(err)
	}
	waitForConsistency()

	// update a row
	_, err = db.ExecContext(ctx,
		"update temp_test_table set number = ? where id = ?",
		100, "ID007",
	)
	exitIfError(err)

	// delete a row
	_, err = db.ExecContext(ctx, "delete from temp_test_table where id = 'ID008'")
	exitIfError(err)

	// select rows
	rows, err := db.QueryContext(ctx,
		"consistent select id, name, number from temp_test_table where name is not null order by name desc",
	)
	exitIfError(err)

	for rows.Next() {
		var (
			id     string
			name   string
			number int
		)

		err = rows.Scan(&id, &name, &number)
		exitIfError(err)
		fmt.Printf("%s,%s,%d\n", id, name, number)
	}

	_, err = db.ExecContext(ctx, "drop table temp_test_table")
	exitIfError(err)

	// Output:
	// ID009,name-9,81
	// ID007,name-7,100
	// ID006,name-6,36
	// ID005,name-5,25
	// ID004,name-4,16
	// ID003,name-3,9
	// ID002,name-2,4
	// ID001,name-1,1
	// ID000,name-0,0
}

func exitIfError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// waitForConsistency waits a short time for the SimpleDB domain
// to be consistent across all copies.
func waitForConsistency() {
	time.Sleep(500 * time.Millisecond)
}
