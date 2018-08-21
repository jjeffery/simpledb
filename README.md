# AWS SimpleDB driver for Go's database/sql package

[![GoDoc](https://godoc.org/github.com/jjeffery/simpledb?status.svg)](https://godoc.org/github.com/jjeffery/simpledb)
[![License](http://img.shields.io/badge/license-MIT-green.svg?style=flat)](https://raw.githubusercontent.com/jjeffery/simpledb/master/LICENSE.md)
[![Build Status (Linux)](https://travis-ci.org/jjeffery/simpledb.svg?branch=master)](https://travis-ci.org/jjeffery/simpledb)
[![Coverage Status](https://codecov.io/github/jjeffery/simpledb/badge.svg?branch=master)](https://codecov.io/github/jjeffery/simpledb?branch=master)
[![GoReportCard](https://goreportcard.com/badge/github.com/jjeffery/simpledb)](https://goreportcard.com/report/github.com/jjeffery/simpledb)

This package provides an SimpleDB driver for Go's `database/sql` package. AWS SimpleDB is a
highly available data store that requires no database administration on the part of the user.
Although SimpleDB is a [NoSQL](https://en.wikipedia.org/wiki/NoSQL) datastore, it supports an
SQL-like syntax for querying data.

This driver can be useful for applications that are using other AWS services, and have a need
for a simple database that supports flexible querying. It can be handy when DynamoDB is not
flexible enough, but an RDS instance seems like overkill.

Using the `database/sql` package to access SimpleDB provides an upgrade path to using a more
fully-featured SQL database at a future time.
If [Aurora Serverless](https://aws.amazon.com/rds/aurora/serverless/) is available in your
chosen AWS region, it might be a better alternative.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Install](#install)
- [Example](#example)
- [SQL](#sql)
  - [Placeholders](#placeholders)
  - [`id` column](#id-column)
  - [Select](#select)
  - [Insert](#insert)
  - [Update](#update)
  - [Delete](#delete)
  - [Consistent Read](#consistent-read)
  - [Create Table / Drop Table](#create-table--drop-table)
- [Testing](#testing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Install

```bash
go get github.com/jjeffery/simpledb/...
```

Requires go 1.10 or later.

## Example

See the [GoDoc package example](https://godoc.org/github.com/jjeffery/simpledb#example-package).

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "log"
    "time"

    _ "github.com/jjeffery/simpledb/driver"
)

func main() {
    ctx := context.Background()

    // create DB handle using default AWS credentials
    db, err := sql.Open("simpledb", "")
    exitIfError(err)

    // create a table
    _, err = db.ExecContext(ctx, "create table temp_test_table")
    exitIfError(err)

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
}

func exitIfError(err error) {
    if err != nil {
        log.Fatal(err)
    }
}
```

## SQL

### Placeholders

Placeholders can be used to substitute arguments.

```sql
select id, a, b, c from my_table where a = ?
```

### `id` column

The column `id` is special, and refers to the SimpleDB item name.

### Select

All the restrictions of the SimpleDB `select` statement apply.

```sql
select output_list
from domain_name
[where expression]
[sort_instructions]
[limit limit]
```

See the [SimpleDB documentation](https://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/UsingSelect.html)
for more details.

### Insert

Insert statements can insert one row at a time. The `id` column is mandatory.

```sql
insert into my_table(id, a, b, c)
values (?, ?, ?, ?)
```

### Update

Update statements can update one row at a time. The `id` column is the only column
allowed in the `where` clause.

```sql
update my_table
set a = ?, b = ?, c = ?
where id = ?
```

### Delete

Delete statements can delete one row at a time. The `id` column is the only column
allowed in the `where` clause.

```sql
delete from my_table
where id = ?
```

### Consistent Read

If the select statement starts with the word "consistent", then a consistent read will be performed.

```sql
consistent select id, a, b, c from my_table where a = ?
```

### Create Table / Drop Table

Create and delete SimpleDB domains using the `create table` and `drop table` commands.

```sql
create table my_domain

drop table my_domain
```

## Testing

There is no option for running SimpleDB locally, so all tests require a valid AWS account. The account
credentials are detected using the default mechanism, using:

- Environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
- Environment variable AWS_PROFILE and file ~/.aws/credentisls
- Environment variable AWS_REGION, or if not set file ~/.aws/config

The tests require the following permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "simpledb_driver_test",
      "Effect": "Allow",
      "Action": [
        "sdb:GetAttributes",
        "sdb:DeleteDomain",
        "sdb:PutAttributes",
        "sdb:DeleteAttributes",
        "sdb:Select",
        "sdb:CreateDomain"
      ],
      "Resource": "arn:aws:sdb:*:*:domain/temp_test_*"
    }
  ]
}
```
