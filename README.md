# AWS SimpleDB driver for Go's database/sql package

[![GoDoc](https://godoc.org/github.com/jjeffery/simpledb?status.svg)](https://godoc.org/github.com/jjeffery/simpledb)
[![License](http://img.shields.io/badge/license-MIT-green.svg?style=flat)](https://raw.githubusercontent.com/jjeffery/simpledb/master/LICENSE.md)
[![Build Status (Linux)](https://travis-ci.org/jjeffery/simpledb.svg?branch=master)](https://travis-ci.org/jjeffery/simpledb)
[![Coverage Status](https://codecov.io/github/jjeffery/simpledb/badge.svg?branch=master)](https://codecov.io/github/jjeffery/simpledb?branch=master)
[![GoReportCard](https://goreportcard.com/badge/github.com/jjeffery/simpledb)](https://goreportcard.com/report/github.com/jjeffery/simpledb)

## Install

```bash
go get github.com/jjeffery/simpledb/...
```

Requires go 1.10 or later.

## Example

See the [GoDoc package example](https://godoc.org/github.com/jjeffery/simpledb#example-package).

## SQL

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

## Placeholders

Placeholders can be used to substitute arguments.

```sql
select id, a, b, c from my_table where a = ?
```

## id column

The column `id` is special, and refers to the item name.

## Insert

Insert statements can insert one row at a time. The `id` column is mandatory.

```sql
insert into my_table(id, a, b, c)
values (?, ?, ?, ?)
```

## Update

Update statements can update one row at a time. The `id` column is the only column
allowed in the `where` clause.

```sql
update my_table
set a = ?, b = ?, c = ?
where id = ?
```

## Delete

Delete statements can delete one row at a time. The `id` column is the only column
allowed in the `where` clause.

```sql
delete from my_table
where id = ?
```

## Consistent read

If the select statement starts with the word "consistent", then a consistent read will be performed.

```sql
consistent select id, a, b, c from my_table where a = ?
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
