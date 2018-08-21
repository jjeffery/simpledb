/*
Package simpledb provides an AWS SimpleDB driver for the database/sql package.

See the package example for an overview of how to use the driver.

SQL

Refer to https://github.com/jjeffery/simpledb for a description of the SQL
dialect supported by this driver. The following examples can be used as a
guide.

 select id, a, b, c
 from my_table
 where a > ?
 and b = ?
 order by a

 insert into my_table(id, a, b, c)
 values(?, ?, ?, 'c value')

 update my_table
 set a = ?, b = ?, c = 'processed'
 where id = ?

 delete from my_table
 where id = ?

 create table my_table

 drop table my_table

For consistent-read select statements, prefix the `select` with the word `consistent`

 consistent select id, a, b, c
 from my_table
 where a > ?
 and b = ?
 order by a

*/
package simpledb

import (
	// load the simpledb driver
	_ "github.com/jjeffery/simpledb/driver"
)
