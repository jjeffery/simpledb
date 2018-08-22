package simpledbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/simpledb"
	"github.com/aws/aws-sdk-go/service/simpledb/simpledbiface"
	"github.com/jjeffery/errors"
)

func init() {
	sql.Register("simpledb", &Driver{})
}

// Driver implements the driver.Driver interface.
type Driver struct {
	mutex sync.Mutex
	sdb   simpledbiface.SimpleDBAPI
}

// Open returns a new connection to the database.
// The name is currently ignored and should be a blank
// string, but in future may include parameters like
// region, profile, consistent-read, schema, etc.
func (d *Driver) Open(name string) (driver.Conn, error) {
	d.mutex.Lock()
	sdb := d.sdb
	d.mutex.Unlock()

	if sdb == nil {
		sess, err := session.NewSessionWithOptions(session.Options{
			// this option obtains the region setting from the ~/.aws/config file
			// if it is set
			SharedConfigState: session.SharedConfigEnable,
		})
		if err != nil {
			return nil, err
		}
		d.mutex.Lock()
		if d.sdb == nil {
			d.sdb = simpledb.New(sess)
		}
		sdb = d.sdb
		d.mutex.Unlock()
	}
	c := &conn{
		SimpleDB: sdb,
	}
	return c, nil
}

// Connector implements the driver.Connector interface,
// and is useful for passing to the sql.OpenDB function.
type Connector struct {
	// SimpleDB is the AWS SDK handle used for all SimpleDB operations.
	SimpleDB simpledbiface.SimpleDBAPI

	// Schema is used to derive the SimpleDB domain name from the
	// table name in the SQL. If Schema is not blank, then it is
	// prefixed in front of any table name with a period. So if
	// Schema is "dev" and table name is "tbl" then the corresponding
	// SimpleDB domain would be "dev.tbl".
	Schema string

	// Synonyms is a map of table names to their corresponding SimpleDB
	// domain names. Useful in an environment where the SimpleDB domains
	// are created by CloudFormation and have randomly generated names.
	// Create an entry in Synonym and use a constant table name in the SQL.
	//
	// If a table name has an entry in Synonyms, Schema is ignored.
	Synonyms map[string]string
}

// Connect returns a connection to the database.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if c.SimpleDB == nil {
		return nil, errors.New("SimpleDB cannot be nil")
	}
	return &conn{
		SimpleDB: c.SimpleDB,
		Schema:   c.Schema,
		Synonyms: c.Synonyms,
	}, nil
}

// Driver returns the underlying Driver of the Connector.
func (c *Connector) Driver() driver.Driver {
	return &Driver{
		sdb: c.SimpleDB,
	}
}
