package localscale

import (
	"database/sql"
	"fmt"

	"github.com/block/mysql"
)

// mysqlDriverName is block/mysql, Block's fork of the Go MySQL driver. It
// registers itself under this name rather than upstream's "mysql".
const mysqlDriverName = "block-mysql"

// mysqlDSN builds the DSN for every connection LocalScale opens to one of its
// own MySQL endpoints: a managed cluster's mysqld, its vtgate, or a branch
// proxy's upstream. Pass database to select a schema on top of a DSN that
// names none — the shape of databaseBackend.mysqlDSNBase — or "" to keep
// whatever the DSN already names.
//
// LocalScale is local test and dev infrastructure and deliberately does not
// route through pkg/mysqlconn, whose RDS transport settings it has no use for.
func mysqlDSN(dsn, database string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse MySQL DSN: %w", err)
	}
	if database != "" {
		cfg.DBName = database
	}
	return cfg.FormatDSN(), nil
}

// openMySQL opens a pool on the DSN mysqlDSN builds. Callers verify the
// connection themselves, since what a failed dial means differs by call site.
func openMySQL(dsn, database string) (*sql.DB, error) {
	connectionDSN, err := mysqlDSN(dsn, database)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(mysqlDriverName, connectionDSN)
	if err != nil {
		return nil, fmt.Errorf("open MySQL connection: %w", err)
	}
	return db, nil
}
