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
// Every DSN comes from here so that no pool maps a signed TINYINT(1) column to
// a Go bool, which the driver does by default. The (1) is a display width
// rather than a range: the column legally holds -128..127, so the mapping
// answers true for every non-zero value and the stored number is gone before
// the caller sees it. LocalScale reads columns whose types it knows nothing
// about on two paths — the admin query endpoints scan every column into a
// sql.NullString, and the branch proxy scans into an any before forwarding the
// value on the wire — so neither can recognize a bool that should have been a
// number. Disabling the mapping is what keeps a stored 2 a 2 on both.
//
// LocalScale is local test and dev infrastructure and deliberately does not
// route through pkg/mysqlconn, whose RDS transport settings it has no use for.
func mysqlDSN(dsn, database string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse MySQL DSN: %w", err)
	}
	if err := cfg.Apply(mysql.TinyInt1IsBool(false)); err != nil {
		return "", fmt.Errorf("disable TINYINT(1) bool mapping: %w", err)
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
