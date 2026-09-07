package localsetup

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/block/mysql"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/spirit/pkg/utils"
)

// DiscoverNamespaces reads only the target catalog. It neither registers a
// runtime nor opens the state database, so abandoning setup has no side effects.
// MySQL stays within the DSN's database, matching the existing pull semantics.
func DiscoverNamespaces(ctx context.Context, engine, dsn string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var db *sql.DB
	var err error
	var query string
	switch engine {
	case "mysql":
		cfg, parseErr := mysql.ParseDSN(dsn)
		if parseErr != nil || cfg.DBName == "" {
			return nil, fmt.Errorf("your MySQL connection needs to name a database")
		}
		db, err = mysqlconn.Open(dsn)
		query = "SELECT DATABASE()"
	case "postgres":
		db, err = postgresconn.Open(dsn)
		query = `SELECT nspname FROM pg_catalog.pg_namespace
WHERE pg_catalog.has_schema_privilege(oid, 'USAGE') ORDER BY nspname`
	default:
		return nil, fmt.Errorf("namespace discovery is not available for this engine")
	}
	// Driver errors can contain connection material. Keep credentials out of the UI.
	if err != nil {
		return nil, fmt.Errorf("we couldn’t read that connection; check its format and try again")
	}
	defer utils.CloseAndLog(db)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("we couldn’t read your namespaces; check the connection and catalog permissions, then try again")
	}
	defer utils.CloseAndLog(rows)
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("we couldn’t read a namespace from the catalog")
		}
		if !schema.IsReservedPullNamespaceForDialect(schema.DialectForDatabaseType(engine), name) {
			names = append(names, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("namespace discovery was interrupted; try again")
	}
	return names, nil
}
