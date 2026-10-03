package localsetup

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
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
// Vitess keyspaces come from the PlanetScale API, the same source pull uses.
func DiscoverNamespaces(ctx context.Context, target Target) ([]string, error) {
	if target.Engine == "vitess" {
		return listPlanetScaleKeyspaces(ctx, target)
	}
	engine, dsn := target.Engine, target.DSN
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("set a database connection before discovering namespaces")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var db *sql.DB
	var err error
	var query string
	switch engine {
	case "mysql":
		cfg, parseErr := mysql.ParseDSN(dsn)
		if parseErr != nil {
			slog.DebugContext(ctx, "parse namespace connection failed", "engine", engine, "error", parseErr)
			return nil, &setupConnectionError{message: "your MySQL connection needs a valid database address", cause: parseErr}
		}
		if cfg.DBName == "" {
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
		slog.DebugContext(ctx, "discover setup namespaces failed", "engine", engine, "error", err)
		return nil, &setupConnectionError{message: "we couldn’t read that connection; check its format and try again", cause: err}
	}
	defer utils.CloseAndLog(db)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		slog.DebugContext(ctx, "discover setup namespaces failed", "engine", engine, "error", err)
		return nil, &setupConnectionError{message: "we couldn’t read your namespaces; check the connection and catalog permissions, then try again", cause: err}
	}
	defer utils.CloseAndLog(rows)
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			slog.DebugContext(ctx, "discover setup namespaces failed", "engine", engine, "error", err)
			return nil, &setupConnectionError{message: "we couldn’t read a namespace from the catalog", cause: err}
		}
		if !schema.IsReservedPullNamespaceForDialect(schema.DialectForDatabaseType(engine), name) {
			names = append(names, name)
		}
	}
	if err := rows.Err(); err != nil {
		slog.DebugContext(ctx, "discover setup namespaces failed", "engine", engine, "error", err)
		return nil, &setupConnectionError{message: "namespace discovery was interrupted; try again", cause: err}
	}
	return names, nil
}
