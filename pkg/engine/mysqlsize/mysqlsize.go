// Package mysqlsize reads the plan-time size of MySQL-family tables from
// information_schema statistics, for any engine that plans against MySQL
// servers. One read covers any number of tables across any number of schemas
// on one server, so an engine whose tables are spread over many schemas pays
// one round trip per server rather than one per schema.
//
// Sizes are display-only plan context. Callers bound a read with
// engine.TableSizeProbeTimeout and treat a failure as "no sizes": a size read
// never fails or stalls a plan.
package mysqlsize

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/utils"
)

// Table names one table in one schema.
type Table struct {
	Schema string
	Name   string
}

// Estimate is one table's approximate size: its row count and its on-disk
// footprint (data plus indexes). Either is nil when statistics did not report
// it, and the other is still kept.
type Estimate struct {
	Rows  *int64
	Bytes *int64
}

// Conn is the part of a dedicated connection a read uses. A *sql.Conn
// satisfies it. The read sets a session variable and then queries, so both
// calls must reach the same session, which a *sql.DB pool does not promise.
type Conn interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Read returns the estimates statistics report for the named base tables,
// keyed by the Table as the caller named it. A table that does not exist, is
// not a base table, or whose statistics report neither figure has no entry.
//
// It disables statistics caching for the session first, as the direct
// execution size gate does, so a plan never shows a size older than the one
// that gate reads. The setting stays on conn's session after Read returns.
func Read(ctx context.Context, conn Conn, logger *slog.Logger, tables []Table) (map[Table]Estimate, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		return nil, fmt.Errorf("disable cached statistics for size estimates: %w", err)
	}

	requested := requestedTables(tables)
	schemas, names := distinctSchemasAndNames(tables)
	args := make([]any, 0, len(schemas)+len(names))
	for _, s := range schemas {
		args = append(args, s)
	}
	for _, n := range names {
		args = append(args, n)
	}
	// Filtering schemas and names separately lets information_schema look
	// both up directly. When different schemas ask for different tables the
	// result can include pairs nobody asked for, which the loop below drops.
	query := `
		SELECT table_schema, table_name, table_rows, data_length + index_length
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
		  AND table_schema IN (` + placeholders(len(schemas)) + `)
		  AND table_name IN (` + placeholders(len(names)) + `)`

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query size estimates for %d tables in schemas %v: %w", len(tables), schemas, err)
	}
	defer utils.CloseAndLog(rows)

	estimates := make(map[Table]Estimate, len(tables))
	for rows.Next() {
		var schema, name string
		var tableRows, tableBytes sql.NullInt64
		if err := rows.Scan(&schema, &name, &tableRows, &tableBytes); err != nil {
			return nil, fmt.Errorf("scan table size estimate: %w", err)
		}
		// information_schema can match a name case-insensitively and return
		// it in the server's case, so the estimate is keyed by the Table the
		// caller asked for, which is the name its plan changes carry.
		asked, ok := requested[foldTable(schema, name)]
		if !ok {
			logger.Debug("size read returned a table no caller asked for in that schema; ignoring it",
				"schema", schema, "table", name)
			continue
		}
		est := Estimate{
			Rows:  validStatistic(logger, asked, "table_rows", tableRows),
			Bytes: validStatistic(logger, asked, "data_length + index_length", tableBytes),
		}
		if est.Rows == nil && est.Bytes == nil {
			// validStatistic logged why each one is missing.
			continue
		}
		estimates[asked] = est
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table size estimates: %w", err)
	}
	return estimates, nil
}

// validStatistic returns a base table's statistic as a display estimate, or
// nil with a warning when statistics could not report it. A NULL value is a
// base table whose statistics are unreadable (for example a discarded
// tablespace), and a negative one is a sentinel for "no real estimate", not a
// count, as it is for the direct execution size gate.
func validStatistic(logger *slog.Logger, t Table, statistic string, v sql.NullInt64) *int64 {
	if !v.Valid {
		logger.Warn("table statistics report no value; the plan will omit it from the table's size",
			"schema", t.Schema, "table", t.Name, "statistic", statistic)
		return nil
	}
	if v.Int64 < 0 {
		logger.Warn("table statistics report a negative value; the plan will omit it from the table's size",
			"schema", t.Schema, "table", t.Name, "statistic", statistic, "value", v.Int64)
		return nil
	}
	n := v.Int64
	return &n
}

// requestedTables indexes the requested tables by their case-folded schema
// and name, so a row information_schema returns in the server's case maps
// back to the Table the caller named.
func requestedTables(tables []Table) map[Table]Table {
	requested := make(map[Table]Table, len(tables))
	for _, t := range tables {
		requested[foldTable(t.Schema, t.Name)] = t
	}
	return requested
}

// distinctSchemasAndNames returns the distinct schemas and table names the
// read filters on, each in first-seen order.
func distinctSchemasAndNames(tables []Table) (schemas, names []string) {
	seenSchema := make(map[string]bool, len(tables))
	seenName := make(map[string]bool, len(tables))
	for _, t := range tables {
		if !seenSchema[t.Schema] {
			seenSchema[t.Schema] = true
			schemas = append(schemas, t.Schema)
		}
		if !seenName[t.Name] {
			seenName[t.Name] = true
			names = append(names, t.Name)
		}
	}
	return schemas, names
}

func foldTable(schema, name string) Table {
	return Table{Schema: strings.ToLower(schema), Name: strings.ToLower(name)}
}

func placeholders(n int) string {
	return "?" + strings.Repeat(", ?", n-1)
}
