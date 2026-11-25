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
// not a base table, or whose statistics report neither figure has no entry. A
// nil logger logs to slog.Default().
//
// It disables statistics caching for the session first, as the direct
// execution size gate does, so a plan never shows a size older than the one
// that gate reads. The setting stays on conn's session after Read returns.
//
// A request too large for one statement's placeholders is read in several
// statements on the same session.
func Read(ctx context.Context, conn Conn, logger *slog.Logger, tables []Table) (map[Table]Estimate, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		return nil, fmt.Errorf("disable cached statistics for size estimates: %w", err)
	}
	foldCase, err := namesCompareIgnoringCase(ctx, conn)
	if err != nil {
		return nil, err
	}

	var found []sizeRow
	for _, batch := range batchesWithin(tables, maxPlaceholders) {
		rows, err := readBatch(ctx, conn, batch)
		if err != nil {
			return nil, err
		}
		found = append(found, rows...)
	}
	return matchEstimates(logger, tables, found, foldCase), nil
}

// maxPlaceholders is the most bound parameters one MySQL prepared statement
// accepts.
const maxPlaceholders = 65535

// namesCompareIgnoringCase reports whether the server compares schema and
// table names ignoring case, which lower_case_table_names sets to 1 or 2. On
// such a server information_schema returns a table in its own case, which
// can differ from the case the caller asked in.
func namesCompareIgnoringCase(ctx context.Context, conn Conn) (bool, error) {
	rows, err := conn.QueryContext(ctx, "SELECT @@lower_case_table_names")
	if err != nil {
		return false, fmt.Errorf("read lower_case_table_names for size estimates: %w", err)
	}
	defer utils.CloseAndLog(rows)
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return false, fmt.Errorf("read lower_case_table_names for size estimates: %w", err)
		}
		return false, fmt.Errorf("read lower_case_table_names for size estimates: the server returned no row")
	}
	var mode int
	if err := rows.Scan(&mode); err != nil {
		return false, fmt.Errorf("scan lower_case_table_names for size estimates: %w", err)
	}
	return mode != 0, nil
}

// readBatch returns the statistics information_schema reports for one batch
// of tables.
func readBatch(ctx context.Context, conn Conn, tables []Table) ([]sizeRow, error) {
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
	// result can include pairs nobody asked for, which matchEstimates drops.
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

	var found []sizeRow
	for rows.Next() {
		var r sizeRow
		if err := rows.Scan(&r.table.Schema, &r.table.Name, &r.rows, &r.bytes); err != nil {
			return nil, fmt.Errorf("scan table size estimate: %w", err)
		}
		found = append(found, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table size estimates: %w", err)
	}
	return found, nil
}

// batchesWithin splits tables, in order, into batches whose distinct schemas
// and names together need at most limit placeholders.
func batchesWithin(tables []Table, limit int) [][]Table {
	var batches [][]Table
	var batch []Table
	schemas := make(map[string]bool)
	names := make(map[string]bool)
	for _, t := range tables {
		added := 0
		if !schemas[t.Schema] {
			added++
		}
		if !names[t.Name] {
			added++
		}
		if len(batch) > 0 && len(schemas)+len(names)+added > limit {
			batches = append(batches, batch)
			batch = nil
			schemas = make(map[string]bool)
			names = make(map[string]bool)
		}
		batch = append(batch, t)
		schemas[t.Schema] = true
		names[t.Name] = true
	}
	return append(batches, batch)
}

// sizeRow is one table's statistics as information_schema returned them.
type sizeRow struct {
	table       Table
	rows, bytes sql.NullInt64
}

// matchEstimates keys the returned statistics by the Table each caller asked
// for. A row whose schema and name match a request exactly belongs to it, and
// on a server that compares names case-sensitively (foldCase false) that is
// the only match: a row that differs from a request only in case is another
// table. On a server that compares names ignoring case, information_schema
// returns a table in its own case, so a request with no exact row takes the
// one row that matches it ignoring case, provided no other request matched
// that row exactly. A request that would match more than one row ignoring case
// is left without an estimate rather than given either.
func matchEstimates(logger *slog.Logger, tables []Table, found []sizeRow, foldCase bool) map[Table]Estimate {
	requested := make(map[Table]bool, len(tables))
	requestedFolded := make(map[Table]bool, len(tables))
	for _, t := range tables {
		requested[t] = true
		requestedFolded[foldTable(t)] = true
	}
	exact := make(map[Table]sizeRow, len(found))
	unclaimed := make(map[Table][]sizeRow, len(found))
	for _, r := range found {
		if requested[r.table] {
			exact[r.table] = r
			continue
		}
		if !foldCase || !requestedFolded[foldTable(r.table)] {
			logger.Debug("size read returned a table no caller asked for in that schema; ignoring it",
				"schema", r.table.Schema, "table", r.table.Name)
			continue
		}
		unclaimed[foldTable(r.table)] = append(unclaimed[foldTable(r.table)], r)
	}

	estimates := make(map[Table]Estimate, len(tables))
	for t := range requested {
		r, ok := exact[t]
		if !ok {
			candidates := unclaimed[foldTable(t)]
			if len(candidates) > 1 {
				logger.Warn("size read returned more than one table matching this name ignoring case; the plan will omit its size",
					"schema", t.Schema, "table", t.Name, "matches", len(candidates))
				continue
			}
			if len(candidates) == 0 {
				logger.Debug("size read returned no base table for this name; the plan will omit its size",
					"schema", t.Schema, "table", t.Name)
				continue
			}
			r = candidates[0]
		}
		est := Estimate{
			Rows:  validStatistic(logger, t, "table_rows", r.rows),
			Bytes: validStatistic(logger, t, "data_length + index_length", r.bytes),
		}
		if est.Rows == nil && est.Bytes == nil {
			// validStatistic logged why each one is missing.
			continue
		}
		estimates[t] = est
	}
	return estimates
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

func foldTable(t Table) Table {
	return Table{Schema: strings.ToLower(t.Schema), Name: strings.ToLower(t.Name)}
}

func placeholders(n int) string {
	return "?" + strings.Repeat(", ?", n-1)
}
