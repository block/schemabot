package spirit

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/utils"
)

// tableStatistics is one table's size statistics as information_schema.TABLES
// reports them. Every figure is an InnoDB statistics estimate rather than a
// measurement, and any of them can be NULL, so each caller decides what a
// missing or implausible figure means: the direct execution size gate blocks
// on one, while a display-only reader can simply omit the size.
type tableStatistics struct {
	// rows is TABLE_ROWS, the optimizer's sampled row estimate.
	rows sql.NullInt64
	// dataBytes is DATA_LENGTH, the clustered index's allocated pages times
	// the page size.
	dataBytes sql.NullInt64
	// indexBytes is INDEX_LENGTH, the secondary indexes' allocated pages
	// times the page size.
	indexBytes sql.NullInt64
}

// readTableStatistics reads the size statistics of the named base tables in
// schema, keyed by table name. A table information_schema does not list as a
// base table is absent from the result rather than an error, so the caller
// decides whether a missing table matters.
//
// The read happens on a dedicated connection with statistics caching
// disabled. MySQL otherwise serves information_schema statistics cached for up
// to information_schema_stats_expiry seconds (a day by default), and neither a
// safety bound nor a size shown to the operator should rest on a day-old
// figure. The setting is per session, which is why the connection is
// dedicated rather than taken from the pool for each statement. Uncached is
// still not exact: InnoDB refreshes persistent statistics in the background,
// so every figure can lag the table, most sharply right after a bulk load.
func readTableStatistics(ctx context.Context, db *sql.DB, schema string, tables []string) (map[string]tableStatistics, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection for table statistics in `%s`: %w", schema, err)
	}
	defer utils.CloseAndLog(conn)
	if _, err := conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		return nil, fmt.Errorf("disable cached statistics for table statistics in `%s`: %w", schema, err)
	}

	args := make([]any, 0, len(tables)+1)
	args = append(args, schema)
	for _, name := range tables {
		args = append(args, name)
	}
	query := "SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH FROM information_schema.TABLES" +
		" WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'" +
		" AND TABLE_NAME IN (?" + strings.Repeat(", ?", len(tables)-1) + ")"
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query table statistics for %v in `%s`: %w", tables, schema, err)
	}
	defer utils.CloseAndLog(rows)

	stats := make(map[string]tableStatistics, len(tables))
	for rows.Next() {
		var name string
		var s tableStatistics
		if err := rows.Scan(&name, &s.rows, &s.dataBytes, &s.indexBytes); err != nil {
			return nil, fmt.Errorf("scan table statistics in `%s`: %w", schema, err)
		}
		stats[name] = s
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table statistics in `%s`: %w", schema, err)
	}
	return stats, nil
}
