package spirit

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/mysqlerr"
)

// dropTablesQuietPeriod is how long a drop_tables entry must have gone without
// a write before a plan proposes dropping it. A table another tool is still
// copying rows into, such as the shadow table of an online schema change
// SchemaBot did not start, is written continuously, so it fails this check
// while the change runs.
const dropTablesQuietPeriod = 24 * time.Hour

// mysqlErrUnknownSystemVariable is the server's error for SET of a variable it
// does not have. MySQL 5.7 has no information_schema_stats_expiry, and reads
// UPDATE_TIME uncached, so the SET is not needed there.
const mysqlErrUnknownSystemVariable = 1193

// loadTableSchema reads one live table's definition the way the schema loader
// reads every other, for a table the loader's naming filters left out.
func loadTableSchema(ctx context.Context, db *sql.DB, name string) (table.TableSchema, error) {
	var tbl, createStmt string
	if err := db.QueryRowContext(ctx, "SHOW CREATE TABLE "+sqlescape.EscapeIdentifier(name)).Scan(&tbl, &createStmt); err != nil {
		return table.TableSchema{}, fmt.Errorf("read CREATE TABLE: %w", err)
	}
	createStmt, err := table.StripAutoIncrement(createStmt)
	if err != nil {
		return table.TableSchema{}, fmt.Errorf("strip AUTO_INCREMENT: %w", err)
	}
	return table.TableSchema{Name: tbl, Schema: createStmt}, nil
}

// refuseRecentlyWrittenDrops refuses a plan that would drop a drop_tables
// entry written within the quiet period. A name alone does not prove a table
// is abandoned: the shadow table of an online schema change another tool is
// running looks like a leftover, and SchemaBot cannot see that tool's state.
// Rows still arriving is the one sign every such tool leaves on the target.
//
// The check reads information_schema.TABLES.UPDATE_TIME with the statistics
// cache disabled for the session, so the value is the server's own rather than
// one cached for up to a day. The server tracks it in memory, so it is empty
// for a table not written since the server last started; that table passes.
func refuseRecentlyWrittenDrops(ctx context.Context, db *sql.DB, database string, drops engine.DroppedTables) error {
	entries := drops.Entries()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("check when drop_tables entries were last written: %w", err)
	}
	defer utils.CloseAndLog(conn)

	if _, err := conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		if code, ok := mysqlerr.Number(err); !ok || code != mysqlErrUnknownSystemVariable {
			return fmt.Errorf("check when drop_tables entries were last written: disable the statistics cache: %w", err)
		}
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(entries)), ",")
	args := make([]any, 0, len(entries)+2)
	args = append(args, database)
	for _, name := range entries {
		args = append(args, name)
	}
	args = append(args, int64(dropTablesQuietPeriod/time.Second))
	rows, err := conn.QueryContext(ctx,
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN ("+placeholders+") "+
			"AND UPDATE_TIME >= NOW() - INTERVAL ? SECOND ORDER BY TABLE_NAME",
		args...)
	if err != nil {
		return fmt.Errorf("check when drop_tables entries were last written: %w", err)
	}
	defer utils.CloseAndLog(rows)
	var written []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("check when drop_tables entries were last written: %w", err)
		}
		written = append(written, fmt.Sprintf("%q", name))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("check when drop_tables entries were last written: %w", err)
	}
	if len(written) == 0 {
		return nil
	}
	return fmt.Errorf("drop_tables entries %s were written in the last %s, so something may still be using them, such as an online schema change another tool is running. Wait until nothing writes to them, or remove them from drop_tables",
		strings.Join(written, ", "), dropTablesQuietPeriod)
}
