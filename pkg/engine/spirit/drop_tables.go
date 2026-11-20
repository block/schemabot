package spirit

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/mysqlerr"
	"github.com/block/schemabot/pkg/ui"
)

// defaultDropTablesQuietPeriod is how long a drop_tables entry must have gone
// without a write before a plan proposes dropping it. A table another tool is
// still copying rows into, such as the shadow table of an online schema change
// SchemaBot did not start, is written continuously, so it fails this check
// while the change runs.
const defaultDropTablesQuietPeriod = 24 * time.Hour

// mysqlErrUnknownSystemVariable is the server's error for SET of a variable it
// does not have. MySQL 5.7 has no information_schema_stats_expiry, and reads
// UPDATE_TIME uncached, so the SET is not needed there.
const mysqlErrUnknownSystemVariable = 1193

// loadTableSchema reads one live table's definition the way the schema loader
// reads every other, for a table the loader's naming filters left out.
//
// Spirit's loader cannot be asked to keep a named table its filters exclude.
// A loader option that exempts named tables from the naming filters would
// return these definitions with the rest and retire this read.
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

// dropTableWrite is what the target reports about the last write to one live
// drop_tables entry.
type dropTableWrite struct {
	table string
	// known is false when the server has no write time for the table.
	known bool
	// age is how long ago the table was last written. It is set only when
	// known is true.
	age time.Duration
}

// requireQuietDropTables refuses a plan unless the target shows that every
// live drop_tables entry has gone the quiet period without a write. A name
// alone does not prove a table is abandoned: the shadow table of an online
// schema change another tool is running looks like a leftover, and SchemaBot
// cannot see that tool's state. Rows still arriving is the one sign every such
// tool leaves on the target.
//
// The check reads information_schema.TABLES.UPDATE_TIME with the statistics
// cache disabled for the session, so the value is the server's own rather than
// one cached for up to a day. The server keeps that time in memory, so it has
// none for a table not written since the server started. For such a table the
// server's uptime is the evidence: a server that has been up for the whole
// quiet period would have recorded any write in it, and one that started more
// recently cannot say, so the plan is refused until it can.
//
// Two cases leave no write time and no sign that one is missing: the server
// dropped the table from its dictionary cache, which it does only to a table
// nothing has open, and a replica that was already running took over as the
// writer. A copy that is still running writes again after either, which sets
// the time. A copy that is stopped writes nothing and is not seen here at all.
func (e *Engine) requireQuietDropTables(ctx context.Context, db *sql.DB, database string, drops engine.DroppedTables) error {
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

	writes, err := readDropTableWrites(ctx, conn, database, drops.Entries())
	if err != nil {
		return fmt.Errorf("check when drop_tables entries were last written: %w", err)
	}
	var uptime time.Duration
	if hasUnknownWriteTime(writes) {
		uptime, err = readServerUptime(ctx, conn)
		if err != nil {
			return fmt.Errorf("check when drop_tables entries were last written: %w", err)
		}
	}
	return refuseDropsNotQuiet(writes, uptime, e.dropTablesQuietPeriod)
}

// readDropTableWrites reads how long ago each of the named tables that exists
// in the database was last written. A name with no table returns no row.
func readDropTableWrites(ctx context.Context, conn *sql.Conn, database string, names []string) ([]dropTableWrite, error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(names)), ",")
	args := make([]any, 0, len(names)+1)
	args = append(args, database)
	for _, name := range names {
		args = append(args, name)
	}
	// UPDATE_TIME and NOW() are both in the session's time zone, so their
	// difference does not depend on it.
	rows, err := conn.QueryContext(ctx,
		"SELECT TABLE_NAME, TIMESTAMPDIFF(SECOND, UPDATE_TIME, NOW()) FROM information_schema.TABLES "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN ("+placeholders+") ORDER BY TABLE_NAME",
		args...)
	if err != nil {
		return nil, fmt.Errorf("read UPDATE_TIME: %w", err)
	}
	defer utils.CloseAndLog(rows)
	var writes []dropTableWrite
	for rows.Next() {
		var name string
		var ageSeconds sql.NullInt64
		if err := rows.Scan(&name, &ageSeconds); err != nil {
			return nil, fmt.Errorf("read UPDATE_TIME: %w", err)
		}
		writes = append(writes, dropTableWrite{
			table: name,
			known: ageSeconds.Valid,
			age:   time.Duration(ageSeconds.Int64) * time.Second,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read UPDATE_TIME: %w", err)
	}
	return writes, nil
}

// hasUnknownWriteTime reports whether the server has no write time for at
// least one of the tables.
func hasUnknownWriteTime(writes []dropTableWrite) bool {
	for _, w := range writes {
		if !w.known {
			return true
		}
	}
	return false
}

// readServerUptime reads how long ago the target server started.
func readServerUptime(ctx context.Context, conn *sql.Conn) (time.Duration, error) {
	var name, value string
	if err := conn.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE 'Uptime'").Scan(&name, &value); err != nil {
		return 0, fmt.Errorf("read server uptime: %w", err)
	}
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("read server uptime: parse %q: %w", value, err)
	}
	return time.Duration(seconds) * time.Second, nil
}

// refuseDropsNotQuiet returns an error naming the tables that are not known to
// have gone the quiet period without a write: first the ones written inside
// it, and otherwise the ones with no write time on a server that started
// inside it. uptime is read only when a table has no write time.
func refuseDropsNotQuiet(writes []dropTableWrite, uptime, quietPeriod time.Duration) error {
	var written, unknown []string
	for _, w := range writes {
		switch {
		case !w.known:
			unknown = append(unknown, fmt.Sprintf("%q", w.table))
		case w.age < quietPeriod:
			written = append(written, fmt.Sprintf("%q (%s ago)", w.table, ui.FormatHumanDuration(w.age)))
		}
	}
	period := ui.FormatHumanDuration(quietPeriod)
	if len(written) > 0 {
		return fmt.Errorf("drop_tables entries were written in the last %s, so something may still be using them, such as an online schema change another tool is running: %s. Plan again once they have gone %s without a write, or remove them from drop_tables",
			period, strings.Join(written, ", "), period)
	}
	if len(unknown) > 0 && uptime < quietPeriod {
		return fmt.Errorf("SchemaBot cannot tell whether drop_tables entries %s were written in the last %s: the server has no write time for them and started %s ago, so it has no record of a write before that. Plan again in %s, or remove them from drop_tables",
			strings.Join(unknown, ", "), period, ui.FormatHumanDuration(uptime), ui.FormatHumanDuration(quietPeriod-uptime))
	}
	return nil
}
