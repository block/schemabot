// pendingdrops.go quarantines dropped tables instead of executing DROP TABLE.
// The table is renamed into the pending drops database with a timestamp prefix
// so its data stays recoverable until the retention period expires.
package spirit

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/parser/ast"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/pendingdrops"
)

// quarantineDroppedTables executes a DROP TABLE statement as a quarantine:
// every table named in the statement is renamed into the pending drops
// database instead of being dropped. IF EXISTS semantics are preserved —
// missing tables are skipped when the statement allows it. runIdentifier
// scopes the quarantine intent ledger to this engine run, so a re-run of the
// same apply can prove its own rename landed without accepting a same-named
// quarantine from an unrelated apply as proof.
func (e *Engine) quarantineDroppedTables(ctx context.Context, host, username, password, database, stmt, runIdentifier string) error {
	parsed, err := statement.New(stmt)
	if err != nil {
		return fmt.Errorf("parse DROP TABLE statement: %w", err)
	}
	if len(parsed) != 1 {
		return fmt.Errorf("expected exactly 1 parsed DROP TABLE statement, got %d", len(parsed))
	}
	dropStmt, ok := (*parsed[0].StmtNode).(*ast.DropTableStmt)
	if !ok {
		return fmt.Errorf("statement is not DROP TABLE: %s", stmt)
	}
	// DROP VIEW and DROP TEMPORARY TABLE also parse as DropTableStmt. Neither
	// holds recoverable table data, and neither can be renamed into the pending
	// drops database with table semantics, so execute them as written.
	if dropStmt.IsView || dropStmt.TemporaryKeyword != ast.TemporaryNone {
		e.logger.Info("executing non-table drop directly without pending drops quarantine",
			"database", database,
			"statement", stmt,
			"is_view", dropStmt.IsView,
			"temporary_keyword", dropStmt.TemporaryKeyword,
		)
		if err := e.executeSingleStatement(ctx, host, username, password, database, stmt); err != nil {
			return fmt.Errorf("execute non-table drop directly: %w", err)
		}
		return nil
	}

	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = host
	cfg.User = username
	cfg.Passwd = password
	cfg.DBName = database

	db, err := mysqlconn.Open(cfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("open database %s: %w", database, err)
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping database %s: %w", database, err)
	}

	tables := make([]pendingdrops.TableMove, 0, len(dropStmt.Tables))
	for _, table := range dropStmt.Tables {
		tableName := table.Name.String()
		schemaName := table.Schema.String()
		if schemaName == "" {
			schemaName = database
		}

		exists, err := tableExistsInSchema(ctx, db, schemaName, tableName)
		if err != nil {
			return fmt.Errorf("check table `%s`.`%s` exists: %w", schemaName, tableName, err)
		}
		if exists {
			tables = append(tables, pendingdrops.TableMove{SchemaName: schemaName, TableName: tableName})
			continue
		}

		if dropStmt.IfExists {
			e.logger.Info("DROP TABLE IF EXISTS target does not exist, skipping quarantine",
				"database", schemaName,
				"table", tableName,
			)
			continue
		}

		// The DROP phase re-runs in full after a mid-phase interruption (pod
		// loss, lease handover, a stop landing between the quarantine rename
		// and completion being recorded). A missing source whose quarantine
		// destination was recorded by this same run and still exists means
		// this statement already executed: the table is gone from the source
		// schema and its data is preserved. A missing source without that
		// proof is drift from outside this apply and fails closed — including
		// when a same-named table was quarantined by a different apply, whose
		// copy holds that apply's data, not this one's.
		quarantineTable, quarantined, err := pendingdrops.AlreadyQuarantined(ctx, db, schemaName, tableName, runIdentifier)
		if err != nil {
			return fmt.Errorf("look up quarantine record for `%s`.`%s`: %w", schemaName, tableName, err)
		}
		if !quarantined {
			return e.missingDropTargetError(ctx, db, schemaName, tableName, runIdentifier)
		}
		e.logger.Info("DROP TABLE target already quarantined in pending drops, treating the statement as executed",
			"database", schemaName,
			"table", tableName,
			"quarantine_database", pendingdrops.Database,
			"quarantine_table", quarantineTable,
		)
		e.notifyQuarantineLocation(tableName, pendingdrops.Database, quarantineTable)
	}

	moved, err := pendingdrops.MoveTables(ctx, db, tables, runIdentifier, time.Now())
	if err != nil {
		return fmt.Errorf("quarantine DROP TABLE targets: %w", err)
	}
	for _, table := range moved {
		e.logger.Info("DROP TABLE intercepted: table quarantined in pending drops and recoverable until the retention period expires",
			"database", table.SchemaName,
			"table", table.TableName,
			"quarantine_database", table.QuarantineSchema,
			"quarantine_table", table.QuarantineTable,
		)
		e.notifyQuarantineLocation(table.TableName, table.QuarantineSchema, table.QuarantineTable)
		metrics.RecordPendingDropMoved(ctx, table.SchemaName)
	}
	return nil
}

// missingDropTargetError builds the fail-closed error for a DROP TABLE target
// that is missing without proof that this run quarantined it. The causes are
// distinguished so an operator can act on the message alone: no record at all
// means the table was removed outside SchemaBot; a record from a different
// run points at another apply's quarantined copy to reconcile against; a
// record from this run whose destination is gone means the quarantined copy
// itself was removed.
func (e *Engine) missingDropTargetError(ctx context.Context, db *sql.DB, schemaName, tableName, runIdentifier string) error {
	record, found, err := pendingdrops.LatestIntent(ctx, db, schemaName, tableName)
	if err != nil {
		return fmt.Errorf("look up latest quarantine intent for `%s`.`%s`: %w", schemaName, tableName, err)
	}
	switch {
	case !found:
		return fmt.Errorf("DROP TABLE target `%s`.`%s` does not exist and has no quarantined copy recorded in `%s`",
			schemaName, tableName, pendingdrops.Database)
	case record.RunIdentifier != runIdentifier:
		return fmt.Errorf("DROP TABLE target `%s`.`%s` does not exist; the latest quarantine record in `%s` belongs to a different run (%q, destination `%s` exists: %t), so this run cannot claim it — reconcile the target schema manually",
			schemaName, tableName, pendingdrops.Database, record.RunIdentifier, record.QuarantineTable, record.DestinationExists)
	default:
		return fmt.Errorf("DROP TABLE target `%s`.`%s` does not exist and its recorded quarantine destination `%s`.`%s` is gone — the quarantined copy was removed outside this apply",
			schemaName, tableName, pendingdrops.Database, record.QuarantineTable)
	}
}

// notifyQuarantineLocation routes the quarantine location to the apply log so
// operators can find the table for recovery without querying
// information_schema.
func (e *Engine) notifyQuarantineLocation(tableName, quarantineSchema, quarantineTable string) {
	e.mu.Lock()
	onLog := e.onLog
	e.mu.Unlock()
	if onLog == nil {
		return
	}
	onLog(slog.LevelInfo, tableName,
		fmt.Sprintf("table quarantined as `%s`.`%s`; recoverable until the pending drops retention period expires",
			quarantineSchema, quarantineTable))
}

// tableExistsInSchema checks if a table exists in the given schema.
func tableExistsInSchema(ctx context.Context, db *sql.DB, schemaName, tableName string) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		schemaName, tableName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("query information_schema.tables for `%s`.`%s`: %w", schemaName, tableName, err)
	}
	return count > 0, nil
}
