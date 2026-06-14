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
// missing tables are skipped when the statement allows it.
func (e *Engine) quarantineDroppedTables(ctx context.Context, host, username, password, database, stmt string) error {
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

		if dropStmt.IfExists {
			exists, err := tableExistsInSchema(ctx, db, schemaName, tableName)
			if err != nil {
				return fmt.Errorf("check table `%s`.`%s` exists: %w", schemaName, tableName, err)
			}
			if !exists {
				e.logger.Info("DROP TABLE IF EXISTS target does not exist, skipping quarantine",
					"database", schemaName,
					"table", tableName,
				)
				continue
			}
		}
		tables = append(tables, pendingdrops.TableMove{SchemaName: schemaName, TableName: tableName})
	}

	moved, err := pendingdrops.MoveTables(ctx, db, tables, time.Now())
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
		// Route the quarantine location to the apply log so operators can find
		// the table for recovery without querying information_schema.
		e.mu.Lock()
		onLog := e.onLog
		e.mu.Unlock()
		if onLog != nil {
			onLog(slog.LevelInfo, table.TableName,
				fmt.Sprintf("table quarantined as `%s`.`%s`; recoverable until the pending drops retention period expires",
					table.QuarantineSchema, table.QuarantineTable))
		}
		metrics.RecordPendingDropMoved(ctx, table.SchemaName)
	}
	return nil
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
