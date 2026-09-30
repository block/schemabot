// pendingdrops.go quarantines dropped tables instead of executing DROP TABLE.
// The table is renamed into the pending drops database with a timestamp prefix
// so its data stays recoverable until the retention period expires.
package spirit

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/pendingdrops"
)

// quarantineDroppedTables executes a DROP TABLE statement as a quarantine:
// every table named in the statement is renamed into the pending drops
// database instead of being dropped. IF EXISTS semantics are preserved —
// missing tables are skipped when the statement allows it — and a table this
// schema change already quarantined is skipped when the DROP phase replays.
func (e *Engine) quarantineDroppedTables(ctx context.Context, host, username, password, database, stmt string) error {
	dropStmt, err := parseDropTableStatement(stmt)
	if err != nil {
		return err
	}
	// DROP VIEW and DROP TEMPORARY TABLE also parse as DropTableStmt. Neither
	// holds recoverable table data, and neither can be renamed into the pending
	// drops database with table semantics, so execute them as written.
	if isNonTableDrop(dropStmt) {
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

	db, err := mysqlconn.Open(targetDSN(host, username, password, database))
	if err != nil {
		return fmt.Errorf("open database %s: %w", database, err)
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping database %s: %w", database, err)
	}

	targets := dropTableTargets(dropStmt, database)
	tables := make([]pendingdrops.TableMove, 0, len(targets))
	for _, target := range targets {
		exists, err := tableExistsInSchema(ctx, db, target.schema, target.table)
		if err != nil {
			return fmt.Errorf("check table `%s`.`%s` exists: %w", target.schema, target.table, err)
		}
		if !exists {
			if err := e.resolveMissingDropTarget(ctx, db, target, dropStmt.IfExists); err != nil {
				return err
			}
			continue
		}
		tables = append(tables, pendingdrops.TableMove{SchemaName: target.schema, TableName: target.table})
	}

	// The destinations are recorded before the RENAME is issued, not after it
	// returns. A RENAME that is waiting on a metadata lock when the schema
	// change is stopped is abandoned by the client with a context error, yet
	// the server still completes it once the lock is released, so the table
	// ends up in pending drops with nothing to say this attempt put it there.
	// Recording first keeps the replay honest either way: if the RENAME never
	// landed the source table still exists and is quarantined again, which
	// overwrites the record; if it landed the recorded copy exists and the
	// replay skips the table.
	moved := pendingdrops.Destinations(tables, time.Now())
	for _, table := range moved {
		e.recordQuarantinedDrop(table)
	}
	if err := pendingdrops.MoveTablesTo(ctx, db, moved); err != nil {
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
		e.emitTableLog(table.TableName,
			fmt.Sprintf("table quarantined as `%s`.`%s`; recoverable until the pending drops retention period expires",
				table.QuarantineSchema, table.QuarantineTable))
		metrics.RecordPendingDropMoved(ctx, table.SchemaName)
	}
	return nil
}

// resolveMissingDropTarget decides what to do with a DROP TABLE target that is
// no longer in the target database. The DROP phase replays from its first
// statement when a stopped schema change resumes through the engine's own
// Start, so a table this attempt already quarantined is found missing on the
// replay. That is the state the plan asked for, with the data in pending
// drops, so it is skipped. IF EXISTS tolerates any other missing table, as
// MySQL does. A missing table this attempt never quarantined fails the
// statement: SchemaBot holds no copy of it, and reporting the drop done would
// tell an operator the data is recoverable when it is not.
//
// The record lives on the running schema change, in this process. A resume
// that goes through Apply, on this server or another, starts with an empty
// record; the driver plans that resume again against the live schema first,
// so a table an earlier attempt quarantined is no longer in its diff and this
// path never sees it.
func (e *Engine) resolveMissingDropTarget(ctx context.Context, db *sql.DB, target dropTarget, ifExists bool) error {
	logger := e.changeLogger()
	if prior, ok := e.quarantinedDrop(target); ok {
		exists, err := tableExistsInSchema(ctx, db, prior.QuarantineSchema, prior.QuarantineTable)
		if err != nil {
			return fmt.Errorf("check recorded pending drops table `%s` exists: %w", prior.QuarantineTable, err)
		}
		if !exists {
			// The record is written before the RENAME, so a missing copy means
			// either that retention has removed a copy that did land or that
			// the RENAME never landed and the table went missing some other
			// way. Neither leaves SchemaBot holding the data, so the statement
			// fails rather than report a drop that is final as recoverable.
			return engine.OperatorErrorf(nil,
				"DROP TABLE target `%s` does not exist and the pending drops copy this attempt recorded for it, `%s`.`%s`, is not present: "+
					"either the pending drops retention period has removed it or the quarantine never completed. "+
					"The drop is final and its data is not recoverable through SchemaBot; plan the schema change again against the live schema before resuming it, so a table that is already gone is not dropped again",
				target.table, prior.QuarantineSchema, prior.QuarantineTable)
		}
		logger.Info("DROP TABLE target was already quarantined by this attempt, skipping quarantine",
			"database", target.schema,
			"table", target.table,
			"quarantine_database", prior.QuarantineSchema,
			"quarantine_table", prior.QuarantineTable,
		)
		e.emitTableLog(target.table,
			fmt.Sprintf("table already quarantined as `%s`.`%s` earlier in this schema change; nothing left to quarantine",
				prior.QuarantineSchema, prior.QuarantineTable))
		return nil
	}
	if ifExists {
		logger.Info("DROP TABLE IF EXISTS target does not exist, skipping quarantine",
			"database", target.schema,
			"table", target.table,
		)
		return nil
	}
	return engine.OperatorErrorf(nil,
		"DROP TABLE target `%s` does not exist and was not quarantined by this attempt of the schema change",
		target.table)
}

// recordQuarantinedDrop remembers that the running schema change is moving a
// table into pending drops, so a replay of the DROP phase recognizes the
// table as its own work rather than as a table that vanished. The record is
// keyed by the exact source schema and table names the DROP statement
// carries, the same exact matching the differ applies to table names. A
// replay re-parses the same statement text, so the key matches; a statement
// edited between attempts to spell the name in a different case, which MySQL
// treats as the same table under lower_case_table_names=1, misses the record
// and takes the failing path.
func (e *Engine) recordQuarantinedDrop(table pendingdrops.QuarantinedTable) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rm := e.runningSchemaChange
	if rm == nil {
		// Only a concurrent Drain clears the tracked change while a
		// quarantine is in flight. Losing the record is not a reason to fail
		// the quarantine itself; the consequence is confined to a later replay.
		e.logger.Warn("no running schema change to record quarantined table on; a replayed DROP phase will fail on this table",
			"database", table.SchemaName,
			"table", table.TableName,
			"quarantine_table", table.QuarantineTable,
		)
		return
	}
	if rm.quarantinedDrops == nil {
		rm.quarantinedDrops = make(map[dropTarget]pendingdrops.QuarantinedTable)
	}
	rm.quarantinedDrops[dropTarget{schema: table.SchemaName, table: table.TableName}] = table
}

// quarantinedDrop returns where the running schema change quarantined target,
// if it did.
func (e *Engine) quarantinedDrop(target dropTarget) (pendingdrops.QuarantinedTable, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runningSchemaChange == nil {
		return pendingdrops.QuarantinedTable{}, false
	}
	prior, ok := e.runningSchemaChange.quarantinedDrops[target]
	return prior, ok
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
