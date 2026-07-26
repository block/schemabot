package pendingdrops

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// IntentsTable is the ledger table inside the pending drops database that
// records each quarantine before its RENAME TABLE executes. A DROP phase
// re-runs in full after a mid-phase interruption, so when a re-run finds a
// source table missing, the latest intent row tells it whether the table was
// already quarantined (the recorded destination exists) or removed from
// outside SchemaBot (no record, or the destination is gone) — the latter must
// fail closed.
const IntentsTable = "quarantine_intents"

// TableMove is one source table to quarantine.
type TableMove struct {
	SchemaName string
	TableName  string
}

// QuarantinedTable records the quarantine destination for a source table.
type QuarantinedTable struct {
	SchemaName       string
	TableName        string
	QuarantineSchema string
	QuarantineTable  string
}

// MoveTable quarantines a table by renaming it into the _pending_drops
// database with a timestamp prefix instead of dropping it. It creates the
// _pending_drops database when missing. Returns the quarantine table name.
//
// RENAME TABLE is atomic and metadata-only, so the move is fast regardless of
// table size and the data is preserved until the cleaner's retention period
// expires.
func MoveTable(ctx context.Context, db *sql.DB, schemaName, tableName string, now time.Time) (string, error) {
	moved, err := MoveTables(ctx, db, []TableMove{{SchemaName: schemaName, TableName: tableName}}, now)
	if err != nil {
		return "", err
	}
	if len(moved) != 1 {
		return "", fmt.Errorf("expected 1 quarantined table, got %d", len(moved))
	}
	return moved[0].QuarantineTable, nil
}

// MoveTables quarantines source tables with a single atomic RENAME TABLE
// statement. Either all source tables move to the pending drops database or
// none of them do. Each destination is recorded in the quarantine intents
// ledger before the rename executes, so an interrupted run that re-executes
// the same DROP can prove the rename already landed via AlreadyQuarantined.
func MoveTables(ctx context.Context, db *sql.DB, tables []TableMove, now time.Time) ([]QuarantinedTable, error) {
	if len(tables) == 0 {
		return nil, nil
	}

	if err := ensureQuarantineSchema(ctx, db); err != nil {
		return nil, err
	}

	moved := quarantineDestinations(tables, now)
	if err := recordQuarantineIntents(ctx, db, moved, now); err != nil {
		return nil, err
	}
	renameSQL, err := renameStatement(moved)
	if err != nil {
		return nil, err
	}
	if _, err := db.ExecContext(ctx, renameSQL); err != nil {
		return nil, fmt.Errorf("rename tables to pending drops (query = %s): %w", renameSQL, err)
	}
	return moved, nil
}

// AlreadyQuarantined reports whether a prior MoveTables run already renamed
// the source table into the pending drops database. It returns the quarantine
// table name when the latest recorded intent for the source table points at a
// destination that still exists. A missing ledger, a missing intent row, or a
// recorded destination that no longer exists all report false: none of them
// prove the rename landed, so the caller must not treat the missing source
// table as quarantined.
func AlreadyQuarantined(ctx context.Context, db *sql.DB, schemaName, tableName string) (string, bool, error) {
	ledgerExists, err := tableExists(ctx, db, Database, IntentsTable)
	if err != nil {
		return "", false, fmt.Errorf("check quarantine intents ledger exists: %w", err)
	}
	if !ledgerExists {
		return "", false, nil
	}

	var quarantineTable string
	err = db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT quarantine_table FROM %s.%s WHERE source_schema = ? AND source_table = ? ORDER BY id DESC LIMIT 1",
		quoteIdentifier(Database), quoteIdentifier(IntentsTable)),
		schemaName, tableName).Scan(&quarantineTable)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("query quarantine intent for `%s`.`%s`: %w", schemaName, tableName, err)
	}

	destinationExists, err := tableExists(ctx, db, Database, quarantineTable)
	if err != nil {
		return "", false, fmt.Errorf("check quarantine destination `%s`.`%s` exists: %w", Database, quarantineTable, err)
	}
	if !destinationExists {
		return "", false, nil
	}
	return quarantineTable, true, nil
}

// ensureQuarantineSchema creates the pending drops database and the quarantine
// intents ledger when missing.
func ensureQuarantineSchema(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", quoteIdentifier(Database))); err != nil {
		return fmt.Errorf("create %s database: %w", Database, err)
	}
	createIntents := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
  `+"`id`"+` bigint unsigned NOT NULL AUTO_INCREMENT,
  `+"`source_schema`"+` varchar(64) NOT NULL,
  `+"`source_table`"+` varchar(64) NOT NULL,
  `+"`quarantine_table`"+` varchar(64) NOT NULL,
  `+"`created_at`"+` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`+"`id`"+`),
  KEY `+"`idx_source`"+` (`+"`source_schema`"+`,`+"`source_table`"+`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`,
		quoteIdentifier(Database), quoteIdentifier(IntentsTable))
	if _, err := db.ExecContext(ctx, createIntents); err != nil {
		return fmt.Errorf("create %s.%s ledger: %w", Database, IntentsTable, err)
	}
	return nil
}

// recordQuarantineIntents writes one ledger row per destination before the
// rename executes. A stale intent whose rename never ran is harmless: the
// source table still exists, so the next run renames it under a fresh
// destination and a newer intent row supersedes the stale one.
func recordQuarantineIntents(ctx context.Context, db *sql.DB, moved []QuarantinedTable, now time.Time) error {
	values := make([]string, 0, len(moved))
	args := make([]any, 0, len(moved)*4)
	for _, table := range moved {
		values = append(values, "(?, ?, ?, ?)")
		args = append(args, table.SchemaName, table.TableName, table.QuarantineTable, now.UTC())
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s.%s (source_schema, source_table, quarantine_table, created_at) VALUES %s",
		quoteIdentifier(Database), quoteIdentifier(IntentsTable), strings.Join(values, ", "))
	if _, err := db.ExecContext(ctx, insertSQL, args...); err != nil {
		return fmt.Errorf("record quarantine intents: %w", err)
	}
	return nil
}

// tableExists checks if a table exists in the given schema.
func tableExists(ctx context.Context, db *sql.DB, schemaName, tableName string) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		schemaName, tableName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("query information_schema.tables for `%s`.`%s`: %w", schemaName, tableName, err)
	}
	return count > 0, nil
}

func quarantineDestinations(tables []TableMove, now time.Time) []QuarantinedTable {
	moved := make([]QuarantinedTable, 0, len(tables))
	for _, table := range tables {
		moved = append(moved, QuarantinedTable{
			SchemaName:       table.SchemaName,
			TableName:        table.TableName,
			QuarantineSchema: Database,
			QuarantineTable:  TableName(table.SchemaName, table.TableName, now),
		})
	}
	return moved
}

func renameStatement(moved []QuarantinedTable) (string, error) {
	if len(moved) == 0 {
		return "", fmt.Errorf("at least one table is required")
	}
	parts := make([]string, 0, len(moved))
	for _, table := range moved {
		parts = append(parts, fmt.Sprintf("%s.%s TO %s.%s",
			quoteIdentifier(table.SchemaName), quoteIdentifier(table.TableName),
			quoteIdentifier(table.QuarantineSchema), quoteIdentifier(table.QuarantineTable)))
	}
	return "RENAME TABLE " + strings.Join(parts, ", "), nil
}

func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
