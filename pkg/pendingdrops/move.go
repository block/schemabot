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
//
// Each intent carries the identifier of the apply or task run that recorded
// it. Proof of a landed rename is only valid for re-runs of that same run: a
// same-named table quarantined by an earlier, unrelated apply must not let a
// later apply treat an externally dropped table as executed.
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
func MoveTable(ctx context.Context, db *sql.DB, schemaName, tableName, runIdentifier string, now time.Time) (string, error) {
	moved, err := MoveTables(ctx, db, []TableMove{{SchemaName: schemaName, TableName: tableName}}, runIdentifier, now)
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
// ledger before the rename executes, tagged with the runIdentifier of the
// apply or task run performing the quarantine, so an interrupted run that
// re-executes the same DROP can prove the rename already landed via
// AlreadyQuarantined.
func MoveTables(ctx context.Context, db *sql.DB, tables []TableMove, runIdentifier string, now time.Time) ([]QuarantinedTable, error) {
	if len(tables) == 0 {
		return nil, nil
	}

	if err := ensureQuarantineSchema(ctx, db); err != nil {
		return nil, err
	}

	moved := quarantineDestinations(tables, now)
	if err := recordQuarantineIntents(ctx, db, moved, runIdentifier, now); err != nil {
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

// AlreadyQuarantined reports whether a prior execution of the same run
// already renamed the source table into the pending drops database. It
// returns the quarantine table name when the latest intent recorded by
// runIdentifier for the source table points at a destination that still
// exists. A missing ledger, a missing intent row, an intent recorded by a
// different run, or a recorded destination that no longer exists all report
// false: none of them prove this run's rename landed, so the caller must not
// treat the missing source table as quarantined.
func AlreadyQuarantined(ctx context.Context, db *sql.DB, schemaName, tableName, runIdentifier string) (string, bool, error) {
	ledgerExists, err := quarantineTableExists(ctx, db, IntentsTable)
	if err != nil {
		return "", false, fmt.Errorf("check quarantine intents ledger exists: %w", err)
	}
	if !ledgerExists {
		return "", false, nil
	}

	var quarantineTable string
	err = db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT quarantine_table FROM %s.%s WHERE source_schema = ? AND source_table = ? AND run_identifier = ? ORDER BY id DESC LIMIT 1",
		quoteIdentifier(Database), quoteIdentifier(IntentsTable)),
		schemaName, tableName, runIdentifier).Scan(&quarantineTable)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("query quarantine intent for `%s`.`%s`: %w", schemaName, tableName, err)
	}

	destinationExists, err := quarantineTableExists(ctx, db, quarantineTable)
	if err != nil {
		return "", false, fmt.Errorf("check quarantine destination `%s`.`%s` exists: %w", Database, quarantineTable, err)
	}
	if !destinationExists {
		return "", false, nil
	}
	return quarantineTable, true, nil
}

// IntentRecord describes the latest quarantine intent recorded for a source
// table by any run.
type IntentRecord struct {
	RunIdentifier     string
	QuarantineTable   string
	DestinationExists bool
}

// LatestIntent returns the most recent quarantine intent recorded for the
// source table regardless of which run recorded it. Callers use it to explain
// a failed convergence precisely: a missing source whose latest intent belongs
// to a different run means another apply quarantined a same-named table, and
// only an operator can decide whether that copy satisfies this drop.
func LatestIntent(ctx context.Context, db *sql.DB, schemaName, tableName string) (IntentRecord, bool, error) {
	ledgerExists, err := quarantineTableExists(ctx, db, IntentsTable)
	if err != nil {
		return IntentRecord{}, false, fmt.Errorf("check quarantine intents ledger exists: %w", err)
	}
	if !ledgerExists {
		return IntentRecord{}, false, nil
	}

	var record IntentRecord
	err = db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT run_identifier, quarantine_table FROM %s.%s WHERE source_schema = ? AND source_table = ? ORDER BY id DESC LIMIT 1",
		quoteIdentifier(Database), quoteIdentifier(IntentsTable)),
		schemaName, tableName).Scan(&record.RunIdentifier, &record.QuarantineTable)
	if errors.Is(err, sql.ErrNoRows) {
		return IntentRecord{}, false, nil
	}
	if err != nil {
		return IntentRecord{}, false, fmt.Errorf("query latest quarantine intent for `%s`.`%s`: %w", schemaName, tableName, err)
	}

	record.DestinationExists, err = quarantineTableExists(ctx, db, record.QuarantineTable)
	if err != nil {
		return IntentRecord{}, false, fmt.Errorf("check quarantine destination `%s`.`%s` exists: %w", Database, record.QuarantineTable, err)
	}
	return record, true, nil
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
  `+"`run_identifier`"+` varchar(255) NOT NULL,
  `+"`created_at`"+` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`+"`id`"+`),
  KEY `+"`idx_source`"+` (`+"`source_schema`"+`,`+"`source_table`"+`,`+"`run_identifier`"+`),
  KEY `+"`idx_created_at`"+` (`+"`created_at`"+`)
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
func recordQuarantineIntents(ctx context.Context, db *sql.DB, moved []QuarantinedTable, runIdentifier string, now time.Time) error {
	values := make([]string, 0, len(moved))
	args := make([]any, 0, len(moved)*5)
	for _, table := range moved {
		values = append(values, "(?, ?, ?, ?, ?)")
		args = append(args, table.SchemaName, table.TableName, table.QuarantineTable, runIdentifier, now.UTC())
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s.%s (source_schema, source_table, quarantine_table, run_identifier, created_at) VALUES %s",
		quoteIdentifier(Database), quoteIdentifier(IntentsTable), strings.Join(values, ", "))
	if _, err := db.ExecContext(ctx, insertSQL, args...); err != nil {
		return fmt.Errorf("record quarantine intents: %w", err)
	}
	return nil
}

// quarantineTableExists checks if a table exists in the pending drops database.
func quarantineTableExists(ctx context.Context, db *sql.DB, tableName string) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		Database, tableName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("query information_schema.tables for `%s`.`%s`: %w", Database, tableName, err)
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
