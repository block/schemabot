package pendingdrops

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/block/spirit/pkg/utils"
)

// LogTable is the table in the pending drops database that records which apply
// quarantined each table. Its name carries no timestamp prefix, so the cleaner
// never mistakes it for a quarantined table.
const LogTable = "schemabot_quarantine_log"

// createLogTableSQL creates the quarantine log. Source names and the apply
// identifier compare exactly, the way the quarantine records its source.
const createLogTableSQL = "CREATE TABLE IF NOT EXISTS `" + Database + "`.`" + LogTable + "` (" +
	"`quarantine_table` varchar(64) NOT NULL, " +
	"`source_schema` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, " +
	"`source_table` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, " +
	"`apply_identifier` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, " +
	"`quarantined_at` datetime(3) NOT NULL, " +
	"PRIMARY KEY (`quarantine_table`), " +
	"KEY `idx_apply_source` (`apply_identifier`, `source_schema`, `source_table`)" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

// RecordQuarantines records in the target's quarantine log that the apply named
// applyIdentifier is moving each table in moved into pending drops. It creates
// the pending drops database and the log when they are missing.
//
// Record before the RENAME, not after it returns: a RENAME a stopped schema
// change abandoned can still land on the server, and a record written only on
// success would leave that copy unexplained. A record whose RENAME never landed
// names a copy that does not exist, which every reader checks for, and the
// cleaner removes it once the copy's retention has passed.
func RecordQuarantines(ctx context.Context, db *sql.DB, applyIdentifier string, moved []QuarantinedTable, now time.Time) error {
	if applyIdentifier == "" {
		return errors.New("record quarantines: apply identifier is required")
	}
	if len(moved) == 0 {
		return nil
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", quoteIdentifier(Database))); err != nil {
		return fmt.Errorf("create %s database: %w", Database, err)
	}
	if _, err := db.ExecContext(ctx, createLogTableSQL); err != nil {
		return fmt.Errorf("create %s.%s: %w", Database, LogTable, err)
	}
	insert := "INSERT INTO `" + Database + "`.`" + LogTable + "` " +
		"(quarantine_table, source_schema, source_table, apply_identifier, quarantined_at) VALUES (?, ?, ?, ?, ?)"
	for _, table := range moved {
		if _, err := db.ExecContext(ctx, insert, table.QuarantineTable, table.SchemaName, table.TableName, applyIdentifier, now.UTC()); err != nil {
			return fmt.Errorf("record quarantine of %s.%s as %s for apply %s: %w",
				table.SchemaName, table.TableName, table.QuarantineTable, applyIdentifier, err)
		}
	}
	return nil
}

// RecordedQuarantines returns every quarantine the apply named applyIdentifier
// recorded for schemaName.tableName, oldest first, or none when the target has
// no quarantine log. A record says only that the apply set out to move the
// table: the caller checks that the copy it names exists.
func RecordedQuarantines(ctx context.Context, db *sql.DB, applyIdentifier, schemaName, tableName string) ([]QuarantinedTable, error) {
	exists, err := logTableExists(ctx, db)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	rows, err := db.QueryContext(ctx,
		"SELECT quarantine_table FROM `"+Database+"`.`"+LogTable+"` "+
			"WHERE apply_identifier = ? AND source_schema = ? AND source_table = ? ORDER BY quarantined_at, quarantine_table",
		applyIdentifier, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("read recorded quarantines of %s.%s for apply %s: %w", schemaName, tableName, applyIdentifier, err)
	}
	defer utils.CloseAndLog(rows)
	var recorded []QuarantinedTable
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan recorded quarantine of %s.%s: %w", schemaName, tableName, err)
		}
		recorded = append(recorded, QuarantinedTable{
			SchemaName: schemaName, TableName: tableName, QuarantineSchema: Database, QuarantineTable: name,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recorded quarantines of %s.%s: %w", schemaName, tableName, err)
	}
	return recorded, nil
}

func logTableExists(ctx context.Context, db *sql.DB) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		Database, LogTable).Scan(&count); err != nil {
		return false, fmt.Errorf("check for %s.%s: %w", Database, LogTable, err)
	}
	return count > 0, nil
}

// expiredLogRecords returns the log records the cleaner may delete, given the
// pending drops tables still present after the pass (see prunableLogRecords).
func expiredLogRecords(ctx context.Context, conn *sql.Conn, tables []string, cutoff time.Time) ([]string, error) {
	rows, err := conn.QueryContext(ctx, "SELECT quarantine_table FROM `"+Database+"`.`"+LogTable+"`")
	if err != nil {
		return nil, fmt.Errorf("read %s.%s: %w", Database, LogTable, err)
	}
	defer utils.CloseAndLog(rows)
	var recorded []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan %s.%s record: %w", Database, LogTable, err)
		}
		recorded = append(recorded, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s.%s: %w", Database, LogTable, err)
	}
	return prunableLogRecords(recorded, tables, cutoff), nil
}

// prunableLogRecords returns the recorded copies whose log records may be
// deleted: copies past retention that are no longer among remaining, either
// because a pass dropped them or because their RENAME never landed. A record
// of a copy that still exists stays until the copy is dropped.
//
// Names are matched case-insensitively, as Destinations chooses them: a server
// with lower_case_table_names set lists a copy under a folded name while its
// record keeps the casing the quarantine chose, and an exact match would read
// that live copy as gone and delete the only record of which apply moved it.
func prunableLogRecords(recorded, remaining []string, cutoff time.Time) []string {
	existing := make(map[string]struct{}, len(remaining))
	for _, name := range remaining {
		existing[strings.ToLower(name)] = struct{}{}
	}
	var prunable []string
	for _, name := range recorded {
		if _, ok := existing[strings.ToLower(name)]; ok {
			continue
		}
		quarantinedAt, ok := ParseTimestamp(name)
		if !ok || !quarantinedAt.Before(cutoff) {
			continue
		}
		prunable = append(prunable, name)
	}
	return prunable
}
