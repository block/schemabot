package pendingdrops

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

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
// none of them do.
func MoveTables(ctx context.Context, db *sql.DB, tables []TableMove, now time.Time) ([]QuarantinedTable, error) {
	moved := Destinations(tables, now)
	if err := MoveTablesTo(ctx, db, moved); err != nil {
		return nil, err
	}
	return moved, nil
}

// MoveTablesTo renames each source table to the quarantine destination chosen
// for it by Destinations, in a single atomic RENAME TABLE statement. A caller
// that must know the destinations before the rename is issued, because the
// rename may complete on the server after the client has given up on it,
// computes them with Destinations, records them, and then calls this.
func MoveTablesTo(ctx context.Context, db *sql.DB, moved []QuarantinedTable) error {
	if len(moved) == 0 {
		return nil
	}

	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", quoteIdentifier(Database))); err != nil {
		return fmt.Errorf("create %s database: %w", Database, err)
	}

	renameSQL, err := renameStatement(moved)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, renameSQL); err != nil {
		return fmt.Errorf("rename tables to pending drops (query = %s): %w", renameSQL, err)
	}
	return nil
}

// Destinations returns the quarantine destination for each source table,
// stamped with now. It touches no database, so the same tables and now always
// produce the same destinations.
func Destinations(tables []TableMove, now time.Time) []QuarantinedTable {
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
