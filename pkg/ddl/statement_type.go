package ddl

import "strings"

// StatementType identifies the kind of SQL statement a StatementParser
// classified. It is owned by pkg/ddl — not by any dialect's parser library —
// so callers can branch on statement kinds without importing a
// dialect-specific parser, and a non-MySQL StatementParser implementation can
// classify into the same vocabulary.
type StatementType int

const (
	StatementUnknown StatementType = iota
	StatementAlterTable
	StatementCreateTable
	StatementDropTable
	StatementRenameTable
	StatementTruncateTable
	StatementCreateIndex
	StatementDropIndex
	StatementCreateView
	StatementInsert
	StatementUpdate
	StatementDelete
)

// String returns the human-readable name for a StatementType.
func (t StatementType) String() string {
	switch t {
	case StatementUnknown:
		return "UNKNOWN"
	case StatementAlterTable:
		return "ALTER TABLE"
	case StatementCreateTable:
		return "CREATE TABLE"
	case StatementDropTable:
		return "DROP TABLE"
	case StatementRenameTable:
		return "RENAME TABLE"
	case StatementTruncateTable:
		return "TRUNCATE TABLE"
	case StatementCreateIndex:
		return "CREATE INDEX"
	case StatementDropIndex:
		return "DROP INDEX"
	case StatementCreateView:
		return "CREATE VIEW"
	case StatementInsert:
		return "INSERT"
	case StatementUpdate:
		return "UPDATE"
	case StatementDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// IsDDL reports whether the statement type is a schema-changing (DDL)
// statement, as opposed to DML or an unclassified statement. StatementUnknown
// is not DDL: a parser that classified a statement outside the shared
// vocabulary reports it as unknown, and callers gating on IsDDL must treat it
// as unverifiable rather than assume it changes schema.
func (t StatementType) IsDDL() bool {
	switch t {
	case StatementAlterTable, StatementCreateTable, StatementDropTable,
		StatementRenameTable, StatementTruncateTable, StatementCreateIndex,
		StatementDropIndex, StatementCreateView:
		return true
	default:
		return false
	}
}

// StatementTypeToOp converts a StatementType to the lowercase operation
// string used in storage and API layers.
func StatementTypeToOp(t StatementType) string {
	switch t {
	case StatementCreateTable:
		return "create"
	case StatementAlterTable:
		return "alter"
	case StatementDropTable:
		return "drop"
	case StatementRenameTable:
		return "rename"
	case StatementTruncateTable:
		return "truncate"
	case StatementCreateIndex:
		return "create_index"
	case StatementDropIndex:
		return "drop_index"
	case StatementCreateView:
		return "create_view"
	default:
		return "unknown"
	}
}

// OpToStatementType converts a storage operation string back to a
// StatementType. Used when reading from storage/proto boundaries.
func OpToStatementType(op string) StatementType {
	switch strings.ToLower(op) {
	case "create":
		return StatementCreateTable
	case "alter":
		return StatementAlterTable
	case "drop":
		return StatementDropTable
	case "rename":
		return StatementRenameTable
	case "truncate":
		return StatementTruncateTable
	case "create_index":
		return StatementCreateIndex
	case "drop_index":
		return StatementDropIndex
	case "create_view":
		return StatementCreateView
	default:
		return StatementUnknown
	}
}
