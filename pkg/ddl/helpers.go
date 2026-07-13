package ddl

import (
	"strings"

	"github.com/block/spirit/pkg/statement"
)

// SplitStatements splits SQL content into individual DDL statements.
// It delegates to the package's default StatementParser (the TiDB/Spirit
// implementation), so all SQL content must be parseable by that parser.
func SplitStatements(content string) ([]string, error) {
	return defaultParser.Split(content)
}

// ClassifyStatement classifies a single DDL statement using Spirit's parser.
// Returns the typed StatementType and table name. Handles the Classify
// boilerplate (nil check, empty results) so callers don't have to.
//
// The input must be exactly one statement: a compound string could hide a
// destructive statement behind the classification of the first one, so
// multi-statement input is rejected. Callers with multi-statement content
// must split it with SplitStatements first.
func ClassifyStatement(stmt string) (statement.StatementType, string, error) {
	return defaultParser.Classify(stmt)
}

// statementPreview returns the leading text of a statement for error messages,
// truncated so multi-statement blobs do not flood logs. Truncation counts
// runes, not bytes, so multi-byte identifiers are never split into invalid
// UTF-8.
func statementPreview(stmt string) string {
	const maxPreview = 80
	s := strings.TrimSpace(stmt)
	runes := []rune(s)
	if len(runes) <= maxPreview {
		return s
	}
	return string(runes[:maxPreview]) + "..."
}

// ClassifyStatementOp is like ClassifyStatement but returns the operation as a
// lowercase string ("create", "alter", "drop") for storage/API boundaries.
func ClassifyStatementOp(stmt string) (string, string, error) {
	t, table, err := ClassifyStatement(stmt)
	if err != nil {
		return "", "", err
	}
	return StatementTypeToOp(t), table, nil
}

// StatementTypeToOp converts a Spirit StatementType to the lowercase operation
// string used in storage and API layers ("create", "alter", "drop", "rename").
func StatementTypeToOp(t statement.StatementType) string {
	switch t {
	case statement.StatementCreateTable:
		return "create"
	case statement.StatementAlterTable:
		return "alter"
	case statement.StatementDropTable:
		return "drop"
	case statement.StatementRenameTable:
		return "rename"
	default:
		return "unknown"
	}
}

// OpToStatementType converts a storage operation string back to a Spirit
// StatementType. Used when reading from storage/proto boundaries.
func OpToStatementType(op string) statement.StatementType {
	switch strings.ToLower(op) {
	case "create":
		return statement.StatementCreateTable
	case "alter":
		return statement.StatementAlterTable
	case "drop":
		return statement.StatementDropTable
	case "rename":
		return statement.StatementRenameTable
	default:
		return statement.StatementUnknown
	}
}
