package ddl

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

// SplitStatements splits SQL content into individual DDL statements.
// Uses Spirit's statement package which wraps the TiDB parser for proper parsing.
// All SQL content must be parseable by the TiDB parser.
func SplitStatements(content string) ([]string, error) {
	content = strings.TrimSpace(content)
	if content == "" {
		return nil, nil
	}
	parsed, err := statement.NewWithOptions(content, statement.Options{
		AllowMixedStatementTypes: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL statements: %w", err)
	}
	var stmts []string
	for _, s := range parsed {
		stmt := strings.TrimSpace(s.Statement)
		if stmt != "" {
			stmts = append(stmts, stmt)
		}
	}
	return stmts, nil
}

// ClassifyStatement classifies a DDL statement using Spirit's parser.
// Returns the typed StatementType and table name. Handles the Classify
// boilerplate (nil check, empty results) so callers don't have to.
func ClassifyStatement(stmt string) (statement.StatementType, string, error) {
	results, err := statement.Classify(stmt)
	if err != nil {
		return statement.StatementUnknown, "", fmt.Errorf("classify statement %q: %w", stmt, err)
	}
	if len(results) == 0 {
		return statement.StatementUnknown, "", fmt.Errorf("no classification result for statement %q", stmt)
	}
	return results[0].Type, results[0].Table, nil
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
