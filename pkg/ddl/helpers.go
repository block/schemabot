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

// ClassifyStatementAST uses Spirit's Classify for accurate DDL classification.
// Returns (operation, tableName, error) where operation is "create", "alter", "drop", "rename", or "unknown".
func ClassifyStatementAST(stmt string) (string, string, error) {
	results, err := statement.Classify(stmt)
	if err != nil {
		return "", "", fmt.Errorf("failed to classify statement: %w", err)
	}
	c := results[0]
	return StatementTypeToOp(c.Type), c.Table, nil
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
