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
	switch c.Type {
	case statement.StatementCreateTable:
		return "create", c.Table, nil
	case statement.StatementAlterTable:
		return "alter", c.Table, nil
	case statement.StatementDropTable:
		return "drop", c.Table, nil
	case statement.StatementRenameTable:
		return "rename", c.Table, nil
	default:
		return "unknown", c.Table, nil
	}
}
