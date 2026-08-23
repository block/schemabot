package ddl

import (
	"strings"
)

// SplitStatements splits SQL content into individual DDL statements.
// It delegates to the package's default StatementParser (the TiDB/Spirit
// implementation), so all SQL content must be parseable by that parser.
func SplitStatements(content string) ([]string, error) {
	return defaultParser.Split(content)
}

// ClassifyStatement classifies a single DDL statement using the package's
// default StatementParser. Returns the typed StatementType and table name.
// Handles the Classify boilerplate (nil check, empty results) so callers
// don't have to.
//
// The input must be exactly one statement: a compound string could hide a
// destructive statement behind the classification of the first one, so
// multi-statement input is rejected. Callers with multi-statement content
// must split it with SplitStatements first.
func ClassifyStatement(stmt string) (StatementType, string, error) {
	return defaultParser.Classify(stmt)
}

// statementPreview returns the leading text of a statement for error messages,
// truncated so multi-statement blobs do not flood logs. Truncation counts
// runes, not bytes, so multi-byte identifiers are never split into invalid
// UTF-8.
func statementPreview(stmt string) string {
	const maxPreview = 80
	s := strings.TrimSpace(stmt)
	count := 0
	for i := range s {
		if count == maxPreview {
			return s[:i] + "..."
		}
		count++
	}
	return s
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
