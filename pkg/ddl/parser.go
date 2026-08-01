package ddl

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

// StatementParser abstracts the dialect-specific, string-level DDL parsing
// operations that pkg/ddl exposes to its callers (schema discovery, planning,
// the CLI): splitting a schema file into statements, classifying a statement,
// and canonicalizing one.
//
// The default implementation wraps the TiDB parser (via Spirit's statement
// package), so behavior for MySQL and Vitess is exactly what it has always
// been. A non-MySQL dialect (e.g. Postgres) can supply its own implementation
// without changing any pkg/ddl caller.
//
// The seam is intentionally scoped to these string-level operations. The
// TiDB-AST-based differ, validators, and linters remain dialect-specific by
// design — a non-MySQL engine supplies its own equivalents rather than reusing
// the MySQL ones.
type StatementParser interface {
	// Split divides SQL file content into individual DDL statement strings.
	// Empty content yields a nil slice and no error.
	Split(content string) ([]string, error)

	// Classify returns the statement type and table name for a single
	// statement. It rejects multi-statement input so a destructive statement
	// cannot hide behind the classification of the first one.
	Classify(stmt string) (statement.StatementType, string, error)

	// Canonicalize normalizes a single DDL statement's formatting, returning
	// the input unchanged when it cannot be parsed.
	Canonicalize(ddl string) string
}

// defaultParser backs the package-level SplitStatements, ClassifyStatement, and
// Canonicalize helpers. It is the TiDB/Spirit implementation so existing MySQL
// and Vitess behavior is unchanged. Later work (Postgres support) selects a
// dialect-specific parser at the call sites that know the database type.
var defaultParser StatementParser = tidbStatementParser{}

// tidbStatementParser implements StatementParser over the TiDB parser via
// Spirit's statement package — the behavior pkg/ddl has always had.
type tidbStatementParser struct{}

// Split implements StatementParser.
func (tidbStatementParser) Split(content string) ([]string, error) {
	content = strings.TrimSpace(content)
	if content == "" {
		return nil, nil
	}
	parsed, err := statement.NewWithOptions(content, statement.Options{
		AllowMixedStatementTypes: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL statements %q: %w", statementPreview(content), err)
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

// Classify implements StatementParser.
func (tidbStatementParser) Classify(stmt string) (statement.StatementType, string, error) {
	results, err := statement.Classify(stmt)
	if err != nil {
		return statement.StatementUnknown, "", fmt.Errorf("classify statement %q: %w", statementPreview(stmt), err)
	}
	if len(results) == 0 {
		return statement.StatementUnknown, "", fmt.Errorf("no classification result for statement %q", statementPreview(stmt))
	}
	if len(results) > 1 {
		return statement.StatementUnknown, "", fmt.Errorf(
			"expected a single statement but %q parsed as %d statements; split with SplitStatements before classifying",
			statementPreview(stmt), len(results),
		)
	}
	return results[0].Type, results[0].Table, nil
}

// Canonicalize implements StatementParser.
//
// For ALTER TABLE statements it reconstructs from Spirit's normalized Alter
// field; for CREATE TABLE and DROP TABLE it uses TiDB's Restore. It returns the
// original statement when parsing fails.
func (tidbStatementParser) Canonicalize(ddl string) string {
	stmts, err := statement.New(ddl)
	if err != nil || len(stmts) == 0 {
		return ddl
	}

	stmt := stmts[0]

	// For ALTER TABLE, reconstruct from the normalized Alter field.
	if stmt.Alter != "" {
		if stmt.Schema != "" {
			return fmt.Sprintf("ALTER TABLE `%s`.`%s` %s", stmt.Schema, stmt.Table, stmt.Alter)
		}
		return fmt.Sprintf("ALTER TABLE `%s` %s", stmt.Table, stmt.Alter)
	}

	// For CREATE TABLE and DROP TABLE, use TiDB's Restore for canonical format.
	return restoreCanonical(ddl)
}
