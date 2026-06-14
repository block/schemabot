package spirit

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/go-sql-driver/mysql"

	"github.com/block/schemabot/pkg/schema"
)

// parseDSN extracts connection info from a MySQL DSN using the mysql driver's parser.
func parseDSN(dsn string) (host, username, password, database string, err error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", "", "", "", fmt.Errorf("parse DSN: %w", err)
	}
	return cfg.Addr, cfg.User, cfg.Passwd, cfg.DBName, nil
}

// namespaceForTable finds which namespace a table belongs to by checking
// which namespace's schema files define it. Uses Spirit's parser for accurate
// table name extraction across every statement in each file.
//
// The namespace is the per-table progress-matching key, so it must be
// deterministic. When the table's CREATE TABLE cannot be located — for example
// a DROP TABLE plan, where the table no longer has a defining statement — the
// namespace can only be inferred when exactly one namespace exists. With two or
// more namespaces and no match, the table cannot be attributed to a single
// namespace, so this returns an error rather than an arbitrary map key.
func namespaceForTable(table string, sf schema.SchemaFiles) (string, error) {
	for nsName, ns := range sf {
		for filename, content := range ns.Files {
			// Check filename match first (fast path)
			baseName := strings.TrimSuffix(filename, ".sql")
			if baseName == table {
				return nsName, nil
			}
			// Match the table's CREATE TABLE (the defining statement) in any
			// statement of the file, not only the first one.
			defines, err := fileDefinesTable(content, table)
			if err != nil {
				return "", fmt.Errorf("classify schema file %q in namespace %q for table %q: %w", filename, nsName, table, err)
			}
			if defines {
				return nsName, nil
			}
		}
	}
	// When the defining statement is absent, the namespace can only be inferred
	// when there is exactly one namespace to attribute the table to.
	if len(sf) == 1 {
		for nsName := range sf {
			return nsName, nil
		}
	}
	return "", fmt.Errorf("no namespace defines table %q among %d schema namespaces %v", table, len(sf), namespaceNames(sf))
}

// fileDefinesTable reports whether the file content contains a CREATE TABLE
// statement for the given table. It classifies every statement in the file so
// that multi-statement files map all of the tables they define. A file with no
// parseable statements (empty or comment-only) defines no table and is not an
// error.
func fileDefinesTable(content, table string) (bool, error) {
	results, err := statement.Classify(content)
	if errors.Is(err, statement.ErrNoStatements) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("classify content: %w", err)
	}
	for _, c := range results {
		if c.Type == statement.StatementCreateTable && c.Table == table {
			return true, nil
		}
	}
	return false, nil
}

// namespaceNames returns the sorted namespace keys for inclusion in error
// messages so operators can see which namespaces were searched.
func namespaceNames(sf schema.SchemaFiles) []string {
	names := make([]string, 0, len(sf))
	for nsName := range sf {
		names = append(names, nsName)
	}
	sort.Strings(names)
	return names
}
