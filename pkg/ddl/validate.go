package ddl

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

// ValidateCreateTable validates a parsed CREATE TABLE statement for semantic correctness.
// Returns an error if any validation fails.
//
// These checks run after parsing but before diffing, providing clear error
// messages at plan time rather than failing during schema change execution.
func ValidateCreateTable(ct *statement.CreateTable) error {
	if err := ValidateDuplicateColumns(ct); err != nil {
		return err
	}
	if err := ValidateIndexColumns(ct); err != nil {
		return err
	}
	return nil
}

// ValidateDuplicateColumns checks for duplicate column names in a CREATE TABLE statement.
// MySQL column names are case-insensitive, so "Id" and "id" are duplicates.
func ValidateDuplicateColumns(ct *statement.CreateTable) error {
	seen := make(map[string]struct{}, len(ct.Columns))
	for _, col := range ct.Columns {
		name := strings.ToLower(col.Name)
		if _, exists := seen[name]; exists {
			return fmt.Errorf("table %q: duplicate column %q", ct.TableName, col.Name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

// ValidateIndexColumns validates that all index columns in a CREATE TABLE statement
// actually exist in the table's column definitions. This catches typos like
// "INDEX idx_name (full_name1)" when the column is actually "full_name".
//
// The TiDB parser accepts such statements as syntactically valid SQL, but MySQL
// will reject them at execution time. Validating at plan time provides early
// feedback before attempting the schema change.
func ValidateIndexColumns(ct *statement.CreateTable) error {
	// Build a set of valid column names for O(1) lookup
	columnNames := make(map[string]struct{}, len(ct.Columns))
	for _, col := range ct.Columns {
		// MySQL column names are case-insensitive, so normalize to lowercase
		columnNames[strings.ToLower(col.Name)] = struct{}{}
	}

	// Check each index's columns
	for _, idx := range ct.Indexes {
		for _, colName := range idx.Columns {
			// Skip expression-based index columns (they start with a paren or contain functions)
			// Expression indexes like INDEX ((col + 1)) are validated by the parser
			if strings.HasPrefix(colName, "(") || strings.Contains(colName, "(") {
				continue
			}
			if _, exists := columnNames[strings.ToLower(colName)]; !exists {
				return fmt.Errorf("table %q: index %q references non-existent column %q",
					ct.TableName, idx.Name, colName)
			}
		}
	}

	return nil
}
