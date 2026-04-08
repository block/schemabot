// Package ddl provides schema diffing, validation, and DDL formatting utilities.
package ddl

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
)

// Differ compares database schemas and generates ALTER statements.
type Differ struct{}

// NewDiffer creates a new Differ.
func NewDiffer() *Differ {
	return &Differ{}
}

// DiffResult contains the result of comparing two schemas.
type DiffResult struct {
	// Statements is the list of ALTER statements needed to transform source to target.
	Statements []string
	// Tables lists the tables that changed.
	Tables []string
}

// DiffSchemas compares two sets of table schemas and returns the DDL statements
// needed to transform the source schema into the target schema. Target tables
// are validated for semantic correctness before diffing.
//
// This delegates to Spirit's DeclarativeToImperative for the core diffing logic.
func (d *Differ) DiffSchemas(source, target []table.TableSchema) (*DiffResult, error) {
	// Validate target schemas before diffing.
	for _, ts := range target {
		ct, err := statement.ParseCreateTable(ts.Schema)
		if err != nil {
			return nil, fmt.Errorf("SQL syntax error: %w", err)
		}
		// Validate semantic correctness (e.g., index columns exist)
		if err := ValidateCreateTable(ct); err != nil {
			return nil, fmt.Errorf("SQL usage error: %w", err)
		}
	}

	changes, err := statement.DeclarativeToImperative(source, target, nil)
	if err != nil {
		return nil, err
	}

	result := &DiffResult{}
	for _, ch := range changes {
		result.Statements = append(result.Statements, ch.Statement)
		result.Tables = append(result.Tables, ch.Table)
	}
	return result, nil
}

// DiffStatements compares two sets of CREATE TABLE statements and returns the
// ALTER statements needed to transform the source schema to the target schema.
//
// Each statement should be a complete CREATE TABLE statement.
func (d *Differ) DiffStatements(sourceStmts, targetStmts []string) (*DiffResult, error) {
	source, err := stmtsToTableSchemas(sourceStmts)
	if err != nil {
		return nil, err
	}
	target, err := stmtsToTableSchemas(targetStmts)
	if err != nil {
		return nil, err
	}
	return d.DiffSchemas(source, target)
}

// stmtsToTableSchemas converts raw CREATE TABLE SQL strings to table.TableSchema values.
func stmtsToTableSchemas(stmts []string) ([]table.TableSchema, error) {
	var schemas []table.TableSchema
	for _, stmt := range stmts {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		ct, err := statement.ParseCreateTable(stmt)
		if err != nil {
			return nil, fmt.Errorf("SQL syntax error: %w", err)
		}
		schemas = append(schemas, table.TableSchema{Name: ct.TableName, Schema: stmt})
	}
	return schemas, nil
}

// DiffTable compares a single table's CREATE statements and returns ALTER statements.
func (d *Differ) DiffTable(sourceStmt, targetStmt string) ([]string, error) {
	sourceCT, err := statement.ParseCreateTable(sourceStmt)
	if err != nil {
		return nil, fmt.Errorf("parse source: %w", err)
	}

	targetCT, err := statement.ParseCreateTable(targetStmt)
	if err != nil {
		return nil, fmt.Errorf("parse target: %w", err)
	}

	alters, err := sourceCT.Diff(targetCT, nil)
	if err != nil {
		return nil, err
	}

	var statements []string
	for _, alter := range alters {
		statements = append(statements, alter.Statement)
	}
	return statements, nil
}

// IsSpiritInternalTable checks if a table name is a Spirit internal table.
// Spirit uses tables like _spirit_sentinel, _spirit_checkpoint, _tablename_new,
// _tablename_old, _tablename_chkpnt. These are engine artifacts that should not
// be included in schema diffs or user-facing output.
func IsSpiritInternalTable(table string) bool {
	if table == "_spirit_sentinel" || table == "_spirit_checkpoint" {
		return true
	}
	// Spirit temporary tables: _tablename_new, _tablename_old, _tablename_chkpnt
	if strings.HasPrefix(table, "_") &&
		(strings.HasSuffix(table, "_new") || strings.HasSuffix(table, "_old") || strings.HasSuffix(table, "_chkpnt")) {
		return true
	}
	return false
}

// FilterInternalTables filters out Spirit internal tables from a list.
func FilterInternalTables(tables []any) []any {
	var filtered []any
	for _, t := range tables {
		if tbl, ok := t.(map[string]any); ok {
			name, _ := tbl["table_name"].(string)
			if !IsSpiritInternalTable(name) {
				filtered = append(filtered, t)
			}
		}
	}
	return filtered
}

// TableWithName is an interface for typed table responses that have a table name.
type TableWithName interface {
	GetTableName() string
}

// FilterInternalTablesTyped filters out Spirit internal tables from a typed slice.
func FilterInternalTablesTyped[T TableWithName](tables []T) []T {
	var filtered []T
	for _, t := range tables {
		if !IsSpiritInternalTable(t.GetTableName()) {
			filtered = append(filtered, t)
		}
	}
	return filtered
}
