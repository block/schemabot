package ddl

import (
	"fmt"
	"slices"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/statement"
)

// DeclaresForeignKey reports whether a single DDL statement declares a FOREIGN
// KEY constraint — a CREATE TABLE that defines one, or an ALTER TABLE that adds
// one. Dropping a foreign key does not declare one, so an ALTER that removes a
// legacy constraint reports false and stays applicable.
//
// This is deliberately a property of the statement, not of the table's
// resulting shape. A lint that walks the post-state flags every change to a
// table that already carries a foreign key, including an unrelated ADD COLUMN;
// an engine that rejects foreign keys rejects the statement it is handed, so
// the predicate an engine refusal is built on has to match that.
//
// A column-level REFERENCES clause is not a declaration. MySQL parses it and
// silently creates no constraint, so refusing it would refuse a statement that
// in fact adds no foreign key.
//
// The input must be exactly one statement. Multi-statement input is rejected
// rather than reported on its first statement, so a foreign key cannot hide
// behind a leading statement that declares none. Only CREATE TABLE and ALTER
// TABLE can declare one; the caller is expected to classify first and not hand
// this a statement type the parser does not accept.
func DeclaresForeignKey(stmt string) (bool, error) {
	parsed, err := statement.New(stmt)
	if err != nil {
		return false, fmt.Errorf("parse statement %q: %w", statementPreview(stmt), err)
	}
	if len(parsed) != 1 {
		return false, fmt.Errorf("expected exactly one statement, got %d in %q", len(parsed), statementPreview(stmt))
	}
	if parsed[0].StmtNode == nil {
		return false, fmt.Errorf("statement %q parsed to no syntax tree", statementPreview(stmt))
	}

	switch node := (*parsed[0].StmtNode).(type) {
	case *ast.CreateTableStmt:
		if slices.ContainsFunc(node.Constraints, isForeignKeyConstraint) {
			return true, nil
		}
	case *ast.AlterTableStmt:
		for _, spec := range node.Specs {
			if spec == nil {
				continue
			}
			// An added constraint lands in one of two fields depending on the
			// syntax, and the spec type is not a reliable filter for either: a
			// bare ADD CONSTRAINT lands in Constraint, while the parenthesised
			// ADD (CONSTRAINT ...) list lands in NewConstraints under a spec
			// the parser labels as adding columns. Both are walked so a
			// foreign key cannot hide in the form that was not anticipated.
			if isForeignKeyConstraint(spec.Constraint) {
				return true, nil
			}
			if slices.ContainsFunc(spec.NewConstraints, isForeignKeyConstraint) {
				return true, nil
			}
		}
	}
	return false, nil
}

// isForeignKeyConstraint reports whether a parsed constraint is a foreign key.
// A table-level REFERENCES clause is what makes one, which is the same property
// the engine's own foreign key check keys on.
func isForeignKeyConstraint(c *ast.Constraint) bool {
	if c == nil {
		return false
	}
	return c.Refer != nil
}
