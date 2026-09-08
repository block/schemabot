package ddl

import (
	"fmt"
	"strings"

	spiritlint "github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/statement"
)

// UnsafeStatement reports whether a DDL statement is unsafe in Spirit's
// vocabulary: an operation that destroys existing data, such as DROP TABLE
// or an ALTER TABLE clause like DROP COLUMN or DROP PARTITION. Spirit's
// UnsafeLinter is the single authority on the unsafe set, so every surface
// that gates on "unsafe" agrees with the --allow-unsafe plan gate. The
// reason is the linter's violation message, suitable for operator-facing
// logs. Statement types Spirit does not run (for example TRUNCATE TABLE)
// fail the parse and surface as errors — callers treat an error as unsafe,
// which is the fail-closed direction.
//
// The verdict is unconditional: unlike plan-time linting, it cannot be
// relaxed by lint configuration. Callers that gate safety behavior on it
// (the storage-schema bootstrap, the instant DDL decision) must stay gated
// even for changes an operator explicitly allowed, because allowing an
// unsafe change to run is not the same as allowing it to run without its
// safety net.
//
// The input must be parseable by the MySQL-family parser; multi-statement
// content must be split with SplitStatements first.
func UnsafeStatement(stmt string) (bool, string, error) {
	parsed, err := statement.New(stmt)
	if err != nil {
		return false, "", fmt.Errorf("parse statement: %w", err)
	}
	violations := (&spiritlint.UnsafeLinter{}).Lint(nil, parsed)
	if len(violations) == 0 {
		return false, "", nil
	}
	return true, violations[0].Message, nil
}

// SplitUnsafeAlter partitions a single ALTER TABLE statement into a safe
// statement and an unsafe statement in Spirit's unsafe vocabulary. Each
// clause is restored on its own and classified through UnsafeStatement, so
// Spirit's UnsafeLinter remains the single authority on the unsafe set —
// there is no local clause vocabulary to drift out of agreement. Either
// returned statement is empty when no clause falls in that partition; both
// are restored in the parser's canonical form, not sliced from the input
// text.
//
// The safe partition must also be independently executable, because callers
// run it while refusing the unsafe partition. A clause that is safe on its
// own but cannot run without a refused clause moves into the unsafe
// partition with it: the ADD PRIMARY KEY half of a primary-key change is
// only valid while the table has no primary key, so when the DROP PRIMARY
// KEY is refused the add would be rejected by the database rather than
// executed.
//
// The safe partition is re-classified before it is returned, and an unsafe
// verdict there is an error: a split must never widen what the caller will
// execute. The input must be exactly one parseable ALTER TABLE statement;
// anything else is an error, which callers must treat as fail-closed.
func SplitUnsafeAlter(stmt string) (safeDDL, unsafeDDL string, err error) {
	parsed, err := statement.New(stmt)
	if err != nil {
		return "", "", fmt.Errorf("parse statement: %w", err)
	}
	if len(parsed) != 1 {
		return "", "", fmt.Errorf("expected exactly one statement, got %d", len(parsed))
	}
	alter, ok := (*parsed[0].StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return "", "", statement.ErrNotAlterTable
	}

	unsafeClause := make([]bool, len(alter.Specs))
	dropsPrimaryKey := false
	for i, spec := range alter.Specs {
		clause, err := restoreAlterWithSpecs(alter, []*ast.AlterTableSpec{spec})
		if err != nil {
			return "", "", err
		}
		unsafe, _, err := UnsafeStatement(clause)
		if err != nil {
			return "", "", fmt.Errorf("classify clause %q: %w", clause, err)
		}
		unsafeClause[i] = unsafe
		if unsafe && spec.Tp == ast.AlterTableDropPrimaryKey {
			dropsPrimaryKey = true
		}
	}

	// Partition in original clause order, keeping coupled clauses together:
	// an ADD PRIMARY KEY is executable only while the table has no primary
	// key, so when this ALTER's unsafe partition carries the DROP PRIMARY
	// KEY, the add is refused with the drop instead of being orphaned into a
	// statement the database will reject.
	var safeSpecs, unsafeSpecs []*ast.AlterTableSpec
	for i, spec := range alter.Specs {
		if unsafeClause[i] || (dropsPrimaryKey && isAddPrimaryKey(spec)) {
			unsafeSpecs = append(unsafeSpecs, spec)
			continue
		}
		safeSpecs = append(safeSpecs, spec)
	}

	if len(safeSpecs) > 0 {
		safeDDL, err = restoreAlterWithSpecs(alter, safeSpecs)
		if err != nil {
			return "", "", err
		}
		// Defense in depth: Spirit's UnsafeLinter classifies clause by
		// clause with no cross-clause state, so a subset of individually
		// safe clauses cannot re-classify unsafe today and this guard is
		// unreachable. It stays because the linter is an external authority
		// whose rules may grow cross-clause reasoning, and a split must
		// never widen what the caller will execute.
		unsafe, reason, err := UnsafeStatement(safeDDL)
		if err != nil {
			return "", "", fmt.Errorf("re-classify safe partition %q: %w", safeDDL, err)
		}
		if unsafe {
			return "", "", fmt.Errorf("safe partition %q re-classified unsafe: %s", safeDDL, reason)
		}
	}
	if len(unsafeSpecs) > 0 {
		unsafeDDL, err = restoreAlterWithSpecs(alter, unsafeSpecs)
		if err != nil {
			return "", "", err
		}
	}
	return safeDDL, unsafeDDL, nil
}

// isAddPrimaryKey reports whether the ALTER TABLE clause adds a primary key
// constraint.
func isAddPrimaryKey(spec *ast.AlterTableSpec) bool {
	return spec.Tp == ast.AlterTableAddConstraint &&
		spec.Constraint != nil &&
		spec.Constraint.Tp == ast.ConstraintPrimaryKey
}

// restoreAlterWithSpecs restores the given ALTER TABLE statement with its
// clause list replaced by specs, in the parser's canonical form.
func restoreAlterWithSpecs(alter *ast.AlterTableStmt, specs []*ast.AlterTableSpec) (string, error) {
	clone := *alter
	clone.Specs = specs
	var sb strings.Builder
	rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := clone.Restore(rCtx); err != nil {
		return "", fmt.Errorf("restore ALTER TABLE clauses: %w", err)
	}
	return sb.String(), nil
}
