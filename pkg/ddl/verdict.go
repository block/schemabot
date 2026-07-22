package ddl

import (
	"fmt"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// Execution-mode verdicts recorded on a planned table change. The verdict
// answers "how will this statement actually run?" so operators learn about
// engine limitations at plan time instead of at apply time.
const (
	// ExecutionModeBlocked marks a statement the MySQL schema-change engine
	// deterministically refuses. An apply containing it will fail, and
	// retrying cannot succeed until the statement changes.
	ExecutionModeBlocked = "blocked"
)

// EngineRefusalReason reports whether the MySQL schema-change engine (Spirit)
// deterministically refuses the given statement, and the engine's reason.
//
// It mirrors the engine's own run-time decision order, so it only claims a
// refusal when one is certain:
//
//   - ALGORITHM=/LOCK= clauses are rejected at parse level before any DDL is
//     attempted (Spirit prepends its own assertions, and MySQL resolves
//     duplicate options last-one-wins).
//   - DROP PRIMARY KEY never qualifies for MySQL's INSTANT algorithm or for
//     the engine's safe-INPLACE clause set, so it always reaches the engine's
//     preflight primary-key check, which refuses it unconditionally.
//   - ADD FOREIGN KEY is refused unconditionally by the engine's preflight
//     foreign-key check, which runs before any execution attempt — the
//     engine's online copy cannot preserve referential constraints.
//
// The engine also refuses ALTERs on tables that already carry foreign keys,
// but that depends on live table state, not the statement text, so it cannot
// be claimed here — it surfaces as an apply-time failure instead.
//
// Statements a preflight check would refuse but that can complete through the
// engine's instant-DDL fast path never reach that check, so they are not
// reported here — the verdict must never claim an apply will fail when it can
// succeed. Only ALTER TABLE statements can be refused; everything else
// returns false.
func EngineRefusalReason(stmt string) (string, bool, error) {
	stmts, err := statement.New(stmt)
	if err != nil {
		return "", false, fmt.Errorf("parse statement for execution verdict %q: %w", statementPreview(stmt), err)
	}
	if len(stmts) != 1 {
		return "", false, fmt.Errorf("execution verdict requires exactly one statement, got %d in %q", len(stmts), statementPreview(stmt))
	}
	abs := stmts[0]
	if !abs.IsAlterTable() {
		return "", false, nil
	}
	if err := abs.AlterContainsUnsupportedClause(); err != nil {
		return err.Error(), true, nil
	}
	alterStmt, ok := abs.AsAlterTable()
	if !ok {
		return "", false, fmt.Errorf("statement classified as ALTER TABLE has no alter AST node: %q", statementPreview(stmt))
	}
	for _, spec := range alterStmt.Specs {
		if spec.Tp == ast.AlterTableDropPrimaryKey {
			return "dropping primary key is not supported", true, nil
		}
		if addsForeignKey(spec) {
			return "adding foreign key constraints is not supported", true, nil
		}
	}
	return "", false, nil
}

// addsForeignKey reports whether the alter spec adds a referential constraint,
// matching the shapes the engine's preflight foreign-key check refuses: a
// single ADD CONSTRAINT ... FOREIGN KEY, or a constraint list carrying one.
func addsForeignKey(spec *ast.AlterTableSpec) bool {
	if spec.Constraint != nil && spec.Constraint.Refer != nil {
		return true
	}
	for _, constraint := range spec.NewConstraints {
		if constraint.Refer != nil {
			return true
		}
	}
	return false
}
