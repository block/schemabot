package ddl

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
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
// (the instant DDL decision) must stay gated even for changes an operator
// explicitly allowed, because allowing an unsafe change to run is not the
// same as allowing it to run without its safety net.
//
// Data loss is not the only way a statement can hurt. A caller whose exposure
// is availability rather than data — SchemaBot's own storage bootstrap — wants
// StorageDestructiveStatement, which is strictly stricter than this.
//
// The input must be parseable by the MySQL-family parser; multi-statement
// content must be split with SplitStatements first.
func UnsafeStatement(stmt string) (bool, string, error) {
	parsed, err := statement.New(stmt)
	if err != nil {
		return false, "", fmt.Errorf("parse statement: %w", err)
	}
	unsafe, reason := unsafeParsed(parsed)
	return unsafe, reason, nil
}

// unsafeParsed applies Spirit's UnsafeLinter to already-parsed statements and
// returns its first violation message as the reason.
func unsafeParsed(parsed []*statement.AbstractStatement) (bool, string) {
	violations := (&spiritlint.UnsafeLinter{}).Lint(nil, parsed)
	if len(violations) == 0 {
		return false, ""
	}
	return true, violations[0].Message
}

// StorageDestructiveStatement reports whether a DDL statement is destructive
// in the vocabulary SchemaBot's own storage-schema bootstrap needs, which is
// stricter than Spirit's unsafe vocabulary in exactly one way: on top of every
// statement that loses data (UnsafeStatement, still the single authority on
// that set) it also refuses every statement that removes or renames a schema
// object while losing nothing — dropping an index, a foreign key, a check or
// any other named constraint, and renaming a table, column or index.
//
// The two vocabularies differ because the exposures differ. Spirit answers for
// a change an operator proposed and reviewed, where the irreversible cost is
// lost rows. The bootstrap answers for a change nobody proposed: it runs
// unattended at startup, against the storage every instance in the fleet is
// reading, on whatever diff the starting binary's embedded schema produces
// against the live database. Dropping an index that live queries plan around
// loses no rows and can still take that database down, and because the drop is
// metadata-only it completes in milliseconds with nothing looking wrong. The
// same holds for a constraint whose enforcement a newer binary relies on, and
// for a rename, which is a drop and an add to every instance still reading the
// old name.
//
// The refusal set is a denylist over Spirit's exported AST, which is the
// fail-safe direction *here* even though a denylist normally is not: a clause
// this policy does not recognize executes, and the clauses the bootstrap most
// needs to execute are the additive ones the starting binary requires to run
// at all. An allowlist would refuse an unrecognized ADD COLUMN and trade a
// startup that works for a column missing at query time.
//
// Table options (ENGINE, CHARSET, ROW_FORMAT and the rest) are deliberately
// not refused. They remove nothing, and the diff emits them for routine
// convergence, so refusing them would strand the storage schema on a property
// mismatch forever.
//
// The reason is operator-facing, in the same shape as Spirit's violation
// messages. As with UnsafeStatement the verdict is unconditional and the input
// must be exactly the statements the MySQL-family parser accepts; an
// unparseable statement is an error, which callers treat as fail-closed.
func StorageDestructiveStatement(stmt string) (bool, string, error) {
	parsed, err := statement.New(stmt)
	if err != nil {
		return false, "", fmt.Errorf("parse statement: %w", err)
	}
	if unsafe, reason := unsafeParsed(parsed); unsafe {
		return true, reason, nil
	}
	if removal, reason := removesOrRenamesObject(parsed); removal {
		return true, reason, nil
	}
	return false, "", nil
}

// removesOrRenamesObject reports whether any of the parsed statements removes
// or renames a schema object, and names the first one it finds. It covers only
// what Spirit's UnsafeLinter classifies as safe because it loses no data —
// everything lossy is already answered by unsafeParsed.
//
// A statement-level DROP INDEX has no case of its own: Spirit's parser rewrites
// it into the equivalent ALTER TABLE clause, so it arrives here in the clause
// form below.
func removesOrRenamesObject(parsed []*statement.AbstractStatement) (bool, string) {
	for _, change := range parsed {
		switch node := (*change.StmtNode).(type) {
		case *ast.RenameTableStmt:
			return true, removalReason("RENAME TABLE", renamedTableName(node))
		case *ast.AlterTableStmt:
			for _, spec := range node.Specs {
				name, removes := removedObjectName(spec)
				if removes {
					return true, removalReason(spiritlint.AlterTableTypeToString(spec.Tp), name)
				}
			}
		}
	}
	return false, ""
}

// removedObjectName reports whether an ALTER TABLE clause removes or renames a
// schema object, and names the object it acts on when the clause carries a
// name. Clauses that lose data are not listed here: they are Spirit's to
// classify.
func removedObjectName(spec *ast.AlterTableSpec) (name string, removes bool) {
	switch spec.Tp {
	case ast.AlterTableDropIndex, ast.AlterTableDropForeignKey:
		return spec.Name, true
	case ast.AlterTableDropCheck, ast.AlterTableDropConstraint:
		if spec.Constraint != nil {
			return spec.Constraint.Name, true
		}
		return "", true
	case ast.AlterTableRenameIndex:
		return spec.FromKey.O, true
	case ast.AlterTableRenameColumn:
		if spec.OldColumnName != nil {
			return spec.OldColumnName.Name.O, true
		}
		return "", true
	case ast.AlterTableRenameTable:
		// The name the table is moving to: the name it is moving from is the
		// statement's own table, which callers already carry alongside.
		if spec.NewTable != nil {
			return spec.NewTable.Name.O, true
		}
		return "", true
	default:
		return "", false
	}
}

// renamedTableName names the first table a RENAME TABLE statement renames, so
// the refusal reason says which one. It is empty for a statement that renames
// nothing, which the parser does not produce.
func renamedTableName(stmt *ast.RenameTableStmt) string {
	for _, pair := range stmt.TableToTables {
		if pair.OldTable != nil {
			return pair.OldTable.Name.O
		}
	}
	return ""
}

// removalReason renders the operator-facing reason for a refused removal or
// rename, in the shape of Spirit's violation messages so refusals from both
// vocabularies read alike in logs. The name is omitted when the clause does
// not carry one.
func removalReason(operation, name string) string {
	if name != "" {
		operation += " " + sqlescape.EscapeIdentifier(name)
	}
	return fmt.Sprintf("Destructive operation detected: %q", operation)
}

// SplitStorageDestructiveAlter partitions a single ALTER TABLE statement into
// a safe statement and a destructive statement in the storage bootstrap's
// vocabulary. Each clause is restored on its own and classified through
// StorageDestructiveStatement, so the classifier that decides whether a
// statement is refused is the same one that decides which of its clauses are:
// a policy that held only at the top level would let a destructive clause ride
// into the safe partition on a mixed ALTER and execute, which is the shape
// every diff of a drifted table takes, since Spirit's diff emits one combined
// ALTER per table. Either returned statement is empty when no clause falls in
// that partition; both are restored in the parser's canonical form, not sliced
// from the input text.
//
// The safe partition must also be independently executable, because callers
// run it while refusing the destructive partition. A clause that is safe on
// its own but needs a refused clause to have run first moves into the
// destructive partition with it. In practice that is a name collision: the
// diff of an index whose definition changed drops it and adds it back under
// the same name, and of a widened primary key drops and re-adds `PRIMARY`, so
// executing the add against the index the refusal left in place would fail on
// a duplicate key name and take the whole bootstrap statement down with it.
// Names are matched case-insensitively, as MySQL matches them, and across one
// flat namespace: coupling an add to a removal of the same name in a different
// namespace can only move a clause into the refused partition, which never
// widens what the caller executes.
//
// The safe partition is re-classified before it is returned, and a destructive
// verdict there is an error: a split must never widen what the caller will
// execute. The input must be exactly one parseable ALTER TABLE statement;
// anything else is an error, which callers must treat as fail-closed.
func SplitStorageDestructiveAlter(stmt string) (safeDDL, destructiveDDL string, err error) {
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

	destructiveClause := make([]bool, len(alter.Specs))
	// Names still occupied in the live table because the clause that would
	// have freed them is refused.
	occupied := make(map[string]bool)
	for i, spec := range alter.Specs {
		clause, err := restoreAlterWithSpecs(alter, []*ast.AlterTableSpec{spec})
		if err != nil {
			return "", "", err
		}
		destructive, _, err := StorageDestructiveStatement(clause)
		if err != nil {
			return "", "", fmt.Errorf("classify clause %q: %w", clause, err)
		}
		destructiveClause[i] = destructive
		if destructive {
			if name := vacatedName(spec); name != "" {
				occupied[strings.ToLower(name)] = true
			}
		}
	}

	// Partition in original clause order, keeping coupled clauses together: an
	// add whose name a refused removal leaves occupied is refused with it
	// instead of being orphaned into a statement the database will reject.
	var safeSpecs, destructiveSpecs []*ast.AlterTableSpec
	for i, spec := range alter.Specs {
		if destructiveClause[i] || claimsOccupiedName(spec, occupied) {
			destructiveSpecs = append(destructiveSpecs, spec)
			continue
		}
		safeSpecs = append(safeSpecs, spec)
	}

	if len(safeSpecs) > 0 {
		safeDDL, err = restoreAlterWithSpecs(alter, safeSpecs)
		if err != nil {
			return "", "", err
		}
		// Defense in depth: both vocabularies classify clause by clause with
		// no cross-clause state, so a subset of individually safe clauses
		// cannot re-classify destructive today and this guard is unreachable.
		// It stays because Spirit's linter is an external authority whose
		// rules may grow cross-clause reasoning, and a split must never widen
		// what the caller will execute.
		destructive, reason, err := StorageDestructiveStatement(safeDDL)
		if err != nil {
			return "", "", fmt.Errorf("re-classify safe partition %q: %w", safeDDL, err)
		}
		if destructive {
			return "", "", fmt.Errorf("safe partition %q re-classified destructive: %s", safeDDL, reason)
		}
	}
	if len(destructiveSpecs) > 0 {
		destructiveDDL, err = restoreAlterWithSpecs(alter, destructiveSpecs)
		if err != nil {
			return "", "", err
		}
	}
	return safeDDL, destructiveDDL, nil
}

// claimsOccupiedName reports whether an ALTER TABLE clause adds an index or
// constraint under a name that a refused removal leaves occupied, which makes
// the clause unexecutable until that removal runs.
func claimsOccupiedName(spec *ast.AlterTableSpec, occupied map[string]bool) bool {
	name := addedName(spec)
	return name != "" && occupied[strings.ToLower(name)]
}

// primaryKeyIndexName is the name MySQL gives a table's primary key index. It
// is reserved, so using it to couple a DROP PRIMARY KEY to the ADD PRIMARY KEY
// behind it cannot collide with a secondary index's name.
const primaryKeyIndexName = "PRIMARY"

// vacatedName returns the index or constraint name an ALTER TABLE clause would
// free, so refusing the clause is known to leave that name occupied. It is
// empty for a clause that frees no name.
func vacatedName(spec *ast.AlterTableSpec) string {
	switch spec.Tp {
	case ast.AlterTableDropPrimaryKey:
		return primaryKeyIndexName
	case ast.AlterTableDropIndex, ast.AlterTableDropForeignKey:
		return spec.Name
	case ast.AlterTableDropCheck, ast.AlterTableDropConstraint:
		if spec.Constraint != nil {
			return spec.Constraint.Name
		}
		return ""
	case ast.AlterTableRenameIndex:
		return spec.FromKey.O
	default:
		return ""
	}
}

// addedName returns the index or constraint name an ALTER TABLE clause claims,
// so a clause claiming a name a refused removal left occupied can be refused
// with it. It is empty for a clause that claims no name, including an index or
// constraint added without one — MySQL derives that name, so no stated name
// can collide.
func addedName(spec *ast.AlterTableSpec) string {
	if spec.Tp != ast.AlterTableAddConstraint || spec.Constraint == nil {
		return ""
	}
	if spec.Constraint.Tp == ast.ConstraintPrimaryKey {
		return primaryKeyIndexName
	}
	return spec.Constraint.Name
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
