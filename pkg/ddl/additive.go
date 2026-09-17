package ddl

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/statement"
)

// ErrNotAlterTable reports a statement that has no ALTER TABLE clauses to
// partition. It is Spirit's own sentinel, restated here so a caller that
// recognizes it need not import the parser.
var ErrNotAlterTable = statement.ErrNotAlterTable

// SplitAdditiveAlter partitions one ALTER TABLE statement into the clauses that
// only add a schema object and the clauses that do anything else. It answers a
// structural question about clauses, not a safety question about statements: a
// caller decides whether a statement is gated at all by reading the unsafe
// verdict its plan carries, and reaches for this only to run the part of a
// gated statement that adds.
//
// The additive partition is an allowlist — adding columns, and adding an index
// or constraint. Every other clause is withheld, including MODIFY COLUMN and
// the table options, neither of which adds anything. An allowlist is the
// fail-closed direction here because the caller has already been told the
// statement is unsafe: the question is no longer whether to be careful with it
// but how little of it can be run, so a clause shape this policy does not
// recognize waits for an operator rather than riding along with the additions.
//
// The additive partition must also stand on its own, because the caller runs it
// while the rest does not. A clause that adds a name a withheld clause was going
// to free is withheld with it: executing it against the object still in place
// fails on a duplicate name and takes down the statement the split exists to
// keep running. That is how the diff of a redefined index, a widened primary
// key, or a retyped column arrives — as a drop and an add of one name in one
// statement. Names are matched case-insensitively, as MySQL matches them, and
// across one flat namespace, which can only widen what is withheld.
//
// Either string is empty when no clause falls in that partition. Both are
// restored in the parser's canonical form rather than sliced out of the input.
// The input must be exactly one parseable ALTER TABLE statement: anything else
// is an error, which callers treat as fail-closed by refusing the whole
// statement.
func SplitAdditiveAlter(stmt string) (additiveDDL, withheldDDL string, err error) {
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

	// Names the live table still holds because the clause that would have freed
	// them is withheld.
	occupied := make(map[string]bool)
	for _, spec := range alter.Specs {
		if addsSchemaObject(spec) {
			continue
		}
		if name := vacatedName(spec); name != "" {
			occupied[strings.ToLower(name)] = true
		}
	}

	var additiveSpecs, withheldSpecs []*ast.AlterTableSpec
	for _, spec := range alter.Specs {
		if addsSchemaObject(spec) && !claimsOccupiedName(spec, occupied) {
			additiveSpecs = append(additiveSpecs, spec)
			continue
		}
		withheldSpecs = append(withheldSpecs, spec)
	}

	if len(additiveSpecs) > 0 {
		additiveDDL, err = restoreAlterWithSpecs(alter, additiveSpecs)
		if err != nil {
			return "", "", err
		}
	}
	if len(withheldSpecs) > 0 {
		withheldDDL, err = restoreAlterWithSpecs(alter, withheldSpecs)
		if err != nil {
			return "", "", err
		}
	}
	return additiveDDL, withheldDDL, nil
}

// addsSchemaObject reports whether an ALTER TABLE clause only brings a new
// schema object into the table. Adding a partition is not on the list: the
// storage schema declares none, so a partition clause arriving here is a shape
// this policy has not been reasoned about and waits for an operator.
func addsSchemaObject(spec *ast.AlterTableSpec) bool {
	switch spec.Tp {
	case ast.AlterTableAddColumns, ast.AlterTableAddConstraint:
		return true
	default:
		return false
	}
}

// primaryKeyIndexName is the name MySQL gives a table's primary key index. It
// is reserved, so using it to couple a DROP PRIMARY KEY to the ADD PRIMARY KEY
// behind it cannot collide with a secondary index's name.
const primaryKeyIndexName = "PRIMARY"

// vacatedName returns the name an ALTER TABLE clause would free, so withholding
// the clause is known to leave that name occupied. Columns count as much as
// indexes and constraints: a withheld DROP COLUMN or column rename leaves the
// old column in place, and an add of that same name is then a duplicate. It is
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
	case ast.AlterTableDropColumn, ast.AlterTableRenameColumn:
		if spec.OldColumnName != nil {
			return spec.OldColumnName.Name.O
		}
		return ""
	case ast.AlterTableChangeColumn:
		// CHANGE COLUMN states a name as well as a definition, so it is the
		// other form of a column rename and frees the name it renames away
		// from. A clause that restates the same name frees nothing.
		old, _ := renamedColumn(spec)
		return old
	default:
		return ""
	}
}

// renamedColumn returns the name a CHANGE COLUMN clause renames away from, and
// whether it renames at all. Names are compared the way MySQL compares
// identifiers, case-insensitively, so restating a column's name in another case
// is not a rename and frees no name.
func renamedColumn(spec *ast.AlterTableSpec) (old string, renames bool) {
	if spec.OldColumnName == nil || len(spec.NewColumns) == 0 || spec.NewColumns[0].Name == nil {
		return "", false
	}
	old = spec.OldColumnName.Name.O
	if strings.EqualFold(old, spec.NewColumns[0].Name.Name.O) {
		return "", false
	}
	return old, true
}

// claimsOccupiedName reports whether an ALTER TABLE clause adds a column, index
// or constraint under a name that a withheld clause leaves occupied, which makes
// the clause unexecutable until that clause runs.
func claimsOccupiedName(spec *ast.AlterTableSpec, occupied map[string]bool) bool {
	for _, name := range addedNames(spec) {
		if occupied[strings.ToLower(name)] {
			return true
		}
	}
	return false
}

// addedNames returns the names an ALTER TABLE clause claims, so a clause
// claiming a name a withheld clause left occupied can be withheld with it. One
// clause can add several columns. An index or constraint added without a name
// claims none: MySQL derives that name, so no stated name can collide.
func addedNames(spec *ast.AlterTableSpec) []string {
	switch spec.Tp {
	case ast.AlterTableAddColumns:
		names := make([]string, 0, len(spec.NewColumns))
		for _, column := range spec.NewColumns {
			if column.Name != nil {
				names = append(names, column.Name.Name.O)
			}
		}
		return names
	case ast.AlterTableAddConstraint:
		if spec.Constraint == nil {
			return nil
		}
		if spec.Constraint.Tp == ast.ConstraintPrimaryKey {
			return []string{primaryKeyIndexName}
		}
		if spec.Constraint.Name == "" {
			return nil
		}
		return []string{spec.Constraint.Name}
	default:
		return nil
	}
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
