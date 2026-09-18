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
// while the rest does not. An addition naming anything a withheld clause also
// names, in the same namespace, is withheld with it: executing it against the
// object still in place fails on a duplicate name and takes down the statement
// the split exists to keep running. That is how the diff of a redefined index or
// a widened primary key arrives — as a drop and an add of one name in one
// statement.
//
// The namespace is part of that test because MySQL keeps columns and keys
// apart. A table may carry an index named the same as a column, so a withheld
// index drop must not hold back the addition of a column that happens to share
// its name: nothing collides, and withholding it would leave the binary running
// against storage missing a column its own queries name.
//
// Either string is empty when no clause falls in that partition. Both are
// restored in the parser's canonical form rather than sliced out of the input.
// The input must be exactly one parseable ALTER TABLE statement: anything else
// is an error, which callers treat as fail-closed by refusing the whole
// statement.
//
// This reads the differ's output apart because the differ has no way to be asked
// for less. A Spirit diff option that emits only additions retires this file
// entirely: a caller that never receives a removal has nothing to partition, and
// the coupling rule below stops being necessary rather than getting simpler,
// because the differ knows a name is being reused at the point it decides to
// emit the pair.
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

	// Every name a withheld clause touches, so an addition that would collide
	// with one can be withheld alongside it.
	withheldNames := make(map[objectRef]bool)
	for _, spec := range alter.Specs {
		if addsSchemaObject(spec) {
			continue
		}
		for _, ref := range mentionedNames(spec) {
			withheldNames[ref] = true
		}
	}

	var additiveSpecs, withheldSpecs []*ast.AlterTableSpec
	for _, spec := range alter.Specs {
		if addsSchemaObject(spec) && !mentionsAny(spec, withheldNames) {
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

// primaryKeyName stands for a table's single primary key slot, which MySQL
// leaves unnamed in both DROP PRIMARY KEY and ADD PRIMARY KEY. It is a reserved
// index name, so it cannot collide with a secondary index.
const primaryKeyName = "primary"

// objectKind is the namespace an identifier lives in. MySQL keeps columns and
// keys apart, so a table may carry an index and a column of the same name at
// once, and an addition claiming one of them does not collide with a withheld
// clause holding the other.
type objectKind int

const (
	columnObject objectKind = iota
	keyObject
)

// objectRef is an identifier together with the namespace it occupies.
type objectRef struct {
	kind objectKind
	name string
}

// mentionedNames returns every identifier an ALTER TABLE clause names — the
// object it acts on and the columns it references — paired with the namespace
// that identifier lives in, and lowercased, as MySQL compares identifiers.
//
// The namespace comes from where in the clause the name was read, not from what
// the clause does with it, so this stays a flat read of the syntax rather than a
// per-clause-type analysis. The parser only ever fills Name for DROP INDEX and
// DROP FOREIGN KEY, which is why it can be read as a key without asking which
// clause it came from.
//
// The role a name plays is still not distinguished, and a column an index
// references is collected the same way as one a clause holds. Within a
// namespace that can only withhold more clauses than strictly necessary and
// never fewer, which is the safe direction inside a statement the plan already
// reported as unsafe. It cannot cost a column: an addition's only claim in the
// column namespace is the column it defines, so the names collected from an
// index's column list can withhold that index and nothing else.
func mentionedNames(spec *ast.AlterTableSpec) []objectRef {
	var refs []objectRef
	add := func(kind objectKind, name string) {
		if name != "" {
			refs = append(refs, objectRef{kind: kind, name: strings.ToLower(name)})
		}
	}

	if spec.Tp == ast.AlterTableDropPrimaryKey {
		add(keyObject, primaryKeyName)
	}
	add(keyObject, spec.Name)
	add(keyObject, spec.FromKey.O)
	add(keyObject, spec.ToKey.O)
	if spec.OldColumnName != nil {
		add(columnObject, spec.OldColumnName.Name.O)
	}
	if spec.NewColumnName != nil {
		add(columnObject, spec.NewColumnName.Name.O)
	}
	for _, column := range spec.NewColumns {
		if column.Name != nil {
			add(columnObject, column.Name.Name.O)
		}
	}
	if spec.Constraint != nil {
		add(keyObject, spec.Constraint.Name)
		if spec.Constraint.Tp == ast.ConstraintPrimaryKey {
			add(keyObject, primaryKeyName)
		}
		for _, key := range spec.Constraint.Keys {
			if key.Column != nil {
				add(columnObject, key.Column.Name.O)
			}
		}
	}
	return refs
}

// mentionsAny reports whether an ALTER TABLE clause names anything in the given
// set, which for an addition means it cannot run until the withheld clause
// naming the same thing has run.
func mentionsAny(spec *ast.AlterTableSpec, names map[objectRef]bool) bool {
	for _, ref := range mentionedNames(spec) {
		if names[ref] {
			return true
		}
	}
	return false
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
