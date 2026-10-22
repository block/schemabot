package ddl

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/statement"
)

// SameTableChange reports whether a pull request's edit to a single-table
// MySQL schema file generates the same schema change on two different bases.
//
// approvedBase and approved are the file at the approved commit's merge base
// and at the approved commit; headBase and head are the file at the head's
// merge base and at the head. The change is the same when the head is the
// newer base plus the approved edit and nothing else, which takes three
// things:
//
//   - Every column, index, constraint, table option, and partitioning clause
//     is the three-way merge of the approved commit and headBase: the head
//     declares it as the approved commit does where the base left it alone,
//     and as headBase does where the base changed it and the pull request
//     did not. One both changed cannot be merged. Comparing elements, not
//     only statements, matters because statements can hide an override: a
//     column the base made NOT NULL and the pull request widened is modified
//     the same way on both bases, yet on the newer one the statement also
//     drops the NOT NULL.
//   - The columns keep their relative order: the bases order the columns
//     they share the same way, and so do the approved commit and the head.
//     An element's definition carries no position, so a reordering would
//     otherwise go unseen wherever the statements happen to match.
//   - Diffing each base against its commit yields the same statements, so the
//     reviewer approved exactly the statements the head generates, including
//     where each column goes. The diff ignores nothing (no AUTO_INCREMENT,
//     engine, charset, partitioning, or row format exemptions), so any
//     difference the planner could act on counts.
//
// A file the pull request adds is absent at both bases, passed as "". Its
// change is the CREATE TABLE itself, so it is the same when the approved
// commit and the head declare the same elements in the same column order:
// formatting and comments may differ, nothing the table is built from may.
//
// When the change is not the same, reason says why, for operator logs. Each
// file must hold exactly one CREATE TABLE, all four naming the same table,
// and an element may be declared only once per file; an error means the files
// could not be compared and proves nothing.
func SameTableChange(approvedBase, approved, headBase, head string) (same bool, reason string, err error) {
	if approvedBase == "" && headBase == "" {
		return sameAddedTable(approved, head)
	}
	if approvedBase == "" || headBase == "" {
		return false, "", fmt.Errorf("the file is absent at one base but not the other")
	}
	files := [...]struct {
		name    string
		content string
	}{
		{"approved commit's base", approvedBase},
		{"approved commit", approved},
		{"head's base", headBase},
		{"head", head},
	}
	var tables [len(files)]*statement.CreateTable
	var elements [len(files)]map[string]string
	for i, f := range files {
		ct, err := parseSingleCreateTable(f.content)
		if err != nil {
			return false, "", fmt.Errorf("parse the schema file at the %s: %w", f.name, err)
		}
		// Every file is checked for duplicate declarations before any is
		// diffed: the diff assumes each element is declared once.
		e, err := tableElements(ct)
		if err != nil {
			return false, "", fmt.Errorf("read table %s at the %s: %w", ct.TableName, f.name, err)
		}
		tables[i], elements[i] = ct, e
	}
	aBase, a, hBase, h := tables[0], tables[1], tables[2], tables[3]
	for i, ct := range tables {
		if ct.TableName != aBase.TableName {
			return false, fmt.Sprintf("the file declares table %s at the %s but %s at the %s",
				aBase.TableName, files[0].name, ct.TableName, files[i].name), nil
		}
	}

	if why := UnmergedElement(elements[0], elements[1], elements[2], elements[3]); why != "" {
		return false, fmt.Sprintf("table %s: %s", aBase.TableName, why), nil
	}
	if !sameColumnOrder(aBase, hBase) {
		return false, fmt.Sprintf("table %s: the base reordered columns since the approved commit", aBase.TableName), nil
	}
	if !sameColumnOrder(a, h) {
		return false, fmt.Sprintf("table %s: the head orders columns differently from the approved commit", aBase.TableName), nil
	}

	approvedChange, err := strictTableDiff(aBase, a)
	if err != nil {
		return false, "", fmt.Errorf("diff table %s at the approved commit against its base: %w", aBase.TableName, err)
	}
	headChange, err := strictTableDiff(hBase, h)
	if err != nil {
		return false, "", fmt.Errorf("diff table %s at the head against its base: %w", aBase.TableName, err)
	}
	if !slices.Equal(approvedChange, headChange) {
		return false, fmt.Sprintf("table %s: the approved commit generates %q but the head generates %q",
			aBase.TableName, approvedChange, headChange), nil
	}
	return true, "", nil
}

// sameAddedTable compares a table file the pull request adds at the approved
// commit and at the head.
func sameAddedTable(approved, head string) (same bool, reason string, err error) {
	files := [...]struct {
		name    string
		content string
	}{
		{"approved commit", approved},
		{"head", head},
	}
	var tables [len(files)]*statement.CreateTable
	var elements [len(files)]map[string]string
	for i, f := range files {
		ct, err := parseSingleCreateTable(f.content)
		if err != nil {
			return false, "", fmt.Errorf("parse the schema file at the %s: %w", f.name, err)
		}
		e, err := tableElements(ct)
		if err != nil {
			return false, "", fmt.Errorf("read table %s at the %s: %w", ct.TableName, f.name, err)
		}
		tables[i], elements[i] = ct, e
	}
	a, h := tables[0], tables[1]
	if a.TableName != h.TableName {
		return false, fmt.Sprintf("the file declares table %s at the approved commit but %s at the head", a.TableName, h.TableName), nil
	}
	// With no base, every element is the pull request's own, so the merge
	// holds exactly when the head declares each one as approved.
	if why := UnmergedElement(nil, elements[0], nil, elements[1]); why != "" {
		return false, fmt.Sprintf("table %s: %s", a.TableName, why), nil
	}
	if !sameColumnOrder(a, h) {
		return false, fmt.Sprintf("table %s: the head orders columns differently from the approved commit", a.TableName), nil
	}
	return true, "", nil
}

// parseSingleCreateTable parses a schema file that must hold exactly one
// CREATE TABLE statement.
func parseSingleCreateTable(content string) (*statement.CreateTable, error) {
	stmts, err := tidbStatementParser{}.Split(content)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("the file holds %d statements, not one CREATE TABLE", len(stmts))
	}
	ct, err := statement.ParseCreateTable(stmts[0])
	if err != nil {
		return nil, fmt.Errorf("parse CREATE TABLE: %w", err)
	}
	return ct, nil
}

// strictTableDiff returns the statements that turn source into target with no
// difference ignored.
func strictTableDiff(source, target *statement.CreateTable) ([]string, error) {
	changes, err := source.Diff(target, &statement.DiffOptions{})
	if err != nil {
		return nil, err
	}
	stmts := make([]string, 0, len(changes))
	for _, ch := range changes {
		stmts = append(stmts, ch.Statement)
	}
	return stmts, nil
}

// UnmergedElement checks that head is the element-by-element three-way merge
// of approved and headBase over approvedBase, and returns why the first
// element (in name order) that is not breaks it, or "" when every element
// merges. Each map holds one table's elements, keyed by a name that says what
// the element is and mapped to its definition. An element added or removed
// counts as changed, and one that both approved and headBase changed never
// merges. Every dialect's statement comparison applies this one rule.
func UnmergedElement(approvedBase, approved, headBase, head map[string]string) string {
	keys := make(map[string]bool)
	for _, elements := range []map[string]string{approvedBase, approved, headBase, head} {
		for k := range elements {
			keys[k] = true
		}
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		baseChanged := !sameElement(approvedBase, headBase, k)
		prChanged := !sameElement(approvedBase, approved, k)
		switch {
		case baseChanged && prChanged:
			return fmt.Sprintf("both the approved commit and the base changed %s", k)
		case baseChanged && !sameElement(headBase, head, k):
			return fmt.Sprintf("the base changed %s since the approved commit, and the head replaces it with another version", k)
		case !baseChanged && !sameElement(approved, head, k):
			return fmt.Sprintf("the head declares %s differently from the approved commit", k)
		}
	}
	return ""
}

// sameElement reports whether key is absent from both x and y or present in
// both with the same definition.
func sameElement(x, y map[string]string, key string) bool {
	xv, xok := x[key]
	yv, yok := y[key]
	return xok == yok && xv == yv
}

// sameColumnOrder reports whether x and y order the columns they both declare
// the same way. Columns only one of them declares are ignored.
func sameColumnOrder(x, y *statement.CreateTable) bool {
	inY := make(map[string]bool, len(y.Columns))
	for _, col := range y.Columns {
		inY[strings.ToLower(col.Name)] = true
	}
	var shared []string
	for _, col := range x.Columns {
		if name := strings.ToLower(col.Name); inY[name] {
			shared = append(shared, name)
		}
	}
	i := 0
	for _, col := range y.Columns {
		name := strings.ToLower(col.Name)
		if !slices.Contains(shared, name) {
			continue
		}
		if shared[i] != name {
			return false
		}
		i++
	}
	return true
}

// tableElements keys every column, index, and constraint of ct by kind and
// lowercased name (MySQL matches them case-insensitively), every table option
// by name, and everything else the statement declares (its modifiers,
// partitioning, and any LIKE or AS SELECT clause) as one element, and maps
// each to its definition as the parser read it. Elements come from the parse
// tree rather than Spirit's model of the table, so a clause the model does
// not carry (an ENCRYPTION option, a LIKE source) still takes part. Each
// option is its own element, so the pull request and the base can change
// different options of the same table. Column position is not part of a
// column's definition; sameColumnOrder checks it. An element declared twice
// cannot be told apart from the other, so it is an error.
func tableElements(ct *statement.CreateTable) (map[string]string, error) {
	if ct.Raw == nil {
		return nil, fmt.Errorf("table %s has no parse tree", ct.TableName)
	}
	elements := make(map[string]string)
	add := func(key string, node ast.Node) error {
		definition, err := restoreNode(node)
		if err != nil {
			return fmt.Errorf("restore %s: %w", key, err)
		}
		if key == "" {
			key = definition
		}
		if _, dup := elements[key]; dup {
			return fmt.Errorf("%s is declared more than once", key)
		}
		elements[key] = definition
		return nil
	}
	for _, col := range ct.Raw.Cols {
		if err := add("column "+col.Name.Name.L, col); err != nil {
			return nil, err
		}
	}
	for _, c := range ct.Raw.Constraints {
		if err := add(constraintKey(c), c); err != nil {
			return nil, err
		}
	}
	for _, opt := range ct.Raw.Options {
		key, err := tableOptionKey(opt)
		if err != nil {
			return nil, err
		}
		if err := add(key, opt); err != nil {
			return nil, err
		}
	}
	rest := *ct.Raw
	rest.Cols, rest.Constraints, rest.Options = nil, nil, nil
	if err := add("the CREATE TABLE clauses", &rest); err != nil {
		return nil, err
	}
	return elements, nil
}

// constraintKey names an index or constraint the way MySQL does: indexes and
// constraints (foreign keys and checks) are separate namespaces, and the
// primary key has no name of its own. An unnamed one is keyed by its
// definition.
func constraintKey(c *ast.Constraint) string {
	switch {
	case c.Tp == ast.ConstraintPrimaryKey:
		return "the primary key"
	case c.Name == "":
		return ""
	case c.Tp == ast.ConstraintForeignKey || c.Tp == ast.ConstraintCheck:
		return "constraint " + strings.ToLower(c.Name)
	default:
		return "index " + strings.ToLower(c.Name)
	}
}

// tableOptionKey names a table option by what it sets: the restored option
// up to its value.
func tableOptionKey(opt *ast.TableOption) (string, error) {
	restored, err := restoreNode(opt)
	if err != nil {
		return "", fmt.Errorf("restore a table option: %w", err)
	}
	name, _, _ := strings.Cut(restored, " = ")
	return "table option " + name, nil
}

// restoreNode renders a parse tree node as SQL in the parser's canonical
// form, so formatting, comments, and keyword case do not count.
func restoreNode(node ast.Node) (string, error) {
	var sb strings.Builder
	if err := node.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		return "", err
	}
	return sb.String(), nil
}
