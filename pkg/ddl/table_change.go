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
// MySQL schema file generates the same schema change on two different bases:
// the head runs no DDL the reviewer did not approve, and skips none the
// head's base has not already run.
//
// approvedBase and approved are the file at the approved commit's merge base
// and at the approved commit; headBase and head are the file at the head's
// merge base and at the head. The DDL each commit runs is its diff from its
// base. The change is the same when every clause the head's DDL runs is one
// the approved DDL runs, in the same order, and every approved clause the
// head skips is one the diff from the approved commit's base to the head's
// base runs, as when a stacked pull request below this one merged part of the
// edit (see clausesCovered). On the same base that diff is empty, so the head
// must run every approved clause. The diff ignores nothing (no AUTO_INCREMENT,
// engine, charset, partitioning, or row format exemptions), so any
// difference the planner could act on counts.
//
// A file the pull request adds is absent at both bases, passed as "". Its
// DDL is the CREATE TABLE itself, so it is the same when the head's CREATE
// TABLE is the approved one, compared in the parser's canonical form. A file
// absent at one base but not the other is an error.
//
// When the change is not the same, reason says why, for operator logs. Each
// file must hold exactly one CREATE TABLE, all four naming the same table,
// and an element may be declared only once per file; an error means the files
// could not be compared and proves nothing.
func SameTableChange(approvedBase, approved, headBase, head string) (same bool, reason string, err error) {
	switch {
	case approvedBase == "" && headBase == "":
		return sameCreateTable(approved, head)
	case approvedBase == "" || headBase == "":
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
	for i, f := range files {
		ct, err := parseComparableTable(f.name, f.content)
		if err != nil {
			return false, "", err
		}
		tables[i] = ct
	}
	aBase, a, hBase, h := tables[0], tables[1], tables[2], tables[3]
	for i, ct := range tables {
		if ct.TableName != aBase.TableName {
			return false, fmt.Sprintf("the file declares table %s at the %s but %s at the %s",
				aBase.TableName, files[0].name, ct.TableName, files[i].name), nil
		}
	}

	approvedChange, err := strictTableDiff(aBase, a)
	if err != nil {
		return false, "", fmt.Errorf("diff table %s at the approved commit against its base: %w", aBase.TableName, err)
	}
	headChange, err := strictTableDiff(hBase, h)
	if err != nil {
		return false, "", fmt.Errorf("diff table %s at the head against its base: %w", aBase.TableName, err)
	}
	baseMove, err := strictTableDiff(aBase, hBase)
	if err != nil {
		return false, "", fmt.Errorf("diff table %s at the head's base against the approved commit's base: %w", aBase.TableName, err)
	}
	covered, err := clausesCovered(approvedChange, headChange, baseMove)
	if err != nil {
		return false, "", fmt.Errorf("read the clauses of table %s's statements: %w", aBase.TableName, err)
	}
	if !covered {
		return false, fmt.Sprintf("table %s: the approved commit generates %q but the head generates %q, and the head's base already runs %q",
			aBase.TableName, approvedChange, headChange, baseMove), nil
	}
	return true, "", nil
}

// sameCreateTable reports whether the head adds the table with the same
// CREATE TABLE the approved commit adds. The statement runs as written, so
// every clause counts, including any the diff does not model.
func sameCreateTable(approved, head string) (same bool, reason string, err error) {
	a, h, err := parseApprovedAndHead(approved, head)
	if err != nil {
		return false, "", err
	}
	approvedCreate, err := restoreNode(a.Raw)
	if err != nil {
		return false, "", fmt.Errorf("restore table %s at the approved commit: %w", a.TableName, err)
	}
	headCreate, err := restoreNode(h.Raw)
	if err != nil {
		return false, "", fmt.Errorf("restore table %s at the head: %w", h.TableName, err)
	}
	if approvedCreate != headCreate {
		return false, fmt.Sprintf("table %s: the approved commit runs %q but the head runs %q", a.TableName, approvedCreate, headCreate), nil
	}
	return true, "", nil
}

// parseApprovedAndHead parses the schema file at the approved commit and at
// the head.
func parseApprovedAndHead(approved, head string) (a, h *statement.CreateTable, err error) {
	a, err = parseComparableTable("approved commit", approved)
	if err != nil {
		return nil, nil, err
	}
	h, err = parseComparableTable("head", head)
	if err != nil {
		return nil, nil, err
	}
	return a, h, nil
}

// parseComparableTable parses the schema file at the named commit, which must
// hold one CREATE TABLE that declares each element once: the diff assumes it.
func parseComparableTable(name, content string) (*statement.CreateTable, error) {
	ct, err := parseSingleCreateTable(content)
	if err != nil {
		return nil, fmt.Errorf("parse the schema file at the %s: %w", name, err)
	}
	if _, err := tableElements(ct); err != nil {
		return nil, fmt.Errorf("read table %s at the %s: %w", ct.TableName, name, err)
	}
	return ct, nil
}

// clausesCovered reports whether the head's statements are covered by the
// approved ones given the base move (see CoveredByApproval), compared clause
// by clause. A diff that takes more than one statement (Spirit splits some
// column conversions in two) is covered only by the same statements, since
// its clauses cannot be regrouped without changing what each statement does,
// unless the head runs nothing at all.
func clausesCovered(approved, head, baseMove []string) (bool, error) {
	if len(head) > 1 || (len(head) == 1 && len(approved) > 1) {
		return slices.Equal(approved, head), nil
	}
	approvedClauses, err := statementClauses(approved)
	if err != nil {
		return false, fmt.Errorf("the approved commit's statement: %w", err)
	}
	headClauses, err := statementClauses(head)
	if err != nil {
		return false, fmt.Errorf("the head's statement: %w", err)
	}
	baseClauses, err := statementClauses(baseMove)
	if err != nil {
		return false, fmt.Errorf("the base move's statement: %w", err)
	}
	return CoveredByApproval(approvedClauses, headClauses, baseClauses), nil
}

// A ChangeItem is one clause or statement of a schema change: its SQL in the
// parser's canonical form and the elements (columns, indexes, constraints,
// table options) it changes, keyed so the same element gets the same key in
// every diff. Elements is empty when the item's elements are not known; such
// an item stands only for itself.
type ChangeItem struct {
	SQL      string
	Elements []string
}

// touches lists the keys of what item changes: its elements, or the item
// itself when those are not known.
func (item ChangeItem) touches() []string {
	if len(item.Elements) == 0 {
		return []string{"the item " + item.SQL}
	}
	return item.Elements
}

// CoveredByApproval reports whether the head's change is the approved one on
// the head's base. head must run only items approved runs, in the order
// approved lists them. An approved item head skips must be one the base move,
// the diff from the approved commit's base to the head's base, already made:
// the base move runs the item itself, or changes every element the item
// changes, as when a stacked pull request below this one merged the element
// first, possibly with another definition. Either way head must run nothing
// on those elements, so a head that drops half of an approved redefinition
// is never covered. On the same base the base move is empty, so head must
// run every approved item: dropping part of an approved change is a
// different change.
func CoveredByApproval(approved, head, baseMove []ChangeItem) bool {
	headTouches := touchedKeys(head)
	baseTouches := touchedKeys(baseMove)
	madeByBase := func(skipped ChangeItem) bool {
		for _, key := range skipped.touches() {
			if headTouches[key] {
				return false
			}
		}
		if slices.ContainsFunc(baseMove, func(b ChangeItem) bool { return b.SQL == skipped.SQL }) {
			return true
		}
		for _, key := range skipped.touches() {
			if !baseTouches[key] {
				return false
			}
		}
		return true
	}
	next := 0
	for _, item := range head {
		for next < len(approved) && approved[next].SQL != item.SQL {
			if !madeByBase(approved[next]) {
				return false
			}
			next++
		}
		if next == len(approved) {
			return false
		}
		next++
	}
	for _, skipped := range approved[next:] {
		if !madeByBase(skipped) {
			return false
		}
	}
	return true
}

// touchedKeys collects every key items touch.
func touchedKeys(items []ChangeItem) map[string]bool {
	keys := make(map[string]bool)
	for _, item := range items {
		for _, key := range item.touches() {
			keys[key] = true
		}
	}
	return keys
}

// statementClauses returns the clauses of every ALTER TABLE statement in
// stmts, in order.
func statementClauses(stmts []string) ([]ChangeItem, error) {
	var clauses []ChangeItem
	for _, stmt := range stmts {
		c, err := alterClauses(stmt)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, c...)
	}
	return clauses, nil
}

// alterClauses parses an ALTER TABLE statement and returns each of its
// clauses in the parser's canonical form, with the elements it changes, in
// order.
func alterClauses(stmt string) ([]ChangeItem, error) {
	parsed, err := statement.New(stmt)
	if err != nil {
		return nil, fmt.Errorf("parse %q: %w", stmt, err)
	}
	if len(parsed) != 1 {
		return nil, fmt.Errorf("parse %q: expected one statement, got %d", stmt, len(parsed))
	}
	alter, ok := (*parsed[0].StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return nil, fmt.Errorf("parse %q: %w", stmt, statement.ErrNotAlterTable)
	}
	clauses := make([]ChangeItem, 0, len(alter.Specs))
	for _, spec := range alter.Specs {
		clause, err := restoreNode(spec)
		if err != nil {
			return nil, fmt.Errorf("restore a clause of %q: %w", stmt, err)
		}
		elements, err := specElements(spec)
		if err != nil {
			return nil, fmt.Errorf("read the elements of clause %q: %w", clause, err)
		}
		clauses = append(clauses, ChangeItem{SQL: clause, Elements: elements})
	}
	return clauses, nil
}

// specElements keys the elements an ALTER TABLE clause changes the way
// tableElements keys them, or returns none for a clause whose elements it
// does not know.
func specElements(spec *ast.AlterTableSpec) ([]string, error) {
	column := func(name string) string { return "column " + strings.ToLower(name) }
	index := func(name string) string { return "index " + strings.ToLower(name) }
	switch spec.Tp {
	case ast.AlterTableAddColumns, ast.AlterTableModifyColumn, ast.AlterTableAlterColumn:
		elements := make([]string, 0, len(spec.NewColumns))
		for _, col := range spec.NewColumns {
			elements = append(elements, column(col.Name.Name.O))
		}
		return elements, nil
	case ast.AlterTableChangeColumn:
		return []string{column(spec.OldColumnName.Name.O), column(spec.NewColumns[0].Name.Name.O)}, nil
	case ast.AlterTableRenameColumn:
		return []string{column(spec.OldColumnName.Name.O), column(spec.NewColumnName.Name.O)}, nil
	case ast.AlterTableDropColumn, ast.AlterTableAlterColumnVisibility:
		return []string{column(spec.OldColumnName.Name.O)}, nil
	case ast.AlterTableAddConstraint:
		if key := constraintKey(spec.Constraint); key != "" {
			return []string{key}, nil
		}
		return nil, nil
	case ast.AlterTableDropPrimaryKey:
		return []string{"the primary key"}, nil
	case ast.AlterTableDropIndex:
		return []string{index(spec.Name)}, nil
	case ast.AlterTableDropForeignKey:
		return []string{"foreign key " + strings.ToLower(spec.Name)}, nil
	case ast.AlterTableDropCheck, ast.AlterTableAlterCheck:
		return []string{"check constraint " + strings.ToLower(spec.Constraint.Name)}, nil
	case ast.AlterTableRenameIndex:
		return []string{index(spec.FromKey.O), index(spec.ToKey.O)}, nil
	case ast.AlterTableIndexInvisible:
		return []string{index(spec.IndexName.O)}, nil
	case ast.AlterTableOption:
		elements := make([]string, 0, len(spec.Options))
		for _, opt := range spec.Options {
			key, err := tableOptionKey(opt)
			if err != nil {
				return nil, err
			}
			elements = append(elements, key)
		}
		return elements, nil
	default:
		return nil, nil
	}
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

// UncoveredElementChange returns why the head's change differs from the
// approved one, or "" when it is the same: every element (in name order) the
// head changes from its base is one the approved commit changes from its base
// to the same definition, and every element the approved commit changes that
// the head does not is one the base changed since the approved commit's base,
// as when a stacked pull request below this one merged it first. On the same
// base the head must change every element the approved commit changes. Each
// map holds one schema file's elements, keyed by a name that
// says what the element is and mapped to its definition; an element added,
// removed, or redefined counts as changed.
func UncoveredElementChange(approvedBase, approved, headBase, head map[string]string) string {
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
		approvedChanges := !sameElement(approvedBase, approved, k)
		if sameElement(headBase, head, k) {
			if approvedChanges && sameElement(approvedBase, headBase, k) {
				return fmt.Sprintf("the head no longer changes %s, which the approved commit changes and the base did not", k)
			}
			continue
		}
		if !approvedChanges {
			return fmt.Sprintf("the head changes %s, which the approved commit does not", k)
		}
		if !sameElement(approved, head, k) {
			return fmt.Sprintf("the head changes %s differently from the approved commit", k)
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

// tableElements keys every column, index, and constraint of ct by kind and
// lowercased name (MySQL matches them case-insensitively), every table option
// by name, and everything else the statement declares as one element, and
// maps each to its definition as the parser read it. An element declared
// twice is an error: the diff would see only one of the two.
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

// constraintKey names an index or constraint the way MySQL does: indexes,
// foreign keys, and CHECK constraints are each their own namespace, so a
// foreign key and a CHECK may share a name, and the primary key has no name of
// its own. An unnamed one is keyed by its definition.
func constraintKey(c *ast.Constraint) string {
	switch {
	case c.Tp == ast.ConstraintPrimaryKey:
		return "the primary key"
	case c.Name == "":
		return ""
	case c.Tp == ast.ConstraintForeignKey:
		return "foreign key " + strings.ToLower(c.Name)
	case c.Tp == ast.ConstraintCheck:
		return "check constraint " + strings.ToLower(c.Name)
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
