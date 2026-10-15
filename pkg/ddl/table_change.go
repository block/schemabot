package ddl

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

// SameTableChange reports whether a pull request's edit to a single-table
// MySQL schema file generates the same schema change on two different bases.
//
// approvedBase and approved are the file at the approved commit's merge base
// and at the approved commit; headBase and head are the file at the head's
// merge base and at the head. The change is the same when two things hold:
//
//   - Diffing each base against its commit yields the same statements, so the
//     reviewer approved exactly the statements the head generates.
//   - The head keeps every column, index, constraint, table option, and
//     partitioning clause that the base changed between approvedBase and
//     headBase exactly as headBase has it. Otherwise the head would replace
//     newer base content with the pull request's older version of it, and
//     the statements could still match: a column the base made NOT NULL and
//     the pull request widened is modified the same way on both bases, yet on
//     the newer one the statement also drops the NOT NULL.
//
// Together they mean the head is the newer base plus the approved change and
// nothing else. The diff ignores nothing (no AUTO_INCREMENT, engine, charset,
// partitioning, or row format exemptions), so any difference the planner
// could act on counts.
//
// When the change is not the same, reason says why, for operator logs. Each
// file must hold exactly one CREATE TABLE, all four naming the same table,
// and an element may be declared only once per file; an error means the files
// could not be compared and proves nothing.
func SameTableChange(approvedBase, approved, headBase, head string) (same bool, reason string, err error) {
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
		ct, err := parseSingleCreateTable(f.content)
		if err != nil {
			return false, "", fmt.Errorf("parse the schema file at the %s: %w", f.name, err)
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
	if !slices.Equal(approvedChange, headChange) {
		return false, fmt.Sprintf("table %s: the approved commit generates %q but the head generates %q",
			aBase.TableName, approvedChange, headChange), nil
	}

	overridden, err := overriddenBaseElement(aBase, hBase, h)
	if err != nil {
		return false, "", fmt.Errorf("compare elements of table %s: %w", aBase.TableName, err)
	}
	if overridden != "" {
		return false, fmt.Sprintf("table %s: the base changed %s since the approved commit, and the head replaces it with another version",
			aBase.TableName, overridden), nil
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

// overriddenBaseElement returns the first element (in name order) that the
// base changed between approvedBase and headBase but head does not have
// exactly as headBase has it, or "" when head keeps every such change. An
// element added or removed by the base counts as changed.
func overriddenBaseElement(approvedBase, headBase, head *statement.CreateTable) (string, error) {
	var elements [3]map[string]string
	for i, ct := range []*statement.CreateTable{approvedBase, headBase, head} {
		e, err := tableElements(ct)
		if err != nil {
			return "", err
		}
		elements[i] = e
	}
	aBase, hBase, h := elements[0], elements[1], elements[2]
	keys := make(map[string]bool)
	for k := range aBase {
		keys[k] = true
	}
	for k := range hBase {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		if sameElement(aBase, hBase, k) {
			continue
		}
		if !sameElement(hBase, h, k) {
			return k, nil
		}
	}
	return "", nil
}

// sameElement reports whether key is absent from both x and y or present in
// both with the same definition.
func sameElement(x, y map[string]string, key string) bool {
	xv, xok := x[key]
	yv, yok := y[key]
	return xok == yok && xv == yv
}

// tableElements keys every column, index, and constraint of ct by kind and
// lowercased name (MySQL matches them case-insensitively), plus the table
// options and partitioning, and maps each to its parsed definition. Column
// position is not part of a column's definition; a reordering shows up in the
// statements instead. An element declared twice cannot be told apart from the
// other, so it is an error.
func tableElements(ct *statement.CreateTable) (map[string]string, error) {
	elements := make(map[string]string)
	add := func(key string, definition any) error {
		if _, dup := elements[key]; dup {
			return fmt.Errorf("%s is declared more than once", key)
		}
		encoded, err := json.Marshal(definition)
		if err != nil {
			return fmt.Errorf("encode %s: %w", key, err)
		}
		elements[key] = string(encoded)
		return nil
	}
	for _, col := range ct.Columns {
		if err := add("column "+strings.ToLower(col.Name), col); err != nil {
			return nil, err
		}
	}
	for _, idx := range ct.Indexes {
		if err := add("index "+strings.ToLower(idx.Name), idx); err != nil {
			return nil, err
		}
	}
	for _, c := range ct.Constraints {
		if err := add("constraint "+strings.ToLower(c.Name), c); err != nil {
			return nil, err
		}
	}
	options := struct {
		Temporary    bool                    `json:"temporary"`
		IfNotExists  bool                    `json:"if_not_exists"`
		TableOptions *statement.TableOptions `json:"table_options"`
	}{ct.Temporary, ct.IfNotExists, ct.TableOptions}
	if err := add("table options", options); err != nil {
		return nil, err
	}
	if err := add("partitioning", ct.Partition); err != nil {
		return nil, err
	}
	return elements, nil
}
