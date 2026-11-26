package vschema

import (
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// tableChanges describes what differs between two definitions of one table,
// one clause per change, such as "adds vindex `email_lookup` on `email`" or
// "adds column `region`". Each clause names only what changes, so a reviewer
// never has to compare a before and an after to find the difference. A field
// these clauses do not cover reads as "changes `field`".
func tableChanges(current, desired *vschemapb.Table) []string {
	var clauses []string
	clauses = append(clauses, tableTypeChange(current.GetType(), desired.GetType())...)
	clauses = append(clauses, valueChange("source", current.GetSource(), desired.GetSource())...)
	clauses = append(clauses, pinChange(current.GetPinned(), desired.GetPinned())...)
	clauses = append(clauses, columnVindexChanges(current, desired)...)
	clauses = append(clauses, sequenceChange(current.GetAutoIncrement(), desired.GetAutoIncrement())...)
	clauses = append(clauses, columnChanges(current.GetColumns(), desired.GetColumns())...)
	clauses = append(clauses, authoritativeChange(current.GetColumnListAuthoritative(), desired.GetColumnListAuthoritative())...)
	clauses = append(clauses, otherFieldChanges(current, desired, describedTableFields)...)
	return clauses
}

// describedTableFields are the table fields tableChanges has its own clause
// for.
var describedTableFields = []string{"type", "source", "pinned", "column_vindexes", "auto_increment", "columns", "column_list_authoritative"}

// vindexChanges describes what differs between two definitions of one
// vindex, one clause per change.
func vindexChanges(current, desired *vschemapb.Vindex) []string {
	var clauses []string
	if current.GetType() != desired.GetType() {
		clauses = append(clauses, fmt.Sprintf("changes type from %s to %s", code(current.GetType()), code(desired.GetType())))
	}
	clauses = append(clauses, valueChange("owner", current.GetOwner(), desired.GetOwner())...)
	clauses = append(clauses, paramChanges(current.GetParams(), desired.GetParams())...)
	clauses = append(clauses, otherFieldChanges(current, desired, describedVindexFields)...)
	return clauses
}

// describedVindexFields are the vindex fields vindexChanges has its own
// clause for.
var describedVindexFields = []string{"type", "owner", "params"}

// tableTypeChange says what kind of table this becomes: "becomes a reference
// table", or "stops being a sequence table" for a table that becomes plain.
func tableTypeChange(current, desired string) []string {
	switch {
	case current == desired:
		return nil
	case desired == "":
		return []string{"stops being a " + tableKind(current)}
	default:
		return []string{"becomes a " + tableKind(desired)}
	}
}

// valueChange describes a single named value that is set, cleared, or
// replaced: "adds owner `orders`", "removes owner `orders`", or
// "changes owner from `orders` to `users`".
func valueChange(noun, current, desired string) []string {
	switch {
	case current == desired:
		return nil
	case current == "":
		return []string{fmt.Sprintf("adds %s %s", noun, code(desired))}
	case desired == "":
		return []string{fmt.Sprintf("removes %s %s", noun, code(current))}
	default:
		return []string{fmt.Sprintf("changes %s from %s to %s", noun, code(current), code(desired))}
	}
}

func pinChange(current, desired string) []string {
	switch {
	case current == desired:
		return nil
	case current == "":
		return []string{fmt.Sprintf("pins it to %s", code(desired))}
	case desired == "":
		return []string{fmt.Sprintf("unpins it from %s", code(current))}
	default:
		return []string{fmt.Sprintf("moves its pin from %s to %s", code(current), code(desired))}
	}
}

// columnVindexChanges describes how a table's column vindexes change. The
// first one is the primary vindex, which decides the shard each row lives
// on, so replacing it gets its own clause; the others read as added or
// removed.
func columnVindexChanges(current, desired *vschemapb.Table) []string {
	cur, des := columnVindexLabels(current), columnVindexLabels(desired)
	if slices.Equal(cur, des) {
		return nil
	}
	var clauses []string
	var curPrimary, desPrimary string
	primaryReplaced := len(cur) > 0 && len(des) > 0 && cur[0] != des[0]
	if primaryReplaced {
		curPrimary, desPrimary = cur[0], des[0]
		clauses = append(clauses, fmt.Sprintf("replaces primary vindex %s with %s", curPrimary, desPrimary))
		if slices.Contains(des, curPrimary) {
			clauses = append(clauses, fmt.Sprintf("keeps %s as a secondary vindex", curPrimary))
		}
	}
	var removed, added []string
	for _, label := range cur {
		if !slices.Contains(des, label) && label != curPrimary {
			removed = append(removed, label)
		}
	}
	for _, label := range des {
		if !slices.Contains(cur, label) && label != desPrimary {
			added = append(added, label)
		}
	}
	clauses = append(clauses, listChange("removes", "vindex", "vindexes", removed)...)
	clauses = append(clauses, listChange("adds", "vindex", "vindexes", added)...)
	if len(clauses) == 0 {
		clauses = append(clauses, "reorders its secondary vindexes")
	}
	return clauses
}

// columnVindexLabels renders each column vindex as "`hash` on `id`", in the
// table's order.
func columnVindexLabels(t *vschemapb.Table) []string {
	labels := make([]string, 0, len(t.GetColumnVindexes()))
	for _, cv := range t.GetColumnVindexes() {
		labels = append(labels, fmt.Sprintf("%s on %s", code(cv.GetName()), codeList(columnVindexColumns(cv))))
	}
	return labels
}

func sequenceChange(current, desired *vschemapb.AutoIncrement) []string {
	switch {
	case proto.Equal(current, desired):
		return nil
	case current == nil:
		return []string{"adds " + sequenceLabel(desired)}
	case desired == nil:
		return []string{"removes " + sequenceLabel(current)}
	default:
		return []string{fmt.Sprintf("replaces %s with %s", sequenceLabel(current), sequenceLabel(desired))}
	}
}

func sequenceLabel(ai *vschemapb.AutoIncrement) string {
	return fmt.Sprintf("sequence %s for %s", code(ai.GetSequence()), code(ai.GetColumn()))
}

// columnChanges describes a table's declared column list by column name:
// which columns it adds, removes, or redefines.
func columnChanges(current, desired []*vschemapb.Column) []string {
	cur, des := columnsByName(current), columnsByName(desired)
	var added, removed, changed []string
	for _, name := range unionKeys(cur, des) {
		c, inCurrent := cur[name]
		d, inDesired := des[name]
		switch {
		case !inCurrent:
			added = append(added, code(name))
		case !inDesired:
			removed = append(removed, code(name))
		case !proto.Equal(c, d):
			changed = append(changed, code(name))
		}
	}
	var clauses []string
	clauses = append(clauses, listChange("adds", "column", "columns", added)...)
	clauses = append(clauses, listChange("removes", "column", "columns", removed)...)
	clauses = append(clauses, listChange("changes the definition of", "column", "columns", changed)...)
	if len(clauses) == 0 && !columnOrderEqual(current, desired) {
		clauses = append(clauses, "reorders its columns")
	}
	return clauses
}

func columnsByName(columns []*vschemapb.Column) map[string]*vschemapb.Column {
	byName := make(map[string]*vschemapb.Column, len(columns))
	for _, c := range columns {
		byName[c.GetName()] = c
	}
	return byName
}

func columnOrderEqual(current, desired []*vschemapb.Column) bool {
	return slices.EqualFunc(current, desired, func(a, b *vschemapb.Column) bool { return a.GetName() == b.GetName() })
}

func authoritativeChange(current, desired bool) []string {
	switch {
	case current == desired:
		return nil
	case desired:
		return []string{"makes its column list authoritative"}
	default:
		return []string{"stops treating its column list as authoritative"}
	}
}

// paramChanges describes a vindex's params by key: "sets param `from` to
// `email`", "removes param `from`", or "changes param `table` from `a` to
// `b`".
func paramChanges(current, desired map[string]string) []string {
	var clauses []string
	for _, key := range unionKeys(current, desired) {
		c, inCurrent := current[key]
		d, inDesired := desired[key]
		switch {
		case !inCurrent:
			clauses = append(clauses, fmt.Sprintf("sets param %s to %s", code(key), code(d)))
		case !inDesired:
			clauses = append(clauses, fmt.Sprintf("removes param %s", code(key)))
		case c != d:
			clauses = append(clauses, fmt.Sprintf("changes param %s from %s to %s", code(key), code(c), code(d)))
		}
	}
	return clauses
}

// otherFieldChanges names each changed field the clauses above do not cover,
// so a line never leaves a change out.
func otherFieldChanges(current, desired proto.Message, described []string) []string {
	var clauses []string
	for _, field := range changedFields(current, desired) {
		if !slices.Contains(described, field) {
			clauses = append(clauses, "changes "+code(field))
		}
	}
	return clauses
}

// listChange renders a verb over one or more already rendered items:
// "adds vindex `a` on `x`" or "adds vindexes `a` on `x`, `b` on `y`".
func listChange(verb, singular, plural string, items []string) []string {
	switch len(items) {
	case 0:
		return nil
	case 1:
		return []string{fmt.Sprintf("%s %s %s", verb, singular, items[0])}
	default:
		return []string{fmt.Sprintf("%s %s %s", verb, plural, strings.Join(items, ", "))}
	}
}
