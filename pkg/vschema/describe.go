package vschema

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// Describe returns one line per table, vindex, or keyspace setting that
// differs between the current and desired VSchema, in the words a reviewer
// reads a VSchema change in: "adds table `orders`: vindex `hash` on `id`".
// Tables come first, then vindexes, then the keyspace settings, each sorted
// by name. A line diff of the whole document cannot say which table a change
// belongs to, because an added table's lines start inside the table above
// it; comparing table by table can. Returns nil when the documents do not
// differ. Both documents must parse as VSchema keyspace JSON.
func Describe(current, desired string) ([]string, error) {
	cur, err := parseKeyspace(current)
	if err != nil {
		return nil, fmt.Errorf("parse current VSchema: %w", err)
	}
	des, err := parseKeyspace(desired)
	if err != nil {
		return nil, fmt.Errorf("parse desired VSchema: %w", err)
	}
	var lines []string
	lines = append(lines, describeTables(cur.GetTables(), des.GetTables())...)
	lines = append(lines, describeVindexes(cur.GetVindexes(), des.GetVindexes())...)
	lines = append(lines, describeKeyspaceSettings(cur, des)...)
	return lines, nil
}

func describeTables(current, desired map[string]*vschemapb.Table) []string {
	var lines []string
	for _, name := range unionKeys(current, desired) {
		cur, inCurrent := current[name]
		des, inDesired := desired[name]
		switch {
		case !inCurrent:
			lines = append(lines, withDetail(fmt.Sprintf("adds table %s", code(name)), tableRouting(des)))
		case !inDesired:
			lines = append(lines, fmt.Sprintf("removes table %s", code(name)))
		case !proto.Equal(cur, des):
			lines = append(lines, fmt.Sprintf("changes table %s: %s", code(name), tableChange(cur, des)))
		}
	}
	return lines
}

func describeVindexes(current, desired map[string]*vschemapb.Vindex) []string {
	var lines []string
	for _, name := range unionKeys(current, desired) {
		cur, inCurrent := current[name]
		des, inDesired := desired[name]
		switch {
		case !inCurrent:
			lines = append(lines, withDetail(fmt.Sprintf("adds vindex %s", code(name)), vindexDefinition(des)))
		case !inDesired:
			lines = append(lines, fmt.Sprintf("removes vindex %s", code(name)))
		case !proto.Equal(cur, des):
			lines = append(lines, fmt.Sprintf("changes vindex %s: %s", code(name), vindexChange(cur, des)))
		}
	}
	return lines
}

func describeKeyspaceSettings(current, desired *vschemapb.Keyspace) []string {
	cur := proto.CloneOf(current)
	cur.Tables, cur.Vindexes = nil, nil
	des := proto.CloneOf(desired)
	des.Tables, des.Vindexes = nil, nil
	if proto.Equal(cur, des) {
		return nil
	}
	var changes []string
	if cur.GetSharded() != des.GetSharded() {
		changes = append(changes, fmt.Sprintf("makes the keyspace %s", shardedLabel(des.GetSharded())))
	}
	if cur.GetRequireExplicitRouting() != des.GetRequireExplicitRouting() {
		if des.GetRequireExplicitRouting() {
			changes = append(changes, "requires explicit routing")
		} else {
			changes = append(changes, "stops requiring explicit routing")
		}
	}
	cur.Sharded, des.Sharded = false, false
	cur.RequireExplicitRouting, des.RequireExplicitRouting = false, false
	if !proto.Equal(cur, des) {
		changes = append(changes, "changes other keyspace settings")
	}
	return changes
}

// tableRouting describes how Vitess routes a table's rows: its type, pin,
// column vindexes, and sequence. Returns "" for a plain table with none.
func tableRouting(t *vschemapb.Table) string {
	var parts []string
	if t.GetType() != "" {
		parts = append(parts, fmt.Sprintf("%s table", t.GetType()))
	}
	if source := t.GetSource(); source != "" {
		parts = append(parts, fmt.Sprintf("source %s", code(source)))
	}
	if pin := t.GetPinned(); pin != "" {
		parts = append(parts, fmt.Sprintf("pinned to %s", code(pin)))
	}
	if vindexes := columnVindexList(t); vindexes != "" {
		parts = append(parts, vindexes)
	}
	if ai := t.GetAutoIncrement(); ai != nil {
		parts = append(parts, fmt.Sprintf("%s from sequence %s", code(ai.GetColumn()), code(ai.GetSequence())))
	}
	return strings.Join(parts, ", ")
}

// columnVindexList renders a table's column vindexes in order, the first
// being its primary vindex: "vindex `hash` on `id`", or
// "vindexes `hash` on `id`, `email_lookup` on `email`".
func columnVindexList(t *vschemapb.Table) string {
	cvs := t.GetColumnVindexes()
	if len(cvs) == 0 {
		return ""
	}
	entries := make([]string, 0, len(cvs))
	for _, cv := range cvs {
		entries = append(entries, fmt.Sprintf("%s on %s", code(cv.GetName()), codeList(columnVindexColumns(cv))))
	}
	if len(entries) == 1 {
		return "vindex " + entries[0]
	}
	return "vindexes " + strings.Join(entries, ", ")
}

// tableChange describes what differs between two definitions of one table.
// The routing reads as before and after, since that is what a reviewer
// checks; any other field only says which fields change.
func tableChange(current, desired *vschemapb.Table) string {
	before, after := tableRouting(current), tableRouting(desired)
	if before != after {
		return fmt.Sprintf("%s becomes %s", orNone(before), orNone(after))
	}
	return "changes " + codeList(changedFields(current, desired))
}

func vindexDefinition(v *vschemapb.Vindex) string {
	var parts []string
	if v.GetType() != "" {
		parts = append(parts, fmt.Sprintf("type %s", code(v.GetType())))
	}
	if owner := v.GetOwner(); owner != "" {
		parts = append(parts, fmt.Sprintf("owned by %s", code(owner)))
	}
	if table := v.GetParams()["table"]; table != "" {
		parts = append(parts, fmt.Sprintf("backed by %s", code(table)))
	}
	return strings.Join(parts, ", ")
}

func vindexChange(current, desired *vschemapb.Vindex) string {
	if current.GetType() != desired.GetType() {
		return fmt.Sprintf("type %s becomes %s", code(current.GetType()), code(desired.GetType()))
	}
	return "changes " + codeList(changedFields(current, desired))
}

// changedFields names the top-level fields whose values differ between two
// messages of the same type, in declaration order.
func changedFields(current, desired proto.Message) []string {
	fields := current.ProtoReflect().Descriptor().Fields()
	var names []string
	for i := range fields.Len() {
		fd := fields.Get(i)
		cur, des := current.ProtoReflect(), desired.ProtoReflect()
		if cur.Has(fd) != des.Has(fd) || (cur.Has(fd) && !cur.Get(fd).Equal(des.Get(fd))) {
			names = append(names, string(fd.Name()))
		}
	}
	return names
}

func withDetail(line, detail string) string {
	if detail == "" {
		return line
	}
	return line + ": " + detail
}

func orNone(routing string) string {
	if routing == "" {
		return "no routing"
	}
	return routing
}

// code renders a name as inline code. A VSchema name is free text, so a
// newline is folded to a space (a summary is one line per element) and a name
// holding a backtick is fenced with two.
func code(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if strings.Contains(s, "`") {
		return "`` " + s + " ``"
	}
	return "`" + s + "`"
}

func codeList(items []string) string {
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = code(item)
	}
	return strings.Join(quoted, ", ")
}

func unionKeys[V any](a, b map[string]V) []string {
	keys := make(map[string]struct{}, len(a)+len(b))
	for k := range maps.Keys(a) {
		keys[k] = struct{}{}
	}
	for k := range maps.Keys(b) {
		keys[k] = struct{}{}
	}
	return slices.Sorted(maps.Keys(keys))
}
