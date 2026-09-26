package ui

import "fmt"

// PlanCounts is the one place a plan's statement totals are derived. Every
// summary surface (the CLI plan output and the PR plan comment) feeds it and
// renders from it, so the two can style a number differently but never
// disagree on it.
//
// A table is counted once per operation however many statements target it:
// three ALTER statements on one table are one table to alter. Index builds
// and drops on existing tables are counted in their own buckets, one per
// statement, so a plan names the index work it will run without reporting
// the table as created or altered; indexes that ship inside a greenfield
// create set are part of that create and are not counted again. Statements
// outside every named bucket (types, extensions, comments) are counted as
// other so a mixed plan still names them.
type PlanCounts struct {
	Created        int
	Altered        int
	Dropped        int
	IndexesCreated int
	IndexesDropped int
	Other          int

	countedTables map[planTableKey]struct{}
}

// planTableKey identifies one table-level change. The namespace is part of
// the key so equal table names in two namespaces are two changes; within one
// namespace the bare relation name is sufficient.
type planTableKey struct {
	namespace string
	op        string
	table     string
}

// AddTable counts a classified statement. op is the lowercase operation
// ("create", "alter", "drop", "create_index", "drop_index"). A table-level op
// is counted once per (namespace, op, table); an index op is counted once per
// statement, since each names a distinct index and the table it belongs to
// is not what the count reports. Any other op is counted as an other
// statement.
//
// A table-level statement with no table name cannot be judged a duplicate of
// anything, so it is counted rather than keyed: merging every nameless change
// into one would under-report on the surface that received it.
func (c *PlanCounts) AddTable(namespace, op, table string) {
	switch op {
	case "create", "alter", "drop":
	case "create_index":
		c.IndexesCreated++
		return
	case "drop_index":
		c.IndexesDropped++
		return
	default:
		c.AddOther()
		return
	}
	if table != "" {
		key := planTableKey{namespace: namespace, op: op, table: table}
		if c.countedTables == nil {
			c.countedTables = make(map[planTableKey]struct{})
		}
		if _, counted := c.countedTables[key]; counted {
			return
		}
		c.countedTables[key] = struct{}{}
	}
	switch op {
	case "create":
		c.Created++
	case "alter":
		c.Altered++
	case "drop":
		c.Dropped++
	}
}

// AddOther counts a statement outside the named buckets.
func (c *PlanCounts) AddOther() {
	c.Other++
}

// PlanSummaryParts renders the shared summary clauses ("2 tables to alter",
// "1 index to create", "1 other DDL statement") from the counts. bold wraps
// each table and index count in markdown emphasis for the PR comment; the CLI
// renders plain numbers.
//
// Other statements are named only next to typed counts, so the typed counts
// never imply they are all there is. A plan with no typed count at all — only
// unbucketed DDL, or a dialect with no parser to classify it — reports
// rawTotal, the caller's raw statement count, so it never reads as
// "no changes".
func PlanSummaryParts(counts PlanCounts, rawTotal int, bold bool) []string {
	countedClause := func(count int, noun, op string) string {
		formattedCount := fmt.Sprintf("%d", count)
		if bold {
			formattedCount = "**" + formattedCount + "**"
		}
		return fmt.Sprintf("%s %s to %s", formattedCount, PluralizeNoun(noun, count), op)
	}
	ddlClause := func(count int, other bool) string {
		prefix := ""
		if other {
			prefix = "other "
		}
		return fmt.Sprintf("%d %sDDL %s", count, prefix, Pluralize("statement", count))
	}
	return AssemblePlanSummary(counts, rawTotal, countedClause, ddlClause)
}

// PluralizeNoun pluralizes the nouns the plan summary counts. "index" takes
// "-es"; every other noun takes "-s".
func PluralizeNoun(noun string, count int) string {
	if noun == "index" {
		return PluralizeLabel("index", "indexes", count)
	}
	return Pluralize(noun, count)
}

// AssemblePlanSummary applies the shared clause ordering and fallback rules
// while allowing each surface to supply its own wording. countedClause
// renders one typed bucket from its count, its noun ("table" or "index") and
// its operation ("create", "alter", "drop"); ddlClause renders the other
// statements, or the raw total when nothing was typed.
func AssemblePlanSummary(counts PlanCounts, rawTotal int, countedClause func(count int, noun, op string) string, ddlClause func(int, bool) string) []string {
	var parts []string
	for _, item := range []struct {
		count int
		noun  string
		op    string
	}{
		{counts.Created, "table", "create"},
		{counts.Altered, "table", "alter"},
		{counts.Dropped, "table", "drop"},
		{counts.IndexesCreated, "index", "create"},
		{counts.IndexesDropped, "index", "drop"},
	} {
		if item.count > 0 {
			parts = append(parts, countedClause(item.count, item.noun, item.op))
		}
	}
	if counts.Other > 0 && len(parts) > 0 {
		parts = append(parts, ddlClause(counts.Other, true))
	}
	if len(parts) == 0 && rawTotal > 0 {
		parts = append(parts, ddlClause(rawTotal, false))
	}
	return parts
}
