package ui

import "fmt"

// PlanCounts is the one place a plan's statement totals are derived. Every
// summary surface (the CLI plan output and the PR plan comment) feeds it and
// renders from it, so the two can style a number differently but never
// disagree on it.
//
// A table is counted once per operation however many statements target it:
// three ALTER statements on one table are one table to alter. A CREATE INDEX
// on an existing table alters that table's schema and counts as one alter of
// it, so an ALTER and an index build on one table are still one table to
// alter. Statements outside the create/alter/drop buckets (types, extensions,
// comments, index drops) are counted as other so a mixed plan still names
// them.
type PlanCounts struct {
	Created int
	Altered int
	Dropped int
	Other   int

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

// AddTable counts a table-level statement. op is the lowercase operation
// ("create", "alter", "drop", "create_index"); a table is counted once per
// (namespace, bucket, table), and an index build shares its table's alter
// bucket. Any other op is counted as an other statement.
//
// An index drop stays in the other bucket on every surface: a PostgreSQL
// DROP INDEX names only the index, so a surface that counts from statement
// text cannot attribute it to a table, and the surfaces must agree.
//
// A statement with no table name cannot be judged a duplicate of anything,
// so it is counted rather than keyed: merging every nameless change into one
// would under-report on the surface that received it.
func (c *PlanCounts) AddTable(namespace, op, table string) {
	switch op {
	case "create", "alter", "drop":
	case "create_index":
		op = "alter"
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

// AddOther counts a statement outside the create/alter/drop buckets.
func (c *PlanCounts) AddOther() {
	c.Other++
}

// PlanSummaryParts renders the shared summary clauses ("2 tables to alter",
// "1 other DDL statement") from the counts. bold wraps each table count in
// markdown emphasis for the PR comment; the CLI renders plain numbers.
//
// Other statements are named only next to typed counts, so the typed counts
// never imply they are all there is. A plan with no create/alter/drop at all
// — only unbucketed DDL, or a dialect with no parser to classify it — reports
// rawTotal, the caller's raw statement count, so it never reads as
// "no changes".
func PlanSummaryParts(counts PlanCounts, rawTotal int, bold bool) []string {
	tableClause := func(count int, op string) string {
		formattedCount := fmt.Sprintf("%d", count)
		if bold {
			formattedCount = "**" + formattedCount + "**"
		}
		return fmt.Sprintf("%s %s to %s", formattedCount, Pluralize("table", count), op)
	}
	ddlClause := func(count int, other bool) string {
		prefix := ""
		if other {
			prefix = "other "
		}
		return fmt.Sprintf("%d %sDDL %s", count, prefix, Pluralize("statement", count))
	}
	return AssemblePlanSummary(counts, rawTotal, tableClause, ddlClause)
}

// AssemblePlanSummary applies the shared clause ordering and fallback rules
// while allowing each surface to supply its own wording.
func AssemblePlanSummary(counts PlanCounts, rawTotal int, tableClause func(int, string) string, ddlClause func(int, bool) string) []string {
	var parts []string
	for _, item := range []struct {
		count int
		op    string
	}{{counts.Created, "create"}, {counts.Altered, "alter"}, {counts.Dropped, "drop"}} {
		if item.count > 0 {
			parts = append(parts, tableClause(item.count, item.op))
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
