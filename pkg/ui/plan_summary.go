package ui

import "fmt"

// PlanCounts is the one place a plan's statement totals are derived. Every
// summary surface (the CLI plan output and the PR plan comment) feeds it and
// renders from it, so the two can style a number differently but never
// disagree on it.
//
// A table is counted once per operation however many statements target it:
// three ALTER statements on one table are one table to alter. Statements
// outside the create/alter/drop buckets (types, extensions, comments,
// indexes) are counted as other so a mixed plan still names them.
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
// ("create", "alter", "drop"); a table is counted once per (namespace, op,
// table). Any other op is counted as an other statement.
//
// A statement with no table name cannot be judged a duplicate of anything,
// so it is counted rather than keyed: merging every nameless change into one
// would under-report on the surface that received it.
func (c *PlanCounts) AddTable(namespace, op, table string) {
	switch op {
	case "create", "alter", "drop":
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
	var parts []string
	appendTablePart := func(count int, op string) {
		if count == 0 {
			return
		}
		formattedCount := fmt.Sprintf("%d", count)
		if bold {
			formattedCount = "**" + formattedCount + "**"
		}
		parts = append(parts, fmt.Sprintf("%s %s to %s", formattedCount, Pluralize("table", count), op))
	}

	appendTablePart(counts.Created, "create")
	appendTablePart(counts.Altered, "alter")
	appendTablePart(counts.Dropped, "drop")
	if counts.Other > 0 && len(parts) > 0 {
		parts = append(parts, fmt.Sprintf("%d other DDL %s", counts.Other, Pluralize("statement", counts.Other)))
	}
	if len(parts) == 0 && rawTotal > 0 {
		parts = append(parts, fmt.Sprintf("%d DDL %s", rawTotal, Pluralize("statement", rawTotal)))
	}
	return parts
}
