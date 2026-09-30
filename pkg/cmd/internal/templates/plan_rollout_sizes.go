package templates

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/ui"
)

// rolloutTableSizesListLimit caps how many tables the size section lists. A
// plan that changes more tables lists the largest first and counts the rest,
// so a plan that indexes hundreds of tables does not wall the terminal.
const rolloutTableSizesListLimit = 20

// rolloutTableSize is one table's size estimate on every member whose plan
// copies, rebuilds, or scans it, in rollout order.
type rolloutTableSize struct {
	name      string
	perMember []*apitypes.PlanMemberTableSizeResponse
}

// sized returns the members that reported an estimate for the table and their
// summed bytes.
func (t rolloutTableSize) sized() (sized []*apitypes.PlanMemberTableSizeResponse, total int64) {
	for _, s := range t.perMember {
		if s.EstimatedBytes == nil {
			continue
		}
		sized = append(sized, s)
		total += *s.EstimatedBytes
	}
	return sized, total
}

// FormatRolloutTableSizes renders how big each table the rollout copies,
// rebuilds, or scans is, across every member that changes it, the way the PR
// comment does: a table one member changes names that member, and a table
// several members change gives the total, the largest member by name, and the
// smallest. A member with no estimate is counted rather than left out, since
// the total then understates the table. It returns "" when no member reported
// an estimate for any table: an engine that does not estimate sizes would
// otherwise print "unavailable" on every line, which reads as a failed probe
// when none ran.
func FormatRolloutTableSizes(noun presentation.Noun, sizes []*apitypes.PlanMemberTableSizeResponse) string {
	tables := rolloutTableSizes(sizes)
	if !slices.ContainsFunc(tables, func(t rolloutTableSize) bool {
		sized, _ := t.sized()
		return len(sized) > 0
	}) {
		return ""
	}
	members := make(map[string]bool)
	for _, s := range sizes {
		members[s.Member] = true
	}
	label := noun.Plural
	if len(members) == 1 {
		label = noun.Singular
	}

	var more int
	if len(tables) > rolloutTableSizesListLimit {
		slices.SortStableFunc(tables, func(a, b rolloutTableSize) int {
			_, at := a.sized()
			_, bt := b.sized()
			return cmp.Compare(bt, at)
		})
		more = len(tables) - rolloutTableSizesListLimit
		tables = tables[:rolloutTableSizesListLimit]
	}
	width := 0
	for _, t := range tables {
		width = max(width, len(t.name))
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%sTable sizes (%d %s):%s\n", ANSIBold, len(members), label, ANSIReset)
	for _, t := range tables {
		fmt.Fprintf(&b, "  %-*s  %s\n", width, t.name, formatRolloutTableSize(noun, t))
	}
	if more > 0 {
		word := "tables"
		if more == 1 {
			word = "table"
		}
		fmt.Fprintf(&b, "  %s…and %d more %s%s\n", ANSIDim, more, word, ANSIReset)
	}
	b.WriteString("\n")
	return b.String()
}

// rolloutTableSizes groups every member's sizes into one entry per table, in
// the order the tables first appear in rollout order.
func rolloutTableSizes(sizes []*apitypes.PlanMemberTableSizeResponse) []rolloutTableSize {
	type tableKey struct{ namespace, table string }
	at := make(map[tableKey]int)
	var tables []rolloutTableSize
	for _, s := range sizes {
		if s == nil {
			continue
		}
		key := tableKey{s.Namespace, s.Table}
		i, seen := at[key]
		if !seen {
			i = len(tables)
			at[key] = i
			tables = append(tables, rolloutTableSize{name: s.Namespace + "." + s.Table})
		}
		tables[i].perMember = append(tables[i].perMember, s)
	}
	return tables
}

// formatRolloutTableSize renders one table's size clause.
func formatRolloutTableSize(noun presentation.Noun, t rolloutTableSize) string {
	members := len(t.perMember)
	sized, total := t.sized()
	switch {
	case len(sized) == 0 && members == 1:
		return "size estimate unavailable on " + t.perMember[0].Member
	case len(sized) == 0:
		return fmt.Sprintf("size estimate unavailable on all %d %s", members, noun.Plural)
	case members == 1:
		return fmt.Sprintf("%s on %s", ui.FormatApproxBytes(total), t.perMember[0].Member)
	}
	largest, smallest := sized[0], sized[0]
	for _, s := range sized[1:] {
		if *s.EstimatedBytes > *largest.EstimatedBytes {
			largest = s
		}
		if *s.EstimatedBytes < *smallest.EstimatedBytes {
			smallest = s
		}
	}
	totalClause := ui.FormatApproxBytes(total) + " total"
	if len(sized) < members {
		totalClause = fmt.Sprintf("%s total across %d of %d %s", ui.FormatApproxBytes(total), len(sized), members, noun.Plural)
	}
	parts := []string{
		totalClause,
		fmt.Sprintf("largest %s on %s", ui.FormatApproxBytes(*largest.EstimatedBytes), largest.Member),
		"smallest " + ui.FormatApproxBytes(*smallest.EstimatedBytes),
	}
	if unsized := members - len(sized); unsized > 0 {
		verb := "has"
		if unsized > 1 {
			verb = "have"
		}
		parts = append(parts, fmt.Sprintf("%d %s no estimate", unsized, verb))
	}
	return strings.Join(parts, " · ")
}
