package templates

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
)

// rolloutTableSize is one table's size estimate on every member whose plan
// copies, rebuilds, or scans it, in rollout order.
type rolloutTableSize struct {
	name      string
	perMember []presentation.MemberTableSize
}

// rankBytes is the figure the section ranks the table by: the total across
// the members that report an estimate, or nil when none does.
func (t rolloutTableSize) rankBytes() *int64 {
	total, sized := presentation.SumTableSizeBytes(t.perMember)
	if sized == 0 {
		return nil
	}
	return &total
}

// FormatRolloutTableSizes renders how big each table the rollout copies,
// rebuilds, or scans is, across every member that changes it, with the PR
// comment's wording, naming, and limits: up to
// presentation.TableSizesInlineLimit tables in rollout order, otherwise the
// largest first, capped at presentation.TableSizesShown with the rest counted
// and those of them with no estimate counted apart. A terminal cannot fold a
// long section the way the comment does, so it is capped the same way and
// listed in the open. Table names carry their namespace only when the sizes
// span several namespaces. It returns "" when
// no member reported an estimate for any table: an engine that does not
// estimate sizes would otherwise print "unavailable" on every line, which
// reads as a failed probe when none ran.
func FormatRolloutTableSizes(noun presentation.Noun, sizes []*apitypes.PlanMemberTableSizeResponse) string {
	tables := rolloutTableSizes(sizes)
	if !slices.ContainsFunc(tables, func(t rolloutTableSize) bool { return t.rankBytes() != nil }) {
		return ""
	}
	listed, unlisted, unlistedUnsized := presentation.ListTableSizes(tables, rolloutTableSize.rankBytes)

	var b strings.Builder
	b.WriteString("📊 Table sizes:\n")
	for _, t := range listed {
		fmt.Fprintf(&b, "  • %s: %s\n", t.name, presentation.FormatMemberTableSize(noun, t.perMember, plainName))
	}
	if unlisted > 0 {
		fmt.Fprintf(&b, "  %s%s%s\n", ANSIDim, presentation.UnlistedTables(unlisted, unlistedUnsized), ANSIReset)
	}
	b.WriteString("\n")
	return b.String()
}

// plainName renders a member name as written: a terminal has no code span.
func plainName(name string) string { return name }

// rolloutTableSizes groups every member's sizes into one entry per table, in
// the order the tables first appear in rollout order. A table is named with
// its namespace only when the sizes span more than one namespace, so a shared
// table name stays unambiguous.
func rolloutTableSizes(sizes []*apitypes.PlanMemberTableSizeResponse) []rolloutTableSize {
	namespaces := make(map[string]struct{})
	for _, s := range sizes {
		if s != nil {
			namespaces[s.Namespace] = struct{}{}
		}
	}
	qualify := len(namespaces) > 1
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
			name := s.Table
			if qualify {
				name = s.Namespace + "." + s.Table
			}
			i = len(tables)
			at[key] = i
			tables = append(tables, rolloutTableSize{name: name})
		}
		tables[i].perMember = append(tables[i].perMember, presentation.MemberTableSize{Member: s.Member, EstimatedBytes: s.EstimatedBytes})
	}
	return tables
}
