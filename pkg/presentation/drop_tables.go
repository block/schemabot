package presentation

import (
	"fmt"
	"slices"
)

// UnmatchedDropTablesListLimit is how many unmatched drop_tables entries a
// plan names one line each. The list comes from the repository's config,
// which sets no limit on its length, and a line per entry past this would push
// the plan comment towards GitHub's size limit, where the whole plan is
// replaced with a notice. The entries past the limit are counted in one line.
const UnmatchedDropTablesListLimit = 20

// UnmatchedDropTablesEntry is the plan's line for a drop_tables entry that
// names no live table on the target: a typo, a name wrapped in quotes, the
// wrong case, or a table already dropped. Such an entry plans no drop, and
// without the line a plan that still lists the table under a naming
// convention reads as if SchemaBot saw it and chose to leave it.
//
// key and entry arrive already marked up for the surface, so the PR comment
// and the CLI say the same sentence and differ only in how they quote.
func UnmatchedDropTablesEntry(key, entry string) string {
	return fmt.Sprintf("%s entry %s names no table on the target, so this plan drops nothing for it. Fix the name, or remove the entry if the table is already gone.", key, entry)
}

// UnmatchedDropTablesMore is the line that counts the unmatched entries past
// UnmatchedDropTablesListLimit. key arrives marked up for the surface.
func UnmatchedDropTablesMore(key string, more int) string {
	if more == 1 {
		return fmt.Sprintf("1 more %s entry names no table on the target, so this plan drops nothing for it. Fix the name, or remove the entry if the table is already gone.", key)
	}
	return fmt.Sprintf("%d more %s entries name no table on the target, so this plan drops nothing for them. Fix the names, or remove the entries whose tables are already gone.", more, key)
}

// UnmatchedDropTableEntries returns the unmatched entries a plan names one
// line each and how many it counts instead, so the PR comment and the CLI
// list the same entries.
func UnmatchedDropTableEntries(entries []string) (shown []string, more int) {
	if len(entries) <= UnmatchedDropTablesListLimit {
		return entries, 0
	}
	return entries[:UnmatchedDropTablesListLimit], len(entries) - UnmatchedDropTablesListLimit
}

// UnmatchedDropTablesInEveryPlan returns the entries every plan reports as
// naming no table, in the order the first plan lists them. Every environment
// is planned with the same drop_tables list, so an entry one environment's
// plan matched names a table that environment still has and plans a drop for.
// Reported for another environment, the line would tell the reader to remove
// the entry that drop depends on, as it does once a drop has landed in staging
// and not yet in production. A plan that reports no entries, such as one that
// failed, leaves nothing to report: what its target holds is unknown.
func UnmatchedDropTablesInEveryPlan(plans [][]string) []string {
	if len(plans) == 0 {
		return nil
	}
	var common []string
	for _, entry := range plans[0] {
		inEvery := !slices.Contains(common, entry)
		for _, other := range plans[1:] {
			inEvery = inEvery && slices.Contains(other, entry)
		}
		if inEvery {
			common = append(common, entry)
		}
	}
	return common
}
