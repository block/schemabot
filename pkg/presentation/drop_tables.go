package presentation

import "fmt"

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
