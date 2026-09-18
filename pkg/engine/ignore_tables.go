package engine

import (
	"fmt"
	"slices"
	"strings"
)

// ExemptReasonIgnoreTables is the exemption reason a plan carries for a live
// table the repository's ignore_tables config withholds from the planner. It
// names the config key so a reviewer who reads the disclosure knows where the
// decision is recorded and which diff to review it in.
const ExemptReasonIgnoreTables = "ignore_tables"

// IgnoredTables matches a target's live tables against the repository's
// ignore_tables config, which withholds named tables from the planner's view
// so a live table no schema file declares is not proposed for DROP TABLE.
//
// Entries are table names as the target spells them: matching is exact and
// case-sensitive. An entry that matches nothing withholds nothing, which is
// why a plan reports what it actually withheld rather than what it was asked
// to withhold.
type IgnoredTables struct {
	entries map[string]bool

	// folded maps each entry's case-folded form to the entry as the config
	// spells it, for RefuseDeclared. Only the contradiction check consults it:
	// see RefuseDeclared for why that check is the one that ignores case.
	folded map[string]string
}

// NewIgnoredTables indexes the config's ignore_tables entries for matching.
// The zero value and an empty list both withhold nothing.
func NewIgnoredTables(entries []string) IgnoredTables {
	if len(entries) == 0 {
		return IgnoredTables{}
	}
	indexed := make(map[string]bool, len(entries))
	folded := make(map[string]string, len(entries))
	for _, name := range entries {
		indexed[name] = true
		if _, ok := folded[strings.ToLower(name)]; !ok {
			folded[strings.ToLower(name)] = name
		}
	}
	return IgnoredTables{entries: indexed, folded: folded}
}

// Empty reports whether the config withholds nothing, so an engine can skip
// the filtering and disclosure work entirely on the ordinary plan.
func (i IgnoredTables) Empty() bool { return len(i.entries) == 0 }

// Withholds reports whether the config keeps this live table out of the
// planner's view.
func (i IgnoredTables) Withholds(table string) bool { return i.entries[table] }

// RefuseDeclared refuses the one shape in which ignoring a table inverts into
// changing it: a table the config withholds from the planner that a schema
// file also declares to it. The repository is then saying both "manage this
// table" and "do not look at it", and the engines resolve that contradiction
// differently — where the whole schema is diffed as one unit the withheld live
// table has no counterpart to match the declaration against, so the diff
// proposes CREATE TABLE for a table that already exists, and where each
// declaration is diffed on its own the declaring file keeps managing the table
// and the ignore does nothing at all. Neither is "ignore", so the
// contradiction fails the plan on every engine instead.
//
// The comparison ignores case, which is the one place this type does. Whether
// a name and its differently-cased spelling are the same table is the target's
// answer, not the config's: identifiers fold to lower case wherever a database
// is configured to store them that way, so a file declaring Orders and an
// entry naming orders are a contradiction there and two distinct tables
// elsewhere. The two directions fail differently, so they are not treated the
// same. Withholding stays exact, because an entry must never withhold a table
// it does not name, and an entry that matched nothing is reported. Refusing
// ignores case, because refusing a repository that meant two tables costs a
// plan and an error naming both spellings, while letting a real contradiction
// through costs an apply that fails part way with the target already holding
// a table the plan believed it was creating.
//
// declared is the tables the namespace's schema files declare.
func (i IgnoredTables) RefuseDeclared(namespace string, declared []string) error {
	if i.Empty() {
		return nil
	}
	var collisions []string
	seen := make(map[string]bool)
	for _, table := range declared {
		entry, withheld := i.folded[strings.ToLower(table)]
		if !withheld {
			continue
		}
		collision := fmt.Sprintf("%q", entry)
		if entry != table {
			collision = fmt.Sprintf("%q (declared as %q)", entry, table)
		}
		if seen[collision] {
			continue
		}
		seen[collision] = true
		collisions = append(collisions, collision)
	}
	if len(collisions) == 0 {
		return nil
	}
	slices.Sort(collisions)
	return fmt.Errorf(
		"ignore_tables withholds table(s) %s that schema files in namespace %q also declare: a table cannot be both withheld from the planner and declared to it, so the plan would propose creating a table that already exists or would manage a table the config says to leave alone; remove the ignore_tables entry or delete the declaring schema file",
		strings.Join(collisions, ", "), namespace)
}

// Exemption builds the plan's disclosure for one namespace's withheld tables,
// so a reviewer can tell a table the plan withheld from one it found declared.
// Returns nil when the namespace withheld nothing, which is the ordinary case.
func (i IgnoredTables) Exemption(namespace string, withheld []string) *ExemptTables {
	if len(withheld) == 0 {
		return nil
	}
	tables := slices.Clone(withheld)
	slices.Sort(tables)
	return &ExemptTables{
		Namespace: namespace,
		Tables:    tables,
		Reason:    ExemptReasonIgnoreTables,
	}
}
