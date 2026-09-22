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

	// folded maps each entry's case-folded form to every entry the config
	// spells that way, for RefuseDeclared. Only the contradiction check
	// consults it: see RefuseDeclared for why that check is the one that
	// ignores case. A config can spell one folded name several ways, and
	// resolving the contradiction then means removing all of them, so the
	// error has to name all of them.
	folded map[string][]string
}

// NewIgnoredTables indexes the config's ignore_tables entries for matching.
// The zero value and an empty list both withhold nothing.
func NewIgnoredTables(entries []string) IgnoredTables {
	if len(entries) == 0 {
		return IgnoredTables{}
	}
	indexed := make(map[string]bool, len(entries))
	folded := make(map[string][]string, len(entries))
	for _, name := range entries {
		if indexed[name] {
			continue
		}
		indexed[name] = true
		key := strings.ToLower(name)
		folded[key] = append(folded[key], name)
	}
	for _, spellings := range folded {
		slices.Sort(spellings)
	}
	return IgnoredTables{entries: indexed, folded: folded}
}

// Empty reports whether the config withholds nothing, so an engine can skip
// the filtering and disclosure work entirely on the ordinary plan.
func (i IgnoredTables) Empty() bool { return len(i.entries) == 0 }

// Withholds reports whether the config keeps this live table out of the
// planner's view.
func (i IgnoredTables) Withholds(table string) bool { return i.entries[table] }

// NamesAny reports whether any entry satisfies match. An engine whose own
// exclusions are cheaper to apply before reading a table asks this first: the
// config's entries have to resolve ahead of those exclusions, but only a
// config that reaches the same tables makes that ordering observable, and
// every other plan can leave the engine's exclusion where it was.
func (i IgnoredTables) NamesAny(match func(string) bool) bool {
	for name := range i.entries {
		if match(name) {
			return true
		}
	}
	return false
}

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
// The error names every entry that folds to the declared name, not just the
// first: a config that spells one table two ways is resolved only by removing
// both, so naming one would send an operator to make a change that leaves the
// plan failing for the same reason. It names them rather than saying how many,
// and its remedy points at the entries it just listed, so an operator reading
// the error alone knows exactly what to delete.
//
// declared is the tables the namespace's schema files declare.
func (i IgnoredTables) RefuseDeclared(namespace string, declared []string) error {
	if i.Empty() {
		return nil
	}
	var collisions []string
	seen := make(map[string]bool)
	for _, table := range declared {
		spellings, withheld := i.folded[strings.ToLower(table)]
		if !withheld {
			continue
		}
		for _, entry := range spellings {
			collision := fmt.Sprintf("%q", entry)
			if entry != table {
				collision = fmt.Sprintf("%q (the file spells it %q)", entry, table)
			}
			if seen[collision] {
				continue
			}
			seen[collision] = true
			collisions = append(collisions, collision)
		}
	}
	if len(collisions) == 0 {
		return nil
	}
	slices.Sort(collisions)
	if len(collisions) == 1 {
		return fmt.Errorf(
			"ignore_tables entry %s is also declared by a schema file in namespace %q. Remove the entry or the schema file",
			collisions[0], namespace)
	}
	return fmt.Errorf(
		"ignore_tables entries %s are also declared by schema files in namespace %q. Remove the entries or the schema files",
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
