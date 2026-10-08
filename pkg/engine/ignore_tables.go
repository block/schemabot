package engine

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/schema"
)

// ExemptReasonIgnoreTables is the exemption reason a plan carries for a live
// table the repository's ignore_tables config withholds from the planner. It
// names the config key so a reviewer who reads the disclosure knows where the
// decision is recorded and which diff to review it in.
const ExemptReasonIgnoreTables = "ignore_tables"

// IgnoredTables matches a target's live tables against the repository's
// ignore_tables config, which withholds tables from the planner's view so a
// live table no schema file declares is not proposed for DROP TABLE.
//
// A plain entry is a table name as the target spells it: matching is exact and
// case-sensitive. An entry wrapped in slashes is a regular expression matched
// against the whole name, also case-sensitively unless the expression itself
// says otherwise, for a family of tables an application creates at runtime and
// that no list of names can cover ahead of time. An entry that matches nothing
// withholds nothing, which is why a plan reports what it actually withheld
// rather than what it was asked to withhold.
type IgnoredTables struct {
	entries map[string]bool

	// folded maps each entry's case-folded form to every entry the config
	// spells that way, for RefuseDeclared. Only the contradiction check
	// consults it: see RefuseDeclared for why that check is the one that
	// ignores case. A config can spell one folded name several ways, and
	// resolving the contradiction then means removing all of them, so the
	// error has to name all of them.
	folded map[string][]string

	// patterns holds the config's pattern entries, in the order the config
	// lists them.
	patterns []ignoredTablePattern

	// order is every distinct entry, plain and pattern, in the order the
	// config lists them, so Unmatched reports entries the way an operator
	// wrote them down.
	order []string
}

// ignoredTablePattern is one pattern entry, compiled twice: once as written,
// which is what withholds, and once ignoring case, which is what RefuseDeclared
// checks declarations against.
type ignoredTablePattern struct {
	entry  string
	match  *regexp.Regexp
	folded *regexp.Regexp
}

// NewIgnoredTables indexes the config's ignore_tables entries for matching.
// The zero value and an empty list both withhold nothing.
//
// A pattern entry that does not compile is an error naming it. It fails the
// plan or apply that asked for it rather than being skipped: an entry dropped
// for being malformed withholds nothing, and the live tables it was written to
// protect would come back as DROP TABLE proposals.
func NewIgnoredTables(entries []string) (IgnoredTables, error) {
	if len(entries) == 0 {
		return IgnoredTables{}, nil
	}
	indexed := make(map[string]bool, len(entries))
	folded := make(map[string][]string, len(entries))
	seenPatterns := make(map[string]bool)
	var patterns []ignoredTablePattern
	var order []string
	for _, name := range entries {
		if schema.IsIgnoreTablePattern(name) {
			if seenPatterns[name] {
				continue
			}
			seenPatterns[name] = true
			match, err := schema.CompileIgnoreTablePattern(name, false)
			if err != nil {
				return IgnoredTables{}, err
			}
			foldedMatch, err := schema.CompileIgnoreTablePattern(name, true)
			if err != nil {
				return IgnoredTables{}, err
			}
			patterns = append(patterns, ignoredTablePattern{entry: name, match: match, folded: foldedMatch})
			order = append(order, name)
			continue
		}
		if indexed[name] {
			continue
		}
		indexed[name] = true
		order = append(order, name)
		key := strings.ToLower(name)
		folded[key] = append(folded[key], name)
	}
	for _, spellings := range folded {
		slices.Sort(spellings)
	}
	return IgnoredTables{entries: indexed, folded: folded, patterns: patterns, order: order}, nil
}

// Empty reports whether the config withholds nothing, so an engine can skip
// the filtering and disclosure work entirely on the ordinary plan.
func (i IgnoredTables) Empty() bool { return len(i.order) == 0 }

// HasPatterns reports whether any entry is a pattern.
func (i IgnoredTables) HasPatterns() bool { return len(i.patterns) > 0 }

// Withholds reports whether the config keeps this live table out of the
// planner's view: an entry names it exactly, or a pattern entry matches the
// whole name.
func (i IgnoredTables) Withholds(table string) bool {
	if i.entries[table] {
		return true
	}
	for _, p := range i.patterns {
		if p.match.MatchString(table) {
			return true
		}
	}
	return false
}

// MayWithhold reports whether any entry could withhold a table that satisfies
// match. An engine whose own exclusions are cheaper to apply before reading a
// table asks this first: the config's entries have to resolve ahead of those
// exclusions, but only a config that reaches the same tables makes that
// ordering observable, and every other plan can leave the engine's exclusion
// where it was.
//
// A plain entry answers by its name. A pattern entry always answers yes: which
// names a pattern reaches is only known once the target's catalog is read, and
// answering no for a pattern that does reach such a table would disclose that
// table under the engine's exclusion instead of the config's.
func (i IgnoredTables) MayWithhold(match func(string) bool) bool {
	if len(i.patterns) > 0 {
		return true
	}
	for name := range i.entries {
		if match(name) {
			return true
		}
	}
	return false
}

// Unmatched returns the entries that withheld no live table, in the order the
// config lists them: a typo, a case mismatch, a pattern that reaches nothing,
// or a stale entry for a table that no longer exists. Such an entry excludes
// nothing, so callers report it rather than letting the config imply an
// exclusion that is not happening.
//
// withheld is the union of the tables the plan actually withheld, across every
// namespace it covered: an entry that matched in one namespace and not another
// still withheld a table and is not reported. A pattern counts as matched when
// it matches any table in that union, including one a plain entry also names.
func (i IgnoredTables) Unmatched(withheld []string) []string {
	withheldSet := make(map[string]bool, len(withheld))
	for _, name := range withheld {
		withheldSet[name] = true
	}
	patternMatched := make(map[string]bool, len(i.patterns))
	for _, p := range i.patterns {
		if slices.ContainsFunc(withheld, p.match.MatchString) {
			patternMatched[p.entry] = true
		}
	}
	var unmatched []string
	for _, entry := range i.order {
		if withheldSet[entry] || patternMatched[entry] {
			continue
		}
		unmatched = append(unmatched, entry)
	}
	return unmatched
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
// A pattern entry is checked the same way: it is refused when, ignoring case,
// it matches a declared table. A pattern is written for a family of tables no
// schema file declares, so one that reaches a declared table is written too
// broadly, and the remedy is to narrow it rather than to drop the family's
// protection along with it.
//
// The error names every entry that folds to the declared name, not just the
// first: a config that spells one table two ways is resolved only by removing
// both, so naming one would send an operator to make a change that leaves the
// plan failing for the same reason. It names them rather than saying how many,
// and its remedy points at the entries it just listed, so an operator reading
// the error alone knows exactly what to delete. A pattern is named with every
// declared table it matches, which is what the operator narrows it to avoid.
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
			collision := schema.QuoteIgnoreTablesEntry(entry)
			if entry != table {
				collision = fmt.Sprintf("%s (the file spells it %q)", schema.QuoteIgnoreTablesEntry(entry), table)
			}
			if seen[collision] {
				continue
			}
			seen[collision] = true
			collisions = append(collisions, collision)
		}
	}
	patternCollisions := i.patternsMatchingDeclared(declared)
	if len(collisions) == 0 && len(patternCollisions) == 1 {
		pc := patternCollisions[0]
		if len(pc.tables) == 1 {
			return fmt.Errorf(
				"ignore_tables entry %s matches %s, which a schema file in namespace %q declares. Narrow the pattern so it no longer matches it, or remove the schema file",
				schema.QuoteIgnoreTablesEntry(pc.entry), quotedNames(pc.tables), namespace)
		}
		return fmt.Errorf(
			"ignore_tables entry %s matches %s, which schema files in namespace %q declare. Narrow the pattern so it no longer matches them, or remove the schema files",
			schema.QuoteIgnoreTablesEntry(pc.entry), quotedNames(pc.tables), namespace)
	}
	for _, pc := range patternCollisions {
		collisions = append(collisions, fmt.Sprintf("%s (matching %s)", schema.QuoteIgnoreTablesEntry(pc.entry), quotedNames(pc.tables)))
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
	remedy := "Remove the entries or the schema files"
	if len(patternCollisions) > 0 {
		remedy = "Remove the entries or the schema files, narrowing a pattern instead where it still has tables to withhold"
	}
	return fmt.Errorf(
		"ignore_tables entries %s are also declared by schema files in namespace %q. %s",
		strings.Join(collisions, ", "), namespace, remedy)
}

// patternCollision is one pattern entry and the declared tables it matches.
type patternCollision struct {
	entry  string
	tables []string
}

// patternsMatchingDeclared returns each pattern entry that matches a declared
// table, ignoring case, with the declared tables it matches, sorted.
func (i IgnoredTables) patternsMatchingDeclared(declared []string) []patternCollision {
	var collisions []patternCollision
	for _, p := range i.patterns {
		var tables []string
		for _, table := range declared {
			if p.folded.MatchString(table) {
				tables = append(tables, table)
			}
		}
		if len(tables) == 0 {
			continue
		}
		slices.Sort(tables)
		collisions = append(collisions, patternCollision{entry: p.entry, tables: slices.Compact(tables)})
	}
	return collisions
}

// quotedNames renders table names for an error message, each quoted so a
// name with a space or a trailing character reads as one name.
func quotedNames(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = fmt.Sprintf("%q", name)
	}
	return strings.Join(quoted, ", ")
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
