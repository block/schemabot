package engine

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/sentinel"

	"github.com/block/schemabot/pkg/schema"
)

// DroppedTables matches a target's live tables against the repository's
// drop_tables config, which names tables no schema file declares that the
// repository wants dropped. It is the only way to drop a table an engine
// leaves out of every plan by its name, such as an online schema change's
// leftover "_" table, so it is deliberately narrow: an entry names exactly one
// table, matched exactly and case-sensitively, and a family of tables is
// listed one by one, so the config diff shows every table a plan will drop.
// The one comparison that ignores case is the refusal of the cutover
// sentinel, which errs toward refusing.
//
// The zero value names nothing.
type DroppedTables struct {
	entries map[string]bool
	// order is every distinct entry in the order the config lists them.
	order []string
}

// NewDroppedTables indexes the config's drop_tables entries. An entry that is
// blank, padded with whitespace, a path, or a pattern is an error naming it:
// a malformed entry must fail the plan rather than drop a table nobody named.
// So is an entry naming the cutover sentinel, since dropping it runs a
// cutover nobody asked for.
func NewDroppedTables(entries []string) (DroppedTables, error) {
	if len(entries) == 0 {
		return DroppedTables{}, nil
	}
	indexed := make(map[string]bool, len(entries))
	var order []string
	for _, name := range entries {
		if strings.TrimSpace(name) == "" {
			return DroppedTables{}, fmt.Errorf("drop_tables entries must not be blank")
		}
		if name != strings.TrimSpace(name) {
			return DroppedTables{}, fmt.Errorf("drop_tables entry %s must not have leading or trailing whitespace", schema.QuoteIgnoreTablesEntry(name))
		}
		if strings.Contains(name, "/") {
			return DroppedTables{}, fmt.Errorf("drop_tables entry %s must be an exact table name: drop_tables takes no patterns or paths, so list each table to drop", schema.QuoteIgnoreTablesEntry(name))
		}
		if namesCutoverSentinel(name) {
			return DroppedTables{}, fmt.Errorf("drop_tables entry %s is the table that holds a deferred cutover until someone runs it, so dropping it would run that cutover. Remove the entry, and run the cutover with `schemabot cutover <apply-id> -e <env>` when you are ready", schema.QuoteIgnoreTablesEntry(name))
		}
		if indexed[name] {
			continue
		}
		indexed[name] = true
		order = append(order, name)
	}
	return DroppedTables{entries: indexed, order: order}, nil
}

// namesCutoverSentinel reports whether an entry names Spirit's sentinel table,
// which holds an apply started with a deferred cutover until the table is
// dropped. The comparison ignores case, because whether two spellings are one
// table is the target's answer. Every path that accepts drop_tables indexes it
// here, so the refusal holds for the config, the API and the engine alike.
func namesCutoverSentinel(entry string) bool {
	return strings.EqualFold(entry, sentinel.TableName)
}

// Empty reports whether the config names no table to drop.
func (d DroppedTables) Empty() bool { return len(d.order) == 0 }

// Names reports whether an entry names this live table exactly.
func (d DroppedTables) Names(table string) bool { return d.entries[table] }

// Entries returns the distinct entries in the order the config lists them.
func (d DroppedTables) Entries() []string { return slices.Clone(d.order) }

// Unmatched returns the entries that name no live table, in the order the
// config lists them: a typo, a case mismatch, or a table already dropped. Such
// an entry drops nothing, so callers report it rather than letting the config
// imply a drop that is not happening.
//
// live is every table on the target, including the ones an engine leaves out
// of plans by their names.
func (d DroppedTables) Unmatched(live []string) []string {
	var unmatched []string
	for _, entry := range d.order {
		if !slices.Contains(live, entry) {
			unmatched = append(unmatched, entry)
		}
	}
	return unmatched
}

// RefuseDropTables refuses a plan that carries drop_tables on an engine that
// does not honor the list. Planning as if the list were empty would leave the
// tables the repository asked to drop in place while the plan looked clean.
func RefuseDropTables(engineName string, entries []string) error {
	if len(entries) == 0 {
		return nil
	}
	return fmt.Errorf("drop_tables is supported only on MySQL, and this is a %s target. Remove drop_tables from schemabot.yaml", engineName)
}

// RefuseIgnored refuses an entry ignore_tables also withholds: the repository
// would be both hiding the table from the planner and asking the planner to
// drop it, and neither reading of that is safe to guess.
func (d DroppedTables) RefuseIgnored(ignored IgnoredTables) error {
	for _, entry := range d.order {
		if ignored.Withholds(entry) {
			return fmt.Errorf("drop_tables entry %s is also withheld by ignore_tables. Remove it from one of the two lists",
				schema.QuoteIgnoreTablesEntry(entry))
		}
	}
	return nil
}

// RefuseDeclared refuses an entry a schema file declares: the repository would
// be both managing the table and asking for it to be dropped. Declarations are
// compared ignoring case, the same way IgnoredTables.RefuseDeclared compares
// them, because whether two spellings are one table is the target's answer.
//
// declared is the tables the namespace's schema files declare.
func (d DroppedTables) RefuseDeclared(namespace string, declared []string) error {
	if d.Empty() {
		return nil
	}
	declaredByKey := make(map[string]string, len(declared))
	for _, table := range declared {
		declaredByKey[schema.CaseFoldKey(table)] = table
	}
	for _, entry := range d.order {
		table, ok := declaredByKey[schema.CaseFoldKey(entry)]
		if !ok {
			continue
		}
		spelling := ""
		if table != entry {
			spelling = fmt.Sprintf(" (the file spells it %q)", table)
		}
		return fmt.Errorf("drop_tables entry %s%s is also declared by a schema file in namespace %q. Remove the entry or the schema file",
			schema.QuoteIgnoreTablesEntry(entry), spelling, namespace)
	}
	return nil
}
