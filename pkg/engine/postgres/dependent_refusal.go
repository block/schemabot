package postgres

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/pg-sprite/pkg/planner"
	pgstatement "github.com/block/pg-sprite/pkg/statement"
)

// dependentObjectCauses names the objects that keep PostgreSQL from
// running a refused column change: the views, row security policies,
// triggers, and generated columns that read a column the statement retypes
// or drops, and the partitioned tables whose key includes it. PostgreSQL
// refuses the change while any of them stands, even a type change that
// rewrites nothing, so the operator's next step is to change those objects,
// and naming them saves a search of the catalog. Each cause is a clause of
// the sentence operationRefusalReason composes.
//
// The way forward never passes through a state the application cannot
// work in. A policy dropped in one change and recreated in a later one
// leaves a table with row security on and no policy between the two, so it
// reads no rows and refuses inserts; a generated column dropped to let the
// change run cannot be added back without a full copy of the table. When
// the safe path is one SchemaBot cannot run, the clause says so first.
func dependentObjectCauses(decisions []planner.Decision) []string {
	var causes []string
	for _, d := range decisions {
		if n := len(d.DependentViews); n > 0 {
			causes = append(causes, fmt.Sprintf("it changes or drops a column that %s %s %s; drop or redefine %s outside SchemaBot",
				plural(n, "view", "views"), strings.Join(d.DependentViews, ", "), plural(n, "reads", "read"), plural(n, "the view", "the views")))
		}
		if named, n := recreatableDependents(d); n > 0 {
			causes = append(causes, recreatableDependentsCause(d, named, n))
		}
		if len(d.DependentGeneratedColumns) > 0 {
			causes = append(causes, generatedColumnsCause(d.Kind, d.DependentGeneratedColumns))
		}
		if len(d.PartitionKeyTables) > 0 {
			causes = append(causes, fmt.Sprintf("it changes or drops a column in the partition key of %s, which PostgreSQL never allows; create a new partitioned table with the new key and move the rows into it",
				strings.Join(d.PartitionKeyTables, ", ")))
		}
	}
	return causes
}

// recreatableDependents names the row security policies and triggers that
// read the column, the dependents that can be dropped and recreated around
// the change and that the planner gives one manual path for, and counts
// them.
func recreatableDependents(d planner.Decision) (named string, n int) {
	var parts []string
	if k := len(d.DependentPolicies); k > 0 {
		parts = append(parts, plural(k, "row security policy ", "row security policies ")+strings.Join(d.DependentPolicies, ", "))
	}
	if k := len(d.DependentTriggers); k > 0 {
		parts = append(parts, plural(k, "trigger ", "triggers ")+strings.Join(d.DependentTriggers, ", "))
	}
	return strings.Join(parts, " and "), len(d.DependentPolicies) + len(d.DependentTriggers)
}

// recreatableDependentsCause gives the path past the policies and triggers
// that read the column, from what the change does with them out of the way.
// Dropping them in one change and recreating them in another would leave a
// window without them, so a dependent is changed first, recreated in the
// same transaction as the change, or moved to a new column that writes
// keep in step with the old one, so the dependents never read a stale
// value. SchemaBot
// manages a table's policies through its schema file but never its
// triggers, so a trigger is changed outside SchemaBot.
func recreatableDependentsCause(d planner.Decision, named string, n int) string {
	reads, them := plural(n, "reads", "read"), plural(n, "it", "them")
	switch d.DependentFix {
	case planner.DependentFixRewriteDependents:
		change := fmt.Sprintf("change %s so %s no longer %s the column", them, plural(n, "it", "they"), reads)
		switch {
		case len(d.DependentTriggers) == 0:
			change += " and apply that first"
		case len(d.DependentPolicies) == 0:
			change = "outside SchemaBot, " + change + " first"
		default:
			change += " first, the policies in the schema file and the triggers outside SchemaBot"
		}
		return fmt.Sprintf("it drops a column that %s %s; %s", named, reads, change)
	case planner.DependentFixRecreateAroundChange:
		return fmt.Sprintf("it changes the type of a column that %s %s, which SchemaBot cannot run yet; the change does not rewrite the table, so outside SchemaBot, through a separately reviewed process, drop %s, change the column, and recreate %s in one transaction, which never leaves the table without %s",
			named, reads, them, them, them)
	case planner.DependentFixExpandContract:
		where := ""
		switch {
		case len(d.DependentTriggers) == 0:
		case len(d.DependentPolicies) == 0:
			where = " outside SchemaBot"
		default:
			where = " (the policies in the schema file, the triggers outside SchemaBot)"
		}
		return fmt.Sprintf("it changes the type of a column that %s %s, and the change rewrites the table, which SchemaBot cannot run yet; add a column of the new type instead, have the application write to both and backfill it, point %s at the new column%s and switch reads to it, and last drop the old column once nothing uses it, each as its own change",
			named, reads, them, where)
	}
	slog.Warn("planner names a dependent fix with no sentence; rendering the generic one", "dependent_fix", string(d.DependentFix))
	return fmt.Sprintf("it changes or drops a column that %s %s, which SchemaBot cannot run yet; make the change outside SchemaBot through a separately reviewed process",
		named, reads)
}

// generatedColumnsCause gives the path past the generated columns that read
// the column. PostgreSQL refuses the change while they stand, and adding a
// stored generated column to an existing table rewrites it, which SchemaBot
// does not run, so dropping one to let the change through would lose it. A
// dropped column takes the generated columns that read it only when the
// file drops them too, which is the operator's call.
func generatedColumnsCause(kind pgstatement.OpKind, columns []string) string {
	n := len(columns)
	named := plural(n, "generated column ", "generated columns ") + strings.Join(columns, ", ")
	reads, it, they := plural(n, "reads", "read"), plural(n, "it", "them"), plural(n, "it", "they")
	if kind == pgstatement.OpDropColumn {
		return fmt.Sprintf("it drops a column that %s %s; if %s should go too, remove %s from the file and apply that first, or to keep %s, make the change outside SchemaBot through a separately reviewed process",
			named, reads, they, it, it)
	}
	return fmt.Sprintf("it changes the type of a column that %s %s, which SchemaBot cannot run yet: PostgreSQL refuses the change while %s %s, and adding a stored generated column back rewrites the table; to keep %s, outside SchemaBot, through a separately reviewed process, drop %s, change the column, and add %s back in one transaction, which blocks reads and writes while the table is rewritten",
		named, reads, they, plural(n, "stands", "stand"), it, it, it)
}

// plural picks the singular or plural form for n.
func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}
