package postgres

import (
	"fmt"
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
// change run cannot be added back without a full copy of the table when it
// is stored. When the safe path is one SchemaBot cannot run, the clause
// says so first.
//
// A manual path the planner names that this build has no sentence for is
// returned as unrecognized planner vocabulary, so the caller renders the
// generic refusal and reports the value with the plan's identifiers.
func dependentObjectCauses(decisions []planner.Decision) ([]string, *unrecognizedPlannerVocabulary) {
	var causes []string
	for _, d := range decisions {
		if n := len(d.DependentViews); n > 0 {
			causes = append(causes, fmt.Sprintf("it changes or drops a column that %s %s %s; drop or redefine %s outside SchemaBot",
				plural(n, "view", "views"), strings.Join(d.DependentViews, ", "), plural(n, "reads", "read"), plural(n, "the view", "the views")))
		}
		cause, unknown := recreatableAndGeneratedCause(d)
		if unknown != nil {
			return nil, unknown
		}
		if cause != "" {
			causes = append(causes, cause)
		}
		if len(d.PartitionKeyTables) > 0 {
			causes = append(causes, fmt.Sprintf("it changes or drops a column in the partition key of %s, which PostgreSQL never allows; create a new partitioned table with the new key and move the rows into it",
				strings.Join(d.PartitionKeyTables, ", ")))
		}
	}
	return causes, nil
}

// recreatableAndGeneratedCause gives the one path past the policies,
// triggers, and generated columns that read the column. PostgreSQL refuses
// the change while any of them stands, so a type change they all block gets
// one recipe that moves every one of them out of the way at once, rather
// than a recipe per kind that each fails while the others stand.
func recreatableAndGeneratedCause(d planner.Decision) (string, *unrecognizedPlannerVocabulary) {
	recreatable := len(d.DependentPolicies)+len(d.DependentTriggers) > 0
	generated := len(d.DependentGeneratedColumns) > 0
	switch {
	case recreatable && generated && d.Kind == pgstatement.OpAlterColumnType:
		return retypeBlockedByRecreatableAndGeneratedCause(d)
	case recreatable && generated:
		cause, unknown := recreatableDependentsCause(d)
		if unknown != nil {
			return "", unknown
		}
		return cause + "; and " + generatedColumnsCause(d.Kind, d.DependentGeneratedColumns), nil
	case recreatable:
		return recreatableDependentsCause(d)
	case generated:
		return generatedColumnsCause(d.Kind, d.DependentGeneratedColumns), nil
	}
	return "", nil
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

// recreatableDependentsNoun names the kinds of recreatable dependents that
// read the column, such as "the policy" or "the policies and the trigger",
// for the steps that act on them, where a bare pronoun could be read as
// the column.
func recreatableDependentsNoun(d planner.Decision) string {
	var parts []string
	if k := len(d.DependentPolicies); k > 0 {
		parts = append(parts, "the "+plural(k, "policy", "policies"))
	}
	if k := len(d.DependentTriggers); k > 0 {
		parts = append(parts, "the "+plural(k, "trigger", "triggers"))
	}
	return strings.Join(parts, " and ")
}

// recreatableDependentsWhere names the recreatable dependents with where
// each is changed. SchemaBot manages a policy through the schema file that
// declares its table, which for a policy that reads the column through a
// subquery is another table's file, and never manages a trigger, so a
// trigger is changed outside SchemaBot.
func recreatableDependentsWhere(d planner.Decision) string {
	var parts []string
	if k := len(d.DependentPolicies); k > 0 {
		parts = append(parts, plural(k, "the policy in the schema file that declares it", "the policies in the schema files that declare them"))
	}
	if k := len(d.DependentTriggers); k > 0 {
		parts = append(parts, plural(k, "the trigger outside SchemaBot", "the triggers outside SchemaBot"))
	}
	return strings.Join(parts, " and ")
}

// recreatableDependentsCause gives the path past the policies and triggers
// that read the column, from what the change does with them out of the way.
// Dropping them in one change and recreating them in another would leave a
// window without them, so a dependent is changed first, recreated in the
// same transaction as the change, or moved to a new column that writes
// keep in step with the old one, so the dependents never read a stale
// value.
func recreatableDependentsCause(d planner.Decision) (string, *unrecognizedPlannerVocabulary) {
	named, n := recreatableDependents(d)
	reads, them, noun, where := plural(n, "reads", "read"), plural(n, "it", "them"), recreatableDependentsNoun(d), recreatableDependentsWhere(d)
	switch d.DependentFix {
	case planner.DependentFixRewriteDependents:
		return fmt.Sprintf("it drops a column that %s %s; first change %s so %s no longer %s the column",
			named, reads, where, plural(n, "it", "they"), reads), nil
	case planner.DependentFixRecreateAroundChange:
		return fmt.Sprintf("it changes the type of a column that %s %s, which SchemaBot cannot run yet; the change does not rewrite the table, so outside SchemaBot, through a separately reviewed process, drop %s, change the column, and recreate %s in one transaction, which never leaves the table without %s",
			named, reads, noun, them, them), nil
	case planner.DependentFixExpandContract:
		return fmt.Sprintf("it changes the type of a column that %s %s and rewrites the table, which SchemaBot cannot run yet; add a column of the new type, have the application write to both while you backfill it, change %s to read the new column and move the application's reads to it, and drop the old column once unused, each as its own change",
			named, reads, where), nil
	}
	return "", &unrecognizedPlannerVocabulary{kind: "dependent_fix", value: string(d.DependentFix)}
}

// retypeBlockedByRecreatableAndGeneratedCause gives the path past a type
// change that policies or triggers and generated columns all block. A
// generated column cannot move to a new column the way a policy or trigger
// can, so the one path that keeps every dependent drops them all, changes
// the column, and recreates them in one transaction, which never leaves the
// table without a policy. That transaction holds a lock that blocks reads
// and writes until it commits, through the rewrite when the change rewrites
// the table or a generated column added back is stored.
func retypeBlockedByRecreatableAndGeneratedCause(d planner.Decision) (string, *unrecognizedPlannerVocabulary) {
	recreatable, _ := recreatableDependents(d)
	g := len(d.DependentGeneratedColumns)
	named := recreatable + " and " + plural(g, "generated column ", "generated columns ") + strings.Join(d.DependentGeneratedColumns, ", ")
	noun := recreatableDependentsNoun(d) + " and " + plural(g, "the generated column", "the generated columns")
	var lock string
	switch d.DependentFix {
	case planner.DependentFixExpandContract:
		lock = "which blocks reads and writes while the table is rewritten"
	case planner.DependentFixRecreateAroundChange:
		lock = "which blocks reads and writes until it commits, through a rewrite of the table when a generated column added back is stored"
	default:
		return "", &unrecognizedPlannerVocabulary{kind: "dependent_fix", value: string(d.DependentFix)}
	}
	return fmt.Sprintf("it changes the type of a column that %s read, which SchemaBot cannot run yet: PostgreSQL refuses the change while any of them stands; to keep them, outside SchemaBot, through a separately reviewed process, drop %s, change the column, and recreate them in one transaction, %s",
		named, noun, lock), nil
}

// generatedColumnsCause gives the path past the generated columns that read
// the column. PostgreSQL refuses the change while they stand. Adding a
// stored generated column to an existing table rewrites it, which SchemaBot
// does not run, while adding a virtual one changes only the catalog, so the
// recipe keeps them in one transaction and names the rewrite only for a
// stored one. A dropped column takes the generated columns that read it
// only when the file drops them too, which is the operator's call.
func generatedColumnsCause(kind pgstatement.OpKind, columns []string) string {
	n := len(columns)
	named := plural(n, "generated column ", "generated columns ") + strings.Join(columns, ", ")
	reads, it, they := plural(n, "reads", "read"), plural(n, "it", "them"), plural(n, "it", "they")
	switch kind {
	case pgstatement.OpDropColumn:
		return fmt.Sprintf("it drops a column that %s %s; if %s should go too, remove %s from the file and apply that first, or to keep %s, make the change outside SchemaBot through a separately reviewed process",
			named, reads, they, it, it)
	case pgstatement.OpAlterColumnType:
		return fmt.Sprintf("it changes the type of a column that %s %s, which SchemaBot cannot run yet: PostgreSQL refuses the change while %s %s; to keep %s, outside SchemaBot, through a separately reviewed process, drop %s, change the column, and add %s back in one transaction, which blocks reads and writes until it commits, through a rewrite of the table when a generated column added back is stored",
			named, reads, they, plural(n, "stands", "stand"), it, it, it)
	}
	return fmt.Sprintf("it changes or drops a column that %s %s, which SchemaBot cannot run yet; make the change outside SchemaBot through a separately reviewed process",
		named, reads)
}

// plural picks the singular or plural form for n.
func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}
