package postgres

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/pg-sprite/pkg/planner"

	"github.com/block/schemabot/pkg/engine"
)

// plannerRefusalClauses holds, for each planner reason that refuses an
// operation outright, the clause that says why in SchemaBot's words and what
// the operator does instead. The reasons that name the objects in the way —
// the dependent views, policies, triggers, and generated columns, and the
// partition key — render through dependentObjectCauses, which lists
// those objects, so they are not here.
var plannerRefusalClauses = map[planner.Reason]string{
	planner.ReasonNotNullWithoutDefault: "it adds a NOT NULL column with no default to a table that holds rows, which PostgreSQL refuses because every existing row would hold NULL; " +
		"give the column a default, or add it without NOT NULL, fill every row, then declare NOT NULL in a later change, which SchemaBot runs online",
	planner.ReasonReplicaIdentity: "it drops the index, key, or column the table's replica identity uses, which makes every UPDATE and DELETE on the table fail while a publication publishes them; " +
		"point the table's replica identity at another index outside SchemaBot with ALTER TABLE ... REPLICA IDENTITY",
	planner.ReasonCascadingDrop: "it drops with CASCADE, which also drops the views, policies, and foreign keys that depend on what it drops, none of which the plan shows; " +
		"drop each dependent object as its own change, and remove CASCADE",
	planner.ReasonUnsupportedOperation: "it uses an operation SchemaBot's PostgreSQL support has no safe way to run; " +
		"make the change outside SchemaBot through a separately reviewed process",
}

// plannerRefusalClauseFuncs holds the clause for each refusing planner
// reason whose decision carries what the clause names. A column-limit
// decision names the tables at the limit, the altered table or its
// partitions and inheritance children, and how many column numbers are left,
// so the clause points the operator at the tables to rebuild.
var plannerRefusalClauseFuncs = map[planner.Reason]func(planner.Decision) string{
	planner.ReasonColumnLimit: func(d planner.Decision) string {
		return "it adds a column to " + planner.ColumnLimitSubject(d.ColumnLimit) +
			", and a dropped column keeps its number until its table is rebuilt; " + planner.ColumnLimitHelp(d.ColumnLimit)
	},
}

// dependentRefusalReasons are the planner reasons whose refusal names the
// objects in the way, which dependentObjectCauses renders.
var dependentRefusalReasons = []planner.Reason{
	planner.ReasonDependentView,
	planner.ReasonDependentPolicy,
	planner.ReasonDependentTrigger,
	planner.ReasonDependentGeneratedColumn,
	planner.ReasonDependentPublication,
	planner.ReasonPartitionKey,
}

// operationRefusalReason renders the refusal of a statement's operations as
// one sentence: first the objects in the way of a column change, then each
// other refused operation's reason as a clause, so the operator reads every
// fix the statement needs at once rather than one per plan. A refused
// operation whose reason has no clause here is returned as unknown, even
// beside reasons that render, since a sentence naming only the known fixes
// would tell the operator that following them is enough; the caller renders
// the generic refusal and reports the reason as planner vocabulary that
// moved. A manual path for the objects in the way that this build has no
// sentence for is returned as unknown the same way. It reports false when
// no operation names a cause this function renders.
//
// Dependent object names come from the catalog, so the composed reason is
// collapsed to one line and its blocked-cause separator neutralized, as for
// every other reason built from a database identifier.
func operationRefusalReason(decisions []planner.Decision, table string) (reason string, unknown *unrecognizedPlannerVocabulary, ok bool) {
	clauses, unknownReason := plannerRefusalClauseList(decisions)
	if unknownReason != "" {
		return "", &unrecognizedPlannerVocabulary{kind: "refusal_reason", value: string(unknownReason)}, false
	}
	causes, unknown := dependentObjectCauses(decisions)
	if unknown != nil {
		return "", unknown, false
	}
	causes = append(causes, clauses...)
	if len(causes) == 0 {
		return "", nil, false
	}
	reason = fmt.Sprintf("statement for table %q is refused: %s, then plan again", table, strings.Join(causes, "; and "))
	return engine.SanitizeBlockedCause(singleLine(reason)), nil, true
}

// plannerRefusalClauseList returns the clause for each distinct reason the
// planner refused an operation for, leaving out the reasons that name the
// objects in the way, or the first refused reason that has no clause.
func plannerRefusalClauseList(decisions []planner.Decision) (clauses []string, unknown planner.Reason) {
	for _, d := range decisions {
		if d.Route != planner.RouteRefuse || slices.Contains(dependentRefusalReasons, d.Reason) {
			continue
		}
		clause, known := plannerRefusalClauses[d.Reason]
		if render, detailed := plannerRefusalClauseFuncs[d.Reason]; detailed {
			clause, known = render(d), true
		}
		if !known {
			return nil, d.Reason
		}
		if !slices.Contains(clauses, clause) {
			clauses = append(clauses, clause)
		}
	}
	return clauses, ""
}
