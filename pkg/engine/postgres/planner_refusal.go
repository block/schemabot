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

// dependentRefusalReasons are the planner reasons whose refusal names the
// objects in the way, which dependentObjectCauses renders.
var dependentRefusalReasons = []planner.Reason{
	planner.ReasonDependentView,
	planner.ReasonDependentPolicy,
	planner.ReasonDependentTrigger,
	planner.ReasonDependentGeneratedColumn,
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
// moved. It reports false when no operation names a cause this function
// renders.
//
// Dependent object names come from the catalog, so the composed reason is
// collapsed to one line and its blocked-cause separator neutralized, as for
// every other reason built from a database identifier.
func operationRefusalReason(decisions []planner.Decision, table string) (reason string, unknown planner.Reason, ok bool) {
	clauses, unknown := plannerRefusalClauseList(decisions)
	if unknown != "" {
		return "", unknown, false
	}
	causes := append(dependentObjectCauses(decisions), clauses...)
	if len(causes) == 0 {
		return "", "", false
	}
	reason = fmt.Sprintf("statement for table %q is refused: %s, then plan again", table, strings.Join(causes, "; and "))
	return engine.SanitizeBlockedCause(singleLine(reason)), "", true
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
		if !known {
			return nil, d.Reason
		}
		if !slices.Contains(clauses, clause) {
			clauses = append(clauses, clause)
		}
	}
	return clauses, ""
}
