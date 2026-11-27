package postgres

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/pg-sprite/pkg/planner"
)

// plannerRefusalClauses holds, for each planner reason that refuses an
// operation outright, the clause that says why in SchemaBot's words and what
// the operator does instead. The reasons that name the objects in the way —
// the dependent views, policies, triggers, and generated columns, and the
// partition key — render through dependentObjectRefusalReason, which lists
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
// objects in the way, which dependentObjectRefusalReason renders.
var dependentRefusalReasons = []planner.Reason{
	planner.ReasonDependentView,
	planner.ReasonDependentPolicy,
	planner.ReasonDependentTrigger,
	planner.ReasonDependentGeneratedColumn,
	planner.ReasonPartitionKey,
}

// plannerRefusalReason renders the planner's refusal of a statement's
// operations: each refused operation's reason as a clause, joined into one
// sentence, so the operator reads both why the change cannot run and what to
// do instead. A refused operation whose reason has no clause here is
// returned as unknown, so the caller renders the generic refusal and reports
// the reason as planner vocabulary that moved. It reports false when no
// operation was refused for a reason this function renders.
func plannerRefusalReason(decisions []planner.Decision, table string) (reason string, unknown planner.Reason, ok bool) {
	var clauses []string
	for _, d := range decisions {
		if d.Route != planner.RouteRefuse || slices.Contains(dependentRefusalReasons, d.Reason) {
			continue
		}
		clause, known := plannerRefusalClauses[d.Reason]
		if !known {
			return "", d.Reason, false
		}
		if !slices.Contains(clauses, clause) {
			clauses = append(clauses, clause)
		}
	}
	if len(clauses) == 0 {
		return "", "", false
	}
	return fmt.Sprintf("statement for table %q is refused: %s, then plan again", table, strings.Join(clauses, "; and ")), "", true
}
