package postgres

import (
	"slices"
	"testing"

	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/planner"
	"github.com/block/pg-sprite/pkg/router"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// plannerRefusalSentences is the sentence each refusing planner reason
// renders for a statement on "users", written out in full so a wording change
// is a reviewed change.
var plannerRefusalSentences = map[planner.Reason]string{
	planner.ReasonNotNullWithoutDefault: `statement for table "users" is refused: it adds a NOT NULL column with no default to a table that holds rows, which PostgreSQL refuses because every existing row would hold NULL; give the column a default, or add it without NOT NULL, fill every row, then declare NOT NULL in a later change, which SchemaBot runs online, then plan again`,
	planner.ReasonReplicaIdentity:       `statement for table "users" is refused: it drops the index, key, or column the table's replica identity uses, which makes every UPDATE and DELETE on the table fail while a publication publishes them; point the table's replica identity at another index outside SchemaBot with ALTER TABLE ... REPLICA IDENTITY, then plan again`,
	planner.ReasonCascadingDrop:         `statement for table "users" is refused: it drops with CASCADE, which also drops the views, policies, and foreign keys that depend on what it drops, none of which the plan shows; drop each dependent object as its own change, and remove CASCADE, then plan again`,
	planner.ReasonUnsupportedOperation:  `statement for table "users" is refused: it uses an operation SchemaBot's PostgreSQL support has no safe way to run; make the change outside SchemaBot through a separately reviewed process, then plan again`,
}

// plannerNonRefusingReasons are the planner reasons that route an operation
// to a backend rather than refuse it.
var plannerNonRefusingReasons = []planner.Reason{
	planner.ReasonMetadataOnly,
	planner.ReasonOnlineIdiom,
	planner.ReasonFastDefault,
	planner.ReasonBinaryCoercible,
	planner.ReasonSaferIdiom,
	planner.ReasonAppBreakingRename,
	planner.ReasonVolatileDefault,
	planner.ReasonGeneratedStored,
	planner.ReasonTypeRewrite,
	planner.ReasonRelocation,
	planner.ReasonPartitionParentLock,
}

// refusedStatement is a statement the planner refused, with one refused
// operation per reason.
func refusedStatement(reasons ...planner.Reason) pgplan.Statement {
	st := pgplan.Statement{Disposition: router.DispositionRefuse}
	for _, r := range reasons {
		st.Decisions = append(st.Decisions, planner.Decision{Route: planner.RouteRefuse, Reason: r})
	}
	return st
}

// Every reason the planner can give is either one that does not refuse, one
// whose refusal names the objects in the way, or one with a sentence of its
// own that says why the change cannot run and what to do instead. The set
// comes from the planner, so a reason added upstream fails here by name.
func TestPlannerRefusalReasonCoversEveryReason(t *testing.T) {
	for _, reason := range planner.Reasons() {
		t.Run(string(reason), func(t *testing.T) {
			if slices.Contains(plannerNonRefusingReasons, reason) || slices.Contains(dependentRefusalReasons, reason) {
				return
			}
			want, ok := plannerRefusalSentences[reason]
			require.True(t, ok, "planner reason %q has no refusal sentence", reason)
			assert.Equal(t, stepVerdict{mode: engine.ExecutionModeBlocked, reason: want},
				executionVerdict(pgplan.FormatVersion, refusedStatement(reason), "users", nil))
		})
	}
	assert.Len(t, plannerRefusalClauses, len(plannerRefusalSentences), "a clause has no golden sentence")
}

// A statement refused for two reasons names both fixes in one sentence, and
// two operations refused for the same reason name it once.
func TestPlannerRefusalReasonJoinsDistinctReasons(t *testing.T) {
	got := executionVerdict(pgplan.FormatVersion,
		refusedStatement(planner.ReasonNotNullWithoutDefault, planner.ReasonNotNullWithoutDefault, planner.ReasonUnsupportedOperation), "users", nil)
	assert.Equal(t, stepVerdict{
		mode: engine.ExecutionModeBlocked,
		reason: `statement for table "users" is refused: it adds a NOT NULL column with no default to a table that holds rows, which PostgreSQL refuses because every existing row would hold NULL; give the column a default, or add it without NOT NULL, fill every row, then declare NOT NULL in a later change, which SchemaBot runs online; and ` +
			`it uses an operation SchemaBot's PostgreSQL support has no safe way to run; make the change outside SchemaBot through a separately reviewed process, then plan again`,
	}, got)
}

// A refusal reason this build has no sentence for renders the generic
// refusal and reports the reason for the server log, since the planner's
// vocabulary grew; a known reason beside it does not hide that.
func TestPlannerRefusalReasonUnknownReason(t *testing.T) {
	assert.Equal(t, stepVerdict{
		mode:         engine.ExecutionModeBlocked,
		reason:       `statement for table "users" is refused: it cannot be executed safely as written`,
		unrecognized: &unrecognizedPlannerVocabulary{kind: "refusal_reason", value: "future-reason"},
	}, executionVerdict(pgplan.FormatVersion,
		refusedStatement(planner.ReasonNotNullWithoutDefault, planner.Reason("future-reason")), "users", nil))
}

// Operations the planner routed to a backend do not count as refused, so a
// statement refused with none of its operations refused renders the generic
// sentence with nothing to report.
func TestPlannerRefusalReasonSkipsOperationsNotRefused(t *testing.T) {
	st := pgplan.Statement{Disposition: router.DispositionRefuse, Decisions: []planner.Decision{
		{Route: planner.RouteNative, Reason: planner.ReasonMetadataOnly},
	}}
	assert.Equal(t, stepVerdict{
		mode:   engine.ExecutionModeBlocked,
		reason: `statement for table "users" is refused: it cannot be executed safely as written`,
	}, executionVerdict(pgplan.FormatVersion, st, "users", nil))
}

// A statement that retypes a column a view reads and adds a NOT NULL column
// with no default names both fixes in one sentence, the object in the way
// first, so the operator does not fix one, plan again, and only then learn
// of the other.
func TestPlannerRefusalReasonJoinsDependentObjectsAndOtherReasons(t *testing.T) {
	st := refusedStatement(planner.ReasonNotNullWithoutDefault)
	st.Decisions = append(st.Decisions, planner.Decision{
		Route: planner.RouteRefuse, Reason: planner.ReasonDependentView, DependentViews: []string{`"public"."user_emails"`},
	})
	assert.Equal(t, stepVerdict{
		mode: engine.ExecutionModeBlocked,
		reason: `statement for table "users" is refused: it changes or drops a column that view "public"."user_emails" reads; drop or redefine the view outside SchemaBot; and ` +
			`it adds a NOT NULL column with no default to a table that holds rows, which PostgreSQL refuses because every existing row would hold NULL; give the column a default, or add it without NOT NULL, fill every row, then declare NOT NULL in a later change, which SchemaBot runs online, then plan again`,
	}, executionVerdict(pgplan.FormatVersion, st, "users", nil))
}

// A refusal reason this build has no sentence for is reported even beside a
// dependent object, and the generic sentence stands in for both: naming only
// the view would tell the operator that redefining it is enough.
func TestPlannerRefusalReasonUnknownReasonBesideADependentObject(t *testing.T) {
	st := refusedStatement(planner.Reason("future-reason"))
	st.Decisions = append(st.Decisions, planner.Decision{
		Route: planner.RouteRefuse, Reason: planner.ReasonDependentView, DependentViews: []string{`"public"."user_emails"`},
	})
	assert.Equal(t, stepVerdict{
		mode:         engine.ExecutionModeBlocked,
		reason:       `statement for table "users" is refused: it cannot be executed safely as written`,
		unrecognized: &unrecognizedPlannerVocabulary{kind: "refusal_reason", value: "future-reason"},
	}, executionVerdict(pgplan.FormatVersion, st, "users", nil))
}
