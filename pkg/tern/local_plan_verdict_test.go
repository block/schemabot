package tern

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

const localBlockedReason = "requires privileges unavailable to the engine"

func TestBuildDispatchTasksCopiesAsymmetricVerdicts(t *testing.T) {
	plan := &storage.Plan{ID: 7, Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
	scope := dispatchScope{ddlChanges: []storage.TableChange{
		{Namespace: "testdb", Table: "orders", DDL: "ALTER TABLE orders ADD COLUMN note text", Operation: "alter", ExecutionMode: "blocked", ModeReason: localBlockedReason},
		{Namespace: "testdb", Table: "users", DDL: "ALTER TABLE users ADD COLUMN note text", Operation: "alter", ExecutionMode: "direct"},
	}}

	tasks := buildDispatchTasks(plan, scope, "production", storage.EngineSpirit, []byte("{}"), time.Now())
	require.Len(t, tasks, 2)
	assert.Equal(t, "blocked", tasks[0].ExecutionMode)
	assert.Equal(t, localBlockedReason, tasks[0].ModeReason)
	assert.Equal(t, "direct", tasks[1].ExecutionMode)
	assert.Empty(t, tasks[1].ModeReason)
}

// A shard-scoped dispatch is built from task rows, so its changes arrive with
// no execution verdict. The rows it creates must still carry the admitting
// deployment's verdict, which lives on the stored plan — per shard where the
// plan judged shards separately — so a drive that later loads those rows
// judges the same verdict the plan gate did.
func TestBlockedTaskError(t *testing.T) {
	require.NoError(t, blockedTaskError(nil))
	require.NoError(t, blockedTaskError([]*storage.Task{{ExecutionMode: "direct"}}))

	tasks := []*storage.Task{
		{TaskIdentifier: "task-direct", TableName: "users", ExecutionMode: "direct"},
		{TaskIdentifier: "task-orders", TableName: "orders", ExecutionMode: "BLOCKED", ModeReason: localBlockedReason},
		{TaskIdentifier: "task-later", TableName: "payments", ExecutionMode: "blocked", ModeReason: "later reason"},
	}
	require.EqualError(t, blockedTaskError(tasks), `stored task task-orders contains a blocked change for table "orders": `+localBlockedReason)

	require.EqualError(t, blockedTaskError([]*storage.Task{{TaskIdentifier: "task-fallback", TableName: "orders", ExecutionMode: "blocked"}}),
		`stored task task-fallback contains a blocked change for table "orders": the engine refuses this statement`)

	// Independent causes render one per line, exactly as the whole-plan
	// refusal renders them, so an operator sees every cause on the first
	// attempt rather than the raw encoded separator.
	multiCause := engine.JoinBlockedCauses([]string{"requires privileges unavailable to the engine", "table size could not be measured"})
	tasks = []*storage.Task{{TaskIdentifier: "task-multi", TableName: "orders", ExecutionMode: "blocked", ModeReason: multiCause}}
	taskErr := blockedTaskError(tasks)
	require.EqualError(t, taskErr,
		"stored task task-multi contains a blocked change for table \"orders\":\n- requires privileges unavailable to the engine\n- table size could not be measured")
	assert.NotContains(t, taskErr.Error(), "‖")
	planErr := (&storage.Plan{PlanIdentifier: "plan-multi", Namespaces: map[string]*storage.NamespacePlanData{
		"testdb": {Tables: []storage.TableChange{{Table: "orders", ExecutionMode: "blocked", ModeReason: multiCause}}},
	}}).BlockedApplyError()
	require.Error(t, planErr)
	assert.Equal(t,
		strings.TrimPrefix(planErr.Error(), `stored plan plan-multi contains a blocked change for table "orders"`),
		strings.TrimPrefix(taskErr.Error(), `stored task task-multi contains a blocked change for table "orders"`),
		"row and plan refusals render the same causes the same way")
}

// alterUsersEmailDispatch is the dispatch a non-primary deployment receives for
// the reviewed ALTER: built from task rows, so it carries no execution-mode
// verdict.
func alterUsersEmailDispatch(planID string, targetShards ...string) *ternv1.ApplyRequest {
	return &ternv1.ApplyRequest{
		PlanId:       planID,
		TargetShards: targetShards,
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	}
}

// A statement the primary's target admitted can be one this deployment's own
// engine refuses (a missing grant here, a table over the size bound here). The
// dispatch cannot carry that verdict, so the plan materialized from it must
// take this deployment's re-plan verdict — otherwise the blocked-step admission
// gate reads a plan with no verdicts and admits work guaranteed to fail.
func TestMaterializedPlanCarriesLocalBlockedVerdict(t *testing.T) {
	recomputed := alterUsersEmailPlan()
	recomputed.Changes[0].TableChanges[0].ExecutionMode = engine.ExecutionModeBlocked
	recomputed.Changes[0].TableChanges[0].ModeReason = localBlockedReason
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 21}
	c := newPlanMaterializeClientWithPlan(store, recomputed)

	got, err := c.planForApplyRequest(t.Context(), alterUsersEmailDispatch("plan_local_blocked"))

	require.NoError(t, err, "a blocked verdict is not drift; the plan still materializes and admission refuses it")
	require.NotNil(t, got)
	require.NotNil(t, store.created)
	change := store.created.Namespaces["testapp"].Tables[0]
	assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode)
	assert.Equal(t, localBlockedReason, change.ModeReason)
	require.EqualError(t, got.BlockedApplyError(),
		`stored plan plan_local_blocked contains a blocked change for table "users": `+localBlockedReason)
}

// The verdict stamped is this target's, whatever it is: a direct-execution
// verdict is carried too, so the materialized plan describes how the statement
// runs here rather than only whether it is refused.
func TestMaterializedPlanCarriesLocalDirectVerdict(t *testing.T) {
	recomputed := alterUsersEmailPlan()
	recomputed.Changes[0].TableChanges[0].ExecutionMode = engine.ExecutionModeDirect
	recomputed.Changes[0].TableChanges[0].ModeReason = "instant DDL routed to native execution"
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 22}
	c := newPlanMaterializeClientWithPlan(store, recomputed)

	got, err := c.planForApplyRequest(t.Context(), alterUsersEmailDispatch("plan_local_direct"))

	require.NoError(t, err)
	require.NoError(t, got.BlockedApplyError())
	change := store.created.Namespaces["testapp"].Tables[0]
	assert.Equal(t, engine.ExecutionModeDirect, change.ExecutionMode)
	assert.Equal(t, "instant DDL routed to native execution", change.ModeReason)
}

// A shard-scoped dispatch is judged by the re-plan restricted to that shard, so
// a verdict the engine reached for that shard's copy of the table refuses the
// dispatch for it.
func TestMaterializedShardPlanCarriesLocalBlockedVerdict(t *testing.T) {
	recomputed := alterUsersEmailShardPlan("-80")
	recomputed.Changes[0].TableChanges[0].ExecutionMode = engine.ExecutionModeBlocked
	recomputed.Changes[0].TableChanges[0].ModeReason = localBlockedReason
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 23}
	c := newPlanMaterializeClientWithPlan(store, recomputed)

	got, err := c.planForApplyRequest(t.Context(), alterUsersEmailDispatch("plan_shard_blocked", "-80"))

	require.NoError(t, err)
	require.Error(t, got.BlockedApplyError())
	assert.Equal(t, engine.ExecutionModeBlocked, store.created.Namespaces["testapp"].Tables[0].ExecutionMode)
}

// A dispatch that already carries a blocked verdict keeps it even when this
// deployment's re-plan would run the statement: the verdict is only ever
// tightened at materialization, never relaxed.
func TestMaterializedPlanKeepsDispatchedBlockedVerdict(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 24}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())
	req := alterUsersEmailDispatch("plan_dispatched_blocked")
	req.DdlChanges[0].ExecutionMode = engine.ExecutionModeBlocked
	req.DdlChanges[0].ModeReason = "the primary's engine refuses this statement"

	got, err := c.planForApplyRequest(t.Context(), req)

	require.NoError(t, err)
	change := store.created.Namespaces["testapp"].Tables[0]
	assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode)
	assert.Equal(t, "the primary's engine refuses this statement", change.ModeReason)
	require.Error(t, got.BlockedApplyError())
}

// When the re-plan holds the same change twice with different verdicts, the
// blocked one wins regardless of order: admitting a plan one copy would refuse
// is the failure the gate exists to prevent.
func TestReplannedChangesRecordBlockedWins(t *testing.T) {
	key := driftChangeKey{namespace: "testapp", table: "users", operation: "alter", ddl: "ALTER TABLE users ADD COLUMN email varchar(255)"}
	statement := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	blocked := replannedChange{ddl: statement, mode: engine.ExecutionModeBlocked, reason: localBlockedReason}
	direct := replannedChange{ddl: statement, mode: engine.ExecutionModeDirect, reason: "direct"}
	unjudged := replannedChange{ddl: statement}

	blockedFirst := replannedChanges{byChange: map[driftChangeKey]replannedChange{}}
	blockedFirst.record(key, blocked)
	blockedFirst.record(key, unjudged)
	assert.Equal(t, blocked, blockedFirst.byChange[key])

	blockedLast := replannedChanges{byChange: map[driftChangeKey]replannedChange{}}
	blockedLast.record(key, direct)
	blockedLast.record(key, blocked)
	assert.Equal(t, blocked, blockedLast.byChange[key])

	neitherBlocked := replannedChanges{byChange: map[driftChangeKey]replannedChange{}}
	neitherBlocked.record(key, direct)
	neitherBlocked.record(key, unjudged)
	assert.Equal(t, direct, neitherBlocked.byChange[key], "the first non-blocked change stands")
}

// A blocked verdict on one shard's copy of a table already seen on another
// shard is deduped out of the namespace-level tables but survives on the shard
// plan, which is what the admission gate reads.
func TestBlockedVerdictSurvivesShardedDedupe(t *testing.T) {
	client := &LocalClient{}
	changes := []engine.SchemaChange{
		{Namespace: "ks", Shard: engine.Shard{Name: "-80"}, TableChanges: []engine.TableChange{
			{Table: "users", DDL: "ALTER TABLE users ADD COLUMN email text", Operation: ddl.StatementAlterTable},
		}},
		{Namespace: "ks", Shard: engine.Shard{Name: "80-"}, TableChanges: []engine.TableChange{
			{Table: "users", DDL: "ALTER TABLE users ADD COLUMN email text", Operation: ddl.StatementAlterTable,
				ExecutionMode: engine.ExecutionModeBlocked, ModeReason: localBlockedReason},
		}},
	}

	namespaces, shards := client.namespacesFromEngineChanges(changes, nil)
	require.Len(t, namespaces["ks"].Tables, 1, "the namespace-level copy is deduped by table name")
	assert.Empty(t, namespaces["ks"].Tables[0].ExecutionMode, "and the surviving copy is the executable one")

	plan := &storage.Plan{PlanIdentifier: "plan-shard", Namespaces: namespaces, Shards: shards}
	blocked := plan.BlockedChanges()
	require.Len(t, blocked, 1, "the per-shard verdict is what refuses the apply")
	assert.Equal(t, "ks", blocked[0].Namespace)
	require.EqualError(t, plan.BlockedApplyError(),
		`stored plan plan-shard contains a blocked change for table "users": `+localBlockedReason)
}
