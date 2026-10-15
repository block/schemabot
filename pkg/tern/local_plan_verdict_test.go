package tern

import (
	"log/slog"
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

// Dispatched tasks carry the table's planned size so progress can show it
// beside the row counts. A dispatch scoped to one shard creates tasks without
// one, since the plan's figure covers every shard of the table.
func TestBuildDispatchTasksCarryPlannedSize(t *testing.T) {
	plan := &storage.Plan{ID: 7, Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
	bytes := int64(23_400_000_000)
	changes := []storage.TableChange{
		{Namespace: "testdb", Table: "orders", DDL: "ALTER TABLE orders ADD INDEX idx_created_at (created_at)", Operation: "alter", EstimatedBytes: &bytes},
		{Namespace: "testdb", Table: "users", DDL: "ALTER TABLE users ADD INDEX idx_email (email)", Operation: "alter"},
	}

	tasks := buildDispatchTasks(plan, dispatchScope{ddlChanges: changes}, "production", storage.EngineSpirit, []byte("{}"), time.Now())
	require.Len(t, tasks, 2)
	require.NotNil(t, tasks[0].EstimatedBytes)
	assert.Equal(t, bytes, *tasks[0].EstimatedBytes)
	assert.Nil(t, tasks[1].EstimatedBytes)

	shardTasks := buildDispatchTasks(plan, dispatchScope{ddlChanges: changes, shard: "-80"}, "production", storage.EngineSpirit, []byte("{}"), time.Now())
	require.Len(t, shardTasks, 2)
	assert.Nil(t, shardTasks[0].EstimatedBytes)
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

const dropLegacyIDReason = "DROP COLUMN removes data"

// dropUsersLegacyIDReplan is this deployment's re-plan of a dropped column: an
// alter by operation, which its engine judges unsafe.
func dropUsersLegacyIDReplan(shard string) *engine.PlanResult {
	return &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			Namespace: "testapp",
			Shard:     engine.Shard{Name: shard},
			TableChanges: []engine.TableChange{{
				Table:        "users",
				Operation:    ddl.StatementAlterTable,
				DDL:          "ALTER TABLE `users` DROP COLUMN `legacy_id`",
				IsUnsafe:     true,
				UnsafeReason: dropLegacyIDReason,
			}},
		}},
	}
}

// dropUsersLegacyIDDispatch is the dispatch for the dropped column as task rows
// build it: no unsafe flag travels with it.
func dropUsersLegacyIDDispatch(planID string, targetShards ...string) *ternv1.ApplyRequest {
	return &ternv1.ApplyRequest{
		PlanId:       planID,
		TargetShards: targetShards,
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` DROP COLUMN `legacy_id`", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	}
}

// A dispatched column drop carries no unsafe flag, and its operation is an
// alter, so only this deployment's re-plan knows it destroys data. The
// materialized plan and the dispatch scope built from it must carry that
// judgement, so the unsafe-change admission gate refuses the apply unless the
// operator opted in — whether the dispatch covers the whole deployment or one
// shard.
func TestDispatchScopeCarriesReplannedUnsafeVerdict(t *testing.T) {
	cases := []struct {
		name         string
		shard        string
		targetShards []string
	}{
		{name: "whole deployment"},
		{name: "shard scoped", shard: "-80", targetShards: []string{"-80"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 25}
			c := newPlanMaterializeClientWithPlan(store, dropUsersLegacyIDReplan(tc.shard))
			req := dropUsersLegacyIDDispatch("plan_drop_legacy_id", tc.targetShards...)

			plan, err := c.planForApplyRequest(t.Context(), req)
			require.NoError(t, err)
			require.NotNil(t, plan)
			stored := plan.Namespaces["testapp"].Tables[0]
			assert.True(t, stored.IsUnsafe, "the plan row carries the re-plan's unsafe verdict")
			assert.Equal(t, dropLegacyIDReason, stored.UnsafeReason)

			scope, err := c.dispatchScopeForApply(plan, req)
			require.NoError(t, err)
			require.Len(t, scope.ddlChanges, 1)
			assert.True(t, scope.ddlChanges[0].IsUnsafe, "the task rows carry it too")

			require.EqualError(t, rejectUnsafeDDLChangesWithoutOptIn(plan.PlanIdentifier, scope.ddlChanges, storage.ApplyOptions{}),
				`stored plan plan_drop_legacy_id contains unsafe change for table "users": `+dropLegacyIDReason+`; retry with allow_unsafe=true`)
			require.NoError(t, rejectUnsafeDDLChangesWithoutOptIn(plan.PlanIdentifier, scope.ddlChanges, storage.ApplyOptions{AllowUnsafe: true}))
		})
	}
}

// The re-plan's unsafe verdict only ever tightens a change: an unsafe re-plan
// marks a safe change unsafe, and a safe re-plan leaves a change already
// marked unsafe, and its reason, as they were.
func TestReplannedChangeStampOntoOnlyTightensUnsafe(t *testing.T) {
	const statement = "ALTER TABLE `users` DROP COLUMN `legacy_id`"

	tightened := storage.TableChange{Table: "users", Operation: "alter", DDL: statement}
	replannedChange{ddl: statement, unsafe: true, unsafeReason: dropLegacyIDReason}.stampOnto(&tightened)
	assert.True(t, tightened.IsUnsafe)
	assert.Equal(t, dropLegacyIDReason, tightened.UnsafeReason)

	kept := storage.TableChange{Table: "users", Operation: "alter", DDL: statement, IsUnsafe: true, UnsafeReason: "reviewed as unsafe"}
	replannedChange{ddl: statement}.stampOnto(&kept)
	assert.True(t, kept.IsUnsafe, "a safe re-plan never clears the unsafe flag")
	assert.Equal(t, "reviewed as unsafe", kept.UnsafeReason)

	keptReason := storage.TableChange{Table: "users", Operation: "alter", DDL: statement, IsUnsafe: true, UnsafeReason: "reviewed as unsafe"}
	replannedChange{ddl: statement, unsafe: true}.stampOnto(&keptReason)
	assert.True(t, keptReason.IsUnsafe)
	assert.Equal(t, "reviewed as unsafe", keptReason.UnsafeReason, "an unsafe re-plan with no reason keeps the existing one")

	safe := storage.TableChange{Table: "users", Operation: "alter", DDL: statement}
	replannedChange{ddl: statement}.stampOnto(&safe)
	assert.False(t, safe.IsUnsafe)
	assert.Empty(t, safe.UnsafeReason)
}

// A blocked verdict on one shard's copy of a table already seen on another
// shard is deduped out of the namespace-level tables but survives on the shard
// plan, which is what the admission gate reads.
func TestBlockedVerdictSurvivesShardedDedupe(t *testing.T) {
	client := &LocalClient{logger: slog.New(slog.DiscardHandler)}
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
