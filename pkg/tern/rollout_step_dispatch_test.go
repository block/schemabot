package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// rolloutStepPlan is payments-001's plan of a rollout run table by table: it
// alters `orders` and creates `refunds`, each its own table step.
func rolloutStepPlan() *storage.Plan {
	return &storage.Plan{
		PlanIdentifier: "plan-payments-001",
		Target:         "payments-001",
		Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {Tables: []storage.TableChange{
				{Table: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `note` varchar(255)", Operation: "alter"},
				{Table: "refunds", DDL: "CREATE TABLE `refunds` (`id` bigint NOT NULL, PRIMARY KEY (`id`))", Operation: "create"},
			}},
		},
	}
}

func stepDispatch(step string, tables ...string) *ternv1.ApplyRequest {
	req := &ternv1.ApplyRequest{Options: map[string]string{dispatchMemberTargetOption: "payments-001", dispatchRolloutStepOption: step}}
	for _, table := range tables {
		req.DdlChanges = append(req.DdlChanges, &ternv1.TableChange{Namespace: "payments", TableName: table, Ddl: "dispatched text", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER})
	}
	return req
}

// A dispatch of payments-001's second table step runs only `refunds`, with the
// statement payments-001's own plan holds rather than the dispatched text, and
// is keyed by its step behind the target, the key the planner stored for it.
func TestDeriveDispatchScope_RolloutStepRunsOnlyItsTables(t *testing.T) {
	scope, err := deriveDispatchScope(rolloutStepPlan(), stepDispatch("2", "refunds"))
	require.NoError(t, err)

	assert.Equal(t, 2, scope.rolloutStep)
	require.Len(t, scope.ddlChanges, 1)
	assert.Equal(t, "refunds", scope.ddlChanges[0].Table)
	assert.Equal(t, "CREATE TABLE `refunds` (`id` bigint NOT NULL, PRIMARY KEY (`id`))", scope.ddlChanges[0].DDL)
	key, kind, err := operationIdentityForDispatch(scope)
	require.NoError(t, err)
	assert.Equal(t, "payments-001/step-2", key)
	assert.Empty(t, kind, "a table step is work")
}

// A step dispatch that cannot be run as exactly the step the control plane
// recorded is refused before it is admitted: a step that is not a positive
// number, one with no member target or with target shards, one naming a table
// the target's plan does not change, one naming two tables, and one naming no
// table at all.
func TestDeriveDispatchScope_RefusesAStepItCannotRunAsRecorded(t *testing.T) {
	noTarget := stepDispatch("1", "orders")
	delete(noTarget.Options, dispatchMemberTargetOption)
	sharded := stepDispatch("1", "orders")
	sharded.TargetShards = []string{"-80"}
	unplannedTwice := stepDispatch("2", "payouts")
	unplannedTwice.DdlChanges = append(unplannedTwice.DdlChanges, &ternv1.TableChange{Namespace: "billing", TableName: "payouts", Ddl: "dispatched text", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER})

	for name, tc := range map[string]struct {
		req  *ternv1.ApplyRequest
		want string
	}{
		"step zero":         {stepDispatch("0", "orders"), `dispatch rollout step "0" is not a positive number`},
		"step not a number": {stepDispatch("two", "orders"), `dispatch rollout step "two" is not a positive number`},
		"no member target":  {noTarget, "names rollout step 1 but no member target"},
		"target shards":     {sharded, "names rollout step 1 and target shards [-80]"},
		"unplanned table":   {stepDispatch("2", "payouts"), `plan plan-payments-001 has no change to table "payouts" in namespace "payments"`},
		// Named in two namespaces, the refusal names the first in order, so a
		// rerun reports the same one.
		"unplanned table in two namespaces": {unplannedTwice, `plan plan-payments-001 has no change to table "payouts" in namespace "billing"`},
		"two tables":                        {stepDispatch("1", "orders", "refunds"), `the dispatch names tables "orders" and "refunds"`},
		"no table":                          {stepDispatch("1"), "the dispatch names no table"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := deriveDispatchScope(rolloutStepPlan(), tc.req)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// A target holding the step's table in two of its namespaces runs both as
// tasks of the one step: a step is a table, wherever the target holds it.
func TestDeriveDispatchScope_RolloutStepRunsItsTableInEveryNamespace(t *testing.T) {
	plan := rolloutStepPlan()
	plan.Namespaces["payments_archive"] = &storage.NamespacePlanData{Tables: []storage.TableChange{
		{Table: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `note` varchar(255)", Operation: "alter"},
	}}
	req := stepDispatch("1", "orders")
	req.DdlChanges = append(req.DdlChanges, &ternv1.TableChange{Namespace: "payments_archive", TableName: "orders", Ddl: "dispatched text", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER})

	scope, err := deriveDispatchScope(plan, req)
	require.NoError(t, err)

	require.Len(t, scope.ddlChanges, 2)
	namespaces := []string{scope.ddlChanges[0].Namespace, scope.ddlChanges[1].Namespace}
	assert.ElementsMatch(t, []string{"payments", "payments_archive"}, namespaces)
	for _, change := range scope.ddlChanges {
		assert.Equal(t, "orders", change.Table)
	}
}

// memberStepDispatch is the dispatch of one table step of testapp-002, a
// rollout member other than the primary: its ddl_changes name the step's
// table, and beside them it carries every change of testapp-002's plan, which
// alters `users` (step 1) and creates `orders` (step 2).
func memberStepDispatch(step, table string) *ternv1.ApplyRequest {
	return &ternv1.ApplyRequest{
		PlanId:      "plan-testapp-002",
		Environment: "production",
		Target:      "testapp-002",
		Options:     map[string]string{dispatchMemberTargetOption: "testapp-002", dispatchRolloutStepOption: step},
		DdlChanges:  []*ternv1.TableChange{{Namespace: "testapp", TableName: table, Ddl: "dispatched text", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER}},
		PlanDdlChanges: []*ternv1.TableChange{
			{Namespace: "testapp", TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER},
			{Namespace: "testapp", TableName: "orders", Ddl: "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))", ChangeType: ternv1.ChangeType_CHANGE_TYPE_CREATE},
		},
		SchemaFiles: map[string]*ternv1.SchemaFiles{"testapp": {Files: map[string]string{"users.sql": "CREATE TABLE `users` ...", "orders.sql": "CREATE TABLE `orders` ..."}}},
	}
}

// testapp002LivePlan is what testapp-002's own live schema re-plans to before
// any of its steps has run: both of its plan's changes.
func testapp002LivePlan() *engine.PlanResult {
	return &engine.PlanResult{Changes: []engine.SchemaChange{{
		Namespace: "testapp",
		TableChanges: []engine.TableChange{
			{Table: "users", Operation: ddl.StatementAlterTable, DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
			{Table: "orders", Operation: ddl.StatementCreateTable, DDL: "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"},
		},
	}}}
}

// A rollout member other than the primary is planned without the data plane
// storing its plan, so the first of its table steps to reach the data plane
// builds the plan there from the whole plan the dispatch carries: both of
// testapp-002's changes are verified against its live schema and stored under
// its plan ID, and the step itself runs only its own table from that plan.
func TestPlanForApplyRequest_MemberStepBuildsItsTargetsWholePlan(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 42}
	c := newPlanMaterializeClientWithPlan(store, testapp002LivePlan())
	req := memberStepDispatch("2", "orders")

	got, err := c.planForApplyRequest(t.Context(), req)
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, "plan-testapp-002", got.PlanIdentifier)
	assert.Equal(t, "testapp-002", got.Target)
	require.NotNil(t, store.created)
	tables := map[string]string{}
	for _, change := range store.created.FlatDDLChanges() {
		tables[change.Table] = change.Operation
	}
	assert.Equal(t, map[string]string{"users": "alter", "orders": "create"}, tables, "the stored plan is the target's whole plan, not the step's table")

	scope, err := deriveDispatchScope(got, req)
	require.NoError(t, err)
	assert.Equal(t, 2, scope.rolloutStep)
	require.Len(t, scope.ddlChanges, 1)
	assert.Equal(t, "orders", scope.ddlChanges[0].Table)
	assert.Equal(t, "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))", scope.ddlChanges[0].DDL)
}

// The whole plan a step carries is held to the same drift check as any plan a
// deployment builds from a dispatch: testapp-002's live schema already holds
// `email`, so it re-plans to `orders` alone, the carried plan no longer
// describes it, and the step is refused before anything is stored.
func TestPlanForApplyRequest_MemberStepRefusesAPlanItsLiveSchemaDisagreesWith(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 42}
	live := testapp002LivePlan()
	live.Changes[0].TableChanges = live.Changes[0].TableChanges[1:]
	c := newPlanMaterializeClientWithPlan(store, live)

	got, err := c.planForApplyRequest(t.Context(), memberStepDispatch("2", "orders"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "local schema has drifted from the reviewed plan")
	assert.Nil(t, got)
	assert.Nil(t, store.created)
}

// A step dispatch with no stored plan and no whole plan beside it has nothing
// its target's plan can be built from, so it is refused. Only a control plane
// older than the whole-plan payload sends one, and the refusal stays
// retryable so the step runs once an upgraded control plane redispatches it.
func TestPlanForApplyRequest_RefusesAStepWithNoPlanToBuildFrom(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClient(store)
	req := memberStepDispatch("1", "users")
	req.PlanDdlChanges = nil

	got, err := c.planForApplyRequest(t.Context(), req)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "dispatch for rollout step 1 has no plan plan-testapp-002 stored on this deployment and carries no plan to build it from")
	assert.True(t, engine.IsRetryable(err), "a redispatch from an upgraded control plane carries the plan")
	assert.Nil(t, got)
	assert.Nil(t, store.created, "nothing is built from one step's table")
}

// The control plane names a stepped operation's table step on its dispatch,
// alongside its member target and its target's whole plan, and leaves both off
// an operation that runs its member's whole change.
func TestStampMemberTarget_NamesTheOperationsRolloutStep(t *testing.T) {
	stepped := applyTaskScope{memberTarget: "payments-001", operation: &storage.ApplyOperation{Target: "payments-001", OperationKey: "payments-001/step-2", RolloutStep: 2}}
	req := &ternv1.ApplyRequest{}
	stepped.stampMemberTarget(req, rolloutStepPlan())
	assert.Equal(t, map[string]string{dispatchMemberTargetOption: "payments-001", dispatchRolloutStepOption: "2"}, req.Options)
	assert.Equal(t, []*ternv1.TableChange{
		{Namespace: "payments", TableName: "orders", Ddl: "ALTER TABLE `orders` ADD COLUMN `note` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER},
		{Namespace: "payments", TableName: "refunds", Ddl: "CREATE TABLE `refunds` (`id` bigint NOT NULL, PRIMARY KEY (`id`))", ChangeType: ternv1.ChangeType_CHANGE_TYPE_CREATE},
	}, req.PlanDdlChanges, "a step carries its target's whole plan")

	whole := applyTaskScope{memberTarget: "payments-001", operation: &storage.ApplyOperation{Target: "payments-001", OperationKey: "payments-001"}}
	req = &ternv1.ApplyRequest{}
	whole.stampMemberTarget(req, rolloutStepPlan())
	assert.Equal(t, map[string]string{dispatchMemberTargetOption: "payments-001"}, req.Options)
	assert.Empty(t, req.PlanDdlChanges, "a whole-target dispatch already carries its plan in ddl_changes")
}
