package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	for name, tc := range map[string]struct {
		req  *ternv1.ApplyRequest
		want string
	}{
		"step zero":         {stepDispatch("0", "orders"), `dispatch rollout step "0" is not a positive number`},
		"step not a number": {stepDispatch("two", "orders"), `dispatch rollout step "two" is not a positive number`},
		"no member target":  {noTarget, "names rollout step 1 but no member target"},
		"target shards":     {sharded, "names rollout step 1 and target shards [-80]"},
		"unplanned table":   {stepDispatch("2", "payouts"), `plan plan-payments-001 has no change to table "payouts" in namespace "payments"`},
		"two tables":        {stepDispatch("1", "orders", "refunds"), `the dispatch names tables "orders" and "refunds"`},
		"no table":          {stepDispatch("1"), "the dispatch names no table"},
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

// The control plane names a stepped operation's table step on its dispatch,
// alongside its member target, and leaves it off an operation that runs its
// member's whole change.
func TestStampMemberTarget_NamesTheOperationsRolloutStep(t *testing.T) {
	stepped := applyTaskScope{memberTarget: "payments-001", operation: &storage.ApplyOperation{Target: "payments-001", OperationKey: "payments-001/step-2", RolloutStep: 2}}
	req := &ternv1.ApplyRequest{}
	stepped.stampMemberTarget(req)
	assert.Equal(t, map[string]string{dispatchMemberTargetOption: "payments-001", dispatchRolloutStepOption: "2"}, req.Options)

	whole := applyTaskScope{memberTarget: "payments-001", operation: &storage.ApplyOperation{Target: "payments-001", OperationKey: "payments-001"}}
	req = &ternv1.ApplyRequest{}
	whole.stampMemberTarget(req)
	assert.Equal(t, map[string]string{dispatchMemberTargetOption: "payments-001"}, req.Options)
}
