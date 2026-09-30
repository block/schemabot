package api

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
)

// placedNamespacesRequest declares a distinct table in each namespace, so a
// drop names the one namespace that declares it.
func placedNamespacesRequest() PlanRequest {
	files := func(table string) *ternv1.SchemaFiles {
		return &ternv1.SchemaFiles{Files: map[string]string{
			table + ".sql": "CREATE TABLE `" + table + "` (id bigint primary key)",
			"vschema.json": `{"sharded": false}`,
		}}
	}
	return PlanRequest{
		Database:    "orders",
		Environment: "production",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{"ns_0": files("orders"), "ns_1": files("payments"), "ns_2": files("refunds")},
	}
}

func dropsPlan(namespace string, tables ...string) []*ternv1.SchemaChange {
	change := &ternv1.SchemaChange{Namespace: namespace}
	for _, table := range tables {
		change.TableChanges = append(change.TableChanges, &ternv1.TableChange{
			TableName:  table,
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_DROP,
			Ddl:        "DROP TABLE `" + table + "`",
			IsUnsafe:   true,
		})
	}
	return []*ternv1.SchemaChange{change}
}

// The primary selects ns_0, and the other target holds ns_1 and ns_2. A data
// plane that discards the unselected namespaces diffs the whole target and
// proposes drops in ns_1. The schema files never asked for them, so the plan is
// refused before it is stored, whatever the data plane build and whatever the
// table, rather than reviewed as an unsafe drop an operator could accept with
// --allow-unsafe. A drop the plan places in ns_0 is an ordinary drop and plans,
// even when ns_1 declares a table of the same name, since the namespaces of a
// sharded family all declare the same tables. A drop the plan places in no
// namespace fails closed on its name: it is refused when an unselected namespace
// declares that table.
func TestExecutePlan_RefusesDropOfTableAnUnselectedNamespaceDeclares(t *testing.T) {
	plan := func(t *testing.T, changes []*ternv1.SchemaChange) (*capturingPlanStore, error) {
		t.Helper()
		client := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-primary", Changes: changes}}
		plans := &capturingPlanStore{}
		_, err := namespaceSelectionService(t, client, plans).ExecutePlan(t.Context(), placedNamespacesRequest())
		return plans, err
	}

	t.Run("drop in an unselected namespace", func(t *testing.T) {
		plans, err := plan(t, dropsPlan("ns_1", "legacy"))
		var dropErr *UnselectedTableDropError
		require.ErrorAs(t, err, &dropErr)
		assert.Equal(t, []string{"legacy"}, dropErr.Tables, "a drop placed in an unselected namespace is refused whatever the table")
		assert.Equal(t, []string{"ns_1"}, dropErr.Namespaces)
		assert.Contains(t, err.Error(), `target "orders-001" proposes dropping "legacy" in namespaces [ns_1], which this target's entry does not select`)
		assert.Contains(t, err.Error(), "--allow-unsafe included")
		assert.Nil(t, plans.created, "a refused plan is never stored")
	})

	t.Run("drop in the selected namespace of a table an unselected namespace declares", func(t *testing.T) {
		plans, err := plan(t, dropsPlan("ns_0", "payments"))
		require.NoError(t, err)
		require.NotNil(t, plans.created)
	})

	t.Run("unattributed drop of a table an unselected namespace declares", func(t *testing.T) {
		plans, err := plan(t, dropsPlan("", "payments", "legacy"))
		var dropErr *UnselectedTableDropError
		require.ErrorAs(t, err, &dropErr)
		assert.Equal(t, []string{"payments"}, dropErr.Tables, "only the unattributed drop an unselected namespace declares is refused")
		assert.Equal(t, []string{"ns_1"}, dropErr.Namespaces)
		assert.Nil(t, plans.created, "a refused plan is never stored")
	})

	t.Run("unattributed drop of a table no namespace declares", func(t *testing.T) {
		plans, err := plan(t, dropsPlan("", "legacy"))
		require.NoError(t, err)
		require.NotNil(t, plans.created)
	})
}

// A member's diff becomes its stored plan, so a member whose data plane
// proposes a drop in the primary's namespace blocks the review rollup instead
// of staging the drop.
func TestRollupReviewTimeDrift_MemberDropOfUnselectedNamespaceTableBlocks(t *testing.T) {
	primary := routing.ExecutionTarget{Deployment: "eu", Target: "orders-001", Namespaces: []string{"ns_0"}}
	reviewed := &ternv1.PlanResponse{PlanId: "plan-primary", Engine: ternv1.Engine_ENGINE_SPIRIT}
	client := &mockTernClient{planDiffResp: &ternv1.PlanDiffResponse{Engine: ternv1.Engine_ENGINE_SPIRIT, Changes: dropsPlan("ns_0", "orders")}}
	plans := &recordingPlanStore{}
	svc := namespaceSelectionService(t, client, plans)

	rollup, err := svc.RollupReviewTimeDrift(t.Context(), placedNamespacesRequest(), reviewed, primary)
	require.NoError(t, err)
	assert.False(t, rollup.Clean)
	require.Len(t, rollup.Entries, 2)
	assert.Equal(t, DeploymentErrored, rollup.Entries[1].Class)
	var dropErr *UnselectedTableDropError
	require.ErrorAs(t, rollup.Entries[1].Err, &dropErr)
	assert.Equal(t, "orders-002", dropErr.Target)
	assert.Equal(t, []string{"orders"}, dropErr.Tables)
	assert.Equal(t, []string{"ns_0"}, dropErr.Namespaces)
	assert.Empty(t, plans.created, "the refused member plan is never stored")
}

// A shard plan's drops are placed by the shard's namespace or the table
// change's own, and any unselected attribution refuses the drop. A shard drop
// with no attribution falls back to the name match, and a schema file the
// dialect's parser rejects fails that fallback rather than letting a drop
// through unchecked. Attributed drops never parse the files.
func TestRefuseDropsOfUnselectedTables(t *testing.T) {
	svc := namespaceSelectionService(t, &mockTernClient{}, &capturingPlanStore{})
	member := routing.ExecutionTarget{DatabaseType: storage.DatabaseTypeMySQL, Deployment: "eu", Target: "orders-001", Namespaces: []string{"ns_0"}}
	req := placedNamespacesRequest()
	unselected := []string{"ns_1", "ns_2"}
	shardDrops := func(shardNamespace, tableNamespace, table string) []*ternv1.ShardPlan {
		changes := dropsPlan("", table)[0].TableChanges
		changes[0].Namespace = tableNamespace
		return []*ternv1.ShardPlan{{Shard: "-80", Namespace: shardNamespace, Changes: changes}}
	}
	var dropErr *UnselectedTableDropError

	err := svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, unselected, member, nil, shardDrops("", "", "refunds"))
	require.ErrorAs(t, err, &dropErr, "an unattributed shard drop fails closed on its name")
	assert.Equal(t, []string{"refunds"}, dropErr.Tables)
	assert.Equal(t, []string{"ns_2"}, dropErr.Namespaces)

	err = svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, unselected, member, nil, shardDrops("ns_0", "ns_2", "legacy"))
	require.ErrorAs(t, err, &dropErr, "a table change placed in an unselected namespace is refused even on a selected shard")
	assert.Equal(t, []string{"legacy"}, dropErr.Tables)
	assert.Equal(t, []string{"ns_2"}, dropErr.Namespaces)

	assert.NoError(t, svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, unselected, member, nil, shardDrops("ns_0", "", "refunds")),
		"a shard drop placed in the selected namespace passes")

	assert.NoError(t, svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, nil, member, dropsPlan("", "payments"), nil),
		"a member selecting nothing has no unselected namespaces to protect")

	req.SchemaFiles["ns_1"] = &ternv1.SchemaFiles{Files: map[string]string{"payments.sql": "CREATE TABLE `payments` ("}}
	assert.NoError(t, svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, []string{"ns_1"}, member, dropsPlan("ns_0", "payments"), nil),
		"an attributed drop is judged by its namespace without parsing the files")
	err = svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, []string{"ns_1"}, member, dropsPlan("", "payments"), nil)
	require.Error(t, err)
	assert.False(t, errors.As(err, &dropErr), "an unparseable file is a check failure, not a verdict on the drop")
	assert.Contains(t, err.Error(), "ns_1/payments.sql")
}
