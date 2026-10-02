package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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
// proposes drops of ns_1's live tables. The schema files never asked for them,
// so the plan is refused before it is stored, whatever the data plane build,
// rather than reviewed as an unsafe drop an operator could accept with
// --allow-unsafe. A drop the plan places in ns_1 is refused whatever the table.
// The MySQL engine attributes such a drop to the one namespace it was sent,
// ns_0, so on MySQL a drop placed in ns_0 is refused when an unselected
// namespace declares that table, and plans when none does. A drop the plan
// places in no namespace fails closed on its name the same way.
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
		assert.Equal(t, []string{"legacy"}, dropErr.Placed, "a drop placed in an unselected namespace is refused whatever the table")
		assert.Equal(t, []string{"ns_1"}, dropErr.PlacedNamespaces)
		assert.Empty(t, dropErr.Named)
		assert.Contains(t, err.Error(), `target "orders-001" proposes dropping "legacy" in namespaces [ns_1], which this target's entry does not select`)
		assert.Contains(t, err.Error(), "--allow-unsafe included")
		assert.True(t, UnselectedTableDropRefused(err), "the refusal is typed so a merge gate fails the environment's check closed on it")
		assert.Nil(t, plans.created, "a refused plan is never stored")
	})

	t.Run("unselected namespace's live table attributed to the selected namespace", func(t *testing.T) {
		plans, err := plan(t, dropsPlan("ns_0", "payments", "refunds"))
		var dropErr *UnselectedTableDropError
		require.ErrorAs(t, err, &dropErr, "the MySQL engine infers a drop's namespace, so the attribution cannot clear it")
		assert.Equal(t, []string{"payments", "refunds"}, dropErr.Named)
		assert.Equal(t, []string{"ns_1", "ns_2"}, dropErr.NamedNamespaces)
		assert.Empty(t, dropErr.Placed)
		assert.Nil(t, plans.created, "a refused plan is never stored")
	})

	t.Run("drop in the selected namespace of a table no unselected namespace declares", func(t *testing.T) {
		plans, err := plan(t, dropsPlan("ns_0", "legacy"))
		require.NoError(t, err)
		require.NotNil(t, plans.created)
	})

	t.Run("unattributed drop of a table an unselected namespace declares", func(t *testing.T) {
		plans, err := plan(t, dropsPlan("", "payments", "legacy"))
		var dropErr *UnselectedTableDropError
		require.ErrorAs(t, err, &dropErr)
		assert.Equal(t, []string{"payments"}, dropErr.Named, "only the unattributed drop an unselected namespace declares is refused")
		assert.Equal(t, []string{"ns_1"}, dropErr.NamedNamespaces)
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
	assert.Equal(t, []string{"orders"}, dropErr.Placed)
	assert.Equal(t, []string{"ns_0"}, dropErr.PlacedNamespaces)
	assert.Empty(t, plans.created, "the refused member plan is never stored")
}

// A shard plan's drops are placed by the shard's namespace or the table
// change's own, and any unselected attribution refuses the drop. An engine that
// locates a dropped table, such as Vitess, clears a drop it places in a selected
// namespace on that attribution alone, even when an unselected namespace
// declares a table of that name, as the namespaces of a sharded family all do,
// and never parses the files for it. The MySQL engine infers its attribution,
// so its drops are judged by name as well, the same as a drop with no
// attribution. A schema file the dialect's parser rejects fails that name match
// rather than letting a drop through unchecked.
func TestRefuseDropsOfUnselectedTables(t *testing.T) {
	svc := namespaceSelectionService(t, &mockTernClient{}, &capturingPlanStore{})
	member := routing.ExecutionTarget{DatabaseType: storage.DatabaseTypeMySQL, Deployment: "eu", Target: "orders-001", Namespaces: []string{"ns_0"}}
	locating := member
	locating.DatabaseType = storage.DatabaseTypeVitess
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
	assert.Equal(t, []string{"refunds"}, dropErr.Named)
	assert.Equal(t, []string{"ns_2"}, dropErr.NamedNamespaces)

	err = svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, unselected, member, nil, shardDrops("ns_0", "ns_2", "legacy"))
	require.ErrorAs(t, err, &dropErr, "a table change placed in an unselected namespace is refused even on a selected shard")
	assert.Equal(t, []string{"legacy"}, dropErr.Placed)
	assert.Equal(t, []string{"ns_2"}, dropErr.PlacedNamespaces)

	assert.NoError(t, svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, unselected, locating, nil, shardDrops("ns_0", "", "refunds")),
		"a located shard drop placed in the selected namespace passes")
	assert.NoError(t, svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, unselected, locating, dropsPlan("ns_0", "payments"), nil),
		"a located drop placed in the selected namespace passes even when an unselected namespace declares that table")

	err = svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, unselected, member, nil, shardDrops("ns_0", "", "refunds"))
	require.ErrorAs(t, err, &dropErr, "an inferred attribution to the selected namespace does not clear a drop an unselected namespace declares")
	assert.Equal(t, []string{"refunds"}, dropErr.Named)
	assert.Equal(t, []string{"ns_2"}, dropErr.NamedNamespaces)

	assert.NoError(t, svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, nil, member, dropsPlan("", "payments"), nil),
		"a member selecting nothing has no unselected namespaces to protect")

	req.SchemaFiles["ns_1"] = &ternv1.SchemaFiles{Files: map[string]string{"payments.sql": "CREATE TABLE `payments` ("}}
	assert.NoError(t, svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, []string{"ns_1"}, locating, dropsPlan("ns_0", "payments"), nil),
		"a located drop is judged by its namespace without parsing the files")
	err = svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, []string{"ns_1"}, member, dropsPlan("ns_0", "payments"), nil)
	require.ErrorAs(t, err, new(*UnselectedTableDropCheckError), "an inferred attribution parses the files, and a file that cannot be parsed fails the check")
	err = svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, []string{"ns_1"}, member, dropsPlan("", "payments"), nil)
	require.Error(t, err)
	assert.False(t, errors.As(err, &dropErr), "an unparseable file is a check failure, not a verdict on the drop")
	var checkErr *UnselectedTableDropCheckError
	require.ErrorAs(t, err, &checkErr)
	assert.Equal(t, "orders-001", checkErr.Target)
	assert.Contains(t, err.Error(), `check planned drops of database "orders" environment "production" target "orders-001" against its unselected namespaces`)
	assert.Contains(t, err.Error(), "ns_1/payments.sql")
	assert.True(t, UnselectedTableDropRefused(err), "a check failure fails the environment's check closed like a refused drop")
	assert.False(t, NamespacePlacementRefused(err), "a check failure is not a placement defect the API answers 400 for")
}

// A plan the control plane refuses after the data plane answered is counted
// once, as an error, so the plan counter never reports a refused plan as a
// success.
func TestExecutePlan_RefusedDropIsCountedOnlyAsAnError(t *testing.T) {
	reader := installManualMetricReader(t)
	client := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-primary", Changes: dropsPlan("ns_1", "legacy")}}
	_, err := namespaceSelectionService(t, client, &capturingPlanStore{}).ExecutePlan(t.Context(), placedNamespacesRequest())
	require.ErrorAs(t, err, new(*UnselectedTableDropError))

	var statuses []string
	for _, dp := range collectCounterPoints(t, reader, "schemabot.plans.total") {
		for range dp.Value {
			statuses = append(statuses, attributeValue(t, dp, "status"))
		}
	}
	assert.Equal(t, []string{"error"}, statuses)
}

// A drop the plan places in an unselected namespace can only come from a data
// plane that planned another target's namespace, so the refusal names the
// upgrade. A drop refused by name alone is the MySQL case where the engine does
// not say whose table it is: the refusal says the name collides with a table
// an unselected namespace declares, which is also what a legitimate drop of
// this target's own table of that name looks like.
func TestUnselectedTableDropError_NamesTheCause(t *testing.T) {
	svc := namespaceSelectionService(t, &mockTernClient{}, &capturingPlanStore{})
	member := routing.ExecutionTarget{DatabaseType: storage.DatabaseTypeMySQL, Deployment: "eu", Target: "orders-001", Namespaces: []string{"ns_0"}}
	req := placedNamespacesRequest()
	unselected := []string{"ns_1", "ns_2"}

	err := svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, unselected, member, dropsPlan("ns_1", "legacy"), nil)
	var placed *UnselectedTableDropError
	require.ErrorAs(t, err, &placed)
	assert.Equal(t, []string{"legacy"}, placed.Placed)
	assert.Empty(t, placed.Named)
	assert.Contains(t, err.Error(), "Upgrade that deployment to a build that supports selecting namespaces per target")

	err = svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, unselected, member, dropsPlan("ns_0", "payments"), nil)
	var named *UnselectedTableDropError
	require.ErrorAs(t, err, &named)
	assert.Equal(t, []string{"payments"}, named.Named, "a drop the plan places in the selected namespace is refused by name alone")
	assert.Empty(t, named.Placed)
	assert.Contains(t, err.Error(), `proposes dropping "payments", and namespaces [ns_1], which this target's entry does not select, declare tables of the same name`)
	assert.Contains(t, err.Error(), "the drop is intended and its table name collides with a table those namespaces still declare")
	assert.NotContains(t, err.Error(), "Upgrade that deployment", "a name collision does not prescribe an upgrade")
}

// A plan refused both ways reports each table under the cause that refused it,
// with that cause's remedy: a Vitess drop of legacy placed in the unselected
// ns_1 names the upgrade, and an unattributed drop of payments, which ns_1
// declares, names the collision. A drop the plan places in an unselected
// namespace is reported as placed even when that namespace also declares it.
func TestUnselectedTableDropError_MixedRefusalNamesEachCause(t *testing.T) {
	svc := namespaceSelectionService(t, &mockTernClient{}, &capturingPlanStore{})
	member := routing.ExecutionTarget{DatabaseType: storage.DatabaseTypeVitess, Deployment: "eu", Target: "orders-001", Namespaces: []string{"ns_0"}}
	req := placedNamespacesRequest()
	unselected := []string{"ns_1", "ns_2"}

	changes := append(dropsPlan("ns_1", "legacy"), dropsPlan("", "payments")...)
	err := svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, unselected, member, changes, nil)
	var dropErr *UnselectedTableDropError
	require.ErrorAs(t, err, &dropErr)
	assert.Equal(t, []string{"legacy"}, dropErr.Placed)
	assert.Equal(t, []string{"ns_1"}, dropErr.PlacedNamespaces)
	assert.Equal(t, []string{"payments"}, dropErr.Named)
	assert.Equal(t, []string{"ns_1"}, dropErr.NamedNamespaces)
	assert.Contains(t, err.Error(), `target "orders-001" proposes dropping "legacy" in namespaces [ns_1], which this target's entry does not select. The schema files do not ask for these drops`)
	assert.Contains(t, err.Error(), `Upgrade that deployment to a build that supports selecting namespaces per target. It also proposes dropping "payments", and namespaces [ns_1], which this target's entry does not select, declare tables of the same name`)

	member.DatabaseType = storage.DatabaseTypeMySQL
	err = svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, unselected, member, dropsPlan("ns_1", "payments"), nil)
	require.ErrorAs(t, err, &dropErr)
	assert.Equal(t, []string{"payments"}, dropErr.Placed)
	assert.Empty(t, dropErr.Named, "a placed drop is not also reported as a name collision")
	assert.NotContains(t, err.Error(), "declare tables of the same name")
}

// A server that folds table names reports a declared Orders as a drop of
// orders, so a drop judged by name matches an unselected namespace's
// declaration whatever its case, and the drop is refused rather than let
// through on a case mismatch.
func TestRefuseDropsOfUnselectedTables_NameMatchIgnoresCase(t *testing.T) {
	svc := namespaceSelectionService(t, &mockTernClient{}, &capturingPlanStore{})
	member := routing.ExecutionTarget{DatabaseType: storage.DatabaseTypeMySQL, Deployment: "eu", Target: "orders-001", Namespaces: []string{"ns_0"}}
	req := placedNamespacesRequest()
	req.SchemaFiles["ns_1"] = &ternv1.SchemaFiles{Files: map[string]string{"payments.sql": "CREATE TABLE `Payments` (id bigint primary key)"}}

	err := svc.refuseDropsOfUnselectedTables(req, req.SchemaFiles, []string{"ns_1", "ns_2"}, member, dropsPlan("ns_0", "payments", "REFUNDS"), nil)
	var dropErr *UnselectedTableDropError
	require.ErrorAs(t, err, &dropErr)
	assert.Equal(t, []string{"REFUNDS", "payments"}, dropErr.Named, "each drop is reported under the name the plan gave it")
	assert.Equal(t, []string{"ns_1", "ns_2"}, dropErr.NamedNamespaces)
}

// A refused drop is the request's answer, not a server fault, so POST
// /api/plan answers it as a bad request carrying the refusal, the same as a
// namespace placement refusal, and a client that retries server errors does
// not loop on it.
func TestPlanHandler_UnselectedTableDropIsBadRequest(t *testing.T) {
	client := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-primary", Changes: dropsPlan("ns_1", "legacy")}}
	svc := namespaceSelectionService(t, client, &capturingPlanStore{})
	planReq := placedNamespacesRequest()
	planReq.RendersRollout = true
	body, err := json.Marshal(planReq)
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	svc.handlePlan(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), `proposes dropping \"legacy\" in namespaces [ns_1], which this target's entry does not select`)
}
