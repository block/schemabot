package tern

import (
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const createRefundsDDL = "CREATE TABLE `refunds` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"

// The engine, the plan comment, and the CLI read the finalize request under one
// key; the two constants exist only because apitypes stays dependency-free.
func TestNeedsFinalizerKeyIsSharedByEngineAndAPITypes(t *testing.T) {
	assert.Equal(t, engine.MetadataNeedsFinalizer, apitypes.NeedsFinalizerMetadataKey)
}

// A sharded keyspace adds a table and its engine asks to finalize the keyspace
// once the table exists, with no VSchema document in the plan. The stored plan
// records the request on the namespace — from whichever shard's change carries
// it — without an artifact and without VSchema change-metadata, so the VSchema
// safety gate has nothing to inspect and the finalizer is still scheduled.
func TestNamespacesFromEngineChangesRecordsFinalizeRequest(t *testing.T) {
	client := planNamespacesTestClient()
	create := engine.TableChange{Table: "refunds", DDL: createRefundsDDL, Operation: ddl.StatementCreateTable}

	namespaces, _ := client.namespacesFromEngineChanges([]engine.SchemaChange{
		{Namespace: "payments", Shard: engine.Shard{Name: "-80"}, TableChanges: []engine.TableChange{create}},
		{Namespace: "payments", Shard: engine.Shard{Name: "80-"}, TableChanges: []engine.TableChange{create}, Metadata: map[string]string{engine.MetadataNeedsFinalizer: "true"}},
	}, schema.SchemaFiles{"payments": {Files: map[string]string{"refunds.sql": createRefundsDDL}}})

	require.Contains(t, namespaces, "payments")
	nsData := namespaces["payments"]
	assert.True(t, nsData.Finalize)
	assert.False(t, nsData.ChangesVSchema())
	assert.Empty(t, nsData.Metadata)
	plan := &storage.Plan{Namespaces: namespaces}
	assert.Equal(t, []string{"payments"}, plan.FinalizerNamespaces())
	assert.Empty(t, plan.VSchemaNamespaces())
	assert.Empty(t, plan.UnsafeVSchemaChanges())
}

// The finalizer drive tells the engine what each namespace's finalizer is for:
// a namespace with a VSchema document is told its VSchema changed, one the
// engine asked to finalize is told so, and one with both is told both. A
// finalize-only namespace is never told to apply a VSchema it does not have.
func TestFinalizerVSchemaChangesSaysWhatEachFinalizerIsFor(t *testing.T) {
	plan := &storage.Plan{
		ID: 3,
		Namespaces: map[string]*storage.NamespacePlanData{
			"both":     {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables":{}}`}, Finalize: true},
			"finalize": {Finalize: true},
			"vschema":  {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables":{}}`}},
			"neither":  {},
		},
	}

	changes, err := finalizerVSchemaChanges(plan, "")
	require.NoError(t, err)
	assert.Equal(t, []engine.SchemaChange{
		{Namespace: "both", Metadata: map[string]string{storage.PlanMetadataVSchemaChanged: "true", engine.MetadataNeedsFinalizer: "true"}},
		{Namespace: "finalize", Metadata: map[string]string{engine.MetadataNeedsFinalizer: "true"}},
		{Namespace: "vschema", Metadata: map[string]string{storage.PlanMetadataVSchemaChanged: "true"}},
	}, changes)

	scoped, err := finalizerVSchemaChanges(plan, "finalize")
	require.NoError(t, err)
	assert.Equal(t, []engine.SchemaChange{{Namespace: "finalize", Metadata: map[string]string{engine.MetadataNeedsFinalizer: "true"}}}, scoped)

	_, err = finalizerVSchemaChanges(plan, "neither")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `plan 3 has neither a VSchema artifact nor a finalize request for namespace "neither"`)
}

// A control plane dispatches a finalizer to a data plane that did not plan it.
// The data plane rebuilds the same plan from the dispatch: a finalize-only
// namespace comes back finalizing with no artifact demanded, and a namespace
// that also changes its VSchema comes back with both.
func TestFinalizerDispatchRoundTripsFinalizeRequest(t *testing.T) {
	vschemaMeta := map[string]string{storage.PlanMetadataVSchemaChanged: "true"}
	plan := &storage.Plan{
		Namespaces: map[string]*storage.NamespacePlanData{
			"finalize": {Finalize: true},
			"both": {
				Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables":{"refunds":{}}}`},
				Metadata:  vschemaMeta,
				Finalize:  true,
			},
		},
	}
	var dispatched []*ternv1.TableChange
	for _, namespace := range plan.FinalizerNamespaces() {
		dispatched = append(dispatched, &ternv1.TableChange{
			Namespace:  namespace,
			TableName:  "VSchema: " + namespace,
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
			Metadata:   finalizerDispatchMetadata(plan.Namespaces[namespace]),
		})
	}
	c := newPlanMaterializeClient(&fakePlanStore{})

	got, err := c.namespacesFromApplyRequest(dispatched, schema.SchemaFiles{
		"both": {Files: map[string]string{storage.VSchemaArtifactName: `{"tables":{"refunds":{}}}`}},
	})
	require.NoError(t, err)

	require.Contains(t, got, "finalize")
	assert.True(t, got["finalize"].Finalize)
	assert.False(t, got["finalize"].ChangesVSchema())
	require.Contains(t, got, "both")
	assert.True(t, got["both"].Finalize)
	assert.Equal(t, `{"tables":{"refunds":{}}}`, got["both"].Artifacts[storage.VSchemaArtifactName])
	assert.Equal(t, vschemaMeta, got["both"].Metadata)
	rebuilt := &storage.Plan{Namespaces: got}
	assert.Equal(t, []string{"both", "finalize"}, rebuilt.FinalizerNamespaces())
	assert.Empty(t, rebuilt.UnsafeVSchemaChanges())
}

// A dispatch that says its VSchema changed still needs the document, finalize
// request or not: the request never excuses a missing vschema.json.
func TestNamespacesFromApplyRequest_FinalizeWithVSchemaChangeStillNeedsArtifact(t *testing.T) {
	c := newPlanMaterializeClient(&fakePlanStore{})
	_, err := c.namespacesFromApplyRequest([]*ternv1.TableChange{{
		Namespace:  "shop",
		TableName:  "VSchema: shop",
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
		Metadata:   map[string]string{storage.PlanMetadataVSchemaChanged: "true", engine.MetadataNeedsFinalizer: "true"},
	}}, schema.SchemaFiles{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `apply request indicates a vschema change for namespace "shop" but carries no vschema.json artifact`)
}

// A deployment-scoped finalizer dispatch covers every namespace the plan
// finalizes, finalize-only ones included; leaving one out fails closed.
func TestFinalizerDispatchScopeCoversFinalizeOnlyNamespaces(t *testing.T) {
	plan := &storage.Plan{
		ID:             12,
		PlanIdentifier: "plan-scope-finalize",
		Namespaces: map[string]*storage.NamespacePlanData{
			"ks_a": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables": {}}`}},
			"ks_b": {Finalize: true},
		},
	}

	ns, err := finalizerDispatchScope(plan, []string{"ks_b", "ks_a"}, nil)
	require.NoError(t, err)
	assert.Empty(t, ns, "a dispatch naming the full finalizer set is deployment-scoped")

	_, err = finalizerDispatchScope(plan, []string{"ks_a"}, []string{"group_finalizer"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plan plan-scope-finalize finalizes [ks_a ks_b]")
}

// The re-plan that guards a materialized finalizer must still ask to finalize
// exactly what the reviewed plan did. A finalize-only dispatch is not a VSchema
// change, so it is compared as a finalize and not as a VSchema change.
func TestFinalizeParityFailsClosedOnEitherSide(t *testing.T) {
	c := planNamespacesTestClient()
	dispatched := []*ternv1.TableChange{{
		Namespace:  "payments",
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
		Metadata:   map[string]string{engine.MetadataNeedsFinalizer: "true"},
	}}
	replanned := &engine.PlanResult{Changes: []engine.SchemaChange{{
		Namespace: "payments",
		Metadata:  map[string]string{engine.MetadataNeedsFinalizer: "true"},
	}}}

	assert.Empty(t, vschemaNamespacesFromApplyRequest(c, dispatched))
	require.NoError(t, compareVSchemaParity(vschemaNamespacesFromPlanResult(c, replanned), vschemaNamespacesFromApplyRequest(c, dispatched)))
	require.NoError(t, compareFinalizeParity(finalizeNamespacesFromPlanResult(c, replanned), finalizeNamespacesFromApplyRequest(c, dispatched)))

	err := compareFinalizeParity(finalizeNamespacesFromPlanResult(c, &engine.PlanResult{}), finalizeNamespacesFromApplyRequest(c, dispatched))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reviewed keyspace finalizations this deployment would not plan: [payments]")

	err = compareFinalizeParity(finalizeNamespacesFromPlanResult(c, replanned), finalizeNamespacesFromApplyRequest(c, nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "keyspace finalizations this deployment would plan that were not reviewed: [payments]")
}

// Two deployments of one database agree on their DDL, but only one of them
// asks to finalize the keyspace. That is drift: a deployment that mirrors the
// other's plan would skip its own finalize. A change set whose only work is a
// finalize has work.
func TestCompareChangeSetsReportsFinalizeDrift(t *testing.T) {
	finalizing := ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace: "payments",
		Metadata:  map[string]string{apitypes.NeedsFinalizerMetadataKey: "true"},
	}}}
	quiet := ChangeSet{Changes: []*ternv1.SchemaChange{{Namespace: "payments"}}}

	diff, err := CompareChangeSets(schema.DialectMySQL, quiet, finalizing)
	require.NoError(t, err)
	assert.False(t, diff.Empty())
	assert.Equal(t, []string{"payments"}, diff.UnexpectedFinalize)
	assert.Empty(t, diff.MissingFinalize)

	diff, err = CompareChangeSets(schema.DialectMySQL, finalizing, quiet)
	require.NoError(t, err)
	assert.Equal(t, []string{"payments"}, diff.MissingFinalize)
	assert.Empty(t, diff.UnexpectedFinalize)

	diff, err = CompareChangeSets(schema.DialectMySQL, finalizing, finalizing)
	require.NoError(t, err)
	assert.True(t, diff.Empty())

	assert.True(t, finalizing.HasWork())
	assert.False(t, quiet.HasWork())
}

// A plan with no DDL and no VSchema change, whose engine still asks to finalize
// a namespace, is stored and returned with the request rather than skipped as
// empty: skipping it would report a clean plan whose apply never finalizes.
func TestLocalPlanStoresFinalizeOnlyPlan(t *testing.T) {
	plans := &fakePlanStore{createID: 41}
	c := newPlanMaterializeClientWithPlan(plans, &engine.PlanResult{
		PlanID: "plan-finalize-only",
		Changes: []engine.SchemaChange{{
			Namespace: "testapp",
			Metadata:  map[string]string{engine.MetadataNeedsFinalizer: "true"},
		}},
	})

	resp, err := c.Plan(t.Context(), &ternv1.PlanRequest{Database: "testapp"})
	require.NoError(t, err)

	require.NotNil(t, plans.created, "a finalize-only plan is stored")
	assert.Equal(t, []string{"testapp"}, plans.created.FinalizerNamespaces())
	require.Len(t, resp.Changes, 1)
	assert.Equal(t, "true", resp.Changes[0].Metadata[engine.MetadataNeedsFinalizer])
}
