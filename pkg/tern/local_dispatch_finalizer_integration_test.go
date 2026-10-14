//go:build integration

package tern

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A group_finalizer dispatch — no target shards, every change VSchema-typed —
// applies one namespace's VSchema after its sibling shard work completes. When
// the stored plan also carries table DDL (the sibling shard work), the data
// plane must create a task-less group_finalizer operation for the dispatched
// namespace, never fall back to the plan's DDL: that fallback would fabricate
// shard-less work tasks a sharded engine rejects, and the VSchema would never
// apply.
func TestLocalClient_VSchemaOnlyDispatchCreatesGroupFinalizer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	// A mixed plan: one namespace with shard work (table DDL) and a VSchema
	// change, mirroring a sharded column add that also hydrates the VSchema.
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-finalizer-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"ks_sharded": {
				Tables: []storage.TableChange{
					{Namespace: "ks_sharded", Table: "mutes", DDL: "ALTER TABLE `mutes` ADD COLUMN `note` varchar(32)", Operation: "alter"},
				},
				Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded": true}`},
				Metadata:  map[string]string{storage.PlanMetadataVSchemaChanged: "true"},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	resp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      plan.PlanIdentifier,
		Environment: localClientTestEnvironment,
		DdlChanges: []*ternv1.TableChange{{
			Namespace:  "ks_sharded",
			TableName:  "VSchema: ks_sharded",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
		}},
	})
	require.NoError(t, err)
	require.True(t, resp.Accepted, "VSchema-only dispatch was not accepted: %s", resp.ErrorMessage)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, resp.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)

	// The finalizer carries no task rows: the drive reconstructs the VSchema
	// change from the plan. The plan's table DDL must not leak into this apply.
	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	assert.Empty(t, tasks, "a group_finalizer dispatch must not resurrect the plan's table DDL as tasks")

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, storage.ApplyOperationKindGroupFinalizer, ops[0].OperationKind)
	assert.Equal(t, "ks_sharded/group_finalizer", ops[0].OperationKey)
	assert.Equal(t, state.ApplyOperation.Pending, ops[0].State)
}

// A VSchema-only dispatch against a plan that is itself VSchema-only (no table
// DDL anywhere — the plan's entire change is one namespace's VSchema document)
// must create the same task-less group_finalizer operation as a mixed plan's
// finalizer dispatch. A work operation with no tasks has nothing to drive, so
// only the finalizer shape lets the drive apply the VSchema from the plan.
func TestLocalClient_VSchemaOnlyPlanDispatchCreatesGroupFinalizer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-vschema-only-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"ks_sharded": {
				Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded": true}`},
				Metadata:  map[string]string{storage.PlanMetadataVSchemaChanged: "true"},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	resp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      plan.PlanIdentifier,
		Environment: localClientTestEnvironment,
		DdlChanges: []*ternv1.TableChange{{
			Namespace:  "ks_sharded",
			TableName:  "VSchema: ks_sharded",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
		}},
	})
	require.NoError(t, err)
	require.True(t, resp.Accepted, "VSchema-only dispatch was not accepted: %s", resp.ErrorMessage)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, resp.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	assert.Empty(t, tasks, "a VSchema-only plan produces no task rows")

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, storage.ApplyOperationKindGroupFinalizer, ops[0].OperationKind)
	assert.Equal(t, "ks_sharded/group_finalizer", ops[0].OperationKey)
	assert.Equal(t, state.ApplyOperation.Pending, ops[0].State)
}

// A plan whose only work is a finalize its engine asked for carries no table
// DDL and no VSchema document. Its dispatch is VSchema-typed and marked
// needs_finalizer, and the data plane creates the same task-less
// group_finalizer a VSchema-only plan gets, without demanding a vschema.json
// the plan never had.
func TestLocalClient_FinalizeOnlyPlanDispatchCreatesGroupFinalizer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-finalize-only-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"ks_sharded": {Finalize: true},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	resp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      plan.PlanIdentifier,
		Environment: localClientTestEnvironment,
		DdlChanges: []*ternv1.TableChange{{
			Namespace:  "ks_sharded",
			TableName:  "VSchema: ks_sharded",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
			Metadata:   map[string]string{engine.MetadataNeedsFinalizer: "true"},
		}},
	})
	require.NoError(t, err)
	require.True(t, resp.Accepted, "finalize-only dispatch was not accepted: %s", resp.ErrorMessage)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, resp.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	assert.Empty(t, tasks, "a finalize-only plan produces no task rows")

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, storage.ApplyOperationKindGroupFinalizer, ops[0].OperationKind)
	assert.Equal(t, "ks_sharded/group_finalizer", ops[0].OperationKey)
	assert.Equal(t, state.ApplyOperation.Pending, ops[0].State)
}

// A control plane driving a VSchema-only apply keys its single finalizer
// operation deployment-scoped and declares that key in the dispatch's
// generation manifest. The data plane must create its operation under the same
// key — a single-namespace dispatch read as namespace-scoped would carry a key
// outside the manifest and be refused at creation, failing an apply whose
// shape both planes agree on.
func TestLocalClient_VSchemaOnlyKeyedDispatchAdoptsManifestFinalizerScope(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-vschema-manifest-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"ks_sharded": {
				Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded": true}`},
				Metadata:  map[string]string{storage.PlanMetadataVSchemaChanged: "true"},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	resp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      plan.PlanIdentifier,
		Environment: localClientTestEnvironment,
		DdlChanges: []*ternv1.TableChange{{
			Namespace:  "ks_sharded",
			TableName:  "VSchema: ks_sharded",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
		}},
		IdempotencyKey:          "schemabot:v1:vschema-manifest-test",
		GenerationOperationKeys: []string{"group_finalizer"},
	})
	require.NoError(t, err)
	require.True(t, resp.Accepted, "deployment-keyed VSchema-only dispatch was not accepted: %s", resp.ErrorMessage)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, resp.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Equal(t, []string{"group_finalizer"}, apply.ExpectedOperationKeys, "the dispatch's generation manifest must be stored on the keyed apply")

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, storage.ApplyOperationKindGroupFinalizer, ops[0].OperationKind)
	assert.Equal(t, "group_finalizer", ops[0].OperationKey, "the operation must carry the manifest's deployment-scoped key")
	assert.Equal(t, state.ApplyOperation.Pending, ops[0].State)
}

// A VSchema-only dispatch naming every VSchema-changed namespace of a
// VSchema-only plan creates one deployment-scoped group_finalizer operation
// whose drive applies all the namespaces' VSchemas in a single engine apply. A
// branch-based engine stands up one branch covering the whole deployment and
// validates every keyspace in it, so per-namespace operations would each
// validate keyspaces whose VSchema they never applied.
func TestLocalClient_VSchemaOnlyMultiNamespaceDispatchCreatesDeploymentScopedFinalizer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-vschema-multi-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"ks_sharded": {
				Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded": true}`},
				Metadata:  map[string]string{storage.PlanMetadataVSchemaChanged: "true"},
			},
			"ks_unsharded": {
				Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables": {}}`},
				Metadata:  map[string]string{storage.PlanMetadataVSchemaChanged: "true"},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	resp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      plan.PlanIdentifier,
		Environment: localClientTestEnvironment,
		DdlChanges: []*ternv1.TableChange{
			{Namespace: "ks_sharded", TableName: "VSchema: ks_sharded", ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA},
			{Namespace: "ks_unsharded", TableName: "VSchema: ks_unsharded", ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Accepted, "multi-namespace VSchema-only dispatch was not accepted: %s", resp.ErrorMessage)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, resp.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	assert.Empty(t, tasks, "a VSchema-only plan produces no task rows")

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 1, "both namespaces' VSchemas are applied by one deployment-scoped finalizer")
	assert.Equal(t, storage.ApplyOperationKindGroupFinalizer, ops[0].OperationKind)
	assert.Equal(t, "group_finalizer", ops[0].OperationKey)
	assert.Equal(t, state.ApplyOperation.Pending, ops[0].State)
}

// A VSchema-only dispatch naming a namespace whose stored plan carries no
// VSchema artifact has nothing to apply; the dispatch is rejected before any
// apply or operation row is created rather than failing at drive time.
func TestLocalClient_VSchemaOnlyDispatchWithoutArtifactFailsClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-finalizer-noartifact-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"ks_sharded": {
				Tables: []storage.TableChange{
					{Namespace: "ks_sharded", Table: "mutes", DDL: "ALTER TABLE `mutes` ADD COLUMN `note` varchar(32)", Operation: "alter"},
				},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	_, err = client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      plan.PlanIdentifier,
		Environment: localClientTestEnvironment,
		DdlChanges: []*ternv1.TableChange{{
			Namespace:  "ks_sharded",
			TableName:  "VSchema: ks_sharded",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
		}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "neither a VSchema artifact nor a finalize request")
}

// Targets orders and orders-002 of one deployment share its remote apply, and
// each dispatches a deployment-scoped group_finalizer over namespaces orders
// and orders_lookup. Target orders shares a namespace's name, so its key
// "orders/group_finalizer" reads either as its own deployment-scoped finalizer
// or as namespace orders' finalizer, and its finalizer can drive before
// orders-002 attaches anything. The data plane stores each operation under its
// target with the target recorded on the row, the apply records that its keys
// lead with a target, and the finalizer drive resolves each key to the whole
// deployment rather than to namespace orders alone.
func TestLocalClient_MemberTargetFinalizerStoresTargetAndResolvesItsScope(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	storeTargetPlan := func(target string) string {
		plan := &storage.Plan{
			PlanIdentifier: fmt.Sprintf("plan-member-finalizer-%s-%d", target, time.Now().UnixNano()),
			Database:       "testdb",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Deployment:     "testdb",
			Target:         target,
			Environment:    localClientTestEnvironment,
			CreatedAt:      time.Now(),
			Namespaces: map[string]*storage.NamespacePlanData{
				"orders": {
					Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded": true}`},
					Metadata:  map[string]string{storage.PlanMetadataVSchemaChanged: "true"},
				},
				"orders_lookup": {
					Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded": false}`},
					Metadata:  map[string]string{storage.PlanMetadataVSchemaChanged: "true"},
				},
			},
		}
		_, err := stor.Plans().Create(ctx, plan)
		require.NoError(t, err)
		return plan.PlanIdentifier
	}
	const idempotencyKey = "schemabot:v1:member-target-finalizer"
	manifest := []string{"orders-002/group_finalizer", "orders/group_finalizer"}
	finalizerDispatch := func(planID, target string) *ternv1.ApplyRequest {
		return &ternv1.ApplyRequest{
			PlanId:                  planID,
			Environment:             localClientTestEnvironment,
			Database:                "testdb",
			Type:                    storage.DatabaseTypeMySQL,
			IdempotencyKey:          idempotencyKey,
			GenerationOperationKeys: manifest,
			DdlChanges: []*ternv1.TableChange{
				{Namespace: "orders", TableName: "VSchema: orders", ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA},
				{Namespace: "orders_lookup", TableName: "VSchema: orders_lookup", ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA},
			},
			Options: map[string]string{dispatchMemberTargetOption: target},
		}
	}
	firstPlanID := storeTargetPlan("orders")
	secondPlanID := storeTargetPlan("orders-002")

	first, err := client.Apply(ctx, finalizerDispatch(firstPlanID, "orders"))
	require.NoError(t, err)
	require.True(t, first.Accepted, "target orders' finalizer dispatch was not accepted: %s", first.ErrorMessage)
	assert.Equal(t, "orders/group_finalizer", first.OperationKey)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.True(t, apply.GetOptions().OperationKeysLeadWithTarget, "the apply must record that its keys lead with a target")

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, "orders", ops[0].Target, "the operation must record its member target")
	assert.Equal(t, storage.ApplyOperationKindGroupFinalizer, ops[0].OperationKind)
	namespace, err := resolveFinalizerNamespace(ctx, stor, apply, ops[0])
	require.NoError(t, err)
	assert.Empty(t, namespace, "before any sibling attaches, orders/group_finalizer is target orders' deployment-scoped finalizer, not namespace orders'")

	second, err := client.Apply(ctx, finalizerDispatch(secondPlanID, "orders-002"))
	require.NoError(t, err)
	require.True(t, second.Accepted, "target orders-002's finalizer must attach to the shared apply: %s", second.ErrorMessage)
	assert.Equal(t, first.ApplyId, second.ApplyId)
	assert.Equal(t, "orders-002/group_finalizer", second.OperationKey)

	ops, err = stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 2)
	for _, op := range ops {
		assert.Equal(t, storage.TargetOperationKey(op.Target, "group_finalizer"), op.OperationKey)
		namespace, err := resolveFinalizerNamespace(ctx, stor, apply, op)
		require.NoError(t, err)
		assert.Empty(t, namespace, "target %s's finalizer covers the whole deployment", op.Target)
	}
}
