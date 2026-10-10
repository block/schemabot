package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

const shardedTestDDL = "ALTER TABLE `orders` ADD INDEX `idx_created_at`(`created_at`)"

// shardedProgress serves the progress of a running apply made of the given
// operation rows and tasks, with the given stored plan and generation
// manifest, and decodes it.
func shardedProgress(t *testing.T, ops []*storage.ApplyOperation, tasks []*storage.Task, plans *staticPlanStore, manifest ...string) apitypes.ProgressResponse {
	t.Helper()
	apply := activeTestApply("apply-sharded")
	apply.PlanID = 7
	apply.ExternalID = "remote-apply"
	apply.ExpectedOperationKeys = manifest
	for _, op := range ops {
		op.ApplyID = apply.ID
		op.Deployment = "data-plane"
		if _, ok := state.NamespaceFinalizerKey(op.OperationKey); ok {
			op.OperationKind = storage.ApplyOperationKindGroupFinalizer
		} else {
			op.OperationKind = storage.ApplyOperationKindWork
		}
	}
	for _, task := range tasks {
		task.ApplyID = apply.ID
	}
	svc := New(&mockStorageWithApplyStores{
		plans:      plans,
		applies:    &staticApplyStore{apply: apply},
		tasks:      &capturingTaskStore{tasks: tasks},
		controls:   &memoryControlRequestStore{},
		operations: &staticApplyOperationStore{operations: ops},
	}, testServerConfig(), map[string]tern.Client{"default/staging": &mockTernClient{isRemote: true}},
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/progress/apply/apply-sharded", nil))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var resp apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	return resp
}

func shardOp(id int64, key, opState string) *storage.ApplyOperation {
	return &storage.ApplyOperation{ID: id, OperationKey: key, State: opState}
}

func shardTask(opID int64, taskState string, copied, total int64) *storage.Task {
	return &storage.Task{
		ApplyOperationID: &opID, TaskIdentifier: fmt.Sprintf("task-%d", opID),
		Namespace: "shop_001", TableName: "orders", DDLAction: "alter", DDL: shardedTestDDL,
		State: taskState, RowsCopied: copied, RowsTotal: total,
	}
}

// A table copying across a keyspace's shards reads as one table with each
// shard listed under it, as the PR comment shows it: a shard whose wave has
// not started, and so has no task yet, lists as queued, the table's rows
// cover only the shard that reported, and its planned size covers every shard. The finalizer's VSchema change joins the
// VSchema display metadata with the diff the stored plan carries, and every
// operation row is still listed.
func TestProgressByApplyIDRollsAShardedApplyUpByTable(t *testing.T) {
	ops := []*storage.ApplyOperation{
		shardOp(1, "shop_001/-80/orders", state.ApplyOperation.Running),
		shardOp(2, "shop_001/80-/orders", state.ApplyOperation.Pending),
		shardOp(3, "shop_001/group_finalizer", state.ApplyOperation.Pending),
	}
	plan := &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{"shop_001": {
		Tables:    []storage.TableChange{{Table: "orders", EstimatedBytes: new(int64(23_400_000_000)), ShardCount: 2}},
		Finalize:  true,
		Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables":{"orders":{}}}`},
		Metadata:  map[string]string{storage.PlanMetadataVSchemaDiff: "+ orders"},
	}}}

	resp := shardedProgress(t, ops, []*storage.Task{shardTask(1, state.Task.Running, 620, 1000)}, &staticPlanStore{plan: plan})

	assert.True(t, resp.Sharded)
	assert.Len(t, resp.Operations, 3, "every shard and finalizer row is still listed")
	require.Len(t, resp.Tables, 1, "the table reads once across its shards")
	table := resp.Tables[0]
	assert.Equal(t, "orders", table.TableName)
	assert.Equal(t, "shop_001", table.Keyspace)
	assert.Equal(t, shardedTestDDL, table.DDL)
	assert.Equal(t, state.Task.Running, table.Status)
	assert.Equal(t, int64(620), table.RowsCopied)
	assert.Equal(t, int64(1000), table.RowsTotal)
	assert.Equal(t, int32(62), table.PercentComplete)
	require.NotNil(t, table.EstimatedBytes, "the table carries its planned size")
	assert.Equal(t, int64(23_400_000_000), *table.EstimatedBytes)
	assert.Equal(t, int32(2), table.PlannedShards)
	require.Len(t, table.Shards, 2)
	assert.Equal(t, apitypes.ShardProgressResponse{Shard: "-80", Status: state.Task.Running, RowsCopied: 620, RowsTotal: 1000}, *table.Shards[0])
	assert.Equal(t, apitypes.ShardProgressResponse{Shard: "80-", Status: state.ApplyOperation.Pending}, *table.Shards[1])

	changes, err := apitypes.ParseVSchemaChanges(resp.Metadata)
	require.NoError(t, err)
	assert.Equal(t, []apitypes.VSchemaChange{{Namespace: "shop_001", Status: "", Diff: "+ orders"}}, changes)
}

// While a sharded apply's operations attach one dispatch at a time, it reads
// as every operation its manifest declares: with only its first shard
// attached, the apply is still served as the rollout, the declared shard still
// to attach lists as pending under the table and among the operations, and the
// declared finalizer's VSchema change shows.
func TestProgressByApplyIDReadsAShardedApplyAsItsDeclaredOperations(t *testing.T) {
	ops := []*storage.ApplyOperation{shardOp(1, "shop_001/-80/orders", state.ApplyOperation.Running)}
	plan := &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{"shop_001": {
		Tables:   []storage.TableChange{{Table: "orders"}},
		Finalize: true,
		Metadata: map[string]string{storage.PlanMetadataVSchemaDiff: "+ orders"},
	}}}

	resp := shardedProgress(t, ops, []*storage.Task{shardTask(1, state.Task.Running, 620, 1000)}, &staticPlanStore{plan: plan},
		"shop_001/-80/orders", "shop_001/80-/orders", "shop_001/group_finalizer")

	assert.Equal(t, state.Apply.Running, resp.State, "the stored apply state, not one operation's remote view")
	assert.True(t, resp.Sharded)
	require.Len(t, resp.Operations, 3)
	assert.Equal(t, "shop_001/80-/orders", resp.Operations[1].OperationKey)
	assert.Equal(t, state.ApplyOperation.Pending, resp.Operations[1].State)
	assert.Equal(t, storage.ApplyOperationKindWork, resp.Operations[1].OperationKind)
	assert.Equal(t, "shop_001/group_finalizer", resp.Operations[2].OperationKey)
	assert.Equal(t, storage.ApplyOperationKindGroupFinalizer, resp.Operations[2].OperationKind)
	require.Len(t, resp.Tables, 1)
	require.Len(t, resp.Tables[0].Shards, 2)
	assert.Equal(t, apitypes.ShardProgressResponse{Shard: "-80", Status: state.Task.Running, RowsCopied: 620, RowsTotal: 1000}, *resp.Tables[0].Shards[0])
	assert.Equal(t, apitypes.ShardProgressResponse{Shard: "80-", Status: state.ApplyOperation.Pending}, *resp.Tables[0].Shards[1])

	changes, err := apitypes.ParseVSchemaChanges(resp.Metadata)
	require.NoError(t, err)
	assert.Equal(t, []apitypes.VSchemaChange{{Namespace: "shop_001", Status: "", Diff: "+ orders"}}, changes)
}

// A table on its keyspace's only shard keeps its task row as it is, with no
// shard to name, and a finalizer that only finalizes the keyspace adds no
// VSchema change.
func TestProgressByApplyIDKeepsAnOnlyShardTablesRow(t *testing.T) {
	ops := []*storage.ApplyOperation{
		shardOp(1, "shop_001/-/orders", state.ApplyOperation.Completed),
		shardOp(2, "shop_001/group_finalizer", state.ApplyOperation.Completed),
	}
	plan := &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{"shop_001": {Finalize: true}}}

	resp := shardedProgress(t, ops, []*storage.Task{shardTask(1, state.Task.Completed, 80, 80)}, &staticPlanStore{plan: plan})

	assert.True(t, resp.Sharded)
	require.Len(t, resp.Tables, 1)
	assert.Equal(t, "task-1", resp.Tables[0].TaskID, "the task row keeps its identity")
	assert.Empty(t, resp.Tables[0].Shards)
	assert.NotContains(t, resp.Metadata, apitypes.VSchemaChangesMetadataKey)
}

// A finalizer shows a VSchema change only with the diff the stored plan
// carries for its keyspace, so a plan that cannot be read, or carries no diff,
// shows none.
func TestProgressByApplyIDShowsAFinalizerOnlyWithItsVSchemaDiff(t *testing.T) {
	for name, plans := range map[string]*staticPlanStore{
		"a plan read error":           {err: errors.New("storage unavailable")},
		"a missing stored plan":       {},
		"a plan without the keyspace": {plan: &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{}}},
		"a plan without a diff":       {plan: &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{"shop_001": {Finalize: true}}}},
	} {
		t.Run(name, func(t *testing.T) {
			ops := []*storage.ApplyOperation{
				shardOp(1, "shop_001/-80/orders", state.ApplyOperation.Completed),
				shardOp(2, "shop_001/80-/orders", state.ApplyOperation.Completed),
				shardOp(3, "shop_001/group_finalizer", state.ApplyOperation.Running),
			}

			resp := shardedProgress(t, ops, nil, plans)

			assert.True(t, resp.Sharded)
			assert.NotContains(t, resp.Metadata, apitypes.VSchemaChangesMetadataKey)
		})
	}
}

// An apply whose operations are not one change's shards keeps its table rows
// and is not marked sharded.
func TestProgressByApplyIDLeavesAnUnshardedApplysRows(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "us-east", State: state.ApplyOperation.Running},
		{ID: 2, Deployment: "us-west", State: state.ApplyOperation.Pending},
	}
	apply := activeTestApply("apply-sharded")
	opID := int64(1)
	tasks := []*storage.Task{{ApplyID: apply.ID, ApplyOperationID: &opID, TaskIdentifier: "task-1", TableName: "orders", State: state.Task.Running}}
	svc := New(&mockStorageWithApplyStores{
		plans:      &staticPlanStore{},
		applies:    &staticApplyStore{apply: apply},
		tasks:      &capturingTaskStore{tasks: tasks},
		controls:   &memoryControlRequestStore{},
		operations: &staticApplyOperationStore{operations: ops},
	}, testServerConfig(), map[string]tern.Client{"default/staging": &mockTernClient{isRemote: true}},
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/progress/apply/apply-sharded", nil))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var resp apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	assert.False(t, resp.Sharded)
	require.Len(t, resp.Tables, 1)
	assert.Equal(t, "task-1", resp.Tables[0].TaskID)
}
