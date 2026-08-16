//go:build integration

package tern

import (
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// setupAttachDispatchClient provisions the shared fixture for sibling-dispatch
// tests: a target database with a users table, storage, a LocalClient, and a
// stored plan whose change adds the email column.
func setupAttachDispatchClient(t *testing.T) (storage.Storage, *LocalClient, string) {
	t.Helper()

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTestTables(t, dsn)
	cleanupTasks(t, dsn)

	ctx := t.Context()

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(ctx, "CREATE TABLE users (id INT PRIMARY KEY)")
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })

	schemaFiles := buildSchemaWithAllTables(t, dsn, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})
	planResp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Type:     "mysql",
		Database: "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"testdb": {Files: schemaFiles},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, planResp.PlanId)

	return stor, client, planResp.PlanId
}

// shardDispatchRequest is the shard-scoped dispatch the control plane's
// per-shard fan-out sends: one shard's changes under the generation's
// idempotency key.
func shardDispatchRequest(planID, key, shard string) *ternv1.ApplyRequest {
	return &ternv1.ApplyRequest{
		PlanId:         planID,
		Environment:    localClientTestEnvironment,
		Database:       "testdb",
		Type:           "mysql",
		IdempotencyKey: key,
		TargetShards:   []string{shard},
		DdlChanges: []*ternv1.TableChange{{
			Namespace:  "testdb",
			TableName:  "users",
			Ddl:        "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		}},
	}
}

// Sibling shard dispatches sharing one idempotency key land on one apply: the
// first dispatch creates it, each later sibling attaches its own operation and
// shard-tagged tasks instead of bouncing off the active-apply guard, and a
// replay of an already-seen dispatch returns its existing operation without
// writing new rows. Every response echoes the operation key derived from the
// dispatch's shape so the caller can verify it received its own operation.
func TestLocalClient_Apply_SharedKeySiblingDispatchAttaches(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	const key = "schemabot:v1:sibling-dispatch-test"

	first, err := client.Apply(ctx, shardDispatchRequest(planID, key, "-80"))
	require.NoError(t, err)
	require.True(t, first.Accepted, "first shard dispatch must be accepted: %s", first.ErrorMessage)
	require.NotEmpty(t, first.ApplyId)
	require.NotEmpty(t, first.ApplyOperationId)
	assert.Equal(t, "testdb/-80/users", first.OperationKey, "the create response must echo the derived operation key")

	second, err := client.Apply(ctx, shardDispatchRequest(planID, key, "80-"))
	require.NoError(t, err)
	require.True(t, second.Accepted, "sibling shard dispatch must attach, not bounce: %s", second.ErrorMessage)
	assert.Equal(t, first.ApplyId, second.ApplyId, "siblings of one keyed generation share one apply")
	assert.NotEqual(t, first.ApplyOperationId, second.ApplyOperationId, "each sibling resolves to its own operation")
	assert.Equal(t, "testdb/80-/users", second.OperationKey, "the attach response must echo the derived operation key")

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 2, "the apply must carry one operation per sibling dispatch")
	keysByShard := map[string]string{}
	for _, op := range ops {
		assert.Equal(t, "testdb", op.Deployment)
		assert.Equal(t, storage.ApplyOperationKindWork, op.OperationKind)
		assert.Equal(t, state.ApplyOperation.Pending, op.State)
		keysByShard[op.OperationKey] = op.OperationKey
	}
	assert.Contains(t, keysByShard, "testdb/-80/users")
	assert.Contains(t, keysByShard, "testdb/80-/users")

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 2, "each sibling dispatch persists its own shard-tagged task")
	opKeyByID := map[int64]string{}
	for _, op := range ops {
		opKeyByID[op.ID] = op.OperationKey
	}
	for _, task := range tasks {
		require.NotNil(t, task.ApplyOperationID, "task %s must link to its dispatch's operation", task.TaskIdentifier)
		assert.Equal(t, "testdb/"+task.Shard+"/users", opKeyByID[*task.ApplyOperationID],
			"task %s (shard %s) must link to the operation keyed for its shard", task.TaskIdentifier, task.Shard)
	}

	// A replay of the first dispatch (same key, same shard) returns the existing
	// operation instead of attaching again.
	replay, err := client.Apply(ctx, shardDispatchRequest(planID, key, "-80"))
	require.NoError(t, err)
	require.True(t, replay.Accepted, "replay must be accepted: %s", replay.ErrorMessage)
	assert.Equal(t, first.ApplyId, replay.ApplyId)
	assert.Equal(t, first.ApplyOperationId, replay.ApplyOperationId, "replay must resolve to the original operation")
	assert.Equal(t, "testdb/-80/users", replay.OperationKey)

	ops, err = stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 2, "the replay must not attach a third operation")
	tasks, err = stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, tasks, 2, "the replay must not insert new tasks")
}

// A sibling dispatch arriving after its keyed apply terminalized fails closed:
// no drive will claim new work under a terminal apply, so the dispatch is
// rejected (visibly, for the control plane to surface) rather than attached as
// a permanently pending operation or silently aliased to the terminal apply.
func TestLocalClient_Apply_AttachToTerminalApplyFailsClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	const key = "schemabot:v1:terminal-attach-test"

	first, err := client.Apply(ctx, shardDispatchRequest(planID, key, "-80"))
	require.NoError(t, err)
	require.True(t, first.Accepted, "first shard dispatch must be accepted: %s", first.ErrorMessage)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	apply.State = state.Apply.Completed
	require.NoError(t, stor.Applies().Update(ctx, apply))

	second, err := client.Apply(ctx, shardDispatchRequest(planID, key, "80-"))
	require.NoError(t, err)
	assert.False(t, second.Accepted, "attach to a terminal apply must be refused")
	assert.Contains(t, second.ErrorMessage, "terminal", "the refusal must name the terminal state as the cause")

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 1, "the refused dispatch must not attach an operation")
}
