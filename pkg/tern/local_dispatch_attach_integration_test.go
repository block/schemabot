//go:build integration

package tern

import (
	"database/sql"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

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

	db, err := sql.Open("block-mysql", dsn)
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

// attachFixture dispatches the first shard of a keyed generation and returns
// the pieces a direct attachDispatchOperation call needs: the created apply,
// the stored plan, and the sibling dispatch's derived scope.
func attachFixture(t *testing.T, stor storage.Storage, client *LocalClient, planID, key string) (*storage.Apply, *storage.Plan, dispatchScope, *ternv1.ApplyRequest) {
	t.Helper()
	ctx := t.Context()

	first, err := client.Apply(ctx, shardDispatchRequest(planID, key, "-80"))
	require.NoError(t, err)
	require.True(t, first.Accepted, "first shard dispatch must be accepted: %s", first.ErrorMessage)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)

	siblingReq := shardDispatchRequest(planID, key, "80-")
	plan, err := client.planForApplyRequest(ctx, siblingReq)
	require.NoError(t, err)
	require.NotNil(t, plan)
	scope, err := deriveDispatchScope(plan, siblingReq)
	require.NoError(t, err)

	return apply, plan, scope, siblingReq
}

// A dispatch that loses the unique-index race to a concurrent same-operation
// attach is replayed against the winner's row instead of surfacing an error:
// both racers receive the same apply and operation identifiers.
func TestLocalClient_Apply_AttachRaceReplaysWinner(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	apply, plan, scope, siblingReq := attachFixture(t, stor, client, planID, "schemabot:v1:attach-race-test")

	// The concurrent winner inserts the sibling operation between this
	// dispatch's operation lookup (a miss) and its attach.
	now := time.Now()
	winner := &storage.ApplyOperation{
		Deployment:   "testdb",
		OperationKey: "testdb/80-/users",
		Target:       plan.Target,
		State:        state.ApplyOperation.Pending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	require.NoError(t, stor.Applies().AttachOperationWithTasks(ctx, apply, winner, buildDispatchTasks(plan, scope, siblingReq.Environment, "spirit", []byte("{}"), now)))

	resp, err := client.attachDispatchOperation(ctx, siblingReq, apply, plan, scope, "testdb/80-/users", storage.ApplyOperationKindWork)
	require.NoError(t, err)
	require.True(t, resp.Accepted, "the losing racer must be replayed, not refused: %s", resp.ErrorMessage)
	assert.Equal(t, apply.ApplyIdentifier, resp.ApplyId)
	assert.Equal(t, strconv.FormatInt(winner.ID, 10), resp.ApplyOperationId, "the loser must resolve to the winner's operation row")
	assert.Equal(t, "testdb/80-/users", resp.OperationKey)

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 2, "the lost race must not insert a duplicate operation")
}

// An apply that terminalizes between the attach's in-memory state check and
// its storage write is still refused: the row-locked re-read inside the attach
// transaction catches the transition and the dispatch fails closed.
func TestLocalClient_Apply_AttachRefusesApplyTerminalizedUnderneath(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	apply, plan, scope, siblingReq := attachFixture(t, stor, client, planID, "schemabot:v1:attach-toctou-test")

	// Terminalize the stored row while the caller still holds a stale
	// non-terminal snapshot, as a concurrent driver would.
	current, err := stor.Applies().GetByApplyIdentifier(ctx, apply.ApplyIdentifier)
	require.NoError(t, err)
	require.NotNil(t, current)
	current.State = state.Apply.Completed
	require.NoError(t, stor.Applies().Update(ctx, current))
	require.False(t, state.IsTerminalApplyState(apply.State), "the caller's snapshot must still look active for the race to be exercised")

	resp, err := client.attachDispatchOperation(ctx, siblingReq, apply, plan, scope, "testdb/80-/users", storage.ApplyOperationKindWork)
	require.NoError(t, err)
	assert.False(t, resp.Accepted, "the attach must fail closed when the apply terminalized underneath it")
	assert.Contains(t, resp.ErrorMessage, "terminal", "the refusal must name the terminal state as the cause")

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 1, "the refused attach must not insert its operation")
}

// manifestDispatchRequest is a shard dispatch that declares its generation
// manifest, as the control plane's deployment-keyed fan-out sends it.
func manifestDispatchRequest(planID, key, shard string, manifest []string) *ternv1.ApplyRequest {
	req := shardDispatchRequest(planID, key, shard)
	req.GenerationOperationKeys = manifest
	return req
}

// A dispatch that declares its generation manifest has it stored on the keyed
// apply at creation, siblings the manifest names attach normally, and a
// dispatch for an operation outside the manifest is refused fail-closed: an
// undeclared operation would attach work the apply's completion gate never
// waits for, so it signals the two planes disagree about the generation.
func TestLocalClient_Apply_ManifestStoredAndGatesAttach(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	const key = "schemabot:v1:manifest-gate-test"
	manifest := []string{"testdb/-80/users", "testdb/80-/users"}

	first, err := client.Apply(ctx, manifestDispatchRequest(planID, key, "-80", manifest))
	require.NoError(t, err)
	require.True(t, first.Accepted, "first shard dispatch must be accepted: %s", first.ErrorMessage)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Equal(t, manifest, apply.ExpectedOperationKeys, "the dispatch's generation manifest must be stored on the keyed apply")

	second, err := client.Apply(ctx, manifestDispatchRequest(planID, key, "80-", manifest))
	require.NoError(t, err)
	require.True(t, second.Accepted, "a sibling the manifest names must attach: %s", second.ErrorMessage)
	assert.Equal(t, first.ApplyId, second.ApplyId)

	outside, err := client.Apply(ctx, manifestDispatchRequest(planID, key, "c0-", manifest))
	require.NoError(t, err)
	assert.False(t, outside.Accepted, "an operation outside the manifest must be refused")
	assert.Contains(t, outside.ErrorMessage, "generation manifest", "the refusal must name the manifest as the cause")

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 2, "the refused dispatch must not attach an operation")
}

// A dispatch whose declared manifest omits its own operation key is refused at
// creation: the manifest is the completion authority for the apply the
// dispatch would create, and an apply whose first operation is outside its own
// manifest could never complete.
func TestLocalClient_Apply_ManifestOmittingOwnKeyRefusedAtCreate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	const key = "schemabot:v1:manifest-own-key-test"

	resp, err := client.Apply(ctx, manifestDispatchRequest(planID, key, "-80", []string{"testdb/80-/users"}))
	require.NoError(t, err)
	assert.False(t, resp.Accepted, "a dispatch whose manifest omits its own key must be refused")
	assert.Contains(t, resp.ErrorMessage, "does not include its own operation key")

	existing, err := stor.Applies().GetByIdempotencyKey(ctx, key)
	require.NoError(t, err)
	assert.Nil(t, existing, "the refused dispatch must not create an apply")
}

// The keyed apply's own in-flight tasks are sibling work, not conflicts: a
// dispatch attaching more work for a shard that apply is already copying on
// must attach, while another apply's active task on the database still blocks.
func TestLocalClient_Apply_AttachNotBlockedByOwnSiblingTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	apply, plan, scope, siblingReq := attachFixture(t, stor, client, planID, "schemabot:v1:attach-self-conflict-test")

	// The first dispatch's shard task is still pending. Attach a second
	// operation for the same shard: without the sibling skip the conflict
	// gate would treat the apply's own task as active work and refuse.
	sameShardScope := scope
	sameShardScope.shard = "-80"
	resp, err := client.attachDispatchOperation(ctx, siblingReq, apply, plan, sameShardScope, "testdb/-80/orders", storage.ApplyOperationKindWork)
	require.NoError(t, err)
	require.True(t, resp.Accepted, "the apply's own in-flight sibling task must not block the attach: %s", resp.ErrorMessage)
	assert.Equal(t, "testdb/-80/orders", resp.OperationKey)

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 2, "the same-shard sibling must attach as its own operation")
}
