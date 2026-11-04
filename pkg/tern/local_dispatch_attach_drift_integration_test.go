//go:build integration

package tern

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// replanShard80 makes the client's re-plan of its live schema return the given
// table changes on shard 80-, standing in for a sharded engine. The real engine
// is put back before the client is closed.
func replanShard80(t *testing.T, client *LocalClient, changes ...engine.TableChange) {
	t.Helper()
	replanShard80After(t, client, nil, changes...)
}

// replanShard80After is replanShard80 with a hook that runs as the re-plan
// starts reading live schema, for interleaving concurrent work before it.
func replanShard80After(t *testing.T, client *LocalClient, before func(context.Context), changes ...engine.TableChange) {
	t.Helper()
	real := client.spiritEngine
	t.Cleanup(func() { client.spiritEngine = real })
	client.spiritEngine = fakePlanEngine{planFn: func(ctx context.Context, _ *engine.PlanRequest) (*engine.PlanResult, error) {
		if before != nil {
			before(ctx)
		}
		return &engine.PlanResult{Changes: []engine.SchemaChange{{
			Namespace:    "testdb",
			Shard:        engine.Shard{Name: "80-"},
			TableChanges: changes,
		}}}, nil
	}}
}

// siblingDispatchWithSchemaFiles is the 80- sibling dispatch carrying the
// reviewed schema files, as the control plane stamps them on every dispatch.
func siblingDispatchWithSchemaFiles(planID, key string) *ternv1.ApplyRequest {
	req := shardDispatchRequest(planID, key, "80-")
	req.SchemaFiles = map[string]*ternv1.SchemaFiles{
		"testdb": {Files: map[string]string{"users.sql": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))"}},
	}
	return req
}

// The -80 dispatch creates the apply. The 80- sibling of the same generation
// then arrives, but 80-'s live users table has drifted: re-planned there, the
// reviewed column comes out as varchar(100) rather than varchar(255). The
// sibling must be refused before its operation or tasks exist, with an error
// a redispatch cannot clear, rather than running the reviewed DDL on a shard
// it was never checked against.
func TestLocalClient_Apply_SiblingShardDispatchDriftFailsClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	const key = "schemabot:v1:sibling-drift-test"

	first, err := client.Apply(ctx, shardDispatchRequest(planID, key, "-80"))
	require.NoError(t, err)
	require.True(t, first.Accepted, "first shard dispatch must be accepted: %s", first.ErrorMessage)

	replanShard80(t, client, engine.TableChange{
		Table:     "users",
		Operation: ddl.StatementAlterTable,
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(100)",
	})
	_, err = client.Apply(ctx, siblingDispatchWithSchemaFiles(planID, key))
	require.Error(t, err, "a sibling shard whose live schema drifted must be refused")
	assert.Contains(t, err.Error(), "drifted from the reviewed plan")
	assert.Contains(t, err.Error(), `shard "80-"`)
	assert.False(t, engine.IsRetryable(err), "a redispatch carries the same reviewed DDL against the same drifted shard")

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 1, "the refused sibling must not attach an operation")
	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, tasks, 1, "the refused sibling must not insert tasks")
}

// A sibling shard whose live schema still needs exactly the reviewed change
// attaches, and its task runs 80-'s own spelling of the statement. A change
// the re-plan lists for another table on the same shard is that table's own
// operation's work, so it is no reason to refuse this one.
func TestLocalClient_Apply_SiblingShardDispatchWithoutDriftAttaches(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	const key = "schemabot:v1:sibling-no-drift-test"

	first, err := client.Apply(ctx, shardDispatchRequest(planID, key, "-80"))
	require.NoError(t, err)
	require.True(t, first.Accepted, "first shard dispatch must be accepted: %s", first.ErrorMessage)

	replanShard80(t, client,
		engine.TableChange{
			Table:     "users",
			Operation: ddl.StatementAlterTable,
			DDL:       "ALTER TABLE users ADD COLUMN email varchar(255) NULL",
		},
		engine.TableChange{
			Table:     "orders",
			Operation: ddl.StatementAlterTable,
			DDL:       "ALTER TABLE `orders` ADD COLUMN `note` varchar(255)",
		},
	)
	second, err := client.Apply(ctx, siblingDispatchWithSchemaFiles(planID, key))
	require.NoError(t, err)
	require.True(t, second.Accepted, "a sibling shard that has not drifted must attach: %s", second.ErrorMessage)
	assert.Equal(t, first.ApplyId, second.ApplyId)
	assert.Equal(t, "testdb/80-/users", second.OperationKey)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	var sibling80 *storage.Task
	for _, task := range tasks {
		if task.Shard == "80-" {
			sibling80 = task
		}
	}
	require.NotNil(t, sibling80, "the attached sibling must persist a task for shard 80-")
	assert.Equal(t, "users", sibling80.TableName)
	assert.Equal(t, "ALTER TABLE users ADD COLUMN email varchar(255) NULL", sibling80.DDL, "the sibling's task must run 80-'s own rendering of the reviewed change")
}

// siblingOrdersDispatch is a shard-scoped dispatch for a table the plan row
// holds no change for, so nothing on this deployment has judged it yet. It
// carries the reviewed schema files unless withoutSchemaFiles is set.
func siblingOrdersDispatch(planID, key string, withoutSchemaFiles bool) *ternv1.ApplyRequest {
	req := shardDispatchRequest(planID, key, "80-")
	req.DdlChanges[0].TableName = "orders"
	req.DdlChanges[0].Ddl = "ALTER TABLE `orders` ADD COLUMN `note` varchar(255) NULL"
	if !withoutSchemaFiles {
		req.SchemaFiles = map[string]*ternv1.SchemaFiles{
			"testdb": {Files: map[string]string{"orders.sql": "CREATE TABLE orders (id INT PRIMARY KEY, note VARCHAR(255))"}},
		}
	}
	return req
}

// A sibling dispatch for a table the plan row never held is first marked as
// having no confirmed unsafe verdict. When the dispatch carries the reviewed
// schema files, 80-'s re-plan judges the change safe, and that verdict
// replaces the mark: the sibling attaches without allow_unsafe.
func TestLocalClient_Apply_SiblingShardDispatchReplanReplacesUnconfirmedMark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	const key = "schemabot:v1:sibling-replaced-mark-test"

	first, err := client.Apply(ctx, shardDispatchRequest(planID, key, "-80"))
	require.NoError(t, err)
	require.True(t, first.Accepted, "first shard dispatch must be accepted: %s", first.ErrorMessage)

	replanShard80(t, client, engine.TableChange{
		Table:     "orders",
		Operation: ddl.StatementAlterTable,
		DDL:       "ALTER TABLE `orders` ADD COLUMN `note` varchar(255) NULL",
	})
	second, err := client.Apply(ctx, siblingOrdersDispatch(planID, key, false))
	require.NoError(t, err)
	require.True(t, second.Accepted, "a change its own re-plan judged safe must not stay marked unconfirmed: %s", second.ErrorMessage)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 2, "the sibling attaches its own operation")
}

// The same sibling dispatch without the reviewed schema files has nothing to
// re-plan against, so its change stays marked as having no confirmed unsafe
// verdict and the unsafe-change gate refuses it without allow_unsafe.
func TestLocalClient_Apply_SiblingShardDispatchWithoutSchemaFilesFailsClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	const key = "schemabot:v1:sibling-unconfirmed-test"

	first, err := client.Apply(ctx, shardDispatchRequest(planID, key, "-80"))
	require.NoError(t, err)
	require.True(t, first.Accepted, "first shard dispatch must be accepted: %s", first.ErrorMessage)

	second, err := client.Apply(ctx, siblingOrdersDispatch(planID, key, true))
	require.NoError(t, err)
	assert.False(t, second.Accepted, "a change no re-plan judged must require the opt-in")
	assert.Contains(t, second.ErrorMessage, unconfirmedUnsafeReason)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 1, "the refused sibling must not attach an operation")
}

// A sibling shard whose re-plan judges the reviewed change unsafe there is
// held to the unsafe-change gate on that verdict: dispatched without
// allow_unsafe, it is refused and attaches nothing.
func TestLocalClient_Apply_SiblingShardDispatchCarriesItsUnsafeVerdict(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	const key = "schemabot:v1:sibling-unsafe-test"

	first, err := client.Apply(ctx, shardDispatchRequest(planID, key, "-80"))
	require.NoError(t, err)
	require.True(t, first.Accepted, "first shard dispatch must be accepted: %s", first.ErrorMessage)

	replanShard80(t, client, engine.TableChange{
		Table:        "users",
		Operation:    ddl.StatementAlterTable,
		DDL:          "ALTER TABLE `users` ADD COLUMN `email` varchar(255) NULL",
		IsUnsafe:     true,
		UnsafeReason: "rewrites the table on this shard",
	})
	second, err := client.Apply(ctx, siblingDispatchWithSchemaFiles(planID, key))
	require.NoError(t, err)
	assert.False(t, second.Accepted, "an unsafe change on the sibling shard must require the opt-in")
	assert.Contains(t, second.ErrorMessage, "rewrites the table on this shard")

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 1, "the refused sibling must not attach an operation")
}

// A sibling shard whose re-plan still needs exactly the reviewed change, but
// whose engine refuses it there (and does not judge it unsafe), is refused at
// admission like a blocked plan, even with allow_unsafe: no opt-in makes a
// refused statement executable, and no operation or task is left to fail at
// drive time.
func TestLocalClient_Apply_SiblingShardDispatchBlockedOnItsShardIsRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	const key = "schemabot:v1:sibling-blocked-test"

	first, err := client.Apply(ctx, shardDispatchRequest(planID, key, "-80"))
	require.NoError(t, err)
	require.True(t, first.Accepted, "first shard dispatch must be accepted: %s", first.ErrorMessage)

	replanShard80(t, client, engine.TableChange{
		Table:         "users",
		Operation:     ddl.StatementAlterTable,
		DDL:           "ALTER TABLE `users` ADD COLUMN `email` varchar(255) NULL",
		ExecutionMode: engine.ExecutionModeBlocked,
		ModeReason:    "requires privileges unavailable to the engine on this shard",
	})
	sibling := siblingDispatchWithSchemaFiles(planID, key)
	sibling.Options = map[string]string{"allow_unsafe": "true"}
	second, err := client.Apply(ctx, sibling)
	require.NoError(t, err)
	assert.False(t, second.Accepted, "a change the sibling shard's engine refuses must be refused at admission")
	assert.Contains(t, second.ErrorMessage, `contains a blocked change for table "users": requires privileges unavailable to the engine on this shard`)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 1, "the refused sibling must not attach an operation")
	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, tasks, 1, "the refused sibling must not insert tasks")
}

// Two dispatches of the same 80- sibling race. This one's operation lookup
// misses, then the other attaches the operation and its DDL lands before this
// one's re-plan reads live schema, so the re-plan no longer lists the change.
// This dispatch replays the winner's operation instead of failing with a
// drift error for work that succeeded.
func TestLocalClient_Apply_SiblingShardDriftAfterConcurrentAttachReplaysWinner(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	const key = "schemabot:v1:sibling-drift-race-test"
	apply, plan, scope, siblingReq := attachFixture(t, stor, client, planID, key)

	// The re-plan sees 80-'s users table with the column already added: the
	// concurrent winner attached the operation and ran its DDL first.
	var winner *storage.ApplyOperation
	replanShard80After(t, client, func(hookCtx context.Context) {
		now := time.Now()
		winner = &storage.ApplyOperation{
			Deployment:   "testdb",
			OperationKey: "testdb/80-/users",
			Target:       plan.Target,
			State:        state.ApplyOperation.Pending,
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		require.NoError(t, stor.Applies().AttachOperationWithTasks(hookCtx, apply, winner, buildDispatchTasks(plan, scope, siblingReq.Environment, "spirit", []byte("{}"), now)))
	})

	resp, err := client.Apply(ctx, siblingDispatchWithSchemaFiles(planID, key))
	require.NoError(t, err, "the losing dispatch must replay the winner, not fail on drift")
	require.NotNil(t, winner, "the concurrent attach must have run before the re-plan")
	require.True(t, resp.Accepted, "the losing dispatch must be replayed, not refused: %s", resp.ErrorMessage)
	assert.Equal(t, apply.ApplyIdentifier, resp.ApplyId)
	assert.Equal(t, strconv.FormatInt(winner.ID, 10), resp.ApplyOperationId, "the loser must resolve to the winner's operation row")
	assert.Equal(t, "testdb/80-/users", resp.OperationKey)

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 2, "the replayed dispatch must not insert a duplicate operation")
}
