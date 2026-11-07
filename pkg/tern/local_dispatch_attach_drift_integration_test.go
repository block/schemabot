//go:build integration

package tern

import (
	"context"
	"strconv"
	"sync/atomic"
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
	replanShardAfter(t, client, "80-", before, changes...)
}

// replanShardAfter makes the client's re-plan of its live schema return the
// given table changes on shard, running before first when it is set. It
// returns the number of re-plans the client has run since.
func replanShardAfter(t *testing.T, client *LocalClient, shard string, before func(context.Context), changes ...engine.TableChange) *atomic.Int32 {
	t.Helper()
	real := client.spiritEngine
	t.Cleanup(func() { client.spiritEngine = real })
	var replans atomic.Int32
	client.spiritEngine = fakePlanEngine{planFn: func(ctx context.Context, _ *engine.PlanRequest) (*engine.PlanResult, error) {
		replans.Add(1)
		if before != nil {
			before(ctx)
		}
		return &engine.PlanResult{Changes: []engine.SchemaChange{{
			Namespace:    "testdb",
			Shard:        engine.Shard{Name: shard},
			TableChanges: changes,
		}}}, nil
	}}
	return &replans
}

// siblingDispatchWithSchemaFiles is the 80- sibling dispatch carrying the
// reviewed schema files, as the control plane stamps them on every dispatch.
func siblingDispatchWithSchemaFiles(planID, key string) *ternv1.ApplyRequest {
	return shardDispatchWithSchemaFiles(planID, key, "80-")
}

// shardDispatchWithSchemaFiles is shardDispatchRequest carrying the reviewed
// schema files, as the control plane stamps them on every dispatch.
func shardDispatchWithSchemaFiles(planID, key, shard string) *ternv1.ApplyRequest {
	req := shardDispatchRequest(planID, key, shard)
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

// requireNoApplyForKey asserts that no apply was created under the
// idempotency key, so a refused dispatch left no apply, operation or task.
func requireNoApplyForKey(t *testing.T, stor storage.Storage, key string) {
	t.Helper()
	apply, err := stor.Applies().GetByIdempotencyKey(t.Context(), key)
	require.NoError(t, err)
	assert.Nil(t, apply, "a refused dispatch must not create an apply")
}

// The plan row is already stored on the deployment, so the -80 dispatch that
// creates the apply resolves it without materializing it. -80's live users
// table has drifted: re-planned there, the reviewed column comes out as
// varchar(100) rather than varchar(255). The creating dispatch is refused like
// a drifted sibling would be, with an error a redispatch cannot clear, before
// any apply exists, so no shard runs reviewed DDL its own schema was never
// checked against.
func TestLocalClient_Apply_CreatingShardDispatchFromStoredPlanDriftFailsClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	const key = "schemabot:v1:creating-drift-test"

	replans := replanShardAfter(t, client, "-80", nil, engine.TableChange{
		Table:     "users",
		Operation: ddl.StatementAlterTable,
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(100)",
	})
	_, err := client.Apply(t.Context(), shardDispatchWithSchemaFiles(planID, key, "-80"))
	require.Error(t, err, "a creating shard dispatch whose live schema drifted must be refused")
	assert.Contains(t, err.Error(), "drifted from the reviewed plan")
	assert.Contains(t, err.Error(), `shard "-80"`)
	assert.Contains(t, err.Error(), driftRecoveryHint)
	assert.False(t, engine.IsRetryable(err), "a redispatch carries the same reviewed DDL against the same drifted shard")
	assert.Equal(t, int32(1), replans.Load(), "the creating dispatch re-plans its shard once")
	requireNoApplyForKey(t, stor, key)
}

// Two same-key -80 dispatches both find no apply under the key. The winner
// creates the apply while the loser is between that lookup and its re-plan,
// so the loser's re-plan reads -80 as drifted. The loser must resolve the key
// again and replay the winner's apply and operation, as it would had it lost
// the race at the create itself, rather than refuse a dispatch the winner has
// already accepted.
func TestLocalClient_Apply_CreatingShardDispatchDriftAfterConcurrentCreateReplaysWinner(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	const key = "schemabot:v1:creating-drift-race-test"

	// The concurrent winner creates the apply under the key as the loser's
	// re-plan starts; it carries no schema files, so it runs from the plan
	// row without a re-plan of its own.
	var winner *ternv1.ApplyResponse
	replans := replanShardAfter(t, client, "-80", func(hookCtx context.Context) {
		resp, err := client.Apply(hookCtx, shardDispatchRequest(planID, key, "-80"))
		require.NoError(t, err)
		require.True(t, resp.Accepted, "the winning dispatch must be accepted: %s", resp.ErrorMessage)
		winner = resp
	}, engine.TableChange{
		Table:     "users",
		Operation: ddl.StatementAlterTable,
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(100)",
	})

	resp, err := client.Apply(ctx, shardDispatchWithSchemaFiles(planID, key, "-80"))
	require.NoError(t, err, "the losing dispatch must replay the winner, not fail on drift")
	require.NotNil(t, winner, "the concurrent create must have run before the re-plan")
	require.True(t, resp.Accepted, "the losing dispatch must be replayed, not refused: %s", resp.ErrorMessage)
	assert.Equal(t, winner.ApplyId, resp.ApplyId, "the loser must resolve to the winner's apply")
	assert.Equal(t, winner.ApplyOperationId, resp.ApplyOperationId, "the loser must resolve to the winner's operation row")
	assert.Equal(t, "testdb/-80/users", resp.OperationKey)
	assert.Equal(t, int32(1), replans.Load(), "the losing dispatch re-plans its shard once before resolving the key")

	apply, err := stor.Applies().GetByIdempotencyKey(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, apply)
	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 1, "the replayed dispatch must not insert a duplicate operation")
}

// A creating shard dispatch whose shard still needs exactly the reviewed
// change is admitted from the stored plan row, and its task runs -80's own
// rendering of the statement, as an attaching sibling's does.
func TestLocalClient_Apply_CreatingShardDispatchFromStoredPlanWithoutDriftIsAccepted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()
	const key = "schemabot:v1:creating-no-drift-test"

	replans := replanShardAfter(t, client, "-80", nil, engine.TableChange{
		Table:     "users",
		Operation: ddl.StatementAlterTable,
		DDL:       "ALTER TABLE users ADD COLUMN email varchar(255) NULL",
	})
	resp, err := client.Apply(ctx, shardDispatchWithSchemaFiles(planID, key, "-80"))
	require.NoError(t, err)
	require.True(t, resp.Accepted, "a creating shard dispatch that has not drifted must be accepted: %s", resp.ErrorMessage)
	assert.Equal(t, "testdb/-80/users", resp.OperationKey)
	assert.Equal(t, int32(1), replans.Load(), "the creating dispatch re-plans its shard once")

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, resp.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "-80", tasks[0].Shard)
	assert.Equal(t, "users", tasks[0].TableName)
	assert.Equal(t, "ALTER TABLE users ADD COLUMN email varchar(255) NULL", tasks[0].DDL, "the task must run -80's own rendering of the reviewed change")
}

// -80's re-plan lists a change the review never saw, but on orders, which the
// creating dispatch does not carry. Another table's changes on the same shard
// are its own operation's work, so they are no reason to refuse this one.
func TestLocalClient_Apply_CreatingShardDispatchIgnoresDriftOutsideItsTables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, client, planID := setupAttachDispatchClient(t)
	const key = "schemabot:v1:creating-other-table-drift-test"

	replanShardAfter(t, client, "-80", nil,
		engine.TableChange{
			Table:     "users",
			Operation: ddl.StatementAlterTable,
			DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255) NULL",
		},
		engine.TableChange{
			Table:     "orders",
			Operation: ddl.StatementAlterTable,
			DDL:       "ALTER TABLE `orders` ADD COLUMN `note` varchar(100)",
		},
	)
	resp, err := client.Apply(t.Context(), shardDispatchWithSchemaFiles(planID, key, "-80"))
	require.NoError(t, err)
	assert.True(t, resp.Accepted, "drift on a table the dispatch does not carry must not refuse it: %s", resp.ErrorMessage)
}

// A creating shard dispatch whose re-plan judges the reviewed change unsafe on
// its shard carries that verdict into admission: dispatched without
// allow_unsafe, it is refused and creates nothing.
func TestLocalClient_Apply_CreatingShardDispatchCarriesItsUnsafeVerdict(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	const key = "schemabot:v1:creating-unsafe-test"

	replanShardAfter(t, client, "-80", nil, engine.TableChange{
		Table:        "users",
		Operation:    ddl.StatementAlterTable,
		DDL:          "ALTER TABLE `users` ADD COLUMN `email` varchar(255) NULL",
		IsUnsafe:     true,
		UnsafeReason: "rewrites the table on this shard",
	})
	resp, err := client.Apply(t.Context(), shardDispatchWithSchemaFiles(planID, key, "-80"))
	require.NoError(t, err)
	assert.False(t, resp.Accepted, "an unsafe change on the creating shard must require the opt-in")
	assert.Contains(t, resp.ErrorMessage, "rewrites the table on this shard")
	requireNoApplyForKey(t, stor, key)
}

// A creating shard dispatch whose engine refuses the reviewed change on its
// shard is refused at admission even with allow_unsafe, before any apply
// exists.
func TestLocalClient_Apply_CreatingShardDispatchBlockedOnItsShardIsRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	const key = "schemabot:v1:creating-blocked-test"

	replanShardAfter(t, client, "-80", nil, engine.TableChange{
		Table:         "users",
		Operation:     ddl.StatementAlterTable,
		DDL:           "ALTER TABLE `users` ADD COLUMN `email` varchar(255) NULL",
		ExecutionMode: engine.ExecutionModeBlocked,
		ModeReason:    "requires privileges unavailable to the engine on this shard",
	})
	req := shardDispatchWithSchemaFiles(planID, key, "-80")
	req.Options = map[string]string{"allow_unsafe": "true"}
	resp, err := client.Apply(t.Context(), req)
	require.NoError(t, err)
	assert.False(t, resp.Accepted, "a change the creating shard's engine refuses must be refused at admission")
	assert.Contains(t, resp.ErrorMessage, `contains a blocked change for table "users": requires privileges unavailable to the engine on this shard`)
	requireNoApplyForKey(t, stor, key)
}

// A creating shard dispatch without the reviewed schema files has nothing to
// re-plan against. It is admitted against the stored plan row as it is, with
// no re-plan, as an attaching sibling without schema files is.
func TestLocalClient_Apply_CreatingShardDispatchWithoutSchemaFilesSkipsReplan(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, client, planID := setupAttachDispatchClient(t)
	const key = "schemabot:v1:creating-no-schema-files-test"

	replans := replanShardAfter(t, client, "-80", nil, engine.TableChange{
		Table:     "users",
		Operation: ddl.StatementAlterTable,
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(100)",
	})
	resp, err := client.Apply(t.Context(), shardDispatchRequest(planID, key, "-80"))
	require.NoError(t, err)
	assert.True(t, resp.Accepted, "a creating shard dispatch without schema files runs against the plan row: %s", resp.ErrorMessage)
	assert.Equal(t, int32(0), replans.Load(), "there is no reviewed schema to re-plan against")
}

// A whole-deployment dispatch runs the plan row as it was planned on this
// deployment, so it is not re-planned at creation even when a re-plan would
// now differ.
func TestLocalClient_Apply_WholeDeploymentDispatchFromStoredPlanSkipsReplan(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, client, planID := setupAttachDispatchClient(t)
	const key = "schemabot:v1:whole-deployment-test"

	replans := replanShardAfter(t, client, "", nil, engine.TableChange{
		Table:     "users",
		Operation: ddl.StatementAlterTable,
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(100)",
	})
	req := shardDispatchWithSchemaFiles(planID, key, "")
	req.TargetShards = nil
	resp, err := client.Apply(t.Context(), req)
	require.NoError(t, err)
	assert.True(t, resp.Accepted, "a whole-deployment dispatch from the stored plan must be accepted: %s", resp.ErrorMessage)
	assert.Equal(t, int32(0), replans.Load(), "a whole-deployment dispatch is not re-planned at creation")
}

// A creating shard dispatch for a plan this deployment never stored
// materializes the plan, which re-plans the shard against live schema. That
// re-plan is the only one: the stored-row check does not run again.
func TestLocalClient_Apply_CreatingShardDispatchMaterializingPlanReplansOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, client, _ := setupAttachDispatchClient(t)
	const key = "schemabot:v1:creating-materialize-test"

	replans := replanShardAfter(t, client, "-80", nil, engine.TableChange{
		Table:     "users",
		Operation: ddl.StatementAlterTable,
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255) NULL",
	})
	resp, err := client.Apply(t.Context(), shardDispatchWithSchemaFiles("plan_not_stored_here", key, "-80"))
	require.NoError(t, err)
	assert.True(t, resp.Accepted, "a materialized plan that matches live schema must be accepted: %s", resp.ErrorMessage)
	assert.Equal(t, int32(1), replans.Load(), "materializing re-plans once and the stored-row check does not run again")
}
