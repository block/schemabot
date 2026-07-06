//go:build integration

package tern

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
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

// invocationTrackingEngine records every engine call so a test can prove a
// drive settled entirely in storage without touching the engine — the invariant
// for cancelling or stopping a queued apply that has no tasks yet.
type invocationTrackingEngine struct {
	engine.Engine

	mu    sync.Mutex
	calls []string
}

func (e *invocationTrackingEngine) record(call string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, call)
}

func (e *invocationTrackingEngine) recorded() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.calls)
}

func (e *invocationTrackingEngine) Name() string { return "invocation-tracking" }

func (e *invocationTrackingEngine) Plan(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
	e.record("Plan")
	return &engine.PlanResult{}, nil
}

func (e *invocationTrackingEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.record("Apply")
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *invocationTrackingEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.record("Progress")
	return &engine.ProgressResult{State: engine.StateCompleted}, nil
}

func (e *invocationTrackingEngine) Stop(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	e.record("Stop")
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *invocationTrackingEngine) Cancel(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	e.record("Cancel")
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *invocationTrackingEngine) Start(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	e.record("Start")
	return &engine.ControlResult{Accepted: true}, nil
}

// newTasklessControlClient builds a MySQL-type LocalClient backed by the shared
// container with an invocation-tracking engine installed, for control tests on
// queued applies.
func newTasklessControlClient(t *testing.T, dsn string, stor storage.Storage) (*LocalClient, *invocationTrackingEngine) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })
	eng := &invocationTrackingEngine{}
	client.spiritEngine = eng
	return client, eng
}

// dispatchQueuedApply dispatches an apply through LocalClient.Apply from a plan
// with the given table changes (none for the task-less shape) and returns the
// stored apply row, still pending — queued for the operator, not yet driven.
func dispatchQueuedApply(t *testing.T, stor storage.Storage, client *LocalClient, tables []storage.TableChange) *storage.Apply {
	t.Helper()
	ctx := t.Context()
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-taskless-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {Tables: tables},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	resp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      plan.PlanIdentifier,
		Database:    "testdb",
		Type:        storage.DatabaseTypeMySQL,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, resp.Accepted)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, resp.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.Equal(t, state.Apply.Pending, apply.State, "a dispatched apply must be queued, not driven inline")
	return apply
}

// requireControlRequestStatus asserts the durable control request for the apply
// operation reached the given status — the signal that the operator retry loop
// has stopped re-running the request.
func requireControlRequestStatus(t *testing.T, stor storage.Storage, applyID int64, operation storage.ControlOperation, want storage.ControlRequestStatus) {
	t.Helper()
	req, err := stor.ControlRequests().GetByOperation(t.Context(), applyID, operation)
	require.NoError(t, err)
	require.NotNil(t, req, "the %s control request must exist", operation)
	assert.Equal(t, want, req.Status, "the %s control request must be %s", operation, want)
}

// A queued apply can be claimed by an operator driver before its first drive
// has created any tasks, and a cancel can land while it is still queued. The
// drive that claims it must settle the apply to cancelled directly — there is
// no task or engine work to cancel — complete the durable cancel request, and
// leave nothing claimable. Without the settle the cancel would error with "no
// active schema change", the request would stay pending, and the operator would
// re-claim and re-drive the apply forever.
func TestLocalClient_CancelQueuedTasklessApplySettlesCancelled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client, eng := newTasklessControlClient(t, dsn, stor)

	apply := dispatchQueuedApply(t, stor, client, nil)

	cancelResp, err := client.Cancel(ctx, &ternv1.CancelRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, cancelResp.Accepted)
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCancel, storage.ControlRequestPending)

	engineCallsBeforeDrive := eng.recorded()
	driveNextQueuedApply(t, stor, client)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Cancelled, settled.State, "a cancelled queued task-less apply must settle to cancelled")
	assert.NotNil(t, settled.CompletedAt, "a cancelled apply must carry its completion time")

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.NotEmpty(t, ops, "a dispatched apply must dual-write its operation row")
	for _, op := range ops {
		assert.Equal(t, state.ApplyOperation.Cancelled, op.State,
			"the operation row must settle to cancelled alongside its apply")
		assert.NotNil(t, op.CompletedAt, "a cancelled operation row must carry its completion time")
	}

	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCancel, storage.ControlRequestCompleted)
	assert.Equal(t, engineCallsBeforeDrive, eng.recorded(),
		"settling a task-less cancel must never invoke the engine")

	reclaimed, err := stor.Applies().FindNextApply(ctx, "test-reclaim-"+t.Name())
	require.NoError(t, err)
	assert.Nil(t, reclaimed, "a cancelled apply must not be claimable again")
}

// The stop counterpart: a stop that lands on a queued task-less apply (non-Vitess,
// where stop is supported) settles it to stopped — the resumable operator verb —
// completes the durable stop request, and leaves nothing claimable.
func TestLocalClient_StopQueuedTasklessApplySettlesStopped(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client, eng := newTasklessControlClient(t, dsn, stor)

	apply := dispatchQueuedApply(t, stor, client, nil)

	stopResp, err := client.Stop(ctx, &ternv1.StopRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, stopResp.Accepted)
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationStop, storage.ControlRequestPending)

	engineCallsBeforeDrive := eng.recorded()
	driveNextQueuedApply(t, stor, client)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Stopped, settled.State, "a stopped queued task-less apply must settle to stopped")

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.NotEmpty(t, ops, "a dispatched apply must dual-write its operation row")
	for _, op := range ops {
		assert.Equal(t, state.ApplyOperation.Stopped, op.State,
			"the operation row must settle to stopped alongside its apply")
		assert.Nil(t, op.CompletedAt, "a stopped operation row is resumable and must keep completed_at nil")
	}

	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationStop, storage.ControlRequestCompleted)
	assert.Equal(t, engineCallsBeforeDrive, eng.recorded(),
		"settling a task-less stop must never invoke the engine")

	reclaimed, err := stor.Applies().FindNextApply(ctx, "test-reclaim-"+t.Name())
	require.NoError(t, err)
	assert.Nil(t, reclaimed, "a stopped apply with no pending start must not be claimable again")
}

// Regression guard for the tasked shape: a cancel that lands on a queued apply
// WITH tasks still settles through the existing task path — the tasks are marked
// cancelled, the apply follows, and the durable cancel request completes.
func TestLocalClient_CancelQueuedApplyWithTasksSettlesViaTaskPath(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client, _ := newTasklessControlClient(t, dsn, stor)

	apply := dispatchQueuedApply(t, stor, client, []storage.TableChange{{
		Namespace: "testdb",
		Table:     "users",
		DDL:       "ALTER TABLE `users` ADD COLUMN cancel_note VARCHAR(255)",
		Operation: "alter",
	}})

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1, "the dispatched apply must carry its task")

	cancelResp, err := client.Cancel(ctx, &ternv1.CancelRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, cancelResp.Accepted)

	driveNextQueuedApply(t, stor, client)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Cancelled, settled.State)
	assert.NotNil(t, settled.CompletedAt)

	settledTasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, settledTasks, 1)
	assert.Equal(t, state.Task.Cancelled, settledTasks[0].State,
		"a cancelled queued apply's task must settle via the task path")

	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCancel, storage.ControlRequestCompleted)
}
