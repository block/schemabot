//go:build integration

package tern

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// resumeNamedWorkEngine names the work it starts in the resume state it returns
// from Apply, and its control operations refuse a request that does not carry
// that state. Until it is stopped it reports the work running.
type resumeNamedWorkEngine struct {
	controlValidatingCaptureEngine
	resumeState *engine.ResumeState
	// echoRequestResumeState makes Apply return the resume state the request
	// carried instead of naming its own work.
	echoRequestResumeState bool
	// completes makes Progress report the work completed.
	completes bool
	// onApply runs inside Apply, after the engine has accepted the work and
	// before Apply returns, so a test can land a control request mid-flight.
	onApply func()
}

func (e *resumeNamedWorkEngine) Apply(_ context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	if e.onApply != nil {
		e.onApply()
	}
	if e.echoRequestResumeState {
		return &engine.ApplyResult{Accepted: true, ResumeState: req.ResumeState}, nil
	}
	return &engine.ApplyResult{Accepted: true, ResumeState: e.resumeState}, nil
}

func (e *resumeNamedWorkEngine) Progress(_ context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.progressReq = req
	if e.stopReq != nil {
		return &engine.ProgressResult{State: engine.StateStopped}, nil
	}
	if e.completes {
		return &engine.ProgressResult{State: engine.StateCompleted}, nil
	}
	return &engine.ProgressResult{State: engine.StateRunning}, nil
}

// An operator stops a sequential drive while its engine is running the task,
// against an engine that can only reach running work through the resume state
// it returned when it accepted that work. The drive records that resume state
// on the task's apply operation, so the stop it consumes reaches the engine
// with it: the engine stops the work, the task and apply record stopped, and
// the stop request completes instead of failing on every claim.
func TestLocalClient_SequentialStopReachesEngineWithAcceptedResumeState(t *testing.T) {
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

	eng := &resumeNamedWorkEngine{resumeState: &engine.ResumeState{
		MigrationContext: "orders-alter",
		Metadata:         `{"operation_key":"shop/-80/orders"}`,
	}}
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeStrata,
		TargetDSN: dsn,
		EngineFactories: map[string]EngineFactory{
			storage.DatabaseTypeStrata: func(LocalConfig, *slog.Logger) (engine.Engine, error) {
				return eng, nil
			},
		},
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	apply, operation := seedRunningApplyWithTask(t, stor, storage.DatabaseTypeStrata, storage.EngineStrata)
	eng.onApply = func() {
		resp, err := client.Stop(ctx, &ternv1.StopRequest{
			ApplyId:     apply.ApplyIdentifier,
			Environment: localClientTestEnvironment,
		})
		require.NoError(t, err)
		require.True(t, resp.Accepted, "the stop is recorded while the engine runs the task")
	}

	expireApplyLease(t, dsn, apply.ID)
	claimed := claimApplyForDrive(t, stor, apply.ID)
	driveCtx := storage.WithApplyLease(ctx, claimed.Lease())
	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	action := client.runEngineTaskAttempt(driveCtx, claimed, tasks[0], nil)

	assert.Equal(t, taskStopped, action, "the drive stands down for the operator's stop")
	stored, err := stor.ApplyOperations().GetEngineResumeState(ctx, operation.ID)
	require.NoError(t, err, "the accepted task's resume state is recorded on its apply operation")
	assert.Equal(t, "orders-alter", stored.MigrationContext)
	assert.JSONEq(t, `{"operation_key":"shop/-80/orders"}`, stored.Metadata)

	require.NotNil(t, eng.stopReq, "the stop reaches the engine")
	require.NotNil(t, eng.stopReq.ResumeState, "the stop carries the resume state the engine returned")
	assert.Equal(t, "orders-alter", eng.stopReq.ResumeState.MigrationContext)
	assert.JSONEq(t, `{"operation_key":"shop/-80/orders"}`, eng.stopReq.ResumeState.Metadata)

	settledTasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, settledTasks, 1)
	assert.Equal(t, state.Task.Stopped, settledTasks[0].State, "the task records the stop")
	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Stopped, settled.State, "the apply records the stop")
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationStop, storage.ControlRequestCompleted)
}

// unrecordedResumeStateFixture drives one sequential task attempt against an
// engine that names its work in the resume state it returns, while every write
// of that resume state to the task's apply operation fails.
type unrecordedResumeStateFixture struct {
	client  *LocalClient
	eng     *resumeNamedWorkEngine
	stor    storage.Storage
	apply   *storage.Apply
	claimed *storage.Apply
	ctx     context.Context
}

func newUnrecordedResumeStateFixture(t *testing.T) *unrecordedResumeStateFixture {
	t.Helper()
	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	t.Cleanup(func() { utils.CloseAndLog(stor) })

	eng := &resumeNamedWorkEngine{resumeState: &engine.ResumeState{
		MigrationContext: "orders-alter",
		Metadata:         `{"operation_key":"shop/-80/orders"}`,
	}}
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeStrata,
		TargetDSN: dsn,
		EngineFactories: map[string]EngineFactory{
			storage.DatabaseTypeStrata: func(LocalConfig, *slog.Logger) (engine.Engine, error) {
				return eng, nil
			},
		},
	}, &failingFinalizerWritesStorage{Storage: stor, failSaveResumeState: true}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })

	apply, _ := seedRunningApplyWithTask(t, stor, storage.DatabaseTypeStrata, storage.EngineStrata)
	expireApplyLease(t, dsn, apply.ID)
	claimed := claimApplyForDrive(t, stor, apply.ID)
	return &unrecordedResumeStateFixture{
		client:  client,
		eng:     eng,
		stor:    stor,
		apply:   apply,
		claimed: claimed,
		ctx:     storage.WithApplyLease(t.Context(), claimed.Lease()),
	}
}

// attempt runs the task attempt and returns the action and the stored task.
func (f *unrecordedResumeStateFixture) attempt(t *testing.T) (taskAction, *storage.Task) {
	t.Helper()
	tasks, err := f.stor.Tasks().GetByApplyID(t.Context(), f.apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	tasks[0].State = state.Task.Pending
	require.NoError(t, f.stor.Tasks().Update(f.ctx, tasks[0]), "the task has not started yet")

	action := f.client.runEngineTaskAttempt(f.ctx, f.claimed, tasks[0], nil)

	stored, err := f.stor.Tasks().GetByApplyID(t.Context(), f.apply.ID)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	return action, stored[0]
}

// The engine accepts a pending task, but its resume state cannot be recorded,
// so no later stop or cancel could reach the work. The drive halts the work
// with the resume state the engine returned and records the task
// failed_retryable for a later drive, rather than leaving it pending while the
// work runs.
func TestLocalClient_SequentialUnrecordedResumeStateHaltsAcceptedWork(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newUnrecordedResumeStateFixture(t)

	action, task := f.attempt(t)

	assert.Equal(t, taskFailed, action, "the drive settles the attempt over the recorded verdict")
	require.NotNil(t, f.eng.stopReq, "the drive halts the accepted work")
	require.NotNil(t, f.eng.stopReq.ResumeState, "the halt carries the resume state the engine returned")
	assert.Equal(t, "orders-alter", f.eng.stopReq.ResumeState.MigrationContext)
	assert.JSONEq(t, `{"operation_key":"shop/-80/orders"}`, f.eng.stopReq.ResumeState.Metadata)
	assert.Equal(t, state.Task.FailedRetryable, task.State, "a later drive re-plans the task")
	assert.Contains(t, task.ErrorMessage, "record engine resume state")
	assert.Nil(t, f.eng.progressReq, "the drive never polls work it could not record")
}

// The engine accepts a pending task, its resume state cannot be recorded, and
// the engine does not halt the work either. The task is recorded
// failed_retryable, which stop treats as live engine work, so an operator's
// stop goes to the engine without the resume state, the engine refuses it, and
// the stop stays pending instead of recording the apply stopped while the work
// runs.
func TestLocalClient_SequentialStopCannotCompleteOverUnrecordedLiveWork(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newUnrecordedResumeStateFixture(t)
	f.eng.stopErr = errors.New("engine unreachable")

	action, task := f.attempt(t)

	assert.Equal(t, taskFailed, action)
	assert.Equal(t, state.Task.FailedRetryable, task.State)
	assert.True(t, hasLiveEngineWork(task.State), "stop and cancel treat the task's work as live")

	f.eng.stopErr = nil
	f.eng.stopReq = nil
	resp, err := f.client.Stop(t.Context(), &ternv1.StopRequest{
		ApplyId:     f.apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, resp.Accepted, "the stop is recorded for the drive")

	_, err = f.client.processPendingCancelOrStopControlRequest(f.ctx, f.claimed)

	require.Error(t, err, "the stop cannot reach the work without its resume state")
	assert.Contains(t, err.Error(), "no active schema change")
	assert.Nil(t, f.eng.stopReq, "the engine is never sent a stop that cannot name the work")
	requireControlRequestStatus(t, f.stor, f.apply.ID, storage.ControlOperationStop, storage.ControlRequestPending)
	settled, err := f.stor.Applies().Get(t.Context(), f.apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.NotEqual(t, state.Apply.Stopped, settled.State, "the apply is not recorded stopped while the work runs")
}

// The engine accepts a pending task and echoes back the resume state the drive
// sent, the way an engine that reaches its work through its own state does.
// That names nothing a control request could not reach without a record, so
// the drive records nothing: a resume state write that would fail never halts
// the work, and the task runs to completion.
func TestLocalClient_SequentialEchoedResumeStateIsNotRecorded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newUnrecordedResumeStateFixture(t)
	f.eng.echoRequestResumeState = true
	f.eng.completes = true

	action, task := f.attempt(t)

	assert.Equal(t, taskContinue, action, "the task runs to completion")
	assert.Equal(t, state.Task.Completed, task.State)
	assert.Nil(t, f.eng.stopReq, "work that needed no record is never halted")
	require.NotNil(t, f.eng.progressReq, "the drive polls the work")
	require.NotNil(t, f.eng.progressReq.ResumeState)
	assert.Equal(t, task.TaskIdentifier, f.eng.progressReq.ResumeState.MigrationContext, "the poll carries the resume state the engine echoed")
	operation, err := applyOperationIDForTask(task)
	require.NoError(t, err)
	_, err = f.stor.ApplyOperations().GetEngineResumeState(t.Context(), operation)
	assert.ErrorIs(t, err, storage.ErrEngineResumeStateNotFound, "nothing is recorded on the apply operation")
}
