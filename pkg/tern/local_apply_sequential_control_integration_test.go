//go:build integration

package tern

import (
	"context"
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
	// onApply runs inside Apply, after the engine has accepted the work and
	// before Apply returns, so a test can land a control request mid-flight.
	onApply func()
}

func (e *resumeNamedWorkEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	if e.onApply != nil {
		e.onApply()
	}
	return &engine.ApplyResult{Accepted: true, ResumeState: e.resumeState}, nil
}

func (e *resumeNamedWorkEngine) Progress(_ context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.progressReq = req
	if e.stopReq != nil {
		return &engine.ProgressResult{State: engine.StateStopped}, nil
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
