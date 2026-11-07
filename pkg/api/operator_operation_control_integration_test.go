//go:build integration

package api

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// operationControlDatabaseType is a database type with no built-in engine, so
// the data plane drives each shard through operationControlEngine.
const operationControlDatabaseType = "operation-control-test"

// operationControlEngine runs each shard as its own schema change that is
// still copying, accepts every stop and cancel, and counts the controls and
// reclaims it receives.
type operationControlEngine struct {
	engine.Engine

	mu       sync.Mutex
	stops    int
	cancels  int
	reclaims int
}

func (e *operationControlEngine) Name() string { return "operation-control-test" }

func (e *operationControlEngine) Plan(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
	return &engine.PlanResult{}, nil
}

func (e *operationControlEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *operationControlEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return &engine.ProgressResult{State: engine.StateRunning}, nil
}

func (e *operationControlEngine) Start(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *operationControlEngine) Stop(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stops++
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *operationControlEngine) Cancel(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cancels++
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *operationControlEngine) DrainContext(context.Context) error { return nil }

func (e *operationControlEngine) DrainTargetContext(context.Context, engine.DrainTarget) error {
	return nil
}

func (e *operationControlEngine) ReclaimCancelledResumeState(context.Context, *engine.ReclaimResumeStateRequest) (*engine.ReclaimResumeStateResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reclaims++
	return &engine.ReclaimResumeStateResult{}, nil
}

func (e *operationControlEngine) counts() (stops, cancels, reclaims int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stops, e.cancels, e.reclaims
}

// Two shards of one apply are copying under their own operations when the
// operator stops it. The operator claims each operation and drives it on the
// data plane under that operation's lease alone. Each drive records the stop
// on its own shard's task, and once both have, the operator's projection
// settles the apply stopped and completes the stop request. Until the second
// drive runs, the apply holds and the request stays pending for it.
func TestOperatorStopSettlesTwoShardApplyOnceEachDriveRecordsIt(t *testing.T) {
	requireOperatorSettlesTwoShardControl(t, storage.ControlOperationStop,
		state.ApplyOperation.Stopped, state.Task.Stopped, state.Apply.Stopped)
}

// The cancel twin of the stop case above. The shards stored no engine resume
// state, so the cancel has nothing to reclaim and asks the engine for no
// reclaim.
func TestOperatorCancelSettlesTwoShardApplyOnceEachDriveRecordsIt(t *testing.T) {
	requireOperatorSettlesTwoShardControl(t, storage.ControlOperationCancel,
		state.ApplyOperation.Cancelled, state.Task.Cancelled, state.Apply.Cancelled)
}

func requireOperatorSettlesTwoShardControl(t *testing.T, operation storage.ControlOperation, wantOperationState, wantTaskState, wantApplyState string) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := t.Context()
	sdb := newStorageDatabaseWithSchema(t)
	db := openStorageDB(t, sdb.DSN)
	stor := mysqlstore.New(db)

	eng := &operationControlEngine{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	client, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  "payments",
		Type:      operationControlDatabaseType,
		TargetDSN: sdb.DSN,
		EngineFactories: map[string]tern.EngineFactory{
			operationControlDatabaseType: func(tern.LocalConfig, *slog.Logger) (engine.Engine, error) { return eng, nil },
		},
	}, stor, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })

	shards := []string{"-80", "80-"}
	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "operation-control-" + string(operation),
		parentState:     state.Apply.Running,
		cutoverPolicy:   storage.CutoverPolicyRolling,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"region-a"},
		shards:          shards,
		databaseType:    operationControlDatabaseType,
		opState:         state.ApplyOperation.Running,
		taskState:       state.Task.Running,
	})
	if operation == storage.ControlOperationStop {
		requestPendingStop(t, ctx, stor, seed.applyID)
	} else {
		requestPendingCancel(t, ctx, stor, seed.applyID)
	}
	// Each operation's last drive ended with its lease, so a driver claims it
	// to carry the recorded command.
	stalenessBackdate(t, ctx, db, seed.applyID)
	svc := newMatrixService(t, stor, map[string]tern.Client{"region-a/staging": client})

	driveNextOperation(t, ctx, svc, 1)
	settled := 0
	for _, shard := range shards {
		if opState(t, ctx, stor, seed.opID(shard)) == wantOperationState {
			settled++
		}
	}
	assert.Equal(t, 1, settled, "the first drive records the %s on its own shard alone", operation)
	assert.Equal(t, state.Apply.Running, getApply(t, ctx, stor, seed.applyID).State,
		"the apply holds while a sibling shard has not recorded the %s", operation)
	assert.False(t, controlRequestCompleted(t, ctx, stor, seed.applyID, operation),
		"the %s request stays pending for the sibling shard's drive", operation)

	driveNextOperation(t, ctx, svc, 2)

	tasks, err := stor.Tasks().GetByApplyID(ctx, seed.applyID)
	require.NoError(t, err)
	require.Len(t, tasks, len(shards))
	for _, task := range tasks {
		assert.Equal(t, wantTaskState, task.State, "task %s records the %s", task.TaskIdentifier, operation)
	}
	for _, shard := range shards {
		assert.Equal(t, wantOperationState, opState(t, ctx, stor, seed.opID(shard)), "shard %s operation", shard)
	}
	apply := getApply(t, ctx, stor, seed.applyID)
	assert.Equal(t, wantApplyState, apply.State, "the projection settles the apply once every shard recorded the %s", operation)
	assert.Empty(t, apply.ErrorMessage)
	assert.True(t, controlRequestCompleted(t, ctx, stor, seed.applyID, operation),
		"the %s request completes once the apply settles", operation)

	stops, cancels, reclaims := eng.counts()
	if operation == storage.ControlOperationStop {
		assert.Equal(t, len(shards), stops, "each drive stops its own shard's engine work")
		assert.Zero(t, cancels)
	} else {
		assert.Equal(t, len(shards), cancels, "each drive cancels its own shard's engine work")
		assert.Zero(t, stops)
	}
	assert.Zero(t, reclaims, "no shard stored engine resume state, so the cancel reclaims nothing")
}
