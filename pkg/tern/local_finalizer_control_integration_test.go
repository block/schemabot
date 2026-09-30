//go:build integration

package tern

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// finalizerControlEngine records every Apply a group_finalizer drive hands it,
// so a test can prove whether the drive published the namespace's VSchema.
type finalizerControlEngine struct {
	engine.Engine

	mu       sync.Mutex
	requests []*engine.ApplyRequest
}

func (e *finalizerControlEngine) Name() string { return "finalizer-control" }

func (e *finalizerControlEngine) Apply(_ context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.requests = append(e.requests, req)
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *finalizerControlEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return &engine.ProgressResult{State: engine.StateCompleted, Progress: 100}, nil
}

func (e *finalizerControlEngine) applies() []*engine.ApplyRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]*engine.ApplyRequest(nil), e.requests...)
}

// finalizerControlFixture is a two-namespace sharded rollout on one deployment:
// ns_0 and ns_1 each carry shard work and a VSchema change, so each has a work
// operation and a task-less group_finalizer. The ns_0 finalizer is leased to
// the drive under test, the way the operator's operation claim leaves it.
type finalizerControlFixture struct {
	stor        storage.Storage
	client      *LocalClient
	eng         *finalizerControlEngine
	apply       *storage.Apply
	finalizerID int64
	opCtx       context.Context
	leaseDB     *sql.DB
}

const finalizerControlDDL = "ALTER TABLE `orders` ADD COLUMN `region` VARCHAR(32)"

// newFinalizerControlFixture seeds the rollout with the ns_0 finalizer in
// finalizerState, and returns a context carrying that finalizer's operation
// lease alone.
func newFinalizerControlFixture(t *testing.T, finalizerState string) *finalizerControlFixture {
	t.Helper()
	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	t.Cleanup(func() { utils.CloseAndLog(stor) })

	eng := &finalizerControlEngine{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	client, err := NewLocalClient(LocalConfig{
		Database:  "orders",
		Type:      storage.DatabaseTypeStrata,
		TargetDSN: dsn,
		EngineFactories: map[string]EngineFactory{
			storage.DatabaseTypeStrata: func(LocalConfig, *slog.Logger) (engine.Engine, error) {
				return eng, nil
			},
		},
	}, stor, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })

	now := time.Now()
	namespace := func(ns string) *storage.NamespacePlanData {
		return &storage.NamespacePlanData{
			Tables: []storage.TableChange{{
				Namespace: ns, Table: "orders", DDL: finalizerControlDDL, Operation: "alter",
			}},
			Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded": true}`},
			Metadata:  map[string]string{storage.PlanMetadataVSchemaChanged: "true"},
		}
	}
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-finalizer-control-%d", now.UnixNano()),
		Database:       "orders",
		DatabaseType:   storage.DatabaseTypeStrata,
		Deployment:     "orders",
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
		Namespaces: map[string]*storage.NamespacePlanData{
			"ns_0": namespace("ns_0"),
			"ns_1": namespace("ns_1"),
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-finalizer-control-%d", now.UnixNano()),
		PlanID:          planID,
		Database:        "orders",
		DatabaseType:    storage.DatabaseTypeStrata,
		Deployment:      "orders",
		Environment:     localClientTestEnvironment,
		State:           state.Apply.Running,
		StartedAt:       &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	insert := func(key, kind, opState string) int64 {
		id, err := stor.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID:       applyID,
			Deployment:    "orders",
			Target:        "orders",
			OperationKey:  key,
			OperationKind: kind,
			State:         opState,
		})
		require.NoError(t, err, "seed apply_operation %s", key)
		return id
	}
	insert("ns_0", storage.ApplyOperationKindWork, state.ApplyOperation.Stopped)
	insert("ns_1", storage.ApplyOperationKindWork, state.ApplyOperation.Running)
	finalizerID := insert("ns_0/group_finalizer", storage.ApplyOperationKindGroupFinalizer, finalizerState)
	insert("ns_1/group_finalizer", storage.ApplyOperationKindGroupFinalizer, state.ApplyOperation.Stopped)

	leaseDB, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(leaseDB) })
	require.NoError(t, leaseDB.PingContext(ctx))
	_, err = leaseDB.ExecContext(ctx, `
		UPDATE apply_operations SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW() WHERE id = ?
	`, "finalizer-driver", "finalizer-token", finalizerID)
	require.NoError(t, err)

	opCtx := storage.WithOperationLease(ctx, storage.OperationLease{
		ApplyID: applyID, OperationID: finalizerID, Owner: "finalizer-driver", Token: "finalizer-token",
	})
	return &finalizerControlFixture{
		stor: stor, client: client, eng: eng, apply: apply,
		finalizerID: finalizerID, opCtx: opCtx, leaseDB: leaseDB,
	}
}

func (f *finalizerControlFixture) requestPending(t *testing.T, operation storage.ControlOperation) {
	t.Helper()
	_, alreadyPending, err := f.stor.ControlRequests().RequestPending(t.Context(), &storage.ApplyControlRequest{
		ApplyID:     f.apply.ID,
		Operation:   operation,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)
}

func (f *finalizerControlFixture) finalizerState(t *testing.T) string {
	t.Helper()
	op, err := f.stor.ApplyOperations().Get(t.Context(), f.finalizerID)
	require.NoError(t, err)
	require.NotNil(t, op)
	return op.State
}

// requireRequestPending asserts the operator's command is still pending: the
// finalizer owns only its own row, so completing the apply-level request is the
// rollout projection's job once every sibling has settled.
func (f *finalizerControlFixture) requireRequestPending(t *testing.T, operation storage.ControlOperation) {
	t.Helper()
	req, err := f.stor.ControlRequests().GetPending(t.Context(), f.apply.ID, operation)
	require.NoError(t, err)
	assert.NotNil(t, req, "the pending %s request is the projection's to complete", operation)
}

func (f *finalizerControlFixture) requireParentUntouched(t *testing.T) {
	t.Helper()
	parent, err := f.stor.Applies().Get(t.Context(), f.apply.ID)
	require.NoError(t, err)
	require.NotNil(t, parent)
	assert.Equal(t, state.Apply.Running, parent.State,
		"the parent is owned by the rollout projection, not by a finalizer drive")
}

// A sharded rollout is stopped with ns_0's shard work unfinished, which leaves
// ns_0's never-started finalizer stopped. The operator then cancels, and the
// cancel claim hands the stopped finalizer to a drive. The drive must discard
// the finalizer: publishing ns_0's VSchema would expose a VSchema for shard
// work that never landed, on a rollout the operator asked to throw away.
func TestLocalClient_StoppedFinalizerWithPendingCancelSettlesCancelledWithoutApplying(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Resuming)
	f.requestPending(t, storage.ControlOperationCancel)

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))

	assert.Empty(t, f.eng.applies(), "a cancelled finalizer must never hand its VSchema to the engine")
	assert.Equal(t, state.ApplyOperation.Cancelled, f.finalizerState(t))
	f.requireRequestPending(t, storage.ControlOperationCancel)
	f.requireParentUntouched(t)
}

// The cutover drive reaches the same finalizer drive, so it honors a pending
// cancel the same way.
func TestLocalClient_FinalizerCutoverDriveWithPendingCancelSettlesCancelledWithoutApplying(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Running)
	f.requestPending(t, storage.ControlOperationCancel)

	require.NoError(t, f.client.ResumeApplyOperationCutover(f.opCtx, f.apply, f.finalizerID))

	assert.Empty(t, f.eng.applies(), "a cancelled finalizer must never hand its VSchema to the engine")
	assert.Equal(t, state.ApplyOperation.Cancelled, f.finalizerState(t))
	f.requireRequestPending(t, storage.ControlOperationCancel)
	f.requireParentUntouched(t)
}

// A stop lands after a driver claimed ns_0's finalizer but before its drive
// began. The finalizer must not start: it settles stopped, like every other
// never-started operation of a stopped rollout, so a later start resumes it.
func TestLocalClient_FinalizerWithPendingStopSettlesStoppedWithoutApplying(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Running)
	f.requestPending(t, storage.ControlOperationStop)

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))

	assert.Empty(t, f.eng.applies(), "a finalizer must not start while a stop is pending")
	assert.Equal(t, state.ApplyOperation.Stopped, f.finalizerState(t))
	f.requireRequestPending(t, storage.ControlOperationStop)
	f.requireParentUntouched(t)
}

// With no command pending, the finalizer publishes its namespace's VSchema and
// completes.
func TestLocalClient_FinalizerWithoutPendingCommandAppliesVSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Running)

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))

	applies := f.eng.applies()
	require.Len(t, applies, 1)
	require.Len(t, applies[0].Changes, 1)
	assert.Equal(t, "ns_0", applies[0].Changes[0].Namespace)
	assert.Equal(t, "true", applies[0].Changes[0].Metadata[storage.PlanMetadataVSchemaChanged])
	assert.Equal(t, state.ApplyOperation.Completed, f.finalizerState(t))
}

// A finalizer whose VSchema deploy is already in flight on the engine has
// recorded engine resume state. A pending cancel cannot unpublish work the
// engine is carrying out, so the drive reattaches to it and records the
// outcome the engine reports rather than settling storage as cancelled over a
// deploy that is still landing.
func TestLocalClient_InFlightFinalizerWithPendingCancelReattachesToEngineWork(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Running)
	require.NoError(t, f.stor.ApplyOperations().SaveEngineResumeState(f.opCtx, f.finalizerID, &storage.EngineResumeState{
		ApplyOperationID: f.finalizerID,
		MigrationContext: "deploy-ns-0",
	}))
	f.requestPending(t, storage.ControlOperationCancel)

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))

	applies := f.eng.applies()
	require.Len(t, applies, 1, "the drive reattaches to the in-flight deploy")
	require.NotNil(t, applies[0].ResumeState)
	assert.Equal(t, "deploy-ns-0", applies[0].ResumeState.MigrationContext)
	assert.Equal(t, state.ApplyOperation.Completed, f.finalizerState(t))
	f.requireRequestPending(t, storage.ControlOperationCancel)
}
