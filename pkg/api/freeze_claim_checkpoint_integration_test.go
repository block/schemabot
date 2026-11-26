//go:build integration

package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// A driver that claims an operation that has not started checks the stop
// freezes first (the claim checkpoint, AZ-11). A stop freeze covering the
// apply holds the operation: the driver hands the claim back and records the
// freeze's stop, and stop reconciliation then stops the rollout in storage
// without starting any member. This catches a planned stop freeze that took
// effect after the apply was admitted. A freeze that lets running applies
// continue does not act at claim, and one that covers another database does
// not hold this apply.
func TestClaimCheckpointStopsAnUnstartedRolloutUnderAStopFreeze(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)

	setup := func(t *testing.T, identifier string, freeze *storage.EnvironmentFreeze) (seededMultiOpApply, *matrixService, *driveRecorder) {
		t.Helper()
		resetMatrixTables(t, ctx, db)
		resetFreezeTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: identifier,
			parentState:     state.Apply.Pending,
			cutoverPolicy:   storage.CutoverPolicyBarrier,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"eu", "us"},
			opState:         state.ApplyOperation.Pending,
			taskState:       state.Task.Pending,
		})
		_, err := stor.Freezes().Create(ctx, freeze)
		require.NoError(t, err)
		rec := &driveRecorder{}
		svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
			"eu": {taskState: state.Task.Completed},
			"us": {taskState: state.Task.Completed},
		}))
		return seed, svc, rec
	}

	t.Run("StopsInsteadOfStarting", func(t *testing.T) {
		seed, svc, rec := setup(t, "claim-stop-freeze", claimTestFreeze(t, ctx, stor, storage.FreezeRunningStop, nil))

		driveNextOperation(t, ctx, svc, 1)

		assert.Empty(t, rec.resumeOrder(), "no member starts under a stop freeze")
		stop, err := stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStop)
		require.NoError(t, err)
		require.NotNil(t, stop, "the claim recorded the freeze's stop")
		assert.Equal(t, storage.FreezeStopCaller("freeze-claim"), stop.RequestedBy)
		for _, deployment := range []string{"eu", "us"} {
			op, err := stor.ApplyOperations().Get(ctx, seed.opID(deployment))
			require.NoError(t, err)
			assert.Equal(t, state.ApplyOperation.Pending, op.State, "%s has not started", deployment)
			assert.Empty(t, op.LeaseToken, "%s: the claim was handed back", deployment)
		}

		svc.recoverApplies(ctx, 1, openClaimGate(), passPolled)

		assert.Empty(t, rec.resumeOrder(), "stop reconciliation stops the rollout without starting it")
		for _, deployment := range []string{"eu", "us"} {
			assert.Equal(t, state.ApplyOperation.Stopped, opState(t, ctx, stor, seed.opID(deployment)), deployment)
		}
		assert.True(t, stopRequestCompleted(t, ctx, stor, seed.applyID))
		assert.Equal(t, state.Apply.Stopped, getApply(t, ctx, stor, seed.applyID).State)
		assert.True(t, applyLogContains(t, stor, seed.applyID, "Stop requested by freeze freeze-claim"))
		assertProgressStoppedByFreeze(t, ctx, svc.Service, getApply(t, ctx, stor, seed.applyID))
	})

	for _, tc := range []struct {
		name   string
		freeze func() *storage.EnvironmentFreeze
	}{
		{"ContinueFreezeDoesNotHold", func() *storage.EnvironmentFreeze {
			return claimTestFreeze(t, ctx, stor, storage.FreezeRunningContinue, nil)
		}},
		{"StopFreezeOfAnotherDatabaseDoesNotHold", func() *storage.EnvironmentFreeze {
			return claimTestFreeze(t, ctx, stor, storage.FreezeRunningStop, []string{"orders"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seed, svc, rec := setup(t, "claim-"+tc.name, tc.freeze())

			driveNextOperation(t, ctx, svc, 1)

			assert.Len(t, rec.resumeOrder(), 1, "the claimed member starts")
			stop, err := stor.ControlRequests().GetPending(ctx, seed.applyID, storage.ControlOperationStop)
			require.NoError(t, err)
			assert.Nil(t, stop, "nothing stopped the apply")
		})
	}
}

// A stop freeze set while a start of a stopped rollout is queued holds the
// start at claim: the driver puts the operation back to stopped and the
// freeze's stop withdraws the queued start (CO-4), so nothing resumes. An
// approved override for the start that names the freeze lets it resume.
func TestClaimCheckpointHoldsAQueuedStartUnderAStopFreeze(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)

	setup := func(t *testing.T, identifier, admittedOn string) (seededMultiOpApply, *matrixService, *driveRecorder) {
		t.Helper()
		resetMatrixTables(t, ctx, db)
		resetFreezeTables(t, ctx, db)
		_, err := db.ExecContext(ctx, "DELETE FROM freeze_override_requests")
		require.NoError(t, err)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: identifier,
			parentState:     state.Apply.Stopped,
			cutoverPolicy:   storage.CutoverPolicyBarrier,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"eu", "us"},
			opState:         state.ApplyOperation.Stopped,
			taskState:       state.Task.Stopped,
		})
		metadata, err := json.Marshal(startControlRequestMetadata{FreezeOverride: admittedOn})
		require.NoError(t, err)
		_, _, err = stor.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID: seed.applyID, Operation: storage.ControlOperationStart, Status: storage.ControlRequestPending, RequestedBy: "cli:alice",
			Metadata: metadata,
		})
		require.NoError(t, err)
		_, err = stor.Freezes().Create(ctx, claimTestFreeze(t, ctx, stor, storage.FreezeRunningStop, nil))
		require.NoError(t, err)
		rec := &driveRecorder{}
		svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
			"eu": {taskState: state.Task.Completed},
			"us": {taskState: state.Task.Completed},
		}))
		return seed, svc, rec
	}

	t.Run("WithdrawsTheStart", func(t *testing.T) {
		seed, svc, rec := setup(t, "claim-queued-start", "")

		driveNextOperation(t, ctx, svc, 1)

		assert.Empty(t, rec.resumeOrder(), "nothing resumes under a stop freeze")
		start, err := stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStart)
		require.NoError(t, err)
		assert.Equal(t, storage.ControlRequestCompleted, start.Status, "the freeze's stop withdrew the queued start")
		stop, err := stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStop)
		require.NoError(t, err)
		require.NotNil(t, stop)
		assert.Equal(t, storage.FreezeStopCaller("freeze-claim"), stop.RequestedBy)
		assert.Equal(t, storage.ControlRequestCompleted, stop.Status)
		for _, deployment := range []string{"eu", "us"} {
			assert.Equal(t, state.ApplyOperation.Stopped, opState(t, ctx, stor, seed.opID(deployment)), deployment)
		}
		assert.Equal(t, state.Apply.Stopped, getApply(t, ctx, stor, seed.applyID).State)
		assertProgressStoppedByFreeze(t, ctx, svc.Service, getApply(t, ctx, stor, seed.applyID))
	})

	approveStartOverride := func(t *testing.T, target string) {
		t.Helper()
		now, err := stor.Freezes().Now(ctx)
		require.NoError(t, err)
		request, err := stor.FreezeOverrides().Create(ctx, &storage.FreezeOverrideRequest{
			Identifier: "ovr-claim", Environment: "staging", Action: storage.FreezeOverrideActionStart,
			TargetID: target, Database: "payments", Blockers: []string{"freeze freeze-claim"},
			Reason: "SEV-1234 fix forward", RequestedBy: "alice", ExpiresAt: now.Add(time.Hour),
		})
		require.NoError(t, err)
		_, err = stor.FreezeOverrides().Approve(ctx, request.Identifier, "bob")
		require.NoError(t, err)
	}

	t.Run("TheOverrideThatAdmittedTheStartLetsItResume", func(t *testing.T) {
		seed, svc, rec := setup(t, "claim-queued-start-override", "ovr-claim")
		approveStartOverride(t, "claim-queued-start-override")

		driveNextOperation(t, ctx, svc, 1)

		assert.Len(t, rec.resumeOrder(), 1, "the override admits the start")
		stop, err := stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStop)
		require.NoError(t, err)
		assert.Nil(t, stop, "nothing stopped the apply")
	})

	// An approval the queued start was not admitted on, such as another
	// requester's later one for the same apply, does not let it through.
	t.Run("AnotherApprovalDoesNotLetItResume", func(t *testing.T) {
		seed, svc, rec := setup(t, "claim-queued-start-other-approval", "")
		approveStartOverride(t, "claim-queued-start-other-approval")

		driveNextOperation(t, ctx, svc, 1)

		assert.Empty(t, rec.resumeOrder(), "nothing resumes under a stop freeze")
		stop, err := stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStop)
		require.NoError(t, err)
		require.NotNil(t, stop)
		assert.Equal(t, storage.FreezeStopCaller("freeze-claim"), stop.RequestedBy)
	})
}

// A cancel pending on a stopped rollout is delivered under a stop freeze: a
// freeze never holds a cancel (AZ-10, CO-2), so the claim that carries it is
// not returned and the freeze records no stop.
func TestClaimCheckpointDeliversAPendingCancelUnderAStopFreeze(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	resetMatrixTables(t, ctx, db)
	resetFreezeTables(t, ctx, db)
	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "claim-pending-cancel",
		parentState:     state.Apply.Stopped,
		cutoverPolicy:   storage.CutoverPolicyBarrier,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"eu", "us"},
		opState:         state.ApplyOperation.Stopped,
		taskState:       state.Task.Stopped,
	})
	_, _, err := stor.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID: seed.applyID, Operation: storage.ControlOperationCancel, Status: storage.ControlRequestPending, RequestedBy: "cli:alice",
	})
	require.NoError(t, err)
	_, err = stor.Freezes().Create(ctx, claimTestFreeze(t, ctx, stor, storage.FreezeRunningStop, nil))
	require.NoError(t, err)
	rec := &driveRecorder{}
	svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
		"eu": {taskState: state.Task.Completed},
		"us": {taskState: state.Task.Completed},
	}))

	driveNextOperation(t, ctx, svc, 1)

	stop, err := stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.Nil(t, stop, "the freeze recorded no stop over the cancel")
	op, err := stor.ApplyOperations().Get(ctx, seed.opID("eu"))
	require.NoError(t, err)
	assert.NotEqual(t, state.ApplyOperation.Stopped, op.State, "the claim delivered the cancel instead of handing the operation back")
}

// Unstarted work whose claim could not be handed back reads as running, and
// once its lease goes stale its recovery passes the claim checkpoint again: a
// stop freeze in effect has its stop recorded on the apply, which the drive
// delivers, so the work does not run on under the freeze unseen.
func TestClaimCheckpointStopsRecoveredWorkUnderAStopFreeze(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	resetMatrixTables(t, ctx, db)
	resetFreezeTables(t, ctx, db)
	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "claim-recovered",
		parentState:     state.Apply.Running,
		cutoverPolicy:   storage.CutoverPolicyBarrier,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"eu"},
		opState:         state.ApplyOperation.Running,
		taskState:       state.Task.Pending,
	})
	stalenessBackdate(t, ctx, db, seed.applyID)
	_, err := stor.Freezes().Create(ctx, claimTestFreeze(t, ctx, stor, storage.FreezeRunningStop, nil))
	require.NoError(t, err)
	rec := &driveRecorder{}
	svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
		"eu": {taskState: state.Task.Completed},
	}))

	driveNextOperation(t, ctx, svc, 1)

	stop, err := stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStop)
	require.NoError(t, err)
	require.NotNil(t, stop, "the recovery recorded the freeze's stop")
	assert.Equal(t, storage.FreezeStopCaller("freeze-claim"), stop.RequestedBy)
}

// claimTestFreeze is a freeze of staging in effect by the database clock.
func claimTestFreeze(t *testing.T, ctx context.Context, stor storage.Storage, running string, databases []string) *storage.EnvironmentFreeze {
	t.Helper()
	now, err := stor.Freezes().Now(ctx)
	require.NoError(t, err)
	return &storage.EnvironmentFreeze{
		Identifier: "freeze-claim", Environment: "staging", Databases: databases,
		Kind: storage.FreezeKindOnDemand, Running: running, StartsAt: now.Add(-time.Minute),
		Reason: "SEV-1234", CreatedBy: "alice", Source: storage.FreezeSourceManual,
	}
}

// assertProgressStoppedByFreeze checks that status names the freeze that
// stopped apply, with its reason.
func assertProgressStoppedByFreeze(t *testing.T, ctx context.Context, svc *Service, apply *storage.Apply) {
	t.Helper()
	progress, err := svc.progressFromLocalStorage(ctx, apply)
	require.NoError(t, err)
	assert.Equal(t, &apitypes.FreezeStopRef{ID: "freeze-claim", Reason: "SEV-1234"}, progress.StoppedByFreeze,
		"status says the freeze stopped the apply")
}

// resetFreezeTables clears the freezes a test set, so each subtest starts
// with none.
func resetFreezeTables(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	for _, table := range []string{"environment_freezes", "freeze_epochs"} {
		_, err := db.ExecContext(ctx, "DELETE FROM "+table)
		require.NoError(t, err, fmt.Sprintf("reset table %s", table))
	}
}
