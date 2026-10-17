//go:build integration

package api

import (
	"log/slog"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// An operator starts a stopped rollout and stops it again before any driver
// has claimed the start. The stop wins (CO-4): it withdraws the queued start in
// the same write that records it, so no claim resumes the rollout, and it is
// answered on the spot rather than left pending on a stopped apply where no
// drive would consume it (CO-2). Once a claim has already moved an operation to
// resuming, the stop stays pending for the drive that holds it, which answers
// it if it stands down without resuming anything. A stopped
// rollout with no start queued has nothing to stop, and the stop is refused
// with a code the CLI reports as already stopped (CO-6).
func TestStopWithdrawsAQueuedStart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(stor, &ServerConfig{}, nil, logger)

	seedStoppedRollout := func(t *testing.T, identifier string, perOpState map[string]string) int64 {
		t.Helper()
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: identifier,
			parentState:     state.Apply.Stopped,
			cutoverPolicy:   storage.CutoverPolicyBarrier,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"eu", "us", "au"},
			opState:         state.ApplyOperation.Stopped,
			taskState:       state.Task.Stopped,
			perOpState:      perOpState,
		})
		return seed.applyID
	}
	requestStart := func(t *testing.T, applyID int64) {
		t.Helper()
		_, _, err := stor.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID: applyID, Operation: storage.ControlOperationStart, Status: storage.ControlRequestPending, RequestedBy: "cli:alice",
		})
		require.NoError(t, err)
	}
	requireStatus := func(t *testing.T, applyID int64, operation storage.ControlOperation, want storage.ControlRequestStatus) {
		t.Helper()
		row, err := stor.ControlRequests().GetByOperation(ctx, applyID, operation)
		require.NoError(t, err)
		require.NotNil(t, row, "%s request row", operation)
		assert.Equal(t, want, row.Status, "%s request status", operation)
	}
	stop := func(t *testing.T, apply *storage.Apply) (*apitypes.StopResponse, int, error) {
		t.Helper()
		return svc.executeStopForApply(ctx, nil, apply, apply.ApplyIdentifier, apply.Environment, "cli:bob")
	}

	t.Run("WithdrawsTheStartBeforeAnyClaim", func(t *testing.T) {
		applyID := seedStoppedRollout(t, "stop-withdraws-queued-start", nil)
		requestStart(t, applyID)

		resp, httpStatus, err := stop(t, getApply(t, ctx, stor, applyID))
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, httpStatus)
		assert.True(t, resp.Accepted)
		assert.Equal(t, apitypes.ControlStatusStartWithdrawn, resp.Status)

		requireStatus(t, applyID, storage.ControlOperationStart, storage.ControlRequestCompleted)
		requireStatus(t, applyID, storage.ControlOperationStop, storage.ControlRequestCompleted)
		assert.Equal(t, state.Apply.Stopped, getApply(t, ctx, stor, applyID).State, "the rollout stays stopped")
	})

	t.Run("LeavesTheStopToAClaimedOperation", func(t *testing.T) {
		applyID := seedStoppedRollout(t, "stop-after-start-claimed", map[string]string{"eu": state.ApplyOperation.Resuming})
		requestStart(t, applyID)

		resp, _, err := stop(t, getApply(t, ctx, stor, applyID))
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
		assert.Empty(t, resp.Status, "the stop is pending like any stop of a running rollout")

		requireStatus(t, applyID, storage.ControlOperationStart, storage.ControlRequestCompleted)
		requireStatus(t, applyID, storage.ControlOperationStop, storage.ControlRequestPending)
	})

	// A driver claims an operation for the queued start, and the stop withdraws
	// the start before that driver loads the parent. The drive finds nothing
	// left to deliver, stands its operation down, and answers the stop it was
	// left pending for, so the stop does not strand on the stopped rollout.
	t.Run("SettlesTheStopWhenTheClaimedDriveStandsDown", func(t *testing.T) {
		applyID := seedStoppedRollout(t, "stop-after-claim-stands-down", nil)
		requestStart(t, applyID)

		op, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "driver-under-test")
		require.NoError(t, err)
		require.NotNil(t, op, "the queued start leases a stopped operation")
		require.Equal(t, applyID, op.ApplyID)

		resp, _, err := stop(t, getApply(t, ctx, stor, applyID))
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
		assert.Empty(t, resp.Status, "the stop is left pending for the claimed drive")
		requireStatus(t, applyID, storage.ControlOperationStop, storage.ControlRequestPending)

		svc.recoverMultiApplyOperation(ctx, 1, op, op.Lease())

		requireStatus(t, applyID, storage.ControlOperationStart, storage.ControlRequestCompleted)
		requireStatus(t, applyID, storage.ControlOperationStop, storage.ControlRequestCompleted)
		assert.Equal(t, state.Apply.Stopped, getApply(t, ctx, stor, applyID).State, "the rollout stays stopped")
		ops, err := stor.ApplyOperations().ListByApply(ctx, applyID)
		require.NoError(t, err)
		for _, o := range ops {
			assert.Equal(t, state.ApplyOperation.Stopped, o.State, "operation %s rests stopped", o.Deployment)
		}
	})

	// A lone operation whose manifest still expects a sibling drives under the
	// operation lease, so its claim leaves the parent stopped. The stopped parent
	// is no proof the stop landed: the stop stays pending for the claimed drive,
	// with no immediate stop sent to the data plane to complete it early.
	t.Run("LeavesTheStopToTheDriveThatClaimedALoneOperation", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "stop-after-lone-operation-claimed",
			parentState:     state.Apply.Stopped,
			cutoverPolicy:   storage.CutoverPolicyBarrier,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"eu"},
			opState:         state.ApplyOperation.Resuming,
			taskState:       state.Task.Stopped,
		})
		applyID := seed.applyID
		_, err := db.ExecContext(ctx, "UPDATE applies SET expected_operation_keys = ? WHERE id = ?", `["eu","us"]`, applyID)
		require.NoError(t, err)
		requestStart(t, applyID)
		client := &mockTernClient{stopResp: &ternv1.StopResponse{Accepted: true}}
		apply := getApply(t, ctx, stor, applyID)

		resp, _, err := svc.executeStopForApply(ctx, client, apply, apply.ApplyIdentifier, apply.Environment, "cli:bob")
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
		assert.Empty(t, resp.Status, "the stop is pending like any stop of a running rollout")

		assert.Nil(t, client.stopReq, "no immediate stop is sent for a drive that already claimed the operation")
		requireStatus(t, applyID, storage.ControlOperationStart, storage.ControlRequestCompleted)
		requireStatus(t, applyID, storage.ControlOperationStop, storage.ControlRequestPending)
	})

	t.Run("StopsTheRolloutAClaimResumedSinceTheRead", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "stop-after-rollout-resumed",
			parentState:     state.Apply.Running,
			cutoverPolicy:   storage.CutoverPolicyBarrier,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"eu", "us", "au"},
			opState:         state.ApplyOperation.Stopped,
			taskState:       state.Task.Stopped,
			perOpState:      map[string]string{"eu": state.ApplyOperation.Running},
		})
		applyID := seed.applyID
		requestStart(t, applyID)
		// The stop was read while the rollout was still stopped; a claim has
		// resumed it since.
		read := getApply(t, ctx, stor, applyID)
		read.State = state.Apply.Stopped

		resp, _, err := stop(t, read)
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
		assert.Empty(t, resp.Status)
		requireStatus(t, applyID, storage.ControlOperationStop, storage.ControlRequestPending)
	})

	t.Run("RefusesWithNoStartQueued", func(t *testing.T) {
		applyID := seedStoppedRollout(t, "stop-already-stopped", nil)

		_, _, err := stop(t, getApply(t, ctx, stor, applyID))
		require.Error(t, err)
		assert.Equal(t, http.StatusConflict, controlOperationHTTPStatus(err))
		assert.Equal(t, apitypes.ErrCodeAlreadyStopped, controlOperationErrorCode(err))
		assert.ErrorContains(t, err, "schema change is already stopped")
		row, err := stor.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationStop)
		require.NoError(t, err)
		assert.Nil(t, row, "a refused stop records nothing")
	})
}
