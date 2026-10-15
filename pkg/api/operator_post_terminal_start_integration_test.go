//go:build integration

package api

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// unansweredStartClient completes the operation's tasks and stores the parent
// apply completed, then reports that it could not answer the pending start —
// the drive that finds no remaining work, records the outcome, and loses the
// storage write answering the operator's start.
type unansweredStartClient struct {
	*matrixTernClient
}

func (c *unansweredStartClient) ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	if err := c.matrixTernClient.ResumeApplyOperation(ctx, apply, applyOperationID); err != nil {
		return err
	}
	completed := *apply
	now := time.Now()
	completed.State = state.Apply.Completed
	completed.CompletedAt = &now
	if err := c.stor.Applies().Update(ctx, &completed); err != nil {
		return fmt.Errorf("unanswered start fake: store apply %s completed: %w", apply.ApplyIdentifier, err)
	}
	return fmt.Errorf("%w: complete pending start for apply %s: control request store unavailable",
		tern.ErrPostTerminalReconcile, apply.ApplyIdentifier)
}

// An operator started an apply whose resumed drive found nothing left to do.
// The drive stored the apply completed, then storage refused the write
// answering the operator's start. The completed outcome stands and nothing
// re-claims a completed apply, so the operator's post-drive settlement is the
// last chance to answer the start: the start reads as completed, and the
// operation row settles from its completed task instead of staying claimable.
func TestOperator_StartUnansweredAfterTheStoredOutcomeIsSettledByTheOperator(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)

	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "start-unanswered-after-outcome",
		parentState:     state.Apply.Running,
		cutoverPolicy:   storage.CutoverPolicyRolling,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"region-a"},
		opState:         state.ApplyOperation.FailedRetryable,
		taskState:       state.Task.FailedRetryable,
	})
	_, err := db.ExecContext(ctx,
		"UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", seed.applyID)
	require.NoError(t, err, "backdate the parent apply heartbeat")
	stalenessBackdate(t, ctx, db, seed.applyID)
	_, _, err = stor.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     seed.applyID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "matrix-test",
	})
	require.NoError(t, err, "request pending start")

	op, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "recovering-driver")
	require.NoError(t, err)
	require.NotNil(t, op, "the operation must be claimable for the scenario")

	svc := newMatrixService(t, stor, map[string]tern.Client{
		"region-a/staging": &unansweredStartClient{matrixTernClient: &matrixTernClient{
			mockTernClient: &mockTernClient{},
			stor:           stor,
			deployment:     "region-a",
			rec:            &driveRecorder{},
			outcome:        matrixOutcome{taskState: state.Task.Completed},
		}},
	})
	svc.recoverSingleApplyOperation(ctx, 1, "recovering-driver", op, op.Lease())

	start, err := stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStart)
	require.NoError(t, err)
	require.NotNil(t, start)
	assert.Equal(t, storage.ControlRequestCompleted, start.Status,
		"the operator answers the start the drive could not, against the stored completed outcome")

	persistedOp, err := stor.ApplyOperations().Get(ctx, op.ID)
	require.NoError(t, err)
	require.NotNil(t, persistedOp)
	assert.Equal(t, state.ApplyOperation.Completed, persistedOp.State,
		"the operation row settles from its completed task rather than staying claimable")

	apply, err := stor.Applies().Get(ctx, seed.applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Equal(t, state.Apply.Completed, apply.State, "the stored outcome stands")
}

// refusingSettlementStorage refuses the write that settles a pending control
// request for as long as the switch is on, the way a storage blip that outlives
// a drive would, and delegates everything else to real storage.
type refusingSettlementStorage struct {
	storage.Storage
	refuse *atomic.Bool
}

func (s refusingSettlementStorage) ControlRequests() storage.ControlRequestStore {
	return refusingSettlementControlStore{ControlRequestStore: s.Storage.ControlRequests(), refuse: s.refuse}
}

// Wraps a real store rather than standing in for one, so it embeds the
// interface directly and only the settlement writes are overridden.
type refusingSettlementControlStore struct {
	storage.ControlRequestStore
	refuse *atomic.Bool
}

func (s refusingSettlementControlStore) CompletePending(ctx context.Context, applyID int64, operation storage.ControlOperation) error {
	if s.refuse.Load() {
		return errors.New("control request store unavailable")
	}
	return s.ControlRequestStore.CompletePending(ctx, applyID, operation)
}

func (s refusingSettlementControlStore) FailPending(ctx context.Context, applyID int64, operation storage.ControlOperation, reason string) error {
	if s.refuse.Load() {
		return errors.New("control request store unavailable")
	}
	return s.ControlRequestStore.FailPending(ctx, applyID, operation, reason)
}

// An operator started an apply whose drive stored the apply completed and then
// lost the write answering the start — and the operator's own post-drive
// settlement lost it too. Nothing re-claims a completed apply, so no later
// drive will visit the request. The reaper's stranded control-request sweep is
// what answers it: once the apply has been quiescent for the settlement window
// and no driver holds an operation of it, the sweep runs the same settlement
// the drive would have, and the start reads as completed against the stored
// outcome. Until the apply is quiescent the sweep leaves the request to the
// drive that may still be settling it.
func TestOperator_StartUnansweredByEveryDriveSettlementIsSettledByTheReaper(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	var refuse atomic.Bool
	refuse.Store(true)
	refusing := refusingSettlementStorage{Storage: stor, refuse: &refuse}

	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "start-unanswered-by-every-drive-settlement",
		parentState:     state.Apply.Running,
		cutoverPolicy:   storage.CutoverPolicyRolling,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"region-a"},
		opState:         state.ApplyOperation.FailedRetryable,
		taskState:       state.Task.FailedRetryable,
	})
	_, err := db.ExecContext(ctx,
		"UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", seed.applyID)
	require.NoError(t, err, "backdate the parent apply heartbeat")
	stalenessBackdate(t, ctx, db, seed.applyID)
	_, _, err = stor.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     seed.applyID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "matrix-test",
	})
	require.NoError(t, err, "request pending start")

	svc := newMatrixService(t, refusing, map[string]tern.Client{
		"region-a/staging": &unansweredStartClient{matrixTernClient: &matrixTernClient{
			mockTernClient: &mockTernClient{},
			stor:           stor,
			deployment:     "region-a",
			rec:            &driveRecorder{},
			outcome:        matrixOutcome{taskState: state.Task.Completed},
		}},
	})
	driveNextOperation(t, ctx, svc, 1)

	start, err := stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStart)
	require.NoError(t, err)
	require.NotNil(t, start)
	require.Equal(t, storage.ControlRequestPending, start.Status,
		"the drive and the operator both lost the settlement write, so the start is still pending")
	apply, err := stor.Applies().Get(ctx, seed.applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.Equal(t, state.Apply.Completed, apply.State, "the stored outcome stands")

	// Storage recovers, but the apply settled moments ago: a drive could still
	// be running its own post-drive settlement, so the sweep leaves it alone.
	refuse.Store(false)
	svc.runStrandedReaperPass(ctx)
	start, err = stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStart)
	require.NoError(t, err)
	require.NotNil(t, start)
	assert.Equal(t, storage.ControlRequestPending, start.Status,
		"a freshly settled apply is still its drive's to answer")

	_, err = db.ExecContext(ctx,
		"UPDATE applies SET updated_at = NOW() - INTERVAL 11 MINUTE WHERE id = ?", seed.applyID)
	require.NoError(t, err, "age the settled apply past the quiescence window")
	svc.runStrandedReaperPass(ctx)

	start, err = stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStart)
	require.NoError(t, err)
	require.NotNil(t, start)
	assert.Equal(t, storage.ControlRequestCompleted, start.Status,
		"the reaper answers the start every drive settlement lost, against the stored completed outcome")
}

// An operator started a two-deployment rolling rollout. While a sibling is
// still dispatching the start stays pending on purpose — the rollout is not
// resolved and the request still describes what the operator asked for. Once
// the last operation settles the parent, the multi-operation drive's own
// post-drive settlement answers the start from the stored outcome: completed
// when the rollout completed, failed with the apply's reason when it settled
// any other way, even though the start took effect for the operations that ran.
func TestOperator_StartOnAMultiOperationRolloutSettlesWithTheParent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)

	cases := []struct {
		name          string
		identifier    string
		lastOutcome   matrixOutcome
		wantApply     string
		wantStart     storage.ControlRequestStatus
		wantStartText string
	}{
		{
			name:        "a rollout that completes answers the start as completed",
			identifier:  "start-on-rollout-completes",
			lastOutcome: matrixOutcome{taskState: state.Task.Completed},
			wantApply:   state.Apply.Completed,
			wantStart:   storage.ControlRequestCompleted,
		},
		{
			name:          "a rollout whose last deployment fails answers the start with the failure",
			identifier:    "start-on-rollout-fails",
			lastOutcome:   matrixOutcome{taskState: state.Task.Failed, errMsg: "region-b boom"},
			wantApply:     state.Apply.Failed,
			wantStart:     storage.ControlRequestFailed,
			wantStartText: "region-b",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetMatrixTables(t, ctx, db)
			seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
				applyIdentifier: tc.identifier,
				parentState:     state.Apply.Running,
				cutoverPolicy:   storage.CutoverPolicyRolling,
				onFailure:       storage.OnFailureHalt,
				deployments:     []string{"region-a", "region-b"},
				opState:         state.ApplyOperation.Pending,
				taskState:       state.Task.Pending,
			})
			_, _, err := stor.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
				ApplyID:     seed.applyID,
				Operation:   storage.ControlOperationStart,
				Status:      storage.ControlRequestPending,
				RequestedBy: "matrix-test",
			})
			require.NoError(t, err, "request pending start")

			rec := &driveRecorder{}
			svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
				"region-a": {taskState: state.Task.Completed},
				"region-b": tc.lastOutcome,
			}))

			driveNextOperation(t, ctx, svc, 1)

			require.Equal(t, []string{"region-a"}, rec.resumeOrder())
			require.False(t, state.IsTerminalApplyState(getApply(t, ctx, stor, seed.applyID).State),
				"one settled deployment of two leaves the rollout open")
			start, err := stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStart)
			require.NoError(t, err)
			require.NotNil(t, start)
			assert.Equal(t, storage.ControlRequestPending, start.Status,
				"the start stays pending while a sibling is still to dispatch")

			driveNextOperation(t, ctx, svc, 2)

			require.Equal(t, []string{"region-a", "region-b"}, rec.resumeOrder())
			apply := getApply(t, ctx, stor, seed.applyID)
			require.Equal(t, tc.wantApply, apply.State)
			start, err = stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStart)
			require.NoError(t, err)
			require.NotNil(t, start)
			assert.Equal(t, tc.wantStart, start.Status,
				"the drive that settles the parent answers the start from the stored outcome")
			if tc.wantStartText != "" {
				assert.Contains(t, start.ErrorMessage, tc.wantStartText,
					"a failed start carries the apply's own reason")
			}
			assert.Equal(t, 1, svc.matrixSummary.count(), "the terminal summary publishes once, after the start is settled")
		})
	}
}

// An operator stops a two-deployment rollout and then starts it again. The
// first operation claim under the start takes the rollout out of its stop: the
// driver reports that claim once, after the parent has left stopped in storage,
// so the PR Check Run republished from storage no longer says the rollout is
// stopped. The claim of the second operation drives a rollout that is already
// running and reports nothing.
func TestOperator_StartOnAStoppedRolloutReportsTheClaimOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	resetMatrixTables(t, ctx, db)

	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "start-on-stopped-rollout",
		parentState:     state.Apply.Stopped,
		cutoverPolicy:   storage.CutoverPolicyRolling,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"region-a", "region-b"},
		opState:         state.ApplyOperation.Stopped,
		taskState:       state.Task.Stopped,
	})
	// The rollout was copying when it stopped: both operations had started, so
	// the start resumes them as recovered work rather than gating them as new
	// members.
	_, err := db.ExecContext(ctx, "UPDATE apply_operations SET started_at = NOW() WHERE apply_id = ?", seed.applyID)
	require.NoError(t, err)
	_, _, err = stor.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     seed.applyID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "matrix-test",
	})
	require.NoError(t, err, "request pending start")

	rec := &driveRecorder{}
	svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
		"region-a": {taskState: state.Task.Completed},
		"region-b": {taskState: state.Task.Completed},
	}))
	var claimedStates []string
	svc.OnStoppedApplyClaimed = func(apply *storage.Apply) {
		assert.Equal(t, seed.applyID, apply.ID)
		claimedStates = append(claimedStates, getApply(t, ctx, stor, apply.ID).State)
	}

	driveNextOperation(t, ctx, svc, 1)

	require.Equal(t, []string{"region-a"}, rec.resumeOrder())
	require.Len(t, claimedStates, 1, "the claim that takes the rollout out of its stop is reported")
	assert.False(t, state.IsState(claimedStates[0], state.Apply.Stopped),
		"the parent has left stopped in storage by the time the claim is reported, got %s", claimedStates[0])

	driveNextOperation(t, ctx, svc, 2)

	require.Equal(t, []string{"region-a", "region-b"}, rec.resumeOrder())
	assert.Len(t, claimedStates, 1, "a claim on a rollout already running again is not reported")
}
