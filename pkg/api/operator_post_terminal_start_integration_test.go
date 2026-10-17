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

// An operator stops a two-deployment rollout and then starts it again. A
// rolling start drives one deployment at a time, and between the two drives the
// rollout reads stopped again: one deployment has finished and the other is
// still stopped. Each claim that takes the rollout out of stopped is reported,
// after the parent has left stopped in storage, so the PR Check Run republished
// from storage never keeps saying the rollout is stopped while a deployment
// runs, and the PR progress comment observer is attached again, so the comment
// follows the resumed copy instead of staying frozen at stopped.
func TestOperator_StartOnAStoppedRolloutReportsEachClaimOutOfStopped(t *testing.T) {
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
	assert.Equal(t, 1, svc.matrixSummary.recoveredCount(),
		"the drive that takes the rollout out of its stop attaches the PR progress observer")
	require.Equal(t, state.Apply.Stopped, getApply(t, ctx, stor, seed.applyID).State,
		"with region-a done and region-b still stopped, the rollout reads stopped between the drives")

	driveNextOperation(t, ctx, svc, 2)

	require.Equal(t, []string{"region-a", "region-b"}, rec.resumeOrder())
	require.Len(t, claimedStates, 2, "the claim that takes the rollout out of stopped again is reported too")
	assert.False(t, state.IsState(claimedStates[1], state.Apply.Stopped),
		"the parent has left stopped in storage by the time the second claim is reported, got %s", claimedStates[1])
	assert.Equal(t, 2, svc.matrixSummary.recoveredCount(),
		"each drive that takes the rollout out of stopped again attaches the observer again")
}

// A rollout was started again after a stop, and the pod that took it out of
// the stop went away before its progress comment observer posted the resumed
// rollout's fresh comment, so the PR still shows the stop's summary. The next
// pod to claim one of the rollout's operations attaches an observer itself, so
// the PR follows the resumed copy from the bottom of the timeline. It attaches
// once per process, and once an observer has rotated the stop's summary away
// no pod attaches another.
func TestOperator_ResumedRolloutObserverSurvivesTheLossOfItsPod(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	resetMatrixTables(t, ctx, db)

	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "resumed-rollout-lost-pod",
		parentState:     state.Apply.Running,
		cutoverPolicy:   storage.CutoverPolicyRolling,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"region-a", "region-b", "region-c"},
		opState:         state.ApplyOperation.Pending,
		taskState:       state.Task.Pending,
	})
	require.NoError(t, stor.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         seed.applyID,
		CommentState:    state.Comment.Summary,
		GitHubCommentID: 4201,
	}), "record the stop's summary comment")

	rec := &driveRecorder{}
	outcomes := map[string]matrixOutcome{
		"region-a": {taskState: state.Task.Completed},
		"region-b": {taskState: state.Task.Completed},
		"region-c": {taskState: state.Task.Completed},
	}
	replacement := newMatrixService(t, stor, matrixClients(stor, rec, outcomes))

	driveNextOperation(t, ctx, replacement, 1)
	require.Equal(t, []string{"region-a"}, rec.resumeOrder())
	assert.Equal(t, 1, replacement.matrixSummary.recoveredCount(),
		"the first claim after the lost pod attaches an observer while the PR still shows the stop")

	driveNextOperation(t, ctx, replacement, 2)
	require.Equal(t, []string{"region-a", "region-b"}, rec.resumeOrder())
	assert.Equal(t, 1, replacement.matrixSummary.recoveredCount(),
		"a later claim in the same process does not attach a second observer")

	// The attached observer posts the fresh progress comment and consumes the
	// stop's summary.
	require.NoError(t, stor.ApplyComments().Supersede(ctx, seed.applyID, state.Comment.Summary))

	another := newMatrixService(t, stor, matrixClients(stor, rec, outcomes))
	driveNextOperation(t, ctx, another, 3)
	require.Equal(t, []string{"region-a", "region-b", "region-c"}, rec.resumeOrder())
	assert.Zero(t, another.matrixSummary.recoveredCount(),
		"once the stop's summary is rotated away, a claim on another pod attaches no observer")
}

// A rollout was stopped and started twice, and both times the pod that took it
// out of its stop went away before its observer rotated the stop's summary. The
// same surviving pod claims after each resume. Each stop posts its own summary
// comment, so the observer this pod attached for the first resume does not
// stand in for the second: the pod attaches again when the PR shows the second
// stop, and only once.
func TestOperator_ResumedRolloutObserverReattachesAfterALaterStop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	resetMatrixTables(t, ctx, db)

	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "resumed-rollout-second-stop",
		parentState:     state.Apply.Running,
		cutoverPolicy:   storage.CutoverPolicyRolling,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"region-a", "region-b", "region-c"},
		opState:         state.ApplyOperation.Pending,
		taskState:       state.Task.Pending,
	})
	require.NoError(t, stor.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         seed.applyID,
		CommentState:    state.Comment.Summary,
		GitHubCommentID: 4201,
	}), "record the first stop's summary comment")

	rec := &driveRecorder{}
	outcomes := map[string]matrixOutcome{
		"region-a": {taskState: state.Task.Completed},
		"region-b": {taskState: state.Task.Completed},
		"region-c": {taskState: state.Task.Completed},
	}
	survivor := newMatrixService(t, stor, matrixClients(stor, rec, outcomes))

	driveNextOperation(t, ctx, survivor, 1)
	require.Equal(t, []string{"region-a"}, rec.resumeOrder())
	require.Equal(t, 1, survivor.matrixSummary.recoveredCount(),
		"the first resume attaches an observer while the PR shows the first stop")

	// The first resume's observer rotated the first stop's summary away, then the
	// rollout was stopped again and that stop posted a summary of its own.
	require.NoError(t, stor.ApplyComments().Supersede(ctx, seed.applyID, state.Comment.Summary))
	require.NoError(t, stor.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         seed.applyID,
		CommentState:    state.Comment.Summary,
		GitHubCommentID: 4202,
	}), "record the second stop's summary comment")

	driveNextOperation(t, ctx, survivor, 2)
	require.Equal(t, []string{"region-a", "region-b"}, rec.resumeOrder())
	assert.Equal(t, 2, survivor.matrixSummary.recoveredCount(),
		"the second resume attaches a fresh observer while the PR shows the second stop")

	driveNextOperation(t, ctx, survivor, 3)
	require.Equal(t, []string{"region-a", "region-b", "region-c"}, rec.resumeOrder())
	assert.Equal(t, 2, survivor.matrixSummary.recoveredCount(),
		"a later claim of the second resume in the same process does not attach another observer")
}
