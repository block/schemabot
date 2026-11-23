//go:build integration

package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/changeevents"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

type changeEventRecorder []changeevents.Event

func (r *changeEventRecorder) Offer(event changeevents.Event) {
	*r = append(*r, event)
}

func TestChangeEventsFollowCommittedOperationAndApplyOutcomes(t *testing.T) {
	ctx := t.Context()
	var events changeEventRecorder
	publisher := &storage.ChangeEvents{Sink: &events}
	store := mysqlstore.New(openMatrixStorage(t), storage.WithChangeEvents(publisher))
	publisher.Journal = store.ApplyLogs().(storage.OperationDispatchJournal)
	seed := seedGroupedApply(t, ctx, store, multiOpSeed{
		applyIdentifier: "event-commit-fence",
		parentState:     state.Apply.Pending,
		deployments:     []string{"region-a"},
		perOpState:      map[string]string{"region-a": state.ApplyOperation.Pending},
		perTaskState:    map[string]string{"region-a": state.Task.Pending},
	})
	apply := getApply(t, ctx, store, seed.applyID)
	op, err := store.ApplyOperations().FindNextApplyOperation(ctx, "worker")
	require.NoError(t, err)
	require.NotNil(t, op)
	assert.True(t, op.StartsAttempt)

	// Losing a claim before it reaches the engine still leaves a started_at
	// timestamp. The next worker has to report the actual first dispatch.
	released, err := store.ApplyOperations().ReleaseClaim(ctx, op.Lease())
	require.NoError(t, err)
	require.True(t, released)
	op, err = store.ApplyOperations().FindNextApplyOperation(ctx, "first-dispatch")
	require.NoError(t, err)
	require.NotNil(t, op)
	assert.False(t, op.StartsAttempt)
	require.NotNil(t, op.StartedAt)
	opCtx := publisher.WithOperationChangeEvents(ctx, apply, op)
	storage.OfferOperationStarted(opCtx)
	require.Len(t, events, 1)

	// The dispatched attempt outlives this worker. A replacement finds the
	// committed marker and can record the outcome without another start.
	staleLease := op.Lease()
	released, err = store.ApplyOperations().ReleaseClaim(ctx, staleLease)
	require.NoError(t, err)
	require.True(t, released)
	op, err = store.ApplyOperations().FindNextApplyOperation(ctx, "replacement")
	require.NoError(t, err)
	require.NotNil(t, op)
	err = store.ApplyOperations().MarkCompleted(storage.WithOperationLease(opCtx, staleLease), op.ID)
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
	require.Len(t, events, 1, "a refused write cannot announce an outcome")

	opCtx = publisher.WithOperationChangeEvents(ctx, apply, op)
	leaseCtx := storage.WithOperationLease(opCtx, op.Lease())
	require.NoError(t, store.ApplyOperations().MarkCompleted(leaseCtx, op.ID))
	require.NoError(t, store.ApplyOperations().MarkCompleted(leaseCtx, op.ID))

	svc := newMatrixService(t, store, map[string]tern.Client{})
	svc.SetChangeEvents(publisher)
	result, err := svc.updateApplyStateFromOperations(leaseCtx, 1, apply, rejectFailedApplyReopen)
	require.NoError(t, err)
	assert.True(t, result.BecameTerminal)
	result, err = svc.updateApplyStateFromOperations(leaseCtx, 1, apply, rejectFailedApplyReopen)
	require.NoError(t, err)
	assert.False(t, result.Swapped)

	require.Len(t, events, 3)
	assert.Equal(t, changeevents.OperationStarted, events[0].Type)
	assert.Equal(t, changeevents.OperationFinished, events[1].Type)
	assert.Equal(t, changeevents.ApplyFinished, events[2].Type)
	assert.Contains(t, events[2].Description, "outcome=completed deployments=region-a[completed:1]")
}

func TestDirectTerminalWriteEmitsOnceWithoutAnObserver(t *testing.T) {
	ctx := t.Context()
	var events changeEventRecorder
	store := mysqlstore.New(openMatrixStorage(t), storage.WithChangeEvents(&storage.ChangeEvents{Sink: &events}))
	seed := seedGroupedApply(t, ctx, store, multiOpSeed{
		applyIdentifier: "event-direct-write",
		parentState:     state.Apply.Pending,
		deployments:     []string{"region-a"},
		perOpState:      map[string]string{"region-a": state.ApplyOperation.Pending},
		perTaskState:    map[string]string{"region-a": state.Task.Pending},
	})
	apply := getApply(t, ctx, store, seed.applyID)
	apply.State = state.Apply.Cancelled
	require.NoError(t, store.Applies().Update(ctx, apply))
	require.NoError(t, store.Applies().Update(ctx, apply))
	require.Len(t, events, 1)
	assert.Equal(t, changeevents.ApplyFinished, events[0].Type)
	assert.Contains(t, events[0].Description, "apply=event-direct-write outcome=cancelled")
}
