package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/changeevents"
	"github.com/block/schemabot/pkg/state"
)

type recordedChangeEvents []changeevents.Event

func (r *recordedChangeEvents) Offer(event changeevents.Event) {
	*r = append(*r, event)
}

func TestWorkerChangeEventsKeepDispatchIdentityAcrossTakeoverAndRetry(t *testing.T) {
	var events recordedChangeEvents
	publisher := &ChangeEvents{Sink: &events}
	apply := &Apply{
		ApplyIdentifier: "apply-worker", IdempotencyKey: "remote-dispatch",
		Database: "orders", Environment: "test", Caller: "engineer",
		Options: MarshalApplyOptions(ApplyOptions{ChangeEventDispatch: &ChangeEventDispatch{
			ApplyID: "apply-control", App: "storefront", Deployment: "region-a",
			OperationIDs:   map[string]int64{"orders": 42, "customers": 43},
			PullRequestURL: "https://github.com/octocat/example/pull/9",
		}}),
	}
	op := &ApplyOperation{
		ID: 101, OperationKey: "customers", Deployment: "worker-address",
		State: state.ApplyOperation.Pending, StartsAttempt: true,
	}
	ctx := publisher.WithOperationChangeEvents(t.Context(), apply, op)
	OfferOperationStarted(ctx)
	OfferOperationStarted(ctx)
	require.Len(t, events, 1, "multiple engine calls share an attempt")
	assert.Equal(t, changeevents.OperationStarted, events[0].Type)

	// This drive lost its lease without recording an outcome. The new owner
	// reattaches to the same attempt and durably records its retryable failure.
	now := time.Now()
	op.State, op.StartedAt, op.StartsAttempt = state.ApplyOperation.Running, &now, false
	ctx = publisher.WithOperationChangeEvents(t.Context(), apply, op)
	OfferOperationStarted(ctx)
	OfferOperationFinished(ctx, op.ID, state.ApplyOperation.FailedRetryable)
	OfferOperationFinished(ctx, op.ID, state.ApplyOperation.FailedRetryable)
	require.Len(t, events, 2)
	assert.Equal(t, changeevents.OperationFinished, events[1].Type)
	assert.Contains(t, events[1].Description, "outcome=failed_retryable")

	op.Attempt, op.StartsAttempt = 1, true
	ctx = publisher.WithOperationChangeEvents(t.Context(), apply, op)
	OfferOperationStarted(ctx)
	OfferOperationFinished(ctx, op.ID, state.ApplyOperation.Completed)
	publisher.OfferApplyFinished(ctx, apply, state.Apply.Completed)
	require.Len(t, events, 4, "only the control plane owns the aggregate")
	for i, event := range events {
		assert.Equal(t, "storefront", event.App)
		assert.Equal(t, "test", event.Environment)
		assert.Contains(t, event.Description, "apply=apply-control operation=43 deployment=region-a")
		assert.Contains(t, event.Description, `caller="engineer" https://github.com/octocat/example/pull/9`)
		if i < 2 {
			assert.Contains(t, event.Description, "attempt=1")
		} else {
			assert.Contains(t, event.Description, "attempt=2")
		}
	}
}

func TestOperationCancelledBeforeDispatchEmitsOnlyTheAggregate(t *testing.T) {
	var events recordedChangeEvents
	publisher := &ChangeEvents{Sink: &events}
	apply := &Apply{ApplyIdentifier: "apply-local", Deployment: "local", Database: "orders", Environment: "test"}
	op := &ApplyOperation{ID: 7, State: state.ApplyOperation.Pending, StartsAttempt: true}
	ctx := publisher.WithOperationChangeEvents(t.Context(), apply, op)
	OfferOperationFinished(ctx, op.ID, state.ApplyOperation.Cancelled)
	publisher.OfferApplyFinished(ctx, apply, state.Apply.Stopped)
	publisher.OfferApplyFinished(ctx, apply, state.Apply.Cancelled)
	require.Len(t, events, 1, "a resumable stop leaves the final outcome open")
	assert.Equal(t, changeevents.ApplyFinished, events[0].Type)
	assert.Equal(t, "apply.finished apply=apply-local outcome=cancelled deployments=local[cancelled]", events[0].Description)
}
