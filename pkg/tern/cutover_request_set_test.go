package tern

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// pendingCutoverSet records a pending cutover bound to every operation of the
// fixture, the request one operator command leaves on an apply that deferred
// its cutover with both targets held.
func pendingCutoverSet(t *testing.T, fx *sharedApplyFixture) *storage.ApplyControlRequest {
	t.Helper()
	metadata, err := storage.CutoverRequestSetMetadata([]int64{fx.first.ID, fx.second.ID})
	require.NoError(t, err)
	req := &storage.ApplyControlRequest{
		ID: 1, ApplyID: fx.apply.ID, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending,
		RequestedBy: "cli:alice", Metadata: metadata,
	}
	fx.controlRequests.requests = []*storage.ApplyControlRequest{req}
	return req
}

// Both targets of a deferred apply wait at their cutover when the operator
// runs one cutover. Each target's own drive swaps its own target, once, and
// the request stays pending until both have cut over, so the one command
// reaches every target it was issued for instead of only the first.
func TestLocalClient_CutoverSetIsTakenByEveryMembersDrive(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.WaitingForCutover, state.ApplyOperation.WaitingForCutover, state.Task.WaitingForCutover, state.Task.WaitingForCutover)
	req := pendingCutoverSet(t, fx)

	require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.firstTask}))
	assert.Equal(t, []string{"ctx-op-11"}, fx.engine.cutoverContexts, "payments-001's drive swaps payments-001")
	assert.Equal(t, storage.ControlRequestPending, req.Status, "payments-002 has not cut over yet")

	require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.firstTask}))
	assert.Equal(t, []string{"ctx-op-11"}, fx.engine.cutoverContexts, "a drive takes its share of one command once")

	require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.secondTask}))
	assert.Equal(t, []string{"ctx-op-11", "ctx-op-12"}, fx.engine.cutoverContexts, "payments-002's drive swaps payments-002")
	assert.Equal(t, storage.ControlRequestPending, req.Status, "the request waits until both targets have cut over")

	fx.first.State = state.ApplyOperation.Completed
	fx.second.State = state.ApplyOperation.Completed
	require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.secondTask}))
	assert.Equal(t, storage.ControlRequestCompleted, req.Status)
	assert.Len(t, fx.engine.cutoverContexts, 2, "settling the request swaps nothing")
}

// A later cutover command recorded on the same request row is a new command:
// a drive that took its share of the first one takes its share of the second.
func TestLocalClient_CutoverSetShareIsTakenAgainForANewCommand(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.WaitingForCutover, state.ApplyOperation.WaitingForCutover, state.Task.WaitingForCutover, state.Task.WaitingForCutover)
	req := pendingCutoverSet(t, fx)

	require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.firstTask}))
	req.Revision++
	require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.firstTask}))
	assert.Equal(t, []string{"ctx-op-11", "ctx-op-11"}, fx.engine.cutoverContexts)
}

// A drive whose target is not waiting at its cutover, or is held by an
// earlier member, leaves its share for later and swaps nothing.
func TestLocalClient_CutoverSetShareWaitsForItsMember(t *testing.T) {
	t.Run("not waiting at the cutover", func(t *testing.T) {
		fx := newSharedApplyFixture(state.ApplyOperation.WaitingForCutover, state.ApplyOperation.Running, state.Task.WaitingForCutover, state.Task.Running)
		req := pendingCutoverSet(t, fx)
		require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.secondTask}))
		assert.Empty(t, fx.engine.cutoverContexts)
		assert.Equal(t, storage.ControlRequestPending, req.Status)
	})
	t.Run("held by an earlier member", func(t *testing.T) {
		fx := newSharedApplyFixture(state.ApplyOperation.WaitingForCutover, state.ApplyOperation.WaitingForCutover, state.Task.WaitingForCutover, state.Task.WaitingForCutover)
		req := pendingCutoverSet(t, fx)
		fx.client.storage.(*exactProgressStorage).applyOperations.(*perOperationResumeStore).blocker = fx.first
		require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.secondTask}))
		assert.Empty(t, fx.engine.cutoverContexts)
		assert.Equal(t, storage.ControlRequestPending, req.Status)
	})
}

// payments-001 cut over on the command but payments-002 failed before its
// drive took its share. The request settles failed and says how far it got,
// rather than reading as completed or as never applied.
func TestLocalClient_CutoverSetSettlesNamingHowFarItGot(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.Completed, state.ApplyOperation.Failed, state.Task.Completed, state.Task.Failed)
	req := pendingCutoverSet(t, fx)

	require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.firstTask}))
	assert.Empty(t, fx.engine.cutoverContexts)
	assert.Equal(t, storage.ControlRequestFailed, req.Status)
	assert.Equal(t, "cutover reached 1 of the 2 operations it was sent for; cutover request was not applied because the operation it was sent for is failed", req.ErrorMessage)
}

// A stop lands on a deferred apply after payments-001 has cut over on the
// command and before payments-002 took its share. No drive is left to swap
// payments-002, so the request settles at once instead of waiting for it.
func TestSettlePendingCutoverAfterStopSettlesACutoverSet(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.Completed, state.ApplyOperation.Stopped, state.Task.Completed, state.Task.Stopped)
	req := pendingCutoverSet(t, fx)
	fx.apply.State = state.Apply.Stopped

	require.NoError(t, SettlePendingCutoverAfterStop(t.Context(), fx.client.storage, slog.Default(), fx.apply))
	assert.Equal(t, storage.ControlRequestFailed, req.Status)
	assert.Equal(t, "cutover reached 1 of the 2 operations it was sent for; "+cutoverSupersededByStopMessage, req.ErrorMessage)
}
