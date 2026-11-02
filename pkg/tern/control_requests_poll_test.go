package tern

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// pollReadCountingControlRequestStore counts the control-request reads a drive
// makes on one poll, so a test can pin how many queries each poll costs.
type pollReadCountingControlRequestStore struct {
	*testControlRequestStore
	listReads    int
	pendingReads int
	listErr      error
}

func (s *pollReadCountingControlRequestStore) ListPending(ctx context.Context, applyID int64) ([]*storage.ApplyControlRequest, error) {
	s.listReads++
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.testControlRequestStore.ListPending(ctx, applyID)
}

func (s *pollReadCountingControlRequestStore) GetPending(ctx context.Context, applyID int64, operation storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	s.pendingReads++
	return s.testControlRequestStore.GetPending(ctx, applyID, operation)
}

// A remote drive answers cancel, stop, cutover, skip-revert, and revert on every
// progress poll, and on nearly every poll none of them is pending. The poll
// reads the apply's pending requests once and runs only the processors whose
// operation it found, so a quiet poll costs one query however many operations
// the drive answers. A pending request still reaches its processor, which
// reloads the request before acting on it, and a failed read ends the drive for
// the operator to retry instead of skipping the requests it could not see.
func TestGRPCClient_ProcessPendingControlRequestsAtPoll(t *testing.T) {
	newApply := func(applyState string) *storage.Apply {
		return &storage.Apply{
			ID: 1, ApplyIdentifier: "apply-poll-control-grpc",
			Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL,
			Environment: "staging", Engine: storage.EngineSpirit,
			ExternalID: "remote-poll-control-grpc", State: applyState,
		}
	}
	newStore := func(apply *storage.Apply, requests ...*storage.ApplyControlRequest) (*mockStorage, *pollReadCountingControlRequestStore) {
		controlRequests := &pollReadCountingControlRequestStore{testControlRequestStore: &testControlRequestStore{requests: requests}}
		stored := *apply
		return &mockStorage{applies: &mockApplyStore{apply: &stored}, tasks: &mockTaskStore{}, logs: &mockApplyLogStore{}, controlRequests: controlRequests}, controlRequests
	}

	t.Run("nothing pending: one read and no processor runs", func(t *testing.T) {
		server := &capturingTernServer{}
		client, cleanup := testCapturingGRPCClient(t, server)
		defer cleanup()
		apply := newApply(state.Apply.Running)
		store, controlRequests := newStore(apply, &storage.ApplyControlRequest{
			ApplyID: apply.ID, Operation: storage.ControlOperationStop,
			Status: storage.ControlRequestCompleted, RequestedBy: "cli:alice",
		})
		client.storage = store

		standDown, err := client.processPendingControlRequestsAtPoll(t.Context(), apply, applyTaskScope{})
		require.NoError(t, err)
		assert.False(t, standDown)
		assert.Equal(t, 1, controlRequests.listReads, "the poll reads the pending requests once")
		assert.Zero(t, controlRequests.pendingReads, "no per-operation read when nothing is pending")
	})

	t.Run("pending revert: only the revert processor runs", func(t *testing.T) {
		server := &capturingTernServer{}
		client, cleanup := testCapturingGRPCClient(t, server)
		defer cleanup()
		apply := newApply(state.Apply.RevertWindow)
		store, controlRequests := newStore(apply, &storage.ApplyControlRequest{
			ApplyID: apply.ID, Operation: storage.ControlOperationRevert,
			Status: storage.ControlRequestPending, RequestedBy: "cli:alice",
		})
		client.storage = store

		standDown, err := client.processPendingControlRequestsAtPoll(t.Context(), apply, applyTaskScope{})
		require.NoError(t, err)
		assert.False(t, standDown)
		assert.Equal(t, "remote-poll-control-grpc", server.getRevertApplyID(), "revert proxied to the data plane")
		assert.Equal(t, 1, controlRequests.listReads)
		assert.Equal(t, 1, controlRequests.pendingReads, "only the revert processor reloads its request")
		pending, err := controlRequests.testControlRequestStore.GetPending(t.Context(), apply.ID, storage.ControlOperationRevert)
		require.NoError(t, err)
		assert.Nil(t, pending, "the revert request is completed")
	})

	t.Run("read fails: the drive exits with the error", func(t *testing.T) {
		server := &capturingTernServer{}
		client, cleanup := testCapturingGRPCClient(t, server)
		defer cleanup()
		apply := newApply(state.Apply.RevertWindow)
		store, controlRequests := newStore(apply, &storage.ApplyControlRequest{
			ApplyID: apply.ID, Operation: storage.ControlOperationRevert,
			Status: storage.ControlRequestPending, RequestedBy: "cli:alice",
		})
		readErr := errors.New("storage unavailable")
		controlRequests.listErr = readErr
		client.storage = store

		standDown, err := client.processPendingControlRequestsAtPoll(t.Context(), apply, applyTaskScope{})
		require.ErrorIs(t, err, readErr)
		assert.Contains(t, err.Error(), "apply-poll-control-grpc")
		assert.False(t, standDown)
		assert.Empty(t, server.getRevertApplyID(), "no request is acted on when the read failed")
		assert.Zero(t, controlRequests.pendingReads)
	})
}

// The set reports an operation pending when any request for it is in the
// apply's pending read, and nothing else.
func TestPendingControlOperations(t *testing.T) {
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-pending-set"}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
		{ApplyID: 7, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending},
		{ApplyID: 7, Operation: storage.ControlOperationStop, Status: storage.ControlRequestFailed},
		{ApplyID: 8, Operation: storage.ControlOperationCancel, Status: storage.ControlRequestPending},
	}}
	store := &mockStorage{controlRequests: controlRequests}

	pending, err := loadPendingControlOperations(t.Context(), store, apply)
	require.NoError(t, err)
	assert.True(t, pending.has(storage.ControlOperationCutover))
	assert.True(t, pending.has(storage.ControlOperationStop, storage.ControlOperationCutover), "any of the operations")
	assert.False(t, pending.has(storage.ControlOperationStop), "a failed request is not pending")
	assert.False(t, pending.has(storage.ControlOperationCancel), "another apply's request is not this apply's")
	assert.False(t, pending.has(), "no operations asked")
}

// A local grouped drive answers cancel and stop before it asks the engine for
// progress, then cancel, stop, and cutover after recording it, and skip-revert
// and revert while its revert window is open. Each of those two points reads
// the apply's pending requests once, so an open revert window with nothing
// pending costs two reads a tick, and a queued revert still reaches the engine
// with only its own request reloaded.
func TestHandleAtomicProgressTickReadsPendingControlRequestsOncePerCheck(t *testing.T) {
	newFixture := func() (*revertWindowTickFixture, *pollReadCountingControlRequestStore) {
		f := newRevertWindowTickFixture(storage.ApplyOptions{})
		f.ps.stateEnteredAt = time.Now()
		counting := &pollReadCountingControlRequestStore{testControlRequestStore: f.controlRequests}
		f.client.storage.(*exactProgressStorage).controlRequests = counting
		return f, counting
	}

	t.Run("revert window, nothing pending", func(t *testing.T) {
		f, counting := newFixture()

		assert.False(t, f.tick(t), "an open revert window keeps the drive polling")
		assert.Equal(t, 2, counting.listReads, "one read before the progress call and one after it")
		assert.Zero(t, counting.pendingReads, "no per-operation read when nothing is pending")
		assert.Zero(t, f.eng.revertCount)
	})

	t.Run("revert window, revert pending", func(t *testing.T) {
		f, counting := newFixture()
		f.recordControlRequest(storage.ControlOperationRevert, storage.ControlRequestPending)

		f.tick(t)
		assert.Equal(t, 2, counting.listReads)
		assert.Equal(t, 1, counting.pendingReads, "only the revert request is reloaded")
		assert.Equal(t, 1, f.eng.revertCount, "the queued revert reaches the engine")
	})
}
