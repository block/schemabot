package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeControlRequests returns a fixed release request (or error) from
// GetByOperation, standing in for the durable control-request store.
type fakeControlRequests struct {
	ControlRequestStore
	req    *ApplyControlRequest
	err    error
	called bool
}

func (f *fakeControlRequests) GetByOperation(_ context.Context, _ int64, _ ControlOperation) (*ApplyControlRequest, error) {
	f.called = true
	return f.req, f.err
}

// fakeStorage exposes only the ControlRequests accessor ReleaseLatched needs;
// every other store is nil and would panic, keeping the test honest about the
// access ReleaseLatched makes.
type fakeStorage struct {
	Storage
	requests ControlRequestStore
}

func (f fakeStorage) ControlRequests() ControlRequestStore { return f.requests }

func pauseOp() *ApplyOperation    { return &ApplyOperation{OnFailure: OnFailurePause} }
func haltOp() *ApplyOperation     { return &ApplyOperation{} }
func continueOp() *ApplyOperation { return &ApplyOperation{OnFailure: OnFailureContinue} }

func TestAnyPauseOnFailure(t *testing.T) {
	assert.False(t, AnyPauseOnFailure(nil))
	assert.False(t, AnyPauseOnFailure([]*ApplyOperation{haltOp(), continueOp()}))
	assert.True(t, AnyPauseOnFailure([]*ApplyOperation{haltOp(), pauseOp()}))
}

func TestReleaseLatched(t *testing.T) {
	releaseReq := func(s ControlRequestStatus) *ApplyControlRequest {
		return &ApplyControlRequest{Operation: ControlOperationRelease, Status: s}
	}

	t.Run("no pause operation never reads the store", func(t *testing.T) {
		// A latched release row is present, but with no pause operation the read
		// is skipped entirely and the rollout is unreleased.
		reqs := &fakeControlRequests{req: releaseReq(ControlRequestCompleted)}
		released, err := ReleaseLatched(t.Context(), fakeStorage{requests: reqs}, 1, []*ApplyOperation{haltOp(), continueOp()})
		require.NoError(t, err)
		assert.False(t, released)
		assert.False(t, reqs.called, "store must not be consulted without a pause operation")
	})

	t.Run("nil storage fails closed", func(t *testing.T) {
		released, err := ReleaseLatched(t.Context(), nil, 1, []*ApplyOperation{pauseOp()})
		require.NoError(t, err)
		assert.False(t, released)
	})

	t.Run("nil control-request store fails closed", func(t *testing.T) {
		released, err := ReleaseLatched(t.Context(), fakeStorage{requests: nil}, 1, []*ApplyOperation{pauseOp()})
		require.NoError(t, err)
		assert.False(t, released)
	})

	t.Run("pending release latches open", func(t *testing.T) {
		reqs := &fakeControlRequests{req: releaseReq(ControlRequestPending)}
		released, err := ReleaseLatched(t.Context(), fakeStorage{requests: reqs}, 1, []*ApplyOperation{pauseOp()})
		require.NoError(t, err)
		assert.True(t, released)
	})

	t.Run("completed release latches open", func(t *testing.T) {
		reqs := &fakeControlRequests{req: releaseReq(ControlRequestCompleted)}
		released, err := ReleaseLatched(t.Context(), fakeStorage{requests: reqs}, 1, []*ApplyOperation{pauseOp()})
		require.NoError(t, err)
		assert.True(t, released)
	})

	t.Run("failed release does not latch", func(t *testing.T) {
		reqs := &fakeControlRequests{req: releaseReq(ControlRequestFailed)}
		released, err := ReleaseLatched(t.Context(), fakeStorage{requests: reqs}, 1, []*ApplyOperation{pauseOp()})
		require.NoError(t, err)
		assert.False(t, released)
	})

	t.Run("no release row stays paused", func(t *testing.T) {
		reqs := &fakeControlRequests{req: nil}
		released, err := ReleaseLatched(t.Context(), fakeStorage{requests: reqs}, 1, []*ApplyOperation{pauseOp()})
		require.NoError(t, err)
		assert.False(t, released)
	})

	t.Run("store read error is surfaced", func(t *testing.T) {
		reqs := &fakeControlRequests{err: errors.New("boom")}
		_, err := ReleaseLatched(t.Context(), fakeStorage{requests: reqs}, 1, []*ApplyOperation{pauseOp()})
		require.Error(t, err)
	})
}
