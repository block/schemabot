package api

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func newTasklessControlTestService(controls storage.ControlRequestStore) *Service {
	return New(&mockStorageWithApplyStores{
		tasks:    &capturingTaskStore{},
		controls: controls,
	}, testServerConfig(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// A cancel can land on an apply that owns no task rows — it was queued before
// its first drive created them, or it is a VSchema-only apply that never has
// any. The owner must record the durable cancel request rather than refuse:
// the apply row is still active work, and without the request no command could
// ever end it. The drive's task-less settle path consumes the request.
func TestQueueCancelForTasklessApplyRecordsDurableRequest(t *testing.T) {
	controls := &memoryControlRequestStore{}
	svc := newTasklessControlTestService(controls)

	apply := &storage.Apply{
		ID:              11,
		ApplyIdentifier: "apply-taskless",
		Database:        "appdb",
		State:           state.Apply.Pending,
		Environment:     "staging",
	}
	resp, responseStatus, err := svc.queueCancelForApplyOwner(t.Context(), apply, "operator")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted, "a task-less cancel must be accepted, not refused for lack of tasks")
	assert.Zero(t, resp.CancelledCount)
	assert.Zero(t, resp.SkippedCount)
	assert.Empty(t, responseStatus, "a fresh cancel is not an already-requested response")

	req, err := controls.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, req, "the durable cancel request must be recorded for the task-less settle path")
	assert.Equal(t, "operator", req.RequestedBy)
}

// The stop counterpart: a stop on a non-terminal apply with no task rows
// records the durable stop request so the drive's task-less settle path can
// park the apply, instead of refusing and leaving it unstoppable.
func TestQueueStopForTasklessApplyRecordsDurableRequest(t *testing.T) {
	controls := &memoryControlRequestStore{}
	svc := newTasklessControlTestService(controls)

	apply := &storage.Apply{
		ID:              12,
		ApplyIdentifier: "apply-taskless-stop",
		Database:        "appdb",
		State:           state.Apply.Pending,
		Environment:     "staging",
	}
	resp, responseStatus, err := svc.queueStopForApplyOwner(t.Context(), apply, "operator")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted, "a task-less stop must be accepted, not refused for lack of tasks")
	assert.Zero(t, resp.StoppedCount)
	assert.Zero(t, resp.SkippedCount)
	assert.Empty(t, responseStatus, "a fresh stop is not an already-requested response")

	req, err := controls.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	require.NotNil(t, req, "the durable stop request must be recorded for the task-less settle path")
	assert.Equal(t, "operator", req.RequestedBy)
}
