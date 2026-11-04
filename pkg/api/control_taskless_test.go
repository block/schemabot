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

// An operator stopped an apply, started it again, and is stopping it again.
// An apply driven under the operation lease kept its start pending while it
// ran, since the start is what its stopped operations are claimed for, so the
// new stop completes that start before it is recorded: a start left pending
// under it would resume the apply the moment the stop landed. That covers a
// rollout, and also a lone operation that drives under the operation lease
// because its manifest still expects siblings or because it has no tasks. An
// apply driven under the parent lease consumes its own start, so the stop
// leaves that start alone.
func TestQueueStopSupersedesThePendingStartOfAnOperationLeaseDrive(t *testing.T) {
	operationID := int64(1)
	taskOfTheLoneOperation := []*storage.Task{{ID: 5, ApplyID: 13, ApplyOperationID: &operationID, State: state.Task.Running}}
	loneOperation := []*storage.ApplyOperation{
		{ID: operationID, ApplyID: 13, Deployment: "region-a", OperationKey: "orders-001", State: state.ApplyOperation.Resuming},
	}
	for _, tc := range []struct {
		name          string
		operations    []*storage.ApplyOperation
		tasks         []*storage.Task
		expectedKeys  []string
		wantStartLeft bool
	}{
		{
			name: "rollout",
			operations: []*storage.ApplyOperation{
				{ID: 1, ApplyID: 13, Deployment: "region-a", Target: "orders-001", State: state.ApplyOperation.Running},
				{ID: 2, ApplyID: 13, Deployment: "region-a", Target: "orders-002", State: state.ApplyOperation.Stopped},
			},
		},
		{
			name:         "lone operation whose manifest still expects a sibling",
			operations:   loneOperation,
			tasks:        taskOfTheLoneOperation,
			expectedKeys: []string{"orders-001", "orders-002"},
		},
		{
			name:       "lone task-less operation",
			operations: loneOperation,
		},
		{
			name:          "lone operation with tasks",
			operations:    loneOperation,
			tasks:         taskOfTheLoneOperation,
			wantStartLeft: true,
		},
		{
			name:          "lone operation with tasks whose manifest is fully attached",
			operations:    loneOperation,
			tasks:         taskOfTheLoneOperation,
			expectedKeys:  []string{"orders-001"},
			wantStartLeft: true,
		},
		{
			name:          "no operation attached yet",
			wantStartLeft: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			controls := &memoryControlRequestStore{}
			svc := New(&mockStorageWithApplyStores{
				tasks:      &capturingTaskStore{tasks: tc.tasks},
				controls:   controls,
				operations: &staticApplyOperationStore{operations: tc.operations},
			}, testServerConfig(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			apply := &storage.Apply{
				ID: 13, ApplyIdentifier: "apply-restopped", Database: "orders", State: state.Apply.Running, Environment: "staging",
				ExpectedOperationKeys: tc.expectedKeys,
			}
			_, _, err := controls.RequestPending(t.Context(), &storage.ApplyControlRequest{
				ApplyID: apply.ID, Operation: storage.ControlOperationStart, Status: storage.ControlRequestPending, RequestedBy: "first-operator",
			})
			require.NoError(t, err)

			resp, _, err := svc.queueStopForApplyOwner(t.Context(), apply, "second-operator")
			require.NoError(t, err)
			require.True(t, resp.Accepted)

			start, err := controls.GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
			require.NoError(t, err)
			if tc.wantStartLeft {
				assert.NotNil(t, start, "a parent-lease drive consumes its own start")
			} else {
				assert.Nil(t, start, "the stop supersedes the operation-lease drive's pending start, so nothing reopens the apply once it lands")
			}
			stop, err := controls.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
			require.NoError(t, err)
			require.NotNil(t, stop, "the stop is recorded")
			assert.Equal(t, "second-operator", stop.RequestedBy)
		})
	}
}
