package tern

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// splitTaskStore answers the stop's read of this client's database and its
// read of the whole apply separately, so a test can give the apply tasks on a
// database the client does not serve.
type splitTaskStore struct {
	storage.TaskStore
	local, all []*storage.Task
}

func (s *splitTaskStore) GetByDatabase(context.Context, string) ([]*storage.Task, error) {
	return s.local, nil
}

func (s *splitTaskStore) GetByApplyID(context.Context, int64) ([]*storage.Task, error) {
	return s.all, nil
}

// refusingApplyStore refuses the stop's apply write with updateErr, the way
// storage does when another writer finished the apply first, and from then on
// reads the apply back as reloaded, or as missing when reloaded is nil.
type refusingApplyStore struct {
	storage.ApplyStore
	apply     *storage.Apply
	reloaded  *storage.Apply
	updateErr error
	refused   bool
}

func (s *refusingApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	if s.refused {
		return s.reloaded, nil
	}
	return s.apply, nil
}

func (s *refusingApplyStore) GetByApplyIdentifier(context.Context, string) (*storage.Apply, error) {
	return s.apply, nil
}

func (s *refusingApplyStore) Update(context.Context, *storage.Apply) error {
	s.refused = true
	return s.updateErr
}

// newStopSettleClient serves an apply whose only task on this client's
// database, testdb, has completed. all is every task storage holds for the
// apply, on any database.
func newStopSettleClient(all []*storage.Task) (*LocalClient, *storage.Apply, *splitTaskStore) {
	apply := &storage.Apply{
		ID: 42, ApplyIdentifier: "apply-stop-settle", State: state.Apply.Running,
		Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging",
	}
	client := newMySQLControlTestClient(apply, nil, &controlCaptureEngine{})
	tasks := &splitTaskStore{local: []*storage.Task{completedLocalTask()}, all: all}
	client.storage.(*controlTestStorage).tasks = tasks
	return client, apply, tasks
}

func completedLocalTask() *storage.Task {
	return &storage.Task{
		ID: 1, ApplyID: 42, TaskIdentifier: "task-testdb", Database: "testdb",
		Namespace: "testdb", TableName: "users", State: state.Task.Completed,
	}
}

func taskOnOtherDB(taskState string) *storage.Task {
	return &storage.Task{
		ID: 2, ApplyID: 42, TaskIdentifier: "task-otherdb", Database: "otherdb",
		Namespace: "otherdb", TableName: "users", State: taskState,
	}
}

func pendingStopRequests() *testControlRequestStore {
	return &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID: 42, Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending, RequestedBy: "operator",
	}}}
}

func stopRequestStatus(t *testing.T, requests *testControlRequestStore) storage.ControlRequestStatus {
	t.Helper()
	req, err := requests.GetByOperation(t.Context(), 42, storage.ControlOperationStop)
	require.NoError(t, err)
	require.NotNil(t, req)
	return req.Status
}

// A stop whose targeted tasks have all finished, while storage returns no
// tasks at all for the apply, has nothing to derive an outcome from. It fails
// closed and leaves the apply running rather than recording one.
func TestStopFailsClosedWhenStorageReturnsNoTasksForTheApply(t *testing.T) {
	client, apply, _ := newStopSettleClient(nil)

	_, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "operator")

	require.ErrorContains(t, err, "storage returned no tasks")
	assert.Equal(t, state.Apply.Running, apply.State)
	assert.Nil(t, apply.CompletedAt)
}

// An operator's stop reaches the drive while the apply's task on another
// database is still running. The stop fails closed and its durable request
// stays pending, so the next drive retries it. Once that task finishes, the
// retry derives the apply's outcome and completes the request.
func TestPendingStopResolvesOnceTheApplysOtherTasksFinish(t *testing.T) {
	client, apply, tasks := newStopSettleClient([]*storage.Task{completedLocalTask(), taskOnOtherDB(state.Task.Running)})
	requests := pendingStopRequests()
	client.storage.(*controlTestStorage).controlRequests = requests

	_, err := client.processPendingStopControlRequest(t.Context(), apply)

	require.ErrorContains(t, err, "task task-otherdb on database otherdb is running")
	assert.Equal(t, storage.ControlRequestPending, stopRequestStatus(t, requests))
	assert.Equal(t, state.Apply.Running, apply.State)

	tasks.all = []*storage.Task{completedLocalTask(), taskOnOtherDB(state.Task.Completed)}
	_, err = client.processPendingStopControlRequest(t.Context(), apply)

	require.NoError(t, err)
	assert.Equal(t, storage.ControlRequestCompleted, stopRequestStatus(t, requests))
	assert.Equal(t, state.Apply.Completed, apply.State)
}

// Another writer records the apply failed between the stop's read and its
// write, while the stop's own tasks derive completed. The stop reports the
// recorded failure rather than the outcome it derived, and its durable request
// completes instead of retrying against an outcome that can no longer change.
func TestStopThatLosesTheRaceCompletesItsDurableRequest(t *testing.T) {
	newRacingClient := func() (*LocalClient, *storage.Apply) {
		client, apply, _ := newStopSettleClient([]*storage.Task{completedLocalTask()})
		failed := *apply
		failed.State = state.Apply.Failed
		client.storage.(*controlTestStorage).applies = &refusingApplyStore{
			apply: apply, reloaded: &failed, updateErr: storage.ErrApplyOutcomeSettled,
		}
		return client, apply
	}

	client, apply := newRacingClient()
	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "operator")
	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, "Schema change already failed", resp.ErrorMessage)

	client, apply = newRacingClient()
	requests := pendingStopRequests()
	client.storage.(*controlTestStorage).controlRequests = requests
	_, err = client.processPendingStopControlRequest(t.Context(), apply)
	require.NoError(t, err)
	assert.Equal(t, storage.ControlRequestCompleted, stopRequestStatus(t, requests))
}

// The stop accepts a refused write only when the reload shows the apply
// finished. Any other answer is returned, so the request stays pending
// rather than resolving against an outcome nobody recorded.
func TestStopReturnsAnApplyWriteFailureItCannotAccept(t *testing.T) {
	for name, tc := range map[string]struct {
		updateErr   error
		reloadState string
		noReload    bool
		want        string
	}{
		"the write fails for another reason": {
			updateErr:   errors.New("connection reset by peer"),
			reloadState: state.Apply.Failed,
			want:        "connection reset by peer",
		},
		"the reloaded apply is still active": {
			updateErr:   storage.ErrApplyOutcomeSettled,
			reloadState: state.Apply.Running,
			want:        storage.ErrApplyOutcomeSettled.Error(),
		},
		"the reloaded apply is missing": {
			updateErr: storage.ErrApplyReopenRefused,
			noReload:  true,
			want:      storage.ErrApplyNotFound.Error(),
		},
	} {
		t.Run(name, func(t *testing.T) {
			client, apply, _ := newStopSettleClient([]*storage.Task{completedLocalTask()})
			store := &refusingApplyStore{apply: apply, updateErr: tc.updateErr}
			if !tc.noReload {
				reloaded := *apply
				reloaded.State = tc.reloadState
				store.reloaded = &reloaded
			}
			client.storage.(*controlTestStorage).applies = store

			resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "operator")

			require.ErrorContains(t, err, tc.want)
			assert.Nil(t, resp)
		})
	}
}

// A drive that holds only its operation's lease leaves the apply row to the
// operator's projection: the stop is accepted without writing the apply.
func TestStopUnderAnOperationLeaseLeavesTheApplyToTheProjection(t *testing.T) {
	client, apply, tasks := newStopSettleClient([]*storage.Task{completedLocalTask(), taskOnOtherDB(state.Task.Running)})
	operationID := int64(7)
	tasks.local[0].ApplyOperationID = &operationID
	ctx := storage.WithOperationLease(t.Context(), storage.OperationLease{ApplyID: 42, OperationID: operationID, Owner: "driver-0", Token: "operation-token"})

	resp, err := client.stopOwnedApply(ctx, &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "operator")

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(1), resp.SkippedCount)
	assert.Equal(t, state.Apply.Running, apply.State)
	assert.Nil(t, apply.CompletedAt)
}

// A stop over an apply whose other task failed records the apply failed and
// carries that task's failure reason onto the apply, so operators can triage
// from the apply record.
func TestStopRecordsTheFailedTasksReasonOnTheApply(t *testing.T) {
	failedTask := taskOnOtherDB(state.Task.Failed)
	failedTask.TableName = "orders"
	failedTask.ErrorMessage = "duplicate entry for key idx_email"
	client, apply, _ := newStopSettleClient([]*storage.Task{completedLocalTask(), failedTask})

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "operator")

	require.NoError(t, err)
	assert.Equal(t, "Schema change already failed", resp.ErrorMessage)
	assert.Equal(t, state.Apply.Failed, apply.State)
	assert.Equal(t, "table orders failed: duplicate entry for key idx_email", apply.ErrorMessage)
}
