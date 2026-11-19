package tern

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// turnDecisionStorage serves the two stores the cutover turn decision reads.
// A nil field is a store the storage cannot provide.
type turnDecisionStorage struct {
	storage.Storage
	operations storage.ApplyOperationStore
	tasks      storage.TaskStore
}

func (s *turnDecisionStorage) ApplyOperations() storage.ApplyOperationStore { return s.operations }
func (s *turnDecisionStorage) Tasks() storage.TaskStore                     { return s.tasks }

// turnDecisionOperationStore answers the operation reads the turn decision
// makes: the operation a request is bound to, and the earlier member, if any,
// that holds this member's cutover.
type turnDecisionOperationStore struct {
	storage.ApplyOperationStore
	ops        map[int64]*storage.ApplyOperation
	getErr     error
	blocker    *storage.ApplyOperation
	blockerErr error
	// blockerAskedFor records each operation CutoverBlocker was asked about.
	blockerAskedFor []int64
}

func (s *turnDecisionOperationStore) Get(_ context.Context, id int64) (*storage.ApplyOperation, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.ops[id], nil
}

func (s *turnDecisionOperationStore) CutoverBlocker(_ context.Context, operationID int64) (*storage.ApplyOperation, error) {
	s.blockerAskedFor = append(s.blockerAskedFor, operationID)
	return s.blocker, s.blockerErr
}

const (
	turnEUOperationID int64 = 1
	turnUSOperationID int64 = 2
)

// turnMember is one operation of a two-deployment barrier apply, eu first and
// us second in rollout order.
func turnMember(id int64, deployment, opState string) *storage.ApplyOperation {
	return &storage.ApplyOperation{
		ID:            id,
		Deployment:    deployment,
		State:         opState,
		CutoverPolicy: storage.CutoverPolicyBarrier,
	}
}

// turnTask is one task of the operation with the given ID.
func turnTask(operationID int64, taskState string) *storage.Task {
	return &storage.Task{ApplyOperationID: &operationID, State: taskState}
}

// usDriveScope is the scope of the us operation's own drive.
func usDriveScope(us *storage.ApplyOperation) applyTaskScope {
	return applyTaskScope{applyOperationID: us.ID, operation: us, multiOperation: true}
}

// cutoverRequest is a pending operator cutover request, bound to the
// operation with the given ID or unbound when it is zero.
func cutoverRequest(boundTo int64) *storage.ApplyControlRequest {
	req := &storage.ApplyControlRequest{ID: 9, Operation: storage.ControlOperationCutover}
	if boundTo != 0 {
		req.Metadata = storage.CutoverRequestMetadata(boundTo)
	}
	return req
}

// The us drive of a two-deployment barrier apply asks whether it may take the
// operator's pending cutover request. It may only when its own tasks are
// parked at (or already in) the cutover and no earlier member holds it. A
// request bound to eu is eu's alone: us leaves it while eu can still take it,
// and settles it once eu has ended, so one command never cuts over a second
// member.
func TestOperationCutoverRequestTurn(t *testing.T) {
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-turn"}
	tests := []struct {
		name     string
		boundTo  int64
		euState  string
		usTasks  []*storage.Task
		blocker  bool
		want     cutoverRequestTurn
		wantSeen bool // whether CutoverBlocker was consulted for us
	}{
		{
			name:     "us parked and nothing earlier holds it takes the request",
			usTasks:  []*storage.Task{turnTask(turnUSOperationID, state.Task.WaitingForCutover)},
			want:     cutoverRequestTurn{ready: true},
			wantSeen: true,
		},
		{
			name:     "us already cutting over takes the request again after a lease handover",
			usTasks:  []*storage.Task{turnTask(turnUSOperationID, state.Task.CuttingOver)},
			want:     cutoverRequestTurn{ready: true},
			wantSeen: true,
		},
		{
			name:     "a request bound to us is taken like an unbound one",
			boundTo:  turnUSOperationID,
			usTasks:  []*storage.Task{turnTask(turnUSOperationID, state.Task.WaitingForCutover)},
			want:     cutoverRequestTurn{ready: true},
			wantSeen: true,
		},
		{
			name: "us still copying leaves the request even though eu is parked",
			usTasks: []*storage.Task{
				turnTask(turnEUOperationID, state.Task.WaitingForCutover),
				turnTask(turnUSOperationID, state.Task.Running),
			},
			want: cutoverRequestTurn{reason: "this member has not reached cutover"},
		},
		{
			name:    "us already completed has nothing left to cut over",
			usTasks: []*storage.Task{turnTask(turnUSOperationID, state.Task.Completed)},
			want:    cutoverRequestTurn{reason: "this member has not reached cutover"},
		},
		{
			name: "us with no tasks has nothing to cut over",
			want: cutoverRequestTurn{reason: "this member has not reached cutover"},
		},
		{
			name:     "us parked behind an earlier member that holds the cutover leaves the request",
			usTasks:  []*storage.Task{turnTask(turnUSOperationID, state.Task.WaitingForCutover)},
			blocker:  true,
			want:     cutoverRequestTurn{reason: "an earlier member holds the cutover"},
			wantSeen: true,
		},
		{
			name:    "a request bound to eu while eu can still cut over is left for eu",
			boundTo: turnEUOperationID,
			euState: state.ApplyOperation.WaitingForCutover,
			usTasks: []*storage.Task{turnTask(turnUSOperationID, state.Task.WaitingForCutover)},
			want:    cutoverRequestTurn{reason: "the request is bound to another member"},
		},
		{
			name:    "a request bound to eu after eu completed is settled as landed",
			boundTo: turnEUOperationID,
			euState: state.ApplyOperation.Completed,
			usTasks: []*storage.Task{turnTask(turnUSOperationID, state.Task.WaitingForCutover)},
			want:    cutoverRequestTurn{reason: "the member the request was bound to has cut over", settle: cutoverRequestLanded},
		},
		{
			name:    "a request bound to eu after eu failed is settled as ended",
			boundTo: turnEUOperationID,
			euState: state.ApplyOperation.Failed,
			usTasks: []*storage.Task{turnTask(turnUSOperationID, state.Task.WaitingForCutover)},
			want:    cutoverRequestTurn{reason: "the member the request was bound to ended without cutting over", settle: cutoverRequestEnded},
		},
		{
			name:    "a request bound to eu after eu stopped is settled as ended",
			boundTo: turnEUOperationID,
			euState: state.ApplyOperation.Stopped,
			usTasks: []*storage.Task{turnTask(turnUSOperationID, state.Task.WaitingForCutover)},
			want:    cutoverRequestTurn{reason: "the member the request was bound to ended without cutting over", settle: cutoverRequestEnded},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			euState := tt.euState
			if euState == "" {
				euState = state.ApplyOperation.WaitingForCutover
			}
			eu := turnMember(turnEUOperationID, "eu", euState)
			us := turnMember(turnUSOperationID, "us", state.ApplyOperation.WaitingForCutover)
			opStore := &turnDecisionOperationStore{ops: map[int64]*storage.ApplyOperation{eu.ID: eu, us.ID: us}}
			if tt.blocker {
				opStore.blocker = eu
			}
			store := &turnDecisionStorage{operations: opStore, tasks: &mockTaskStore{tasks: tt.usTasks}}

			turn, err := operationCutoverRequestTurn(t.Context(), store, apply, usDriveScope(us), cutoverRequest(tt.boundTo))
			require.NoError(t, err)

			want := tt.want
			if tt.blocker || (tt.boundTo != 0 && tt.boundTo != us.ID) {
				want.blocker = eu
			}
			assert.Equal(t, want, turn)
			if tt.wantSeen {
				assert.Equal(t, []int64{us.ID}, opStore.blockerAskedFor, "the cutover order is checked for this drive's own operation")
			} else {
				assert.Empty(t, opStore.blockerAskedFor, "the cutover order is not consulted when the decision is already made")
			}
		})
	}
}

// Every read the turn decision depends on fails closed. A drive that cannot
// tell whose turn it is gets an error and never a turn that lets it take the
// request, so an unreadable binding, a missing store, or a failed read can
// never send one operator command to the wrong member's cutover.
func TestOperationCutoverRequestTurnFailsClosed(t *testing.T) {
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-turn"}
	storageDown := errors.New("storage unavailable")
	parkedUS := []*storage.Task{turnTask(turnUSOperationID, state.Task.WaitingForCutover)}

	healthyOps := func() *turnDecisionOperationStore {
		return &turnDecisionOperationStore{ops: map[int64]*storage.ApplyOperation{
			turnEUOperationID: turnMember(turnEUOperationID, "eu", state.ApplyOperation.WaitingForCutover),
			turnUSOperationID: turnMember(turnUSOperationID, "us", state.ApplyOperation.WaitingForCutover),
		}}
	}
	unreadable := cutoverRequest(0)
	unreadable.Metadata = []byte(`{"apply_operation_id":`)

	tests := []struct {
		name    string
		store   func() storage.Storage
		request *storage.ApplyControlRequest
		wantErr string
		wantIs  error
	}{
		{
			name:    "no storage",
			store:   func() storage.Storage { return nil },
			request: cutoverRequest(0),
			wantErr: "storage is not available",
		},
		{
			name:    "no apply operation store",
			store:   func() storage.Storage { return &turnDecisionStorage{tasks: &mockTaskStore{tasks: parkedUS}} },
			request: cutoverRequest(0),
			wantErr: "apply operation store is not available",
		},
		{
			name:    "no task store",
			store:   func() storage.Storage { return &turnDecisionStorage{operations: healthyOps()} },
			request: cutoverRequest(0),
			wantErr: "task store is not available",
		},
		{
			name: "binding that does not parse",
			store: func() storage.Storage {
				return &turnDecisionStorage{operations: healthyOps(), tasks: &mockTaskStore{tasks: parkedUS}}
			},
			request: unreadable,
			wantErr: "read the operation the cutover request of apply apply-turn is bound to",
		},
		{
			name: "bound operation read fails",
			store: func() storage.Storage {
				ops := healthyOps()
				ops.getErr = storageDown
				return &turnDecisionStorage{operations: ops, tasks: &mockTaskStore{tasks: parkedUS}}
			},
			request: cutoverRequest(turnEUOperationID),
			wantErr: "load apply_operation 1 the cutover request of apply apply-turn is bound to",
			wantIs:  storageDown,
		},
		{
			name: "bound operation does not exist",
			store: func() storage.Storage {
				ops := healthyOps()
				delete(ops.ops, turnEUOperationID)
				return &turnDecisionStorage{operations: ops, tasks: &mockTaskStore{tasks: parkedUS}}
			},
			request: cutoverRequest(turnEUOperationID),
			wantErr: "cutover request of apply apply-turn is bound to apply_operation 1, which does not exist",
		},
		{
			name: "own tasks read fails",
			store: func() storage.Storage {
				return &turnDecisionStorage{operations: healthyOps(), tasks: &mockTaskStore{tasks: parkedUS, getByOperationIDErr: storageDown}}
			},
			request: cutoverRequest(0),
			wantErr: "load tasks for apply_operation 2 of apply apply-turn before cutover request",
			wantIs:  storageDown,
		},
		{
			name: "cutover order read fails",
			store: func() storage.Storage {
				ops := healthyOps()
				ops.blockerErr = storageDown
				return &turnDecisionStorage{operations: ops, tasks: &mockTaskStore{tasks: parkedUS}}
			},
			request: cutoverRequest(0),
			wantErr: "check cutover order for apply_operation 2 of apply apply-turn",
			wantIs:  storageDown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			us := turnMember(turnUSOperationID, "us", state.ApplyOperation.WaitingForCutover)

			turn, err := operationCutoverRequestTurn(t.Context(), tt.store(), apply, usDriveScope(us), tt.request)
			require.ErrorContains(t, err, tt.wantErr)
			if tt.wantIs != nil {
				require.ErrorIs(t, err, tt.wantIs)
			}
			assert.Equal(t, cutoverRequestTurn{}, turn, "a failed read must never yield a turn")
		})
	}
}
