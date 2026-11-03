package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// perOperationResumeStore answers each operation's engine resume state with a
// context naming that operation, so a test can tell whose engine state a
// control call carried. blocker is the earlier member CutoverBlocker reports
// as holding every cutover, nil when nothing holds.
type perOperationResumeStore struct {
	*exactProgressApplyOperationStore
	blocker *storage.ApplyOperation
}

func (s *perOperationResumeStore) CutoverBlocker(context.Context, int64) (*storage.ApplyOperation, error) {
	return s.blocker, nil
}

func (s *perOperationResumeStore) GetEngineResumeState(_ context.Context, operationID int64) (*storage.EngineResumeState, error) {
	return &storage.EngineResumeState{ApplyOperationID: operationID, MigrationContext: fmt.Sprintf("ctx-op-%d", operationID)}, nil
}

// cutoverRecordingEngine records the resume context of every cutover it is
// asked to perform.
type cutoverRecordingEngine struct {
	*fakeControlEngine
	cutoverContexts []string
}

func (e *cutoverRecordingEngine) Cutover(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	resumeContext := ""
	if req.ResumeState != nil {
		resumeContext = req.ResumeState.MigrationContext
	}
	e.cutoverContexts = append(e.cutoverContexts, resumeContext)
	return e.fakeControlEngine.Cutover(ctx, req)
}

// sharedApplyFixture is one data-plane apply that two rollout member targets
// of a deployment were dispatched into, each as its own operation with one
// task: payments-001 is operation 11 and payments-002 is operation 12.
type sharedApplyFixture struct {
	client          *LocalClient
	apply           *storage.Apply
	first, second   *storage.ApplyOperation
	firstTask       *storage.Task
	secondTask      *storage.Task
	controlRequests *testControlRequestStore
	engine          *cutoverRecordingEngine
}

func newSharedApplyFixture(firstState, secondState, firstTaskState, secondTaskState string) *sharedApplyFixture {
	apply := &storage.Apply{
		ID:              300,
		ApplyIdentifier: "apply-shared-deployment",
		Database:        "payments",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Engine:          storage.EngineSpirit,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	first := &storage.ApplyOperation{ID: 11, ApplyID: apply.ID, Deployment: "default", Target: "payments-001", State: firstState}
	second := &storage.ApplyOperation{ID: 12, ApplyID: apply.ID, Deployment: "default", Target: "payments-002", State: secondState}
	task := func(id int64, op *storage.ApplyOperation, taskState string) *storage.Task {
		opID := op.ID
		return &storage.Task{
			ID: id, ApplyID: apply.ID, ApplyOperationID: &opID,
			TaskIdentifier: fmt.Sprintf("task-%s", op.Target), Database: "payments", Namespace: "payments",
			DatabaseType: storage.DatabaseTypeMySQL, Engine: storage.EngineSpirit,
			TableName: "users", DDLAction: "alter", State: taskState,
		}
	}
	firstTask, secondTask := task(501, first, firstTaskState), task(502, second, secondTaskState)
	controlRequests := &testControlRequestStore{}
	eng := &cutoverRecordingEngine{fakeControlEngine: &fakeControlEngine{}}
	client := &LocalClient{
		config: LocalConfig{Database: "payments", Type: storage.DatabaseTypeMySQL},
		storage: &exactProgressStorage{
			applies:         &exactProgressApplyStore{apply: apply},
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{firstTask, secondTask}},
			controlRequests: controlRequests,
			applyOperations: &perOperationResumeStore{exactProgressApplyOperationStore: &exactProgressApplyOperationStore{ops: []*storage.ApplyOperation{first, second}}},
		},
		spiritEngine: eng,
		logger:       slog.Default(),
	}
	return &sharedApplyFixture{
		client: client, apply: apply, first: first, second: second,
		firstTask: firstTask, secondTask: secondTask,
		controlRequests: controlRequests, engine: eng,
	}
}

// Two member targets of one deployment share its data-plane apply. Under
// `on_failure: continue`, payments-001 has failed and the apply record says
// so, while payments-002 is still copying. The control plane tracks each
// member by polling progress scoped to its own operation, so payments-002's
// answer carries only its own table and its own running state, and names the
// operation it answers for. Unscoped, the same apply reads as failed, which is
// what payments-002's drive would otherwise have recorded as its own verdict.
func TestLocalClient_ProgressScopedToOneRolloutMemberReportsOnlyThatMember(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.Failed, state.ApplyOperation.Running, state.Task.Failed, state.Task.Running)
	fx.apply.State = state.Apply.Failed
	fx.firstTask.TableName = "orders"

	scoped, err := fx.client.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: fx.apply.ApplyIdentifier, Environment: "staging", ApplyOperationId: "12"})
	require.NoError(t, err)
	assert.Equal(t, ternv1.State_STATE_RUNNING, scoped.State, "a sibling member's failure on the apply record must not read as this member's")
	assert.Equal(t, "12", scoped.ApplyOperationId, "the answer names the operation it answers for")
	require.Len(t, scoped.Tables, 1)
	assert.Equal(t, "users", scoped.Tables[0].TableName, "only this member's tables are reported")

	whole, err := fx.client.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: fx.apply.ApplyIdentifier, Environment: "staging"})
	require.NoError(t, err)
	assert.Equal(t, ternv1.State_STATE_FAILED, whole.State, "the whole apply still reports its recorded verdict")
	assert.Empty(t, whole.ApplyOperationId)
	assert.Len(t, whole.Tables, 2)
}

// Two member targets of one deployment share its data-plane apply, which halts
// on failure. payments-001 failed before payments-002 started, so the apply
// settled as failed with payments-002 still pending and unclaimed. Its scoped
// answer reports that it will never start, read from its operation row, and
// names the apply's settled state. A member a driver has claimed, or one that
// has started, may still be running with its tasks pending, and a stopped
// apply starts its pending members again, so neither reports it. Nor does one
// a claim that selected it before the apply settled went on to start: the
// verdict is confirmed under the row's lock, which waits for that claim.
func TestLocalClient_ProgressScopedToOneRolloutMemberReportsItWillNeverStart(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name          string
		applyState    string
		claim         func(op *storage.ApplyOperation)
		inFlightClaim bool
		neverStarts   bool
	}{
		{name: "unclaimed in a failed apply", applyState: state.Apply.Failed, neverStarts: true},
		{name: "claimed in a failed apply", applyState: state.Apply.Failed, claim: func(op *storage.ApplyOperation) { op.LeaseOwner = "driver-3" }},
		{name: "started in a failed apply", applyState: state.Apply.Failed, claim: func(op *storage.ApplyOperation) { op.StartedAt = &now }},
		{name: "started by a claim in flight when first read", applyState: state.Apply.Failed, inFlightClaim: true},
		{name: "unclaimed in a stopped apply", applyState: state.Apply.Stopped},
		{name: "unclaimed in a running apply", applyState: state.Apply.Running},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newSharedApplyFixture(state.ApplyOperation.Failed, state.ApplyOperation.Pending, state.Task.Failed, state.Task.Pending)
			fx.apply.State = tc.applyState
			if tc.claim != nil {
				tc.claim(fx.second)
			}
			if tc.inFlightClaim {
				started := *fx.second
				started.State, started.StartedAt, started.LeaseOwner = state.ApplyOperation.Running, &now, "driver-3"
				fx.client.storage.(*exactProgressStorage).applyOperations.(*perOperationResumeStore).claimedUnderLock = map[int64]*storage.ApplyOperation{fx.second.ID: &started}
			}

			scoped, err := fx.client.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: fx.apply.ApplyIdentifier, Environment: "staging", ApplyOperationId: "12"})
			require.NoError(t, err)
			assert.Equal(t, "12", scoped.ApplyOperationId)
			assert.Equal(t, ternv1.State_STATE_PENDING, scoped.State)
			assert.Equal(t, storageStateToProto(tc.applyState), scoped.ApplyState, "the answer names the state of the apply the member belongs to")
			assert.Equal(t, tc.neverStarts, scoped.OperationNeverStarts)
		})
	}

	fx := newSharedApplyFixture(state.ApplyOperation.Failed, state.ApplyOperation.Pending, state.Task.Failed, state.Task.Pending)
	fx.apply.State = state.Apply.Failed
	whole, err := fx.client.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: fx.apply.ApplyIdentifier, Environment: "staging"})
	require.NoError(t, err)
	assert.False(t, whole.OperationNeverStarts, "an answer for the whole apply speaks for no one operation")
}

// A progress request that names an operation this apply does not have is
// refused rather than answered for the whole apply, which would report the
// member's siblings' work as its own.
func TestLocalClient_ProgressRefusesAnOperationTheApplyDoesNotHave(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.Running, state.ApplyOperation.Running, state.Task.Running, state.Task.Running)
	fx.second.ApplyID = 999

	for _, tc := range []struct {
		name, operationID, want string
	}{
		{name: "operation of another apply", operationID: "12", want: "apply_operation 12 belongs to another apply"},
		{name: "operation that does not exist", operationID: "77", want: "apply_operation 77 does not exist"},
		{name: "not an operation id", operationID: "remote-op", want: `apply_operation_id "remote-op" is not an operation id`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := fx.client.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: fx.apply.ApplyIdentifier, ApplyOperationId: tc.operationID})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// A cutover of a data-plane apply shared by two member targets is recorded
// bound to the operation it names, so only that member swaps. One that names
// no operation is refused, since nothing says which target's swap the caller
// meant, and a cutover for payments-001 is refused while payments-002's is
// still pending rather than being folded into it.
func TestLocalClient_CutoverOfASharedApplyIsBoundToTheNamedMember(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.Running, state.ApplyOperation.WaitingForCutover, state.Task.Running, state.Task.WaitingForCutover)
	cutover := func(operationID string) *ternv1.CutoverResponse {
		resp, err := fx.client.Cutover(t.Context(), &ternv1.CutoverRequest{ApplyId: fx.apply.ApplyIdentifier, Environment: "staging", Caller: "cli:alice", ApplyOperationId: operationID})
		require.NoError(t, err)
		require.NotNil(t, resp)
		return resp
	}

	unbound := cutover("")
	assert.False(t, unbound.Accepted)
	assert.Equal(t, "apply apply-shared-deployment runs on several targets; a cutover must name the operation it is for", unbound.ErrorMessage)
	assert.Empty(t, fx.controlRequests.requests, "a refused cutover records no request")

	require.True(t, cutover("12").Accepted)
	require.Len(t, fx.controlRequests.requests, 1)
	boundTo, err := fx.controlRequests.requests[0].CutoverOperationID()
	require.NoError(t, err)
	assert.Equal(t, int64(12), boundTo, "the recorded request is bound to the member it named")

	sibling := cutover("11")
	assert.False(t, sibling.Accepted)
	assert.Equal(t, "a cutover of apply apply-shared-deployment for operation 12 is still pending; this cutover for operation 11 was not queued", sibling.ErrorMessage)

	assert.True(t, cutover("12").Accepted, "re-sending the pending member's cutover is idempotent")
	assert.Len(t, fx.controlRequests.requests, 1)
	assert.Empty(t, fx.engine.cutoverContexts, "queueing a cutover never swaps anything")
}

// A control plane cuts over both members of a shared data-plane apply with
// one command by listing both operations. The data plane records one request
// for the set, which each member's drive then takes for its own operation, so
// both members swap on one operator command. Every member's drive on the
// control plane sends the same list, so a resend naming the other member is
// the same request, while a cutover for one member alone is refused until the
// set settles rather than being folded into it.
func TestLocalClient_CutoverListingSeveralOperationsIsOneRequestForThemAll(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.WaitingForCutover, state.ApplyOperation.WaitingForCutover, state.Task.WaitingForCutover, state.Task.WaitingForCutover)
	cutover := func(operationID string, operationIDs ...string) *ternv1.CutoverResponse {
		resp, err := fx.client.Cutover(t.Context(), &ternv1.CutoverRequest{
			ApplyId: fx.apply.ApplyIdentifier, Environment: "staging", Caller: "cli:alice",
			ApplyOperationId: operationID, ApplyOperationIds: operationIDs,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		return resp
	}

	require.True(t, cutover("12", "12", "11").Accepted)
	require.Len(t, fx.controlRequests.requests, 1)
	boundTo, err := fx.controlRequests.requests[0].CutoverOperationIDs()
	require.NoError(t, err)
	assert.Equal(t, []int64{11, 12}, boundTo, "the recorded request is bound to every operation listed")
	first, err := fx.controlRequests.requests[0].CutoverOperationID()
	require.NoError(t, err)
	assert.Equal(t, int64(11), first, "a server that reads only the single-operation key takes the request for one listed operation")

	assert.True(t, cutover("11", "11", "12").Accepted, "another member's drive sending the same list is the same request")
	assert.Len(t, fx.controlRequests.requests, 1)

	alone := cutover("11")
	assert.False(t, alone.Accepted)
	assert.Equal(t, "a cutover of apply apply-shared-deployment for operations 11, 12 is still pending; this cutover for operation 11 was not queued", alone.ErrorMessage)
	assert.Len(t, fx.controlRequests.requests, 1)
	assert.Empty(t, fx.engine.cutoverContexts, "queueing a cutover never swaps anything")
}

// A control plane re-sends its cutover command to the data plane on every
// drive tick, carrying the same command id, until the set resolves. The data
// plane records the command once and echoes the id: a re-send while it is
// pending is the same request, a re-send after the engine failed the cutover
// is answered with that failure rather than re-arming the request, and a
// re-send after it completed is accepted without recording anything. Only a
// new command id records a new request.
func TestLocalClient_CutoverCommandIsRecordedOncePerCommandID(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.WaitingForCutover, state.ApplyOperation.WaitingForCutover, state.Task.WaitingForCutover, state.Task.WaitingForCutover)
	cutover := func(commandID string) *ternv1.CutoverResponse {
		resp, err := fx.client.Cutover(t.Context(), &ternv1.CutoverRequest{
			ApplyId: fx.apply.ApplyIdentifier, Environment: "staging", Caller: "tern-grpc",
			ApplyOperationId: "11", ApplyOperationIds: []string{"11", "12"}, CommandId: commandID,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		return resp
	}

	first := cutover("9.0")
	require.True(t, first.Accepted)
	assert.Equal(t, "9.0", first.CommandId, "the data plane echoes the command id it recorded")
	require.Len(t, fx.controlRequests.requests, 1)
	recorded := fx.controlRequests.requests[0]
	commandID, err := recorded.ControlCommandID()
	require.NoError(t, err)
	assert.Equal(t, "9.0", commandID)
	boundTo, err := recorded.CutoverOperationIDs()
	require.NoError(t, err)
	assert.Equal(t, []int64{11, 12}, boundTo, "recording the command keeps the operations the request is bound to")

	assert.True(t, cutover("9.0").Accepted, "a re-send while the command is pending is the same request")
	assert.Equal(t, int64(0), recorded.Revision)

	recorded.Status = storage.ControlRequestFailed
	recorded.ErrorMessage = "cutover reached 1 of the 2 operations it was sent for; engine refused the cutover"
	afterFailure := cutover("9.0")
	assert.False(t, afterFailure.Accepted, "a re-send of a failed command is answered with the failure")
	assert.Equal(t, "cutover reached 1 of the 2 operations it was sent for; engine refused the cutover", afterFailure.ErrorMessage)
	assert.Equal(t, "9.0", afterFailure.CommandId)
	assert.Equal(t, storage.ControlRequestFailed, recorded.Status, "the failed command is not re-armed")
	assert.Equal(t, int64(0), recorded.Revision)

	recorded.Status = storage.ControlRequestCompleted
	recorded.ErrorMessage = ""
	assert.True(t, cutover("9.0").Accepted, "a re-send of a completed command is accepted")
	assert.Equal(t, storage.ControlRequestCompleted, recorded.Status, "the completed command is not re-armed")

	next := cutover("9.1")
	assert.True(t, next.Accepted)
	assert.Equal(t, storage.ControlRequestPending, recorded.Status, "a new command records a new request")
	assert.Equal(t, int64(1), recorded.Revision)
	assert.Empty(t, fx.engine.cutoverContexts, "queueing a cutover never swaps anything")
}

// A cutover list is refused before anything is recorded when it names an
// operation the apply does not have, names one twice, or names a single
// operation outside the list, which a server reading only that field would cut
// over in place of the listed ones.
func TestLocalClient_CutoverListIsRefusedWhenItDoesNotNameTheApplysOperations(t *testing.T) {
	for _, tc := range []struct {
		name         string
		operationID  string
		operationIDs []string
		want         string
	}{
		{name: "operation the apply does not have", operationID: "11", operationIDs: []string{"11", "77"}, want: "cutover names an operation this apply does not have: apply_operation 77 does not exist"},
		{name: "operation listed twice", operationID: "11", operationIDs: []string{"11", "11"}, want: "cutover names operation 11 more than once"},
		{name: "single operation outside the list", operationID: "12", operationIDs: []string{"11"}, want: "cutover names operation 12 apart from the operations it lists"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newSharedApplyFixture(state.ApplyOperation.WaitingForCutover, state.ApplyOperation.WaitingForCutover, state.Task.WaitingForCutover, state.Task.WaitingForCutover)
			resp, err := fx.client.Cutover(t.Context(), &ternv1.CutoverRequest{
				ApplyId: fx.apply.ApplyIdentifier, Environment: "staging",
				ApplyOperationId: tc.operationID, ApplyOperationIds: tc.operationIDs,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.False(t, resp.Accepted)
			assert.Equal(t, tc.want, resp.ErrorMessage)
			assert.Empty(t, fx.controlRequests.requests, "a refused cutover records no request")
		})
	}
}

// A pending cutover bound to payments-002 is left alone by payments-001's
// drive even though payments-001 is parked at its own cutover too, and is
// taken by payments-002's drive, which swaps payments-002 alone: the engine
// is handed payments-002's resume state, not that of the first parked task of
// the apply.
func TestLocalClient_BoundCutoverIsTakenOnlyByItsOwnMembersDrive(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.WaitingForCutover, state.ApplyOperation.WaitingForCutover, state.Task.WaitingForCutover, state.Task.WaitingForCutover)
	fx.controlRequests.requests = []*storage.ApplyControlRequest{{
		ID: 1, ApplyID: fx.apply.ID, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending,
		RequestedBy: "cli:alice", Metadata: storage.CutoverRequestMetadata(fx.second.ID),
	}}

	require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.firstTask}))
	assert.Empty(t, fx.engine.cutoverContexts, "a sibling member's drive must not take the request")
	assert.Equal(t, storage.ControlRequestPending, fx.controlRequests.requests[0].Status)

	require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.secondTask}))
	assert.Equal(t, []string{"ctx-op-12"}, fx.engine.cutoverContexts, "the bound member's drive swaps that member alone")
	assert.Equal(t, storage.ControlRequestCompleted, fx.controlRequests.requests[0].Status)
}

// An operator's cutover for payments-002 was accepted and bound to it while
// payments-001 was still copying, and payments-001 then failed under
// `on_failure: halt` before payments-002's drive took the request. The drive
// re-checks the hold when it takes the request, so payments-002 does not cut
// over past the failure: the request stays pending, and is taken once nothing
// holds payments-002 any longer.
func TestLocalClient_BoundCutoverIsHeldByAnEarlierMemberThatFailedAfterAcceptance(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.Failed, state.ApplyOperation.WaitingForCutover, state.Task.Failed, state.Task.WaitingForCutover)
	fx.controlRequests.requests = []*storage.ApplyControlRequest{{
		ID: 1, ApplyID: fx.apply.ID, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending,
		RequestedBy: "cli:alice", Metadata: storage.CutoverRequestMetadata(fx.second.ID),
	}}
	opStore := fx.client.storage.(*exactProgressStorage).applyOperations.(*perOperationResumeStore)
	opStore.blocker = fx.first

	require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.secondTask}))
	assert.Empty(t, fx.engine.cutoverContexts, "a held member must not cut over past the earlier failure")
	assert.Equal(t, storage.ControlRequestPending, fx.controlRequests.requests[0].Status, "the acknowledged request waits for the hold to clear")

	opStore.blocker = nil
	require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.secondTask}))
	assert.Equal(t, []string{"ctx-op-12"}, fx.engine.cutoverContexts, "once nothing holds it, the bound member's drive takes the request")
	assert.Equal(t, storage.ControlRequestCompleted, fx.controlRequests.requests[0].Status)
}

// A pending cutover bound to a member that is still copying waits for that
// member to park, even when its own drive is the one looking.
func TestLocalClient_BoundCutoverWaitsForItsMemberToPark(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.WaitingForCutover, state.ApplyOperation.Running, state.Task.WaitingForCutover, state.Task.Running)
	fx.controlRequests.requests = []*storage.ApplyControlRequest{{
		ID: 1, ApplyID: fx.apply.ID, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending,
		RequestedBy: "cli:alice", Metadata: storage.CutoverRequestMetadata(fx.second.ID),
	}}

	require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.secondTask}))
	assert.Empty(t, fx.engine.cutoverContexts)
	assert.Equal(t, storage.ControlRequestPending, fx.controlRequests.requests[0].Status)
}

// Once the member a pending cutover is bound to has ended, the next drive of
// the apply settles the request instead of leaving it pending forever: it is
// completed when the member completed, which it can only have done by cutting
// over, and failed when the member ended any other way.
func TestLocalClient_BoundCutoverSettlesOnceItsMemberHasEnded(t *testing.T) {
	for _, tc := range []struct {
		name       string
		boundState string
		want       storage.ControlRequestStatus
	}{
		{name: "member completed", boundState: state.ApplyOperation.Completed, want: storage.ControlRequestCompleted},
		{name: "member failed", boundState: state.ApplyOperation.Failed, want: storage.ControlRequestFailed},
		{name: "member cancelled", boundState: state.ApplyOperation.Cancelled, want: storage.ControlRequestFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newSharedApplyFixture(state.ApplyOperation.WaitingForCutover, tc.boundState, state.Task.WaitingForCutover, state.Task.Completed)
			fx.controlRequests.requests = []*storage.ApplyControlRequest{{
				ID: 1, ApplyID: fx.apply.ID, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending,
				RequestedBy: "cli:alice", Metadata: storage.CutoverRequestMetadata(fx.second.ID),
			}}

			require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.firstTask}))
			assert.Empty(t, fx.engine.cutoverContexts, "settling a request never swaps the sibling")
			assert.Equal(t, tc.want, fx.controlRequests.requests[0].Status)
		})
	}
}

// A cutover naming payments-002's operation while storage cannot be read is a
// failure to decide, not a refusal: the caller gets an error it can retry and
// no request is recorded, where a refusal would tell the operator the
// operation does not exist. An id that names no operation of the apply is
// still refused.
func TestLocalClient_CutoverThatCannotReadItsOperationIsAnErrorNotARefusal(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.Running, state.ApplyOperation.WaitingForCutover, state.Task.Running, state.Task.WaitingForCutover)
	cutover := func(operationID string) (*ternv1.CutoverResponse, error) {
		return fx.client.Cutover(t.Context(), &ternv1.CutoverRequest{ApplyId: fx.apply.ApplyIdentifier, Environment: "staging", Caller: "cli:alice", ApplyOperationId: operationID})
	}

	unknown, err := cutover("77")
	require.NoError(t, err)
	require.NotNil(t, unknown)
	assert.False(t, unknown.Accepted)
	assert.Equal(t, "cutover names an operation this apply does not have: apply_operation 77 does not exist", unknown.ErrorMessage)

	fx.client.storage.(*exactProgressStorage).applyOperations.(*perOperationResumeStore).err = errors.New("storage read timed out")
	resp, err := cutover("12")
	require.Error(t, err, "a storage failure must reach the caller as an error it can retry")
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "resolve the operation a cutover of apply apply-shared-deployment names")
	assert.Contains(t, err.Error(), "storage read timed out")
	assert.Empty(t, fx.controlRequests.requests, "neither call records a request")
}

// payments-002 is parked at its cutover with an acknowledged cutover bound to
// it when the shared apply goes into recovery after a restart. Its drive
// leaves the request pending rather than failing it on the cutover path's
// recovering refusal, and takes it once recovery has finished, so the
// operator never re-issues a cutover that was already acknowledged.
func TestLocalClient_BoundCutoverWaitsForTheApplyToRecover(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.WaitingForCutover, state.ApplyOperation.WaitingForCutover, state.Task.WaitingForCutover, state.Task.WaitingForCutover)
	fx.apply.State = state.Apply.Recovering
	fx.controlRequests.requests = []*storage.ApplyControlRequest{{
		ID: 1, ApplyID: fx.apply.ID, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending,
		RequestedBy: "cli:alice", Metadata: storage.CutoverRequestMetadata(fx.second.ID),
	}}

	require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.secondTask}))
	assert.Empty(t, fx.engine.cutoverContexts, "nothing swaps while the apply is recovering")
	assert.Equal(t, storage.ControlRequestPending, fx.controlRequests.requests[0].Status, "the acknowledged cutover must wait for recovery, not fail")

	fx.apply.State = state.Apply.WaitingForCutover
	require.NoError(t, fx.client.processPendingCutoverControlRequest(t.Context(), fx.apply, []*storage.Task{fx.secondTask}))
	assert.Equal(t, []string{"ctx-op-12"}, fx.engine.cutoverContexts, "the bound member swaps once recovery has finished")
	assert.Equal(t, storage.ControlRequestCompleted, fx.controlRequests.requests[0].Status)
}

// plansByIDStore answers GetByID for the plans it holds, and nil for any
// other id, recording every id asked for.
type plansByIDStore struct {
	storage.PlanStore
	plans     map[int64]*storage.Plan
	requested []int64
}

func (s *plansByIDStore) GetByID(_ context.Context, id int64) (*storage.Plan, error) {
	s.requested = append(s.requested, id)
	return s.plans[id], nil
}

// memberPlanOnly stores only plan 8, the plan payments-002's tasks were built
// from, while the apply they attached to names plan 7, the plan of the target
// dispatched first.
func memberPlanOnly(client *LocalClient, tasks ...*storage.Task) *plansByIDStore {
	for _, task := range tasks {
		task.PlanID = 8
	}
	plans := &plansByIDStore{plans: map[int64]*storage.Plan{8: {ID: 8, Target: "payments-002", SchemaFiles: schema.SchemaFiles{}}}}
	client.storage.(*exactProgressStorage).plans = plans
	return plans
}

// payments-002 attached its grouped work to the apply payments-001's dispatch
// created, so the apply names payments-001's plan. When the engine loses
// payments-002's work, the drive verifies the target against the schema set
// payments-002's tasks were built from, never payments-001's.
func TestPollForCompletionAtomic_LostEngineWorkVerifiesAgainstTheTasksOwnPlan(t *testing.T) {
	eng := &lostWorkEngine{
		phaseSequenceEngine: phaseSequenceEngine{results: []*engine.ProgressResult{{State: engine.StatePending}}},
		planResult:          &engine.PlanResult{NoChanges: true},
	}
	client, apply, tasks, _ := lostWorkAtomicPollFixture(eng, lostWorkTrustBudgetReached)
	plans := memberPlanOnly(client, tasks...)

	require.NoError(t, client.pollForCompletionAtomic(t.Context(), apply, tasks, nil, nil, map[string]string{}, false))

	assert.Equal(t, []int64{8}, plans.requested, "the verification loads the plan the tasks were built from")
	assert.Equal(t, state.Apply.Completed, apply.State)
	for _, task := range tasks {
		assert.Equal(t, state.Task.Completed, task.State, "table %s", task.TableName)
	}
}

// The sequential counterpart: payments-002's lost task is verified against its
// own plan, not the plan of the apply it attached to.
func TestPollTaskToCompletion_LostEngineWorkVerifiesAgainstTheTasksOwnPlan(t *testing.T) {
	eng := &lostWorkEngine{
		phaseSequenceEngine: phaseSequenceEngine{results: []*engine.ProgressResult{{State: engine.StatePending}}},
		planResult:          &engine.PlanResult{NoChanges: true},
	}
	client, apply, task, _ := lostWorkPollFixture(eng, lostWorkTrustBudgetReached)
	task.Shard = ""
	plans := memberPlanOnly(client, task)

	assert.Equal(t, taskContinue, client.pollTaskToCompletion(t.Context(), apply, task, nil, nil))

	assert.Equal(t, []int64{8}, plans.requested, "the verification loads the plan the task was built from")
	assert.Equal(t, state.Task.Completed, task.State)
}

// Tasks built from different plans are never judged against one of them, and
// tasks that record no plan are judged against the apply's.
func TestPlanIDForTasks(t *testing.T) {
	apply := &storage.Apply{ApplyIdentifier: "apply-shared-deployment", PlanID: 7}
	task := func(planID int64) *storage.Task { return &storage.Task{PlanID: planID} }

	got, err := planIDForTasks(apply, []*storage.Task{task(8), task(8)})
	require.NoError(t, err)
	assert.Equal(t, int64(8), got)

	got, err = planIDForTasks(apply, []*storage.Task{task(0)})
	require.NoError(t, err)
	assert.Equal(t, int64(7), got)

	_, err = planIDForTasks(apply, []*storage.Task{task(8), task(9)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tasks of apply apply-shared-deployment were built from different plans (8 and 9)")

	_, err = planIDForTasks(&storage.Apply{ApplyIdentifier: "apply-without-plan"}, []*storage.Task{task(0)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "neither the tasks nor apply apply-without-plan name a plan")
}

// A control plane that orders a rollout's cutovers defers each operation's
// cutover on the data plane, so the data plane's apply carries the deferral,
// and then sends one operation's cutover at a time. The data plane queues that
// cutover for the operation it names while a sibling is still copying, rather
// than waiting for every operation as an operator's --defer-cutover would.
func TestLocalClient_CutoverNamingOneOperationIsQueuedWhileASiblingCopies(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.WaitingForCutover, state.ApplyOperation.Running, state.Task.WaitingForCutover, state.Task.Running)
	fx.apply.Options = storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true})

	resp, err := fx.client.Cutover(t.Context(), &ternv1.CutoverRequest{ApplyId: fx.apply.ApplyIdentifier, Environment: "staging", Caller: "cli:alice", ApplyOperationId: "11"})
	require.NoError(t, err)
	assert.True(t, resp.Accepted, resp.GetErrorMessage())
	require.Len(t, fx.controlRequests.requests, 1)
	assert.Equal(t, storage.ControlRequestPending, fx.controlRequests.requests[0].Status)
	boundIDs, err := fx.controlRequests.requests[0].CutoverOperationIDs()
	require.NoError(t, err)
	assert.Equal(t, []int64{11}, boundIDs, "the cutover is bound to the operation it names")
}
