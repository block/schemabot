package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

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
// control call carried.
type perOperationResumeStore struct {
	*exactProgressApplyOperationStore
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
			applyOperations: &perOperationResumeStore{&exactProgressApplyOperationStore{ops: []*storage.ApplyOperation{first, second}}},
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
