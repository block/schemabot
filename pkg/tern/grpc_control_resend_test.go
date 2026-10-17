package tern

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func TestRemoteControlSendGate(t *testing.T) {
	gate := &remoteControlSendGate{}
	base := time.Now()
	stop := &storage.ApplyControlRequest{ID: 1, CreatedAt: base, UpdatedAt: base}
	cancel := &storage.ApplyControlRequest{ID: 2, CreatedAt: base, UpdatedAt: base}

	assert.True(t, gate.shouldSend(stop, base), "a request never sent by this process transmits immediately")
	assert.True(t, gate.recordSend(stop, base), "the first transmission is reported as first")

	assert.False(t, gate.shouldSend(stop, base.Add(time.Second)), "a just-sent request is not retransmitted within the interval")
	assert.True(t, gate.shouldSend(stop, base.Add(remoteControlResendInterval)), "the request retransmits once the interval elapses")
	assert.False(t, gate.recordSend(stop, base.Add(remoteControlResendInterval)), "a retransmission is not reported as first")

	assert.True(t, gate.shouldSend(cancel, base), "gating is per control request, not per process")

	gate.clear(stop.ID)
	assert.True(t, gate.shouldSend(stop, base.Add(2*time.Second)), "a cleared request transmits immediately again")
	assert.True(t, gate.recordSend(stop, base.Add(2*time.Second)), "a cleared request's next transmission is first again")
}

// A command resolved by another drive is never cleared from this process's
// gate, and re-issuing it re-arms the same request row. The re-issued command
// is the operator's new intent, so it transmits at once and is reported as a
// first send, not throttled behind the transmission of the command before it.
func TestRemoteControlSendGateTransmitsAReArmedRequestImmediately(t *testing.T) {
	gate := &remoteControlSendGate{}
	base := time.Now()
	first := &storage.ApplyControlRequest{ID: 1, CreatedAt: base, UpdatedAt: base}
	require.True(t, gate.recordSend(first, base))

	reArmed := &storage.ApplyControlRequest{ID: 1, CreatedAt: base, UpdatedAt: base.Add(4 * time.Second)}
	assert.True(t, gate.shouldSend(reArmed, base.Add(4*time.Second)), "a re-armed request transmits without waiting out the previous command's interval")
	assert.True(t, gate.recordSend(reArmed, base.Add(4*time.Second)), "a re-armed request's transmission is its first")
	assert.False(t, gate.shouldSend(reArmed, base.Add(5*time.Second)), "the re-armed request is then throttled on its own cadence")
}

// One gate is shared by every apply a process drives, so its three methods are
// reached concurrently by drives that have nothing else in common. The map
// behind it is only safe because they serialize.
func TestRemoteControlSendGateIsConcurrentlyAccessed(t *testing.T) {
	gate := &remoteControlSendGate{}
	base := time.Now()

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			req := &storage.ApplyControlRequest{ID: int64(i), CreatedAt: base}
			for range 50 {
				gate.shouldSend(req, base)
				gate.recordSend(req, base)
				gate.clear(req.ID)
			}
		})
	}
	wg.Wait()

	assert.True(t, gate.shouldSend(&storage.ApplyControlRequest{ID: 1, CreatedAt: base}, base), "every request was cleared, so each transmits immediately again")
}

// An accepted cancel is stored durably by the data plane and consumed by its
// own driver, so the control plane's progress-poll loop must not re-send the
// Cancel RPC (or append another operator-facing accept event) on every tick
// while the remote works to consume it — it retransmits on a bounded cadence
// and keeps polling progress in between.
func TestGRPCClient_PendingCancelRetransmissionThrottled(t *testing.T) {
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_RUNNING,
		progressStateSet: true,
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-cancel-throttle",
		ExternalID:      "remote-grpc-cancel-throttle",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
		CreatedAt:   time.Now(),
	}}}
	storedApply := *apply
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{},
		logs:            logs,
		controlRequests: controlRequests,
	}

	for range 3 {
		standDown, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
		require.NoError(t, err)
		assert.False(t, standDown, "a nonterminal remote keeps the cancel pending")
	}
	assert.Equal(t, 1, server.getCancelCalls(), "polling passes within the resend interval must not re-send the cancel")
	assert.Equal(t, 1, countLogMessages(logs.logs, "Remote cancel accepted"), "only the first transmission appends an operator-facing accept event")

	// Once the resend interval elapses the driver retransmits as redelivery
	// insurance — without appending another accept event.
	client.controlSendGate.mu.Lock()
	for id, sent := range client.controlSendGate.lastSent {
		sent.sentAt = time.Now().Add(-remoteControlResendInterval)
		client.controlSendGate.lastSent[id] = sent
	}
	client.controlSendGate.mu.Unlock()

	standDown, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err)
	assert.False(t, standDown)
	assert.Equal(t, 2, server.getCancelCalls(), "the cancel retransmits after the resend interval elapses")
	assert.Equal(t, 1, countLogMessages(logs.logs, "Remote cancel accepted"), "retransmissions must not append duplicate accept events")
}

// The stop counterpart: an accepted stop retransmits on the same bounded
// cadence while the remote drains, with a single operator-facing accept event.
func TestGRPCClient_PendingStopRetransmissionThrottled(t *testing.T) {
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_RUNNING,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.Running,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-stop-throttle",
		ExternalID:      "remote-grpc-stop-throttle",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
		CreatedAt:   time.Now(),
	}}}
	storedApply := *apply
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            logs,
		controlRequests: controlRequests,
	}

	for range 3 {
		standDown, err := client.processPendingStopControlRequest(t.Context(), apply, wholeApplyTaskScope())
		require.NoError(t, err)
		assert.False(t, standDown, "a nonterminal remote keeps the stop pending")
	}
	assert.Equal(t, 1, server.getStopCalls(), "polling passes within the resend interval must not re-send the stop")
	assert.Equal(t, 1, countLogMessages(logs.logs, "Remote stop accepted"), "only the first transmission appends an operator-facing accept event")
}

func countLogMessages(logs []*storage.ApplyLog, messagePrefix string) int {
	count := 0
	for _, log := range logs {
		if strings.HasPrefix(log.Message, messagePrefix) {
			count++
		}
	}
	return count
}

// A data plane that refuses a pending stop has made a decision, not dropped a
// delivery: the request is already recorded durably there, so re-sending it can
// only collect the same refusal. The drive resolves the request on the refusal
// and keeps the drive going, because the schema change is still running and
// no stop took effect.
func TestGRPCClient_RefusedStopResolvesTheRequest(t *testing.T) {
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_RUNNING,
		progressStateSet: true,
		stopRefusal:      "schema change remote-grpc-stop-refused is in the revert window and has already been applied: use revert to undo it or skip-revert to finalize it",
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-stop-refused",
		ExternalID:      "remote-grpc-stop-refused",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
		CreatedAt:   time.Now(),
	}}}
	storedApply := *apply
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{},
		logs:            logs,
		controlRequests: controlRequests,
	}

	standDown, err := client.processPendingStopControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err, "a refusal is an answer, not a drive failure")
	assert.False(t, standDown, "the schema change is still running, so the drive step must keep going")

	require.Len(t, controlRequests.requests, 1)
	refused := controlRequests.requests[0]
	assert.Equal(t, storage.ControlRequestFailed, refused.Status, "the refused request resolves instead of staying pending")
	assert.Contains(t, refused.ErrorMessage, "use revert to undo it or skip-revert to finalize it", "the operator reads the data plane's reason")
	assert.NotContains(t, refused.ErrorMessage, apply.ExternalID, "the durable record names the apply the operator knows")
	assert.Contains(t, refused.ErrorMessage, apply.ApplyIdentifier)
	require.Equal(t, 1, countLogMessages(logs.logs, "Pending stop request rejected by the data plane"),
		"the refusal is recorded on the apply for the operator")
	require.Equal(t, 0, countLogMessages(logs.logs, "Remote stop accepted"),
		"a refused stop is never also recorded to the operator as accepted")
	for _, entry := range logs.logs {
		assert.NotContains(t, entry.Message, apply.ExternalID,
			"the apply log renders into the PR timeline, so it names the apply the operator knows and not the remote id")
	}

	// A resolved request is not re-sent on the next claim.
	standDown, err = client.processPendingStopControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err)
	assert.False(t, standDown)
	assert.Equal(t, 1, server.getStopCalls(), "the refused request is not re-sent once it is resolved")
}

// The data plane's refusal decides that nothing stopped before the drive tries
// to record it, so a storage failure recording it changes what the operator can
// read, not what happened to their schema change. The drive is owed the same
// answer either way.
func TestGRPCClient_RefusedStopKeepsTheDriveGoingWhenResolvingTheRequestFails(t *testing.T) {
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_RUNNING,
		progressStateSet: true,
		stopRefusal:      "schema change remote-grpc-stop-refusal-write-fails is in the revert window and has already been applied: use revert to undo it or skip-revert to finalize it",
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-stop-refusal-write-fails",
		ExternalID:      "remote-grpc-stop-refusal-write-fails",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
		CreatedAt:   time.Now(),
	}}}
	storedApply := *apply
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{},
		logs:            &mockApplyLogStore{},
		controlRequests: &failPendingErrorStore{testControlRequestStore: controlRequests, err: errors.New("apply lease lost")},
	}

	standDown, err := client.processPendingStopControlRequest(t.Context(), apply, wholeApplyTaskScope())

	require.ErrorContains(t, err, "apply lease lost", "the storage failure must reach the caller")
	assert.False(t, standDown, "the refusal already decided nothing stopped, so a failed write must not stand the drive down")
	require.Len(t, controlRequests.requests, 1)
	assert.Equal(t, storage.ControlRequestPending, controlRequests.requests[0].Status,
		"the request could not be resolved, so it stays pending for a later claim")
}

// The cancel counterpart of the refused-stop case.
func TestGRPCClient_RefusedCancelResolvesTheRequest(t *testing.T) {
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_RUNNING,
		progressStateSet: true,
		cancelRefusal:    "schema change remote-grpc-cancel-refused is in the revert window and has already been applied: use revert to undo it or skip-revert to finalize it",
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-cancel-refused",
		ExternalID:      "remote-grpc-cancel-refused",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
		CreatedAt:   time.Now(),
	}}}
	storedApply := *apply
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{},
		logs:            logs,
		controlRequests: controlRequests,
	}

	standDown, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err, "a refusal is an answer, not a drive failure")
	assert.False(t, standDown, "the schema change is still running, so the drive step must keep going")

	require.Len(t, controlRequests.requests, 1)
	refused := controlRequests.requests[0]
	assert.Equal(t, storage.ControlRequestFailed, refused.Status, "the refused request resolves instead of staying pending")
	assert.Contains(t, refused.ErrorMessage, "use revert to undo it or skip-revert to finalize it", "the operator reads the data plane's reason")
	assert.NotContains(t, refused.ErrorMessage, apply.ExternalID, "the durable record names the apply the operator knows")
	assert.Contains(t, refused.ErrorMessage, apply.ApplyIdentifier)
	require.Equal(t, 1, countLogMessages(logs.logs, "Pending cancel request rejected by the data plane"),
		"the refusal is recorded on the apply for the operator")
	require.Equal(t, 0, countLogMessages(logs.logs, "Remote cancel accepted"),
		"a refused cancel is never also recorded to the operator as accepted")
	for _, entry := range logs.logs {
		assert.NotContains(t, entry.Message, apply.ExternalID,
			"the apply log renders into the PR timeline, so it names the apply the operator knows and not the remote id")
	}
}

// A refusal that arrives with no reason still has to read as an answer to the
// command the operator issued, not as a bare "not accepted".
func TestControlRefusalMessageNamesTheOperation(t *testing.T) {
	assert.Equal(t, "stop was refused with no reason given", controlRefusalMessage(storage.ControlOperationStop, ""))
	assert.Equal(t, "the engine is mid-cutover", controlRefusalMessage(storage.ControlOperationCancel, "the engine is mid-cutover"))
}

// An operation-only drive owns only its operation, so it cannot resolve the
// shared apply-level stop request: the operator projection does that once the
// parent derives terminal. A refusal there must still leave the drive step
// running — nothing stopped — and must be recorded on the send gate, so the
// request that stays pending by design is not re-transmitted on every later
// claim to collect the same refusal.
func TestGRPCClient_RefusedStopOnOperationOnlyDriveLeavesTheRequestPending(t *testing.T) {
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_RUNNING,
		progressStateSet: true,
		stopRefusal:      "schema change remote-grpc-stop-refused-operation is in the revert window and has already been applied: use revert to undo it or skip-revert to finalize it",
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-stop-refused-operation",
		ExternalID:      "remote-grpc-stop-refused-operation",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
		CreatedAt:   time.Now(),
	}}}
	storedApply := *apply
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{},
		logs:            logs,
		controlRequests: controlRequests,
	}

	scope := applyTaskScope{
		applyOperationID:   42,
		operation:          &storage.ApplyOperation{ID: 42, ApplyID: apply.ID, Deployment: "deployment-a"},
		operationLeaseOnly: true,
	}
	require.True(t, scope.suppressesDirectParentApplyWrites(), "the scope under test must be one that cannot write the parent apply")

	standDown, err := client.processPendingStopControlRequest(t.Context(), apply, scope)
	require.NoError(t, err, "a refusal is an answer, not a drive failure")
	assert.False(t, standDown, "nothing stopped, so the drive step must keep running")

	require.Len(t, controlRequests.requests, 1)
	assert.Equal(t, storage.ControlRequestPending, controlRequests.requests[0].Status,
		"the shared apply-level request stays pending for the operator projection")

	standDown, err = client.processPendingStopControlRequest(t.Context(), apply, scope)
	require.NoError(t, err)
	assert.False(t, standDown)
	assert.Equal(t, 1, server.getStopCalls(), "the refused request is not re-sent on the next claim")
}

// A queued start waiting behind an apply-level stop is deferred by an
// operation-only drive rather than waited on: the drive cannot resolve the
// shared stop, so spinning would hold the claim for a request it will never
// complete. A refusal on the way through must leave one account of that
// deferral in the log — an operator triaging a stuck start reads the same
// sentence twice as two drives when it is one.
func TestGRPCClient_RefusedStopBeforeStartLogsTheDeferralOnce(t *testing.T) {
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_RUNNING,
		progressStateSet: true,
		stopRefusal:      "schema change remote-grpc-stop-refused-before-start is in the revert window and has already been applied: use revert to undo it or skip-revert to finalize it",
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	var logged bytes.Buffer
	client.logger = slog.New(slog.NewTextHandler(&logged, nil))

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-stop-refused-before-start",
		ExternalID:      "remote-grpc-stop-refused-before-start",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	startReq := &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
		CreatedAt:   time.Now(),
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
		CreatedAt:   time.Now(),
	}, startReq}}
	storedApply := *apply
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	scope := applyTaskScope{
		applyOperationID:   42,
		operation:          &storage.ApplyOperation{ID: 42, ApplyID: apply.ID, Deployment: "deployment-a"},
		operationLeaseOnly: true,
	}

	deferred, err := client.waitForPendingStopBeforeStart(t.Context(), apply, scope, startReq)
	require.NoError(t, err, "a refusal is an answer, not a drive failure")
	assert.True(t, deferred, "the start defers to the operator while the apply-level stop is unresolved")
	assert.Equal(t, 1, strings.Count(logged.String(), "operation-only drive leaving apply-level stop request for operator projection"),
		"the deferral is accounted for once per pass, not once by the refusal and again by the caller")
}

// A rollout's operation drives leave the shared apply-level stop to the rollout
// projection, which completes it without touching any drive's send gate. When
// the operator then starts the rollout and stops it again, the second stop
// re-arms the same request row while the copy runs. That stop is a new
// command: the next drive pass transmits it, rather than holding it behind the
// first stop's resend interval while the copy it should halt keeps going.
func TestGRPCClient_ReissuedStopOnRolloutOperationTransmitsImmediately(t *testing.T) {
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_RUNNING,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "orders",
			Status:    state.Task.Running,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-stop-reissued",
		ExternalID:      "remote-grpc-stop-reissued",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ID:          1,
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
		CreatedAt:   time.Now(),
	}}}
	storedApply := *apply
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{},
		logs:            logs,
		controlRequests: controlRequests,
	}
	scope := applyTaskScope{
		applyOperationID:   42,
		operation:          &storage.ApplyOperation{ID: 42, ApplyID: apply.ID, Deployment: "deployment-a"},
		operationLeaseOnly: true,
	}
	require.True(t, scope.suppressesDirectParentApplyWrites(), "the scope under test must be one that cannot write the parent apply")

	_, err := client.processPendingStopControlRequest(t.Context(), apply, scope)
	require.NoError(t, err)
	require.Equal(t, 1, server.getStopCalls(), "the first stop transmits")

	// The rollout projection completes the first stop once every operation has
	// stopped; it holds no reference to this drive's gate.
	controlRequests.requests[0].Status = storage.ControlRequestCompleted
	_, alreadyPending, err := controlRequests.RequestPending(t.Context(), &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		RequestedBy: "cli:bob",
	})
	require.NoError(t, err)
	require.False(t, alreadyPending, "the second stop re-arms the resolved row")

	_, err = client.processPendingStopControlRequest(t.Context(), apply, scope)
	require.NoError(t, err)
	assert.Equal(t, 2, server.getStopCalls(), "the re-issued stop transmits on the next drive pass")
	assert.Equal(t, 2, countLogMessages(logs.logs, "Remote stop accepted"), "each operator stop is recorded as accepted once")

	_, err = client.processPendingStopControlRequest(t.Context(), apply, scope)
	require.NoError(t, err)
	assert.Equal(t, 2, server.getStopCalls(), "the re-issued stop is then retransmitted on its own bounded cadence")
}
