//go:build integration

package tern

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// newControlRequestClient builds a MySQL-type LocalClient backed by the shared
// container, for tests that queue and settle durable control requests rather
// than driving an apply to completion.
func newControlRequestClient(t *testing.T, dsn string, stor storage.Storage) *LocalClient {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })
	return client
}

// dispatchQueuedApplyWithOptions dispatches an apply with one table change and
// the given apply options through LocalClient.Apply, returning the stored
// apply row, still queued for the operator.
func dispatchQueuedApplyWithOptions(t *testing.T, stor storage.Storage, client *LocalClient, options map[string]string) *storage.Apply {
	t.Helper()
	ctx := t.Context()
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-control-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {Tables: []storage.TableChange{{
				Namespace: "testdb",
				Table:     "users",
				DDL:       "ALTER TABLE `users` ADD COLUMN control_note VARCHAR(255)",
				Operation: "alter",
			}}},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	resp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      plan.PlanIdentifier,
		Database:    "testdb",
		Type:        storage.DatabaseTypeMySQL,
		Environment: localClientTestEnvironment,
		Options:     options,
	})
	require.NoError(t, err)
	require.True(t, resp.Accepted)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, resp.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.Equal(t, state.Apply.Pending, apply.State, "a dispatched apply must be queued, not driven inline")
	return apply
}

// setApplyEngine names the engine on a stored apply. The apply store's Update
// deliberately leaves the engine alone — it is fixed at apply creation — so a
// test whose stub engine stands in for a real one sets the column directly.
func setApplyEngine(t *testing.T, dsn string, applyID int64, engine string) {
	t.Helper()
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err, "open database to set the apply engine")
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(t.Context(), "UPDATE applies SET engine = ? WHERE id = ?", engine, applyID)
	require.NoError(t, err, "set the apply engine")
}

// A control request the engine rejects is failed in the plane that queued it,
// and its progress response carries that fate. Without it the rejection would
// never leave the data plane: the operator was told the command was accepted,
// which means queued, and would otherwise keep believing it took effect.
func TestLocalClient_ProgressReportsSettledControlRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client := newControlRequestClient(t, dsn, stor)
	apply := dispatchQueuedApplyWithOptions(t, stor, client, nil)

	failLocallyQueuedControlRequest(t, dsn, stor, apply.ID, storage.ControlOperationCutover,
		"cli:alice", "deploy request is not in a cutover-ready state")
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCutover, storage.ControlRequestFailed)

	// Progress projects the apply's engine onto the response, so the row must
	// name a real one.
	setApplyEngine(t, dsn, apply.ID, storage.EngineSpirit)

	progress, err := client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.Len(t, progress.SettledControlRequests, 1, "the failed cutover request must travel on the progress response")

	settled := progress.SettledControlRequests[0]
	assert.Equal(t, string(storage.ControlOperationCutover), settled.Operation)
	assert.Equal(t, string(storage.ControlRequestFailed), settled.Status)
	assert.Contains(t, settled.ErrorMessage, "deploy request is not in a cutover-ready state")
	assert.NotEmpty(t, settled.SettledAt, "an operator triaging the rejection needs when it settled")
	assert.NotEmpty(t, settled.RequestedBy, "an operator triaging the rejection needs who issued the command")
}

// The control plane forwards a control RPC, reads back "accepted", and hears
// nothing more — the engine call happens later on the data plane's own driver
// tick. Mirroring the data plane's terminal rejection is what makes the failure
// reach the operator: it records the fate durably, warns once, and writes the
// apply log entry the operator reads on the schema change.
func TestGRPCClient_MirrorsRemoteControlRejection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	localClient := newControlRequestClient(t, dsn, stor)
	apply := dispatchQueuedApplyWithOptions(t, stor, localClient, nil)

	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	client.storage = stor

	rejection := []*ternv1.SettledControlRequest{{
		Operation:    string(storage.ControlOperationRevert),
		Status:       string(storage.ControlRequestFailed),
		ErrorMessage: "deploy request is outside its revert window",
		RequestedBy:  "cli:alice",
	}, {
		Operation: string(storage.ControlOperationCutover),
		Status:    string(storage.ControlRequestCompleted),
	}}

	client.mirrorRemoteControlRejections(ctx, apply, apply.ExternalID, rejection)

	stored, err := stor.ControlRequests().GetByOperation(ctx, apply.ID, storage.ControlOperationRevert)
	require.NoError(t, err)
	require.NotNil(t, stored, "the control plane must hold the rejection even though it never queued the request itself")
	assert.Equal(t, storage.ControlRequestFailed, stored.Status)
	assert.Contains(t, stored.ErrorMessage, "deploy request is outside its revert window")
	assert.Equal(t, "cli:alice", stored.RequestedBy)

	completed, err := stor.ControlRequests().GetByOperation(ctx, apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	assert.Nil(t, completed, "a request that took effect needs no rejection row")

	logs, err := stor.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, 1, countLogMessages(logs, "Revert was accepted but not applied"),
		"the operator must find the rejection on the schema change's log")

	// The data plane reports the same rejection on every poll until the
	// operator retries the operation; mirroring it again must not append a
	// second entry.
	client.mirrorRemoteControlRejections(ctx, apply, apply.ExternalID, rejection)
	logs, err = stor.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, 1, countLogMessages(logs, "Revert was accepted but not applied"),
		"a rejection already recorded must be surfaced exactly once")
}

// A rejection the data plane already settled describes an attempt that is over.
// If the operator has since issued the command again, the control plane holds a
// pending request that nothing has forwarded yet — and the row is what carries
// it, so overwriting it with the old rejection would drop the command silently:
// the drive looks for a pending request, finds none, and the operator waits for
// an effect that will never come.
func TestGRPCClient_MirrorLeavesAReissuedCommandPending(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	localClient := newControlRequestClient(t, dsn, stor)
	apply := dispatchQueuedApplyWithOptions(t, stor, localClient, nil)

	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	client.storage = stor

	// The operator issues cutover again after an earlier attempt was rejected.
	_, alreadyPending, err := stor.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCutover,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	// The data plane has not seen the new request yet, so it keeps reporting the
	// old one it settled.
	client.mirrorRemoteControlRejections(ctx, apply, apply.ExternalID, []*ternv1.SettledControlRequest{{
		Operation:    string(storage.ControlOperationCutover),
		Status:       string(storage.ControlRequestFailed),
		ErrorMessage: "cutover was not applied because apply is recovering",
		RequestedBy:  "cli:alice",
	}})

	pending, err := stor.ControlRequests().GetPending(ctx, apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	require.NotNil(t, pending, "the re-issued command must survive; nothing forwards it once it stops being pending")
	assert.Equal(t, storage.ControlRequestPending, pending.Status)
	assert.Empty(t, pending.ErrorMessage, "a live request must not carry the superseded attempt's reason")

	logs, err := stor.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, 0, countLogMessages(logs, "was accepted but not applied"),
		"a superseded rejection must not be reported against the command now in flight")
}

// The notice names who issued the command that did not take effect, so a second
// operator whose re-issued command is rejected for the same reason has to be
// reported under their own name. A rejection this plane never queued a request
// for reaches this: the mirrored row is the only record, so nothing else resets
// it.
func TestGRPCClient_MirrorReattributesARejectionToTheOperatorWhoReissuedIt(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	localClient := newControlRequestClient(t, dsn, stor)
	apply := dispatchQueuedApplyWithOptions(t, stor, localClient, nil)

	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	client.storage = stor

	rejection := func(requestedBy string) []*ternv1.SettledControlRequest {
		return []*ternv1.SettledControlRequest{{
			Operation:    string(storage.ControlOperationRevert),
			Status:       string(storage.ControlRequestFailed),
			ErrorMessage: "deploy request is outside its revert window",
			RequestedBy:  requestedBy,
		}}
	}

	client.mirrorRemoteControlRejections(ctx, apply, apply.ExternalID, rejection("cli:alice"))
	client.mirrorRemoteControlRejections(ctx, apply, apply.ExternalID, rejection("cli:bob"))

	stored, err := stor.ControlRequests().GetByOperation(ctx, apply.ID, storage.ControlOperationRevert)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, "cli:bob", stored.RequestedBy,
		"the rejection must name the operator whose command was rejected, not the earlier attempt's")
	assert.Equal(t, storage.ControlRequestFailed, stored.Status)
}

// A rejection this plane never queued a request for leaves the mirrored row as
// the only record the control plane holds, so no local request lifecycle will
// ever reset it. When the operator re-issues the command and it works, the data
// plane reports it completed — and the notice has to go, or the PR keeps warning
// about a command that has since taken effect.
func TestGRPCClient_MirrorRetiresARejectionTheDataPlaneLaterCompleted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	localClient := newControlRequestClient(t, dsn, stor)
	apply := dispatchQueuedApplyWithOptions(t, stor, localClient, nil)

	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	client.storage = stor

	client.mirrorRemoteControlRejections(ctx, apply, apply.ExternalID, []*ternv1.SettledControlRequest{{
		Operation:    string(storage.ControlOperationRevert),
		Status:       string(storage.ControlRequestFailed),
		ErrorMessage: "deploy request is outside its revert window",
		RequestedBy:  "cli:alice",
	}})
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationRevert, storage.ControlRequestFailed)

	// The operator re-issues revert; this time the data plane applies it.
	client.mirrorRemoteControlRejections(ctx, apply, apply.ExternalID, []*ternv1.SettledControlRequest{{
		Operation:   string(storage.ControlOperationRevert),
		Status:      string(storage.ControlRequestCompleted),
		RequestedBy: "cli:alice",
	}})

	stored, err := stor.ControlRequests().GetByOperation(ctx, apply.ID, storage.ControlOperationRevert)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, storage.ControlRequestCompleted, stored.Status,
		"a rejection the data plane has since completed must stop being reported")
	assert.Empty(t, stored.ErrorMessage, "the retired reason must not survive on a completed request")

	// The PR notice is built from the failed rows this returns, so a retired
	// rejection leaving the set is what clears it from the comment.
	settled, err := stor.ControlRequests().ListSettled(ctx, apply.ID)
	require.NoError(t, err)
	for _, req := range settled {
		assert.NotEqual(t, storage.ControlRequestFailed, req.Status,
			"operation %s still reads as rejected after the data plane completed it", req.Operation)
	}
}

// failLocallyQueuedControlRequest queues a control request as the operator who
// issued it and settles it failed, standing in for this plane queueing the
// command and its own drive rejecting it. FailPending needs the driving lease in
// context, which no client here holds, so the row is settled directly.
func failLocallyQueuedControlRequest(t *testing.T, dsn string, stor storage.Storage, applyID int64, operation storage.ControlOperation, requestedBy, errorMessage string) {
	t.Helper()
	_, _, err := stor.ControlRequests().RequestPending(t.Context(), &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   operation,
		Status:      storage.ControlRequestPending,
		RequestedBy: requestedBy,
		Metadata:    []byte(`{}`),
	})
	require.NoError(t, err, "queue the %s control request", operation)

	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err, "open database to settle the control request")
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(t.Context(), `
		UPDATE apply_control_requests
		SET status = ?, error_message = ?, completed_at = NOW()
		WHERE apply_id = ? AND operation = ?
	`, storage.ControlRequestFailed, errorMessage, applyID, operation)
	require.NoError(t, err, "settle the %s control request as failed", operation)
}

// A command this plane queued records the operator who issued it. The mirrored
// report of that same operation arrives over the control RPCs, which carry no
// operator identity, so its requester names the forwarding path. Recording the
// remote reason must not trade the operator's name for that one: the notice
// exists to tell an operator which of their commands did not take effect.
func TestGRPCClient_MirrorKeepsTheOperatorNameOnALocallyQueuedRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	localClient := newControlRequestClient(t, dsn, stor)
	apply := dispatchQueuedApplyWithOptions(t, stor, localClient, nil)

	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	client.storage = stor

	failLocallyQueuedControlRequest(t, dsn, stor, apply.ID, storage.ControlOperationCutover,
		"octocat", "cutover request was not applied because apply is failed")

	client.mirrorRemoteControlRejections(ctx, apply, apply.ExternalID, []*ternv1.SettledControlRequest{{
		Operation:    string(storage.ControlOperationCutover),
		Status:       string(storage.ControlRequestFailed),
		ErrorMessage: "deploy request is not in a cutover-ready state",
		RequestedBy:  storage.ForwardingControlRequestCaller,
	}})

	stored, err := stor.ControlRequests().GetByOperation(ctx, apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, "octocat", stored.RequestedBy,
		"the notice must keep naming the operator who issued the command, not the path the report arrived on")
	assert.Equal(t, "deploy request is not in a cutover-ready state", stored.ErrorMessage,
		"the remote reason is still the one the operator needs to read")
	assert.Equal(t, storage.ControlRequestFailed, stored.Status)
}

// An operator identity that does reach the mirror still re-attributes the
// rejection, so a command re-issued by someone else is not reported under the
// earlier operator's name.
func TestGRPCClient_MirrorReattributesToANamedOperator(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	localClient := newControlRequestClient(t, dsn, stor)
	apply := dispatchQueuedApplyWithOptions(t, stor, localClient, nil)

	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	client.storage = stor

	failLocallyQueuedControlRequest(t, dsn, stor, apply.ID, storage.ControlOperationCutover,
		"octocat", "cutover request was not applied because apply is failed")

	client.mirrorRemoteControlRejections(ctx, apply, apply.ExternalID, []*ternv1.SettledControlRequest{{
		Operation:    string(storage.ControlOperationCutover),
		Status:       string(storage.ControlRequestFailed),
		ErrorMessage: "deploy request is not in a cutover-ready state",
		RequestedBy:  "hubot",
	}})

	stored, err := stor.ControlRequests().GetByOperation(ctx, apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, "hubot", stored.RequestedBy,
		"a named operator on the report is the one whose command was rejected")
}
