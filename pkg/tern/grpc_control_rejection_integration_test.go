//go:build integration

package tern

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// setApplyEngine names the engine on a stored apply. The apply store's Update
// deliberately leaves the engine alone — it is fixed at apply creation — so a
// test whose stub engine stands in for a real one sets the column directly.
func setApplyEngine(t *testing.T, dsn string, applyID int64, engine string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
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
	client, eng := newVolumeControlClient(t, dsn, stor)
	eng.volumeErr = fmt.Errorf("throttle endpoint returned 404")

	apply := dispatchQueuedApplyWithOptions(t, stor, client, nil)

	volumeResp, err := client.Volume(ctx, &ternv1.VolumeRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
		Volume:      7,
	})
	require.NoError(t, err)
	require.True(t, volumeResp.Accepted, "acceptance means the request was queued, not that it took effect")

	driveQueuedApply(t, stor, client, apply.ApplyIdentifier)
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationVolume, storage.ControlRequestFailed)

	// Progress projects the apply's engine onto the response, so the row must
	// name a real one; the test engine stands in only for the Volume call.
	setApplyEngine(t, dsn, apply.ID, storage.EngineSpirit)

	progress, err := client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.Len(t, progress.SettledControlRequests, 1, "the failed volume request must travel on the progress response")

	settled := progress.SettledControlRequests[0]
	assert.Equal(t, string(storage.ControlOperationVolume), settled.Operation)
	assert.Equal(t, string(storage.ControlRequestFailed), settled.Status)
	assert.Contains(t, settled.ErrorMessage, "throttle endpoint returned 404")
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
	localClient, _ := newVolumeControlClient(t, dsn, stor)
	apply := dispatchQueuedApplyWithOptions(t, stor, localClient, nil)

	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	client.storage = stor

	rejection := []*ternv1.SettledControlRequest{{
		Operation:    string(storage.ControlOperationVolume),
		Status:       string(storage.ControlRequestFailed),
		ErrorMessage: "throttle endpoint returned 404",
		RequestedBy:  "cli:alice",
	}, {
		Operation: string(storage.ControlOperationCutover),
		Status:    string(storage.ControlRequestCompleted),
	}}

	client.mirrorRemoteControlRejections(ctx, apply, rejection)

	stored, err := stor.ControlRequests().GetByOperation(ctx, apply.ID, storage.ControlOperationVolume)
	require.NoError(t, err)
	require.NotNil(t, stored, "the control plane must hold the rejection even though it never queued the request itself")
	assert.Equal(t, storage.ControlRequestFailed, stored.Status)
	assert.Contains(t, stored.ErrorMessage, "throttle endpoint returned 404")
	assert.Equal(t, "cli:alice", stored.RequestedBy)

	completed, err := stor.ControlRequests().GetByOperation(ctx, apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	assert.Nil(t, completed, "a request that took effect needs no rejection row")

	logs, err := stor.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, 1, countLogMessages(logs, "Volume was accepted but not applied"),
		"the operator must find the rejection on the schema change's log")

	// The data plane reports the same rejection on every poll until the
	// operator retries the operation; mirroring it again must not append a
	// second entry.
	client.mirrorRemoteControlRejections(ctx, apply, rejection)
	logs, err = stor.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, 1, countLogMessages(logs, "Volume was accepted but not applied"),
		"a rejection already recorded must be surfaced exactly once")
}
