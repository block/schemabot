package api

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The two messages a rung of the claim ladder can log when it fails. They
// differ in full, not only in level, so a test that finds one has ruled out the
// other.
const (
	claimOperationFailureMsg  = "operator: failed to claim apply_operation"
	claimOperationShutdownMsg = claimOperationFailureMsg + "; the operator is shutting down and a successor driver will retry the claim"
)

// claimLadderService wires a service whose apply-side rungs all find nothing,
// so the ladder falls through to the operation claim served by ops, capturing
// everything it logs at every level so a test can assert which level the rung
// chose.
func claimLadderService(ops *claimLadderOperationStore) (*Service, *bytes.Buffer) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	store := &mockStorageWithApplyStores{applies: &operationClaimApplyStore{}, operations: ops}
	return New(store, testServerConfig(), nil, logger), &logs
}

// cancelledContext returns a context that is already done, standing in for a
// driver whose operator has been told to shut down.
func cancelledContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	return ctx
}

// A shutdown cancels the driver context between polls, so whichever rung of the
// claim ladder is mid-query fails, and every rung behind it fails the same way
// moments later. A successor driver reclaims all of that work, so the ending is
// a routine deploy and not a fault: it must not surface as an error, and it must
// not reach the claim-failure counter operators alert on.
func TestClaimFailureDuringShutdownIsNotReportedAsAFailure(t *testing.T) {
	reader := reaperMetricReader(t)
	svc, logs := claimLadderService(&claimLadderOperationStore{recoverOperationStore: &recoverOperationStore{}})

	svc.recoverApplies(cancelledContext(t), 1)

	line := requireLogLine(t, decodeLogLines(t, logs.Bytes()), claimOperationShutdownMsg)
	assert.Equal(t, "DEBUG", line["level"],
		"a claim cut short by shutdown must not be logged as an error")
	assert.Empty(t, claimFailureReasons(t, reader),
		"a claim cut short by shutdown must not tick the claim-failure counter")
}

// The shutdown exemption is keyed on the driver context rather than on the
// error, so a storage failure that lands while the operator is running keeps
// its own reason and stays an error an operator is alerted on.
func TestClaimFailureWithALiveContextStaysAnError(t *testing.T) {
	reader := reaperMetricReader(t)
	svc, logs := claimLadderService(&claimLadderOperationStore{
		recoverOperationStore: &recoverOperationStore{},
		claimErr:              errors.New("storage unavailable"),
	})

	svc.recoverApplies(t.Context(), 1)

	line := requireLogLine(t, decodeLogLines(t, logs.Bytes()), claimOperationFailureMsg)
	assert.Equal(t, "ERROR", line["level"],
		"a storage failure on a running operator must stay an error")
	assert.Equal(t, []string{"operation_storage_error"}, claimFailureReasons(t, reader),
		"a real claim failure must tick the counter under its own reason")
}

// A driver's select can pick a ready ticker over an equally ready ctx.Done(), so
// a tick can begin after the operator has been told to stop. Every claim it made
// would fail against the cancelled context, so the tick does not start the
// ladder at all.
func TestDriveTickSkipsTheClaimLadderAfterShutdown(t *testing.T) {
	ops := &claimLadderOperationStore{recoverOperationStore: &recoverOperationStore{}}
	svc, _ := claimLadderService(ops)

	svc.driveTick(cancelledContext(t), 1)

	assert.Zero(t, ops.claims, "a tick that starts after shutdown must not claim")
}
