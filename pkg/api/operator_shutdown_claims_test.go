package api

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/storage"
)

// The two messages the retryable-expiry rung can log. They differ in full, not
// only in level, so a test that finds one has ruled out the other.
const (
	expireRetryableFailureMsg  = "operator: failed to expire retryable applies"
	expireRetryableShutdownMsg = expireRetryableFailureMsg + "; the operator is shutting down and a successor driver will retry the claim"
)

// claimLadderService wires a service whose first ladder rung is served by
// applies, capturing everything it logs at every level so a test can assert
// which level the rung chose.
func claimLadderService(applies storage.ApplyStore) (*Service, *bytes.Buffer) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return New(&mockStorageWithApplyStores{applies: applies}, testServerConfig(), nil, logger), &logs
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
// a routine deploy and not a fault: it must not page as an error, and it must
// not reach the claim-failure counter operators alert on.
func TestClaimFailureDuringShutdownIsNotReportedAsAFailure(t *testing.T) {
	reader := reaperMetricReader(t)
	svc, logs := claimLadderService(&expiringApplyStore{})

	svc.expireRetryableApplies(cancelledContext(t), 1)

	line := requireLogLine(t, decodeLogLines(t, logs.Bytes()), expireRetryableShutdownMsg)
	assert.Equal(t, "DEBUG", line["level"],
		"a claim cut short by shutdown must not be logged as an error")
	assert.Empty(t, claimFailureReasons(t, reader),
		"a claim cut short by shutdown must not tick the claim-failure counter")
}

// The shutdown exemption is keyed on the driver context rather than on the
// error, so a storage failure that lands while the operator is running keeps
// its own reason and stays an error an operator is paged for.
func TestClaimFailureWithALiveContextStaysAnError(t *testing.T) {
	reader := reaperMetricReader(t)
	svc, logs := claimLadderService(&expiringApplyStore{expireErr: errors.New("storage unavailable")})

	svc.expireRetryableApplies(t.Context(), 1)

	line := requireLogLine(t, decodeLogLines(t, logs.Bytes()), expireRetryableFailureMsg)
	assert.Equal(t, "ERROR", line["level"],
		"a storage failure on a running operator must stay an error")
	assert.Equal(t, []string{"expire_retryable_error"}, claimFailureReasons(t, reader),
		"a real claim failure must tick the counter under its own reason")
}

// A driver's select can pick a ready ticker over an equally ready ctx.Done(), so
// a tick can begin after the operator has been told to stop. Every claim it made
// would fail against the cancelled context, so the tick does not start the
// ladder at all.
func TestDriveTickSkipsTheClaimLadderAfterShutdown(t *testing.T) {
	applies := &expiringApplyStore{}
	svc, _ := claimLadderService(applies)

	svc.driveTick(cancelledContext(t), 1)

	assert.Zero(t, applies.calls, "a tick that starts after shutdown must not claim")
}
