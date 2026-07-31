package tern

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The gRPC drive binds the apply's identity to a drive-scoped logger at the
// drive boundary, so every line of the remote drive inherits apply_id,
// database, environment, repo, and pr without hand-listing them per call. A
// client constructed without a base logger falls back to slog.Default so the
// drive never panics on a nil logger.
func TestGRPCApplyLogger_BindsIdentity(t *testing.T) {
	t.Run("bound logger carries the apply identity", func(t *testing.T) {
		var records []capturedLog
		client := &GRPCClient{logger: slog.New(captureHandler{records: &records})}

		apply := driveLoggerTestApply(state.Apply.Running)
		client.applyLogger(apply).Warn("remote drive line")

		line := requireCapturedLog(t, records, "remote drive line")
		assertLogCarriesApplyIdentity(t, line)
	})

	t.Run("nil base logger falls back to the default logger", func(t *testing.T) {
		client := &GRPCClient{}
		apply := driveLoggerTestApply(state.Apply.Running)
		require.NotNil(t, client.applyLogger(apply))
	})
}

// A failed drive-lease heartbeat is logged through the identity-bound drive
// logger, so the warning an operator sees when a peer can reclaim the work
// carries the apply identity plus the current mutable state without
// duplicated identity keys.
func TestDriveEndingHeartbeatFailure_LogsCarryApplyIdentity(t *testing.T) {
	apply := driveLoggerTestApply(state.Apply.Running)
	apply.ExternalID = "remote-apply-9"
	var records []capturedLog
	logger := slog.New(captureHandler{records: &records}).With(apply.IdentityLogAttrs()...)

	hbErr := errors.New("connection refused")
	lastSuccess := time.Now().Add(-storage.ApplyLeaseStaleAfter)
	require.Error(t, driveEndingHeartbeatFailure(logger, apply, hbErr, lastSuccess))

	line := requireCapturedLog(t, records,
		"gRPC drive heartbeat has failed for the full lease staleness window; a peer driver can reclaim the work, so this owner will stop driving and writing apply state")
	assertLogCarriesApplyIdentity(t, line)
	assert.Equal(t, state.Apply.Running, line.attrs["state"])
	assert.Equal(t, "remote-apply-9", line.attrs["external_id"])
	assert.Equal(t, hbErr, line.attrs["error"])
}

// An out-of-range remote volume level is rejected and logged through the
// identity-bound drive logger, so the warning carries the apply identity and
// the offending remote level an operator needs to triage the data plane.
func TestMirrorRemoteVolume_OutOfRangeLogCarriesApplyIdentity(t *testing.T) {
	apply := driveLoggerTestApply(state.Apply.Running)
	apply.Options = storage.MarshalApplyOptions(storage.ApplyOptions{Volume: 3})
	var records []capturedLog
	logger := slog.New(captureHandler{records: &records}).With(apply.IdentityLogAttrs()...)

	assert.False(t, mirrorRemoteVolume(logger, apply, 12))
	assert.Equal(t, 3, apply.GetOptions().Volume)

	line := requireCapturedLog(t, records,
		"remote progress reported an out-of-range volume level; keeping the stored level")
	assertLogCarriesApplyIdentity(t, line)
	assert.Equal(t, int64(12), line.attrs["remote_volume"])
}
