package templates

import (
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/stretchr/testify/assert"
)

func TestRenderRollbackMissingArguments(t *testing.T) {
	rendered := RenderRollbackMissingArguments()
	assert.Contains(t, rendered, "## Missing Arguments")
	assert.Contains(t, rendered, "`schemabot rollback <apply-id> -e <environment> [-t <tenant>]`")
	assert.Contains(t, rendered, "both an apply ID and the `-e` flag")
}

func TestRenderRollbackMissingEnv(t *testing.T) {
	rendered := RenderRollbackMissingEnv()
	assert.Contains(t, rendered, "## Missing Environment")
	assert.Contains(t, rendered, "`schemabot rollback <apply-id> -e <environment> [-t <tenant>]`")
	assert.Contains(t, rendered, "The `-e` flag is required")
	assert.NotContains(t, rendered, "both an apply ID",
		"missing-env variant should not say both args are missing")
}

func TestRenderUnsupportedAutoConfirm(t *testing.T) {
	rendered := RenderUnsupportedAutoConfirm("plan")
	assert.Equal(t, "The `-y` flag is not supported for `plan`.", rendered)
}

func TestRenderUnsupportedDatabaseFlag(t *testing.T) {
	rendered := RenderUnsupportedDatabaseFlag("rollback")
	assert.Equal(t, "The `-d` flag is not supported for `rollback`.", rendered)
}

func TestRenderUnsupportedDatabaseFlagRollbackConfirm(t *testing.T) {
	rendered := RenderUnsupportedDatabaseFlag("rollback-confirm")
	assert.Equal(t, "The `-d` flag is not supported for `rollback-confirm`.", rendered)
}

func TestRenderControlMissingApplyID(t *testing.T) {
	rendered := RenderControlMissingApplyID("stop")
	assert.Contains(t, rendered, "Missing Apply ID")
	assert.Contains(t, rendered, "schemabot stop <apply-id> -e <environment>")
	assert.Contains(t, rendered, "schemabot status")
}

func TestRenderStopCommandAccepted(t *testing.T) {
	rendered := RenderStopCommandAccepted(StopCommandAcceptedData{
		ApplyID:      "apply_abc123",
		Environment:  "staging",
		RequestedBy:  "alice",
		StoppedCount: 1,
		SkippedCount: 2,
	})
	assert.Contains(t, rendered, "Stop Request Accepted")
	assert.Contains(t, rendered, "`apply_abc123`")
	assert.Contains(t, rendered, "`staging`")
	assert.Contains(t, rendered, "@alice")
	assert.Contains(t, rendered, "1 stopped, 2 skipped")
}

func TestRenderStopCommandAcceptedAlreadyRequested(t *testing.T) {
	rendered := RenderStopCommandAccepted(StopCommandAcceptedData{
		ApplyID:     "apply_abc123",
		Environment: "staging",
		Status:      apitypes.ControlStatusAlreadyRequested,
	})
	assert.Contains(t, rendered, "Stop was already requested")
}

func TestRenderCancelCommandAccepted(t *testing.T) {
	rendered := RenderCancelCommandAccepted(CancelCommandAcceptedData{
		ApplyID:        "apply_abc123",
		Environment:    "staging",
		RequestedBy:    "alice",
		CancelledCount: 1,
		SkippedCount:   2,
	})
	assert.Contains(t, rendered, "Cancel Request Accepted")
	assert.Contains(t, rendered, "`apply_abc123`")
	assert.Contains(t, rendered, "`staging`")
	assert.Contains(t, rendered, "@alice")
	assert.Contains(t, rendered, "1 cancelled, 2 skipped")
}

func TestRenderCancelCommandAcceptedAlreadyRequested(t *testing.T) {
	rendered := RenderCancelCommandAccepted(CancelCommandAcceptedData{
		ApplyID:     "apply_abc123",
		Environment: "staging",
		Status:      apitypes.ControlStatusAlreadyRequested,
	})
	assert.Contains(t, rendered, "Cancel was already requested")
}

func TestRenderStartCommandAccepted(t *testing.T) {
	rendered := RenderStartCommandAccepted(StartCommandAcceptedData{
		ApplyID:      "apply_abc123",
		Environment:  "staging",
		RequestedBy:  "alice",
		StartedCount: 1,
		SkippedCount: 2,
	})
	assert.Contains(t, rendered, "Start Request Accepted")
	assert.Contains(t, rendered, "`apply_abc123`")
	assert.Contains(t, rendered, "`staging`")
	assert.Contains(t, rendered, "@alice")
	assert.Contains(t, rendered, "1 started, 2 skipped")
}

func TestRenderStartCommandAcceptedAlreadyRequested(t *testing.T) {
	rendered := RenderStartCommandAccepted(StartCommandAcceptedData{
		ApplyID:     "apply_abc123",
		Environment: "staging",
		Status:      apitypes.ControlStatusAlreadyRequested,
	})
	assert.Contains(t, rendered, "Start was already requested")
}

func TestRenderReleaseCommandAccepted(t *testing.T) {
	rendered := RenderReleaseCommandAccepted(ReleaseCommandAcceptedData{
		ApplyID:     "apply_abc123",
		Environment: "staging",
		RequestedBy: "alice",
	})
	assert.Contains(t, rendered, "Release Request Accepted")
	assert.Contains(t, rendered, "`apply_abc123`")
	assert.Contains(t, rendered, "`staging`")
	assert.Contains(t, rendered, "@alice")
	assert.Contains(t, rendered, "Release request accepted")
}

func TestRenderReleaseCommandAcceptedAlreadyRequested(t *testing.T) {
	rendered := RenderReleaseCommandAccepted(ReleaseCommandAcceptedData{
		ApplyID:     "apply_abc123",
		Environment: "staging",
		Status:      apitypes.ControlStatusAlreadyRequested,
	})
	assert.Contains(t, rendered, "Release was already requested")
}

func TestRenderCutoverCommandAccepted(t *testing.T) {
	rendered := RenderCutoverCommandAccepted(CutoverCommandAcceptedData{
		ApplyID:     "apply_abc123",
		Environment: "staging",
		RequestedBy: "alice",
	})
	assert.Contains(t, rendered, "Cutover Request Accepted")
	assert.Contains(t, rendered, "`apply_abc123`")
	assert.Contains(t, rendered, "`staging`")
	assert.Contains(t, rendered, "@alice")
	assert.Contains(t, rendered, "Cutover request accepted")
}

func TestRenderCutoverCommandAcceptedAlreadyInProgress(t *testing.T) {
	rendered := RenderCutoverCommandAccepted(CutoverCommandAcceptedData{
		ApplyID:     "apply_abc123",
		Environment: "staging",
		Status:      apitypes.ControlStatusAlreadyInProgress,
	})
	assert.Contains(t, rendered, "Cutover is already in progress")
}

// TestRenderVolumeCommandAccepted verifies the volume acknowledgement says the
// speed changes shortly — rather than claiming the change already took effect —
// and carries the apply id, environment, requester, and requested level.
func TestRenderVolumeCommandAccepted(t *testing.T) {
	rendered := RenderVolumeCommandAccepted(VolumeCommandAcceptedData{
		ApplyID:     "apply_abc123",
		Environment: "staging",
		RequestedBy: "alice",
		Volume:      8,
	})
	assert.Contains(t, rendered, "Volume Request Accepted")
	assert.Contains(t, rendered, "`apply_abc123`")
	assert.Contains(t, rendered, "`staging`")
	assert.Contains(t, rendered, "@alice")
	assert.Contains(t, rendered, "Volume change to 8 requested. SchemaBot will adjust the speed of this schema change shortly")
}

// TestRenderVolumeInvalidLevel verifies the rejection posted for a missing,
// non-numeric, or out-of-range -v value tells the user the exact syntax and
// the valid range.
func TestRenderVolumeInvalidLevel(t *testing.T) {
	rendered := RenderVolumeInvalidLevel()
	assert.Contains(t, rendered, "Missing or Invalid Volume Level")
	assert.Contains(t, rendered, "`schemabot volume <apply-id> -e <environment> -v <level>`")
	assert.Contains(t, rendered, "between 1 (slowest) and 11 (fastest)")
}

func TestRenderRevertCommandAccepted(t *testing.T) {
	rendered := RenderRevertCommandAccepted(RevertCommandAcceptedData{
		ApplyID:     "apply-957642f96d634694",
		Environment: "staging",
		RequestedBy: "alice",
	})
	assert.Contains(t, rendered, "Revert Request Accepted")
	assert.Contains(t, rendered, "`apply-957642f96d634694`")
	assert.Contains(t, rendered, "`staging`")
	assert.Contains(t, rendered, "@alice")
	assert.Contains(t, rendered, "SchemaBot will undo this schema change")
}
