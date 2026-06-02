package templates

import (
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/stretchr/testify/assert"
)

func TestRenderRepositoryNotRegistered(t *testing.T) {
	rendered := RenderRepositoryNotRegistered()
	assert.Contains(t, rendered, "**Repository not registered.**")
	assert.Contains(t, rendered, "`repos`")
	assert.Contains(t, rendered, "redeploy")
}

func TestRenderRollbackMissingArguments(t *testing.T) {
	rendered := RenderRollbackMissingArguments()
	assert.Contains(t, rendered, "## Missing Arguments")
	assert.Contains(t, rendered, "schemabot rollback <apply-id> -e <environment>")
	assert.Contains(t, rendered, "both an apply ID and the `-e` flag")
}

func TestRenderRollbackMissingEnv(t *testing.T) {
	rendered := RenderRollbackMissingEnv()
	assert.Contains(t, rendered, "## Missing Environment")
	assert.Contains(t, rendered, "schemabot rollback <apply-id> -e <environment>")
	assert.Contains(t, rendered, "The `-e` flag is required")
	assert.NotContains(t, rendered, "both an apply ID",
		"missing-env variant should not say both args are missing")
}

func TestRenderUnsupportedAutoConfirm(t *testing.T) {
	rendered := RenderUnsupportedAutoConfirm("plan")
	assert.Equal(t, "The `-y` flag is not supported for `plan`.", rendered)
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
		Status:      "already_requested",
	})
	assert.Contains(t, rendered, "Stop was already requested")
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

func TestRenderCommandNotYetAvailable(t *testing.T) {
	rendered := RenderCommandNotYetAvailable("stop", "staging")
	assert.Contains(t, rendered, "`stop` command is not yet available")
	assert.Contains(t, rendered, "Use the CLI instead")
	assert.Contains(t, rendered, "schemabot stop -e staging")
}
