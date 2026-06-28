package templates

import (
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
)

// RenderRollbackMissingArguments renders the message posted when `schemabot rollback`
// is invoked without an apply ID and without an `-e` flag.
func RenderRollbackMissingArguments() string {
	return "## Missing Arguments\n\n" +
		"Usage: `schemabot rollback <apply-id> -e <environment>`\n\n" +
		"Rollback requires both an apply ID and the `-e` flag to select the target environment."
}

// RenderRollbackMissingEnv renders the message posted when `schemabot rollback`
// is invoked with an apply ID but no `-e` flag. Distinct from RenderMissingEnv —
// the rollback variant tailors the example usage to rollback semantics.
func RenderRollbackMissingEnv() string {
	return "## Missing Environment\n\n" +
		"Usage: `schemabot rollback <apply-id> -e <environment>`\n\n" +
		"The `-e` flag is required to select the target environment."
}

// RenderUnsupportedAutoConfirm renders the message posted when the `-y` /
// `--yes` auto-confirm flag is supplied to a command that does not support it.
func RenderUnsupportedAutoConfirm(action string) string {
	return fmt.Sprintf("The `-y` flag is not supported for `%s`.", action)
}

// RenderUnsupportedDatabaseFlag renders the message posted when `-d` is
// supplied to a command that does not support database scoping.
func RenderUnsupportedDatabaseFlag(action string) string {
	return fmt.Sprintf("The `-d` flag is not supported for `%s`.", action)
}

// StopCommandAcceptedData contains data for a PR comment stop acknowledgement.
type StopCommandAcceptedData struct {
	ApplyID      string
	Environment  string
	RequestedBy  string
	Status       string
	StoppedCount int64
	SkippedCount int64
}

// StartCommandAcceptedData contains data for a PR comment start acknowledgement.
type StartCommandAcceptedData struct {
	ApplyID      string
	Environment  string
	RequestedBy  string
	Status       string
	StartedCount int64
	SkippedCount int64
}

// CutoverCommandAcceptedData contains data for a PR comment cutover acknowledgement.
type CutoverCommandAcceptedData struct {
	ApplyID     string
	Environment string
	RequestedBy string
	Status      string
}

// RenderControlMissingApplyID renders the message posted when an apply-scoped
// control command is invoked without the required apply ID.
func RenderControlMissingApplyID(action string) string {
	return fmt.Sprintf("## Missing Apply ID\n\n"+
		"Usage: `schemabot %s <apply-id> -e <environment>`\n\n"+
		"Use `schemabot status -e <environment>` to find the apply ID.", action)
}

// RenderStopCommandAccepted renders the acknowledgement posted when a PR
// comment stop command records durable stop intent.
func RenderStopCommandAccepted(data StopCommandAcceptedData) string {
	statusLine := "Stop request accepted. SchemaBot will stop this schema change; status remains available from the PR progress comment or CLI."
	if data.Status == apitypes.ControlStatusAlreadyRequested {
		statusLine = "Stop was already requested. SchemaBot will keep the existing stop request pending until the operator owner finishes it."
	}

	body := "## Stop Request Accepted\n\n" +
		fmt.Sprintf("**Apply**: `%s`\n", data.ApplyID) +
		fmt.Sprintf("**Environment**: `%s`\n", data.Environment)
	if data.RequestedBy != "" {
		body += fmt.Sprintf("**Requested by**: @%s\n", data.RequestedBy)
	}
	body += "\n" + statusLine + "\n"
	if data.StoppedCount > 0 || data.SkippedCount > 0 {
		body += fmt.Sprintf("\n**Tasks selected for stop**: %d stopped, %d skipped.\n", data.StoppedCount, data.SkippedCount)
	}
	return body
}

// RenderStartCommandAccepted renders the acknowledgement posted when a PR
// comment start command records durable start intent.
func RenderStartCommandAccepted(data StartCommandAcceptedData) string {
	statusLine := "Start request accepted. SchemaBot will resume this schema change; status remains available from the PR progress comment or CLI."
	if data.Status == apitypes.ControlStatusAlreadyRequested {
		statusLine = "Start was already requested. SchemaBot will keep the existing start request pending until the operator owner finishes it."
	}

	body := "## Start Request Accepted\n\n" +
		fmt.Sprintf("**Apply**: `%s`\n", data.ApplyID) +
		fmt.Sprintf("**Environment**: `%s`\n", data.Environment)
	if data.RequestedBy != "" {
		body += fmt.Sprintf("**Requested by**: @%s\n", data.RequestedBy)
	}
	body += "\n" + statusLine + "\n"
	if data.StartedCount > 0 || data.SkippedCount > 0 {
		body += fmt.Sprintf("\n**Tasks selected for start**: %d started, %d skipped.\n", data.StartedCount, data.SkippedCount)
	}
	return body
}

// SkipRevertCommandAcceptedData contains data for a PR comment skip-revert acknowledgement.
type SkipRevertCommandAcceptedData struct {
	ApplyID     string
	Environment string
	RequestedBy string
}

// RenderSkipRevertCommandAccepted renders the acknowledgement posted when a PR
// comment skip-revert command records durable skip-revert intent.
func RenderSkipRevertCommandAccepted(data SkipRevertCommandAcceptedData) string {
	body := "## Skip-Revert Request Accepted\n\n" +
		fmt.Sprintf("**Apply**: `%s`\n", data.ApplyID) +
		fmt.Sprintf("**Environment**: `%s`\n", data.Environment)
	if data.RequestedBy != "" {
		body += fmt.Sprintf("**Requested by**: @%s\n", data.RequestedBy)
	}
	body += "\nSkip-revert requested. SchemaBot will close the revert window, making this schema change permanent; status remains available from the PR progress comment or CLI.\n"
	return body
}

// PreviewCommentStartCommandAccepted renders a sample start command
// acknowledgement comment.
func PreviewCommentStartCommandAccepted() string {
	return RenderStartCommandAccepted(StartCommandAcceptedData{
		ApplyID:      "apply-a1b2c3d4e5f67890",
		Environment:  "staging",
		RequestedBy:  "alice",
		StartedCount: 1,
		SkippedCount: 0,
	})
}

// PreviewCommentStartCommandAlreadyRequested renders a sample start
// acknowledgement when start is already pending.
func PreviewCommentStartCommandAlreadyRequested() string {
	return RenderStartCommandAccepted(StartCommandAcceptedData{
		ApplyID:      "apply-a1b2c3d4e5f67890",
		Environment:  "staging",
		RequestedBy:  "alice",
		Status:       apitypes.ControlStatusAlreadyRequested,
		StartedCount: 1,
		SkippedCount: 0,
	})
}

// RenderCutoverCommandAccepted renders the acknowledgement posted when a PR
// comment cutover command records durable cutover intent.
func RenderCutoverCommandAccepted(data CutoverCommandAcceptedData) string {
	statusLine := "Cutover request accepted. SchemaBot will complete this schema change; status remains available from the PR progress comment or CLI."
	if data.Status == apitypes.ControlStatusAlreadyInProgress {
		statusLine = "Cutover is already in progress. SchemaBot will keep reporting progress from the existing apply."
	}

	body := "## Cutover Request Accepted\n\n" +
		fmt.Sprintf("**Apply**: `%s`\n", data.ApplyID) +
		fmt.Sprintf("**Environment**: `%s`\n", data.Environment)
	if data.RequestedBy != "" {
		body += fmt.Sprintf("**Requested by**: @%s\n", data.RequestedBy)
	}
	return body + "\n" + statusLine + "\n"
}

// PreviewCommentCutoverCommandAccepted renders a sample cutover command
// acknowledgement comment.
func PreviewCommentCutoverCommandAccepted() string {
	return RenderCutoverCommandAccepted(CutoverCommandAcceptedData{
		ApplyID:     "apply-a1b2c3d4e5f67890",
		Environment: "staging",
		RequestedBy: "alice",
	})
}

// PreviewCommentCutoverCommandAlreadyInProgress renders a sample cutover
// acknowledgement when cutover is already in progress.
func PreviewCommentCutoverCommandAlreadyInProgress() string {
	return RenderCutoverCommandAccepted(CutoverCommandAcceptedData{
		ApplyID:     "apply-a1b2c3d4e5f67890",
		Environment: "staging",
		RequestedBy: "alice",
		Status:      apitypes.ControlStatusAlreadyInProgress,
	})
}

// RenderCommandNotYetAvailable renders the acknowledgement posted when a
// recognised but not-yet-implemented PR comment command is invoked. It points
// the user at the CLI fallback for the same action.
func RenderCommandNotYetAvailable(action, environment string) string {
	return fmt.Sprintf("The `%s` command is not yet available via PR comments. "+
		"Use the CLI instead:\n```\nschemabot %s -e %s\n```",
		action, action, environment)
}
