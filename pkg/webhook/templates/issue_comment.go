package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
)

// RenderRollbackMissingArguments renders the message posted when `schemabot rollback`
// is invoked without an apply ID and without an `-e` flag. The usage line keeps
// the tenant flag in optional form because this message also renders on
// untenanted deployments.
func RenderRollbackMissingArguments() string {
	return "## Missing Arguments\n\n" +
		"Usage: `schemabot rollback <apply-id> -e <environment> [-t <tenant>]`\n\n" +
		"Rollback requires both an apply ID and the `-e` flag to select the target environment."
}

// RenderRollbackMissingEnv renders the message posted when `schemabot rollback`
// is invoked with an apply ID but no `-e` flag. Distinct from RenderMissingEnv —
// the rollback variant tailors the example usage to rollback semantics. The
// usage line keeps the tenant flag in optional form because this message also
// renders on untenanted deployments.
func RenderRollbackMissingEnv() string {
	return "## Missing Environment\n\n" +
		"Usage: `schemabot rollback <apply-id> -e <environment> [-t <tenant>]`\n\n" +
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

// CancelCommandAcceptedData contains data for a PR comment cancel acknowledgement.
type CancelCommandAcceptedData struct {
	ApplyID        string
	Environment    string
	RequestedBy    string
	Status         string
	CancelledCount int64
	SkippedCount   int64
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

// ReleaseCommandAcceptedData contains data for a PR comment release acknowledgement.
type ReleaseCommandAcceptedData struct {
	ApplyID     string
	Environment string
	RequestedBy string
	Status      string
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

// RenderVolumeInvalidLevel renders the message posted when a volume command
// is missing the `-v` flag, carries a non-numeric value, or names a level
// outside the supported range.
func RenderVolumeInvalidLevel() string {
	return fmt.Sprintf("## Missing or Invalid Volume Level\n\n"+
		"Usage: `schemabot volume <apply-id> -e <environment> -v <level>`\n\n"+
		"The `-v` flag is required and must be a number between %d (slowest) and %d (fastest).",
		storage.MinVolume, storage.MaxVolume)
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

// RenderCancelCommandAccepted renders the acknowledgement posted when a PR
// comment cancel command records durable cancel intent.
func RenderCancelCommandAccepted(data CancelCommandAcceptedData) string {
	statusLine := "Cancel request accepted. SchemaBot will permanently cancel this schema change; status remains available from the PR progress comment or CLI."
	if data.Status == apitypes.ControlStatusAlreadyRequested {
		statusLine = "Cancel was already requested. SchemaBot will keep the existing cancel request pending until the operator owner finishes it."
	}

	body := "## Cancel Request Accepted\n\n" +
		fmt.Sprintf("**Apply**: `%s`\n", data.ApplyID) +
		fmt.Sprintf("**Environment**: `%s`\n", data.Environment)
	if data.RequestedBy != "" {
		body += fmt.Sprintf("**Requested by**: @%s\n", data.RequestedBy)
	}
	body += "\n" + statusLine + "\n"
	if data.CancelledCount > 0 || data.SkippedCount > 0 {
		body += fmt.Sprintf("\n**Tasks selected for cancel**: %d cancelled, %d skipped.\n", data.CancelledCount, data.SkippedCount)
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

// RenderReleaseCommandAccepted renders the acknowledgement posted when a PR
// comment release command records a durable release latch for a paused rollout.
func RenderReleaseCommandAccepted(data ReleaseCommandAcceptedData) string {
	statusLine := "Release request accepted. SchemaBot will let the held deployments of this paused rollout proceed; status remains available from the PR progress comment or CLI."
	if data.Status == apitypes.ControlStatusAlreadyRequested {
		statusLine = "Release was already requested. SchemaBot keeps the existing release latch in place; the held deployments continue from where they were."
	}

	body := "## Release Request Accepted\n\n" +
		fmt.Sprintf("**Apply**: `%s`\n", data.ApplyID) +
		fmt.Sprintf("**Environment**: `%s`\n", data.Environment)
	if data.RequestedBy != "" {
		body += fmt.Sprintf("**Requested by**: @%s\n", data.RequestedBy)
	}
	body += "\n" + statusLine + "\n"
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

// RevertCommandAcceptedData contains data for a PR comment revert acknowledgement.
type RevertCommandAcceptedData struct {
	ApplyID     string
	Environment string
	RequestedBy string
}

// RenderRevertCommandAccepted renders the acknowledgement posted when a PR
// comment revert command is accepted.
func RenderRevertCommandAccepted(data RevertCommandAcceptedData) string {
	body := "## Revert Request Accepted\n\n" +
		fmt.Sprintf("**Apply**: `%s`\n", data.ApplyID) +
		fmt.Sprintf("**Environment**: `%s`\n", data.Environment)
	if data.RequestedBy != "" {
		body += fmt.Sprintf("**Requested by**: @%s\n", data.RequestedBy)
	}
	body += "\nRevert requested. SchemaBot will undo this schema change; status remains available from the PR progress comment or CLI.\n"
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

// VolumeCommandAcceptedData contains data for a PR comment volume acknowledgement.
type VolumeCommandAcceptedData struct {
	ApplyID     string
	Environment string
	RequestedBy string
	// Volume is the queued target level (1=slowest, 11=fastest).
	Volume int32
}

// RenderVolumeCommandAccepted renders the acknowledgement posted when a PR
// comment volume command queues a durable volume adjustment. The wording says
// "shortly" rather than implying an immediate change: the new level takes
// effect at the next progress check, so it is not yet in effect when this
// posts.
func RenderVolumeCommandAccepted(data VolumeCommandAcceptedData) string {
	body := "## Volume Request Accepted\n\n" +
		fmt.Sprintf("**Apply**: `%s`\n", data.ApplyID) +
		fmt.Sprintf("**Environment**: `%s`\n", data.Environment)
	if data.RequestedBy != "" {
		body += fmt.Sprintf("**Requested by**: @%s\n", data.RequestedBy)
	}
	body += fmt.Sprintf("\nVolume change to %d requested. SchemaBot will adjust the speed of this schema change shortly; once the new level takes effect, a fresh progress comment will track the schema change at the new volume.\n", data.Volume)
	return body
}

// PreviewCommentVolumeCommandAccepted renders a sample volume command
// acknowledgement comment.
func PreviewCommentVolumeCommandAccepted() string {
	return RenderVolumeCommandAccepted(VolumeCommandAcceptedData{
		ApplyID:     "apply-a1b2c3d4e5f67890",
		Environment: "staging",
		RequestedBy: "alice",
		Volume:      8,
	})
}

// PreviewCommentVolumeInvalidLevel renders the usage comment posted when a
// volume command carries a missing or invalid level.
func PreviewCommentVolumeInvalidLevel() string {
	return RenderVolumeInvalidLevel()
}

// VolumeSupersededProgressData contains data for freezing a progress comment
// that a volume change has superseded.
type VolumeSupersededProgressData struct {
	// Volume is the new level (1=slowest, 11=fastest) that took effect.
	Volume int
	// Repo is the "owner/name" repository, used to link the successor comment.
	Repo string
	// PR is the pull request number, used to link the successor comment.
	PR int
	// NewCommentID is the GitHub comment ID of the fresh progress comment now
	// tracking the schema change.
	NewCommentID int64
	// PreviousBody is the superseded comment's last rendered body, preserved
	// inside the folded details block.
	PreviousBody string
}

// volumeSupersededPrefix opens every frozen body written when a volume change
// superseded a progress comment; IsSupersededProgressComment keys on it so a
// freeze retry can tell an already-frozen comment from a live one.
const volumeSupersededPrefix = "⏩ Volume changed to"

// RenderVolumeSupersededProgressComment renders the frozen body written over a
// progress comment once a volume change rotates in a fresh one. The old
// comment's final progress stays on the PR as a record, collapsed into a
// details block, with a pointer to the comment where progress continues.
func RenderVolumeSupersededProgressComment(data VolumeSupersededProgressData) string {
	return fmt.Sprintf(
		volumeSupersededPrefix+" **%d/%d** — progress continues in [a new progress comment](https://github.com/%s/pull/%d#issuecomment-%d).\n\n"+
			"<details>\n<summary>Progress before the volume change</summary>\n\n%s\n\n</details>\n",
		data.Volume, storage.MaxVolume, data.Repo, data.PR, data.NewCommentID, data.PreviousBody)
}

// ResumeSupersededProgressData contains data for freezing a progress comment
// that a resume has superseded.
type ResumeSupersededProgressData struct {
	// Repo is the "owner/name" repository, used to link the successor comment.
	Repo string
	// PR is the pull request number, used to link the successor comment.
	PR int
	// NewCommentID is the GitHub comment ID of the fresh progress comment now
	// tracking the schema change.
	NewCommentID int64
	// PreviousBody is the superseded comment's last rendered body, preserved
	// inside the folded details block.
	PreviousBody string
}

// resumeSupersededPrefix opens every frozen body written when a resume
// superseded a progress comment; IsSupersededProgressComment keys on it so a
// freeze retry can tell an already-frozen comment from a live one.
const resumeSupersededPrefix = "▶️ Schema change resumed"

// RenderResumeSupersededProgressComment renders the frozen body written over a
// progress comment once a resumed apply rotates in a fresh one. The old
// comment's final pre-stop progress stays on the PR as a record, collapsed
// into a details block, with a pointer to the comment where progress
// continues.
func RenderResumeSupersededProgressComment(data ResumeSupersededProgressData) string {
	return fmt.Sprintf(
		resumeSupersededPrefix+" — progress continues in [a new progress comment](https://github.com/%s/pull/%d#issuecomment-%d).\n\n"+
			"<details>\n<summary>Progress before the stop</summary>\n\n%s\n\n</details>\n",
		data.Repo, data.PR, data.NewCommentID, data.PreviousBody)
}

// SupersededProgressData contains data for freezing a progress comment when
// the rotation that superseded it is no longer known — the retry path for a
// freeze owed by an earlier rotation, where only the owed comment ID was
// recorded.
type SupersededProgressData struct {
	// Repo is the "owner/name" repository, used to link the successor comment.
	Repo string
	// PR is the pull request number, used to link the successor comment.
	PR int
	// NewCommentID is the GitHub comment ID of the fresh progress comment now
	// tracking the schema change.
	NewCommentID int64
	// PreviousBody is the superseded comment's last rendered body, preserved
	// inside the folded details block.
	PreviousBody string
}

// genericSupersededPrefix opens every frozen body written when the superseding
// rotation is no longer known; IsSupersededProgressComment keys on it so a
// freeze retry can tell an already-frozen comment from a live one.
const genericSupersededPrefix = "⏭️ Progress comment superseded"

// RenderSupersededProgressComment renders the frozen body written over a
// progress comment when a fresh one has replaced it but the rotation that did
// so is no longer known. The old comment's final progress stays on the PR as
// a record, collapsed into a details block, with a pointer to the comment
// where progress continues.
func RenderSupersededProgressComment(data SupersededProgressData) string {
	return fmt.Sprintf(
		genericSupersededPrefix+" — progress continues in [a new progress comment](https://github.com/%s/pull/%d#issuecomment-%d).\n\n"+
			"<details>\n<summary>Earlier progress</summary>\n\n%s\n\n</details>\n",
		data.Repo, data.PR, data.NewCommentID, data.PreviousBody)
}

// IsSupersededProgressComment reports whether a comment body is already a
// frozen superseded-progress rendering — either flavor — so a freeze retry
// does not wrap a frozen body in a second fold.
func IsSupersededProgressComment(body string) bool {
	return strings.HasPrefix(body, volumeSupersededPrefix) ||
		strings.HasPrefix(body, resumeSupersededPrefix) ||
		strings.HasPrefix(body, genericSupersededPrefix)
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
