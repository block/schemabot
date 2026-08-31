package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/caller"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

// RenderRollbackMissingArguments renders the message posted when `schemabot rollback`
// is invoked without an apply ID and without an `-e` flag. The usage line keeps
// the tenant flag in optional form because this message also renders on
// untenanted deployments.
func RenderRollbackMissingArguments() string {
	return offerSupportChannel("## Missing Arguments\n\n" +
		"Usage: `schemabot rollback <apply-id> -e <environment> [-t <tenant>]`\n\n" +
		"Rollback requires both an apply ID and the `-e` flag to select the target environment.")
}

// RenderRollbackMissingEnv renders the message posted when `schemabot rollback`
// is invoked with an apply ID but no `-e` flag. Distinct from RenderMissingEnv —
// the rollback variant tailors the example usage to rollback semantics. The
// usage line keeps the tenant flag in optional form because this message also
// renders on untenanted deployments.
func RenderRollbackMissingEnv() string {
	return offerSupportChannel("## Missing Environment\n\n" +
		"Usage: `schemabot rollback <apply-id> -e <environment> [-t <tenant>]`\n\n" +
		"The `-e` flag is required to select the target environment.")
}

// RenderUnsupportedAutoConfirm renders the message posted when the `-y` /
// `--yes` flag is supplied to a comment command. No comment command takes it,
// so the reply names the surface it does belong to rather than leaving an
// operator who copied it from a CLI example with nowhere to go.
func RenderUnsupportedAutoConfirm(action string) string {
	return fmt.Sprintf("The `-y` flag is not supported for `%s`.\n\n"+
		"`-y` belongs to the CLI, where it skips an interactive confirmation prompt. "+
		"A PR comment has no prompt to skip: when a command stops for confirmation, "+
		"it is asking you to read what it discloses and reply with the confirm command it posts.", action)
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
// control command is invoked without the required apply ID. The usage line
// carries every flag the command requires, so a volume command also shows its
// mandatory `-v` level.
func RenderControlMissingApplyID(command string) string {
	usage := fmt.Sprintf("schemabot %s <apply-id> -e <environment>", command)
	if command == action.Volume {
		usage += fmt.Sprintf(" -v <%d-%d>", storage.MinVolume, storage.MaxVolume)
	}
	return offerSupportChannel(fmt.Sprintf("## Missing Apply ID\n\n"+
		"Usage: `%s`\n\n"+
		"Use `schemabot status -e <environment>` to find the apply ID.", usage))
}

// RenderVolumeInvalidLevel renders the message posted when a volume command
// is missing the `-v` flag, carries a non-numeric value, or names a level
// outside the supported range.
func RenderVolumeInvalidLevel() string {
	return offerSupportChannel(fmt.Sprintf("## Missing or Invalid Volume Level\n\n"+
		"Usage: `schemabot volume <apply-id> -e <environment> -v <level>`\n\n"+
		"The `-v` flag is required and must be a number between %d (slowest) and %d (fastest).",
		storage.MinVolume, storage.MaxVolume))
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

// PreviewCommentVolumeMissingApplyID renders the usage comment posted when a
// volume command is missing the required apply ID.
func PreviewCommentVolumeMissingApplyID() string {
	return RenderControlMissingApplyID(action.Volume)
}

// VolumeSupersededProgressData contains data for freezing a progress comment
// that a volume change has superseded.
type VolumeSupersededProgressData struct {
	// Volume is the new level (1=slowest, 11=fastest) that took effect.
	Volume int
	// PreviousVolume is the level the superseded comment was posted at — the
	// era its frozen headline describes. Zero means the comment was posted
	// before any explicit level, so the headline describes the default volume.
	PreviousVolume int
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
const volumeSupersededPrefix = "⏩ Progress at"

// supersededFoldMarker is the successor-link text renderSupersededFold embeds
// in the headline of every frozen body. IsSupersededProgressComment requires
// it alongside a flavor prefix, so a live comment that merely opens with the
// same words as a prefix is never misread as already frozen.
const supersededFoldMarker = " [a new progress comment]("

// SupersededProgressData contains the data every superseded-comment fold
// shares: where the successor comment lives and the superseded comment's last
// rendered body. Rotation flavors with no flavor-specific data render directly
// from it; flavors that carry extra data (volume) keep their own struct.
type SupersededProgressData struct {
	// Repo is the "owner/name" repository, used to link the successor comment.
	Repo string
	// PR is the pull request number, used to link the successor comment.
	PR int
	// NewCommentID is the GitHub comment ID of the fresh comment that now
	// tracks the schema change.
	NewCommentID int64
	// PreviousBody is the superseded comment's last rendered body, preserved
	// inside the folded details block.
	PreviousBody string
}

// renderSupersededFold renders the frozen body written over a superseded
// comment: a headline pointing at the successor comment, with the superseded
// comment's last rendered body preserved inside a collapsed details block.
// Every rotation flavor shares this shape and differs only in its headline
// (which must start with that flavor's superseded prefix) and fold label.
//
// Headlines must describe the era the frozen comment covered, never announce
// the event that retired it: the frozen comment sits above the operator
// command that caused the rotation in the PR timeline, so a headline naming
// the retiring event reads as an effect appearing before its cause. The
// retiring event appears only in subordinate "superseded by …" phrasing after
// the era description.
func renderSupersededFold(headline, foldLabel, repo string, pr int, newCommentID int64, previousBody string) string {
	return fmt.Sprintf(
		"%s"+supersededFoldMarker+"%s#issuecomment-%d).\n\n"+
			"<details>\n<summary>%s</summary>\n\n%s\n\n</details>\n",
		headline, caller.PullRequestURL(repo, pr), newCommentID, foldLabel, previousBody)
}

// RenderVolumeSupersededProgressComment renders the frozen body written over a
// progress comment once a volume change rotates in a fresh one. The headline
// names the level the frozen comment covered (its own era), with the new level
// mentioned only as what superseded it. The old comment's final progress stays
// on the PR as a record, collapsed into a details block, with a pointer to the
// comment where progress continues.
func RenderVolumeSupersededProgressComment(data VolumeSupersededProgressData) string {
	era := fmt.Sprintf("volume **%d/%d**", data.PreviousVolume, storage.MaxVolume)
	if data.PreviousVolume == 0 {
		era = "the default volume"
	}
	headline := fmt.Sprintf("%s %s — superseded by the change to **%d/%d**; progress continues in",
		volumeSupersededPrefix, era, data.Volume, storage.MaxVolume)
	return renderSupersededFold(headline, "Progress before the volume change",
		data.Repo, data.PR, data.NewCommentID, data.PreviousBody)
}

// resumeSupersededPrefix opens every frozen body written when a resume
// superseded a progress comment; IsSupersededProgressComment keys on it so a
// freeze retry can tell an already-frozen comment from a live one.
const resumeSupersededPrefix = "⏸️ Progress before the stop"

// RenderResumeSupersededProgressComment renders the frozen body written over a
// progress comment once a resumed apply rotates in a fresh one. The headline
// names the pre-stop era the frozen comment covered, with the resume mentioned
// only as where progress continues. The old comment's final pre-stop progress
// stays on the PR as a record, collapsed into a details block, with a pointer
// to the comment where progress continues.
func RenderResumeSupersededProgressComment(data SupersededProgressData) string {
	return renderSupersededFold(resumeSupersededPrefix+" — the schema change resumed in", "Progress before the stop",
		data.Repo, data.PR, data.NewCommentID, data.PreviousBody)
}

// revertSupersededPrefix opens every frozen body written when a revert
// superseded a progress comment; IsSupersededProgressComment keys on it so a
// freeze retry can tell an already-frozen comment from a live one.
const revertSupersededPrefix = "⏪ Progress before the revert"

// RenderRevertSupersededProgressComment renders the frozen body written over a
// progress comment once a user revert rotates in a fresh one. The headline
// names the pre-revert era the frozen comment covered, with the revert
// mentioned only as where tracking continues. The old comment's final
// pre-revert progress stays on the PR as a record, collapsed into a details
// block, with a pointer to the comment where the revert is tracked.
func RenderRevertSupersededProgressComment(data SupersededProgressData) string {
	return renderSupersededFold(revertSupersededPrefix+" — the revert is tracked in", "Progress before the revert",
		data.Repo, data.PR, data.NewCommentID, data.PreviousBody)
}

// skipRevertSupersededPrefix opens every frozen body written when a
// skip-revert superseded a progress comment; IsSupersededProgressComment keys
// on it so a freeze retry can tell an already-frozen comment from a live one.
const skipRevertSupersededPrefix = "⏭️ Revert window before the skip"

// RenderSkipRevertSupersededProgressComment renders the frozen body written
// over a progress comment once a user skip-revert rotates in a fresh one. The
// headline names the revert-window era the frozen comment covered, with the
// skip mentioned only as where finalization continues. The old comment's final
// revert-window rendering stays on the PR as a record, collapsed into a
// details block, with a pointer to the comment where the finalization is
// tracked.
func RenderSkipRevertSupersededProgressComment(data SupersededProgressData) string {
	return renderSupersededFold(skipRevertSupersededPrefix+" — the schema change is finalizing in", "Progress before the revert was skipped",
		data.Repo, data.PR, data.NewCommentID, data.PreviousBody)
}

// cutoverSupersededPrefix opens every frozen body written when a completed
// cutover superseded the cutover prompt comment; IsSupersededProgressComment
// keys on it so a freeze retry can tell an already-frozen comment from a live
// one.
const cutoverSupersededPrefix = "⏸️ Cutover prompt"

// RenderCutoverSupersededComment renders the frozen body written over the
// cutover prompt comment once the operator's cutover has completed and the
// apply continues (e.g. into its revert window). The headline names what the
// frozen comment was — the prompt — with the completed cutover mentioned only
// as what superseded it. The prompt stays on the PR as a record, collapsed
// into a details block, with a pointer to the comment where progress is
// tracked.
func RenderCutoverSupersededComment(data SupersededProgressData) string {
	return renderSupersededFold(cutoverSupersededPrefix+" — the cutover completed; progress continues in", "Cutover prompt",
		data.Repo, data.PR, data.NewCommentID, data.PreviousBody)
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
	return renderSupersededFold(genericSupersededPrefix+" — progress continues in", "Earlier progress",
		data.Repo, data.PR, data.NewCommentID, data.PreviousBody)
}

// legacySupersededPrefixes are headline prefixes that earlier server versions
// rendered on frozen bodies. They are recognition-only — nothing renders them
// anymore. A comment frozen by an earlier version can still carry a
// pending-freeze marker if the marker-clear write failed; without recognizing
// its headline, the freeze retry would wrap the already-frozen body in a
// second fold.
var legacySupersededPrefixes = []string{
	"⏩ Volume changed to",
	"▶️ Schema change resumed",
	"Schema change reverting",
	"Revert skipped",
	"Cutover complete",
}

// IsSupersededProgressComment reports whether a comment body is already a
// frozen superseded rendering — any rotation flavor, current or legacy — so a
// freeze retry does not wrap a frozen body in a second fold. A frozen body
// opens with a flavor prefix and carries the successor link on that same
// headline; both are required, so a live body that merely opens with the same
// words as a prefix (e.g. an edited comment starting "Cutover complete") is
// never mistaken for a frozen one and skipped by a freeze retry.
func IsSupersededProgressComment(body string) bool {
	headline, _, _ := strings.Cut(body, "\n")
	if !strings.Contains(headline, supersededFoldMarker) {
		return false
	}
	prefixes := []string{
		volumeSupersededPrefix,
		resumeSupersededPrefix,
		revertSupersededPrefix,
		skipRevertSupersededPrefix,
		cutoverSupersededPrefix,
		genericSupersededPrefix,
	}
	prefixes = append(prefixes, legacySupersededPrefixes...)
	for _, prefix := range prefixes {
		if strings.HasPrefix(headline, prefix) {
			return true
		}
	}
	return false
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
