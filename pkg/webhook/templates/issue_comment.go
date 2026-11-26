package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/caller"
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

// RenderUnsupportedTargetFlag renders the message posted when `--target` is
// supplied to a command that cannot be narrowed to one rollout member.
func RenderUnsupportedTargetFlag(action string) string {
	return fmt.Sprintf("The `--target` flag is not supported for `%s`. Only `plan` and `apply` take it.", action)
}

// RenderUnsupportedOverrideFlag renders the message posted when `--override`
// is supplied to a command that no freeze can refuse.
func RenderUnsupportedOverrideFlag(action string) string {
	return fmt.Sprintf("The `--override` flag is not supported for `%s`. Only `apply`, `apply-confirm`, `start` and `cutover` take it.", action)
}

// RenderInvalidOverrideReason renders the message posted when the reason
// after `--override` is blank or too long. problem is the validator's text,
// which names only lengths.
func RenderInvalidOverrideReason(action, problem string) string {
	return fmt.Sprintf("Could not read `--override` on `%s`: %s. Give the reason in quotes, like `--override \"fix the index\"`.", action, problem)
}

// RenderTargetMissingEnv renders the message posted when `--target` is given
// without `-e`. A target is a member of one environment's rollout, so the
// usage line keeps the target the caller typed and asks only for the
// environment. The target is unvalidated text from the comment, so the usage
// line is a code span it cannot break out of.
func RenderTargetMissingEnv(action, target string) string {
	return "`--target` picks a target inside one environment, so it needs `-e` too.\n\n" +
		"**Usage**: " + inlineCode(fmt.Sprintf("schemabot %s -e <environment> --target %s", action, target))
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

// StartWithdrawnData contains data for the PR comment posted when a stop sent
// from outside the PR withdraws a start no driver had acted on.
type StartWithdrawnData struct {
	ApplyID     string
	Environment string
	// StoppedBy is the GitHub user who sent the stop, empty when the stop's
	// caller is not a GitHub user.
	StoppedBy string
	// StoppedByFreeze is the freeze whose stop withdrew the start, empty when
	// a person sent the stop.
	StoppedByFreeze string
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
// control command is invoked without the required apply ID. The usage line is
// the PR-comment command; the status lookup is a CLI command, so it starts
// with cliName and is scoped to the environment the command named, or to the
// placeholder when it named none.
func RenderControlMissingApplyID(cliName, command, environment string) string {
	usage := fmt.Sprintf("schemabot %s <apply-id> -e <environment>", command)
	return offerSupportChannel(fmt.Sprintf("## Missing Apply ID\n\n"+
		"Usage: `%s`\n\n"+
		"Use `%s` to find the apply ID.", usage, cliCommand(cliName, "status "+environmentFlag(environment))))
}

// RenderStopCommandAccepted renders the acknowledgement posted when a PR
// comment stop command records durable stop intent.
func RenderStopCommandAccepted(data StopCommandAcceptedData) string {
	statusLine := "Stop request accepted. SchemaBot will stop this schema change; status remains available from the PR progress comment or CLI."
	switch data.Status {
	case apitypes.ControlStatusAlreadyRequested:
		statusLine = "Stop was already requested. SchemaBot will keep the existing stop request pending until the operator owner finishes it."
	case apitypes.ControlStatusStartWithdrawn:
		statusLine = "The start requested earlier had not taken effect yet, so this stop withdrew it. The schema change stays stopped; start it again to resume."
	}

	body := "## Stop Request Accepted\n\n" +
		fmt.Sprintf("**Apply**: `%s`\n", data.ApplyID) +
		fmt.Sprintf("**Environment**: `%s`\n", data.Environment)
	if data.RequestedBy != "" {
		body += fmt.Sprintf("**Requested by**: @%s\n", data.RequestedBy)
	}
	body += "\n" + statusLine + "\n"
	if data.Status != apitypes.ControlStatusStartWithdrawn && (data.StoppedCount > 0 || data.SkippedCount > 0) {
		body += fmt.Sprintf("\n**Tasks selected for stop**: %d stopped, %d skipped.\n", data.StoppedCount, data.SkippedCount)
	}
	return body
}

// RenderStartWithdrawn renders the comment posted when a stop sent from outside
// the PR withdraws a start no driver had acted on, so the PR does not end on a
// start acknowledgement that promised a resume.
func RenderStartWithdrawn(data StartWithdrawnData) string {
	body := "## Start Withdrawn\n\n" +
		fmt.Sprintf("**Apply**: `%s`\n", data.ApplyID) +
		fmt.Sprintf("**Environment**: `%s`\n", data.Environment)
	if data.StoppedBy != "" {
		body += fmt.Sprintf("**Stopped by**: @%s\n", data.StoppedBy)
	}
	if data.StoppedByFreeze != "" {
		body += fmt.Sprintf("**Stopped by**: freeze %s\n", inlineCode(data.StoppedByFreeze))
		body += "\nA freeze that stops schema changes in this environment took effect before the start requested earlier did, so the start was withdrawn. " +
			"The schema change stays stopped. Start it again after the freeze ends or is lifted, or add `--override` and your reason to the start command if it can't wait.\n"
		return body
	}
	body += "\nA stop arrived before the start requested earlier took effect, so the start was withdrawn. The schema change stays stopped; start it again to resume.\n"
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

// supersededFoldMarker is the successor-link text renderSupersededFold embeds
// in the headline of every frozen body. IsFrozenByEarlierRelease requires it
// alongside an earlier release's prefix, so a live comment that merely opens
// with the same words as a prefix is never misread as already frozen.
const supersededFoldMarker = " [a new progress comment]("

// SupersededProgressData contains the data every superseded-comment fold
// shares: where the successor comment lives and the superseded comment's last
// rendered body. Every rotation flavor renders from it, differing only in its
// headline and fold label.
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
// and fold label.
func renderSupersededFold(headline, foldLabel, repo string, pr int, newCommentID int64, previousBody string) string {
	return fmt.Sprintf(
		"%s"+supersededFoldMarker+"%s#issuecomment-%d).\n\n"+
			"<details>\n<summary>%s</summary>\n\n%s\n\n</details>\n",
		headline, caller.PullRequestURL(repo, pr), newCommentID, foldLabel, previousBody)
}

// Every rotation's headline describes the period the frozen comment covered
// and names the event that ended it only after the dash. The frozen comment
// sits above the operator command that caused the rotation, so a headline that
// led with the event would announce the effect before its cause. Nothing parses
// these headlines: the observer stores the frozen body before it edits GitHub,
// and a retry sends the stored body again rather than reading the comment.

// RenderResumeSupersededProgressComment renders the frozen body written over a
// progress comment once a resumed apply rotates in a fresh one. The old
// comment's final pre-stop progress stays on the PR as a record, collapsed
// into a details block, with a pointer to the comment where progress
// continues.
func RenderResumeSupersededProgressComment(data SupersededProgressData) string {
	return renderSupersededFold("Progress before the stop — the schema change resumed in", "Progress before the stop",
		data.Repo, data.PR, data.NewCommentID, data.PreviousBody)
}

// RenderRevertSupersededProgressComment renders the frozen body written over a
// progress comment once a user revert rotates in a fresh one. The old
// comment's final pre-revert progress stays on the PR as a record, collapsed
// into a details block, with a pointer to the comment where the revert is
// tracked.
func RenderRevertSupersededProgressComment(data SupersededProgressData) string {
	return renderSupersededFold("Progress before the revert — the revert is tracked in", "Progress before the revert",
		data.Repo, data.PR, data.NewCommentID, data.PreviousBody)
}

// RenderSkipRevertSupersededProgressComment renders the frozen body written
// over a progress comment once a user skip-revert rotates in a fresh one. The
// old comment's final revert-window rendering stays on the PR as a record,
// collapsed into a details block, with a pointer to the comment where the
// finalization is tracked.
func RenderSkipRevertSupersededProgressComment(data SupersededProgressData) string {
	return renderSupersededFold("Progress before the revert was skipped — the schema change is finalizing in", "Progress before the revert was skipped",
		data.Repo, data.PR, data.NewCommentID, data.PreviousBody)
}

// RenderCutoverSupersededComment renders the frozen body written over the
// cutover prompt comment once the operator's cutover has completed and the
// apply continues (e.g. into its revert window). The prompt stays on the PR as
// a record, collapsed into a details block, with a pointer to the comment
// where progress is tracked.
func RenderCutoverSupersededComment(data SupersededProgressData) string {
	return renderSupersededFold("Cutover prompt — the cutover completed, and progress continues in", "Cutover prompt",
		data.Repo, data.PR, data.NewCommentID, data.PreviousBody)
}

// RenderCutoverCommandSupersededProgressComment renders the frozen body
// written over a deferred apply's progress comment once the operator's
// cutover takes effect and rotates in a fresh one. The old comment's
// waiting-for-cutover rendering stays on the PR as a record, collapsed into a
// details block, with a pointer to the comment where the cutover is tracked.
func RenderCutoverCommandSupersededProgressComment(data SupersededProgressData) string {
	return renderSupersededFold("Progress before the cutover — the cutover is tracked in", "Progress before the cutover",
		data.Repo, data.PR, data.NewCommentID, data.PreviousBody)
}

// RenderSupersededProgressComment renders the frozen body written over a
// progress comment when a fresh one has replaced it but the rotation that did
// so is no longer known. The old comment's final progress stays on the PR as
// a record, collapsed into a details block, with a pointer to the comment
// where progress continues.
func RenderSupersededProgressComment(data SupersededProgressData) string {
	return renderSupersededFold("⏭️ Progress comment superseded — progress continues in", "Earlier progress",
		data.Repo, data.PR, data.NewCommentID, data.PreviousBody)
}

// earlierReleaseFrozenPrefixes open every frozen body a release before stored
// frozen bodies could write. The list is closed: no current renderer adds to
// it, because current freezes are tracked in storage, not recognized by text.
var earlierReleaseFrozenPrefixes = []string{
	"⏩ Volume changed to",
	"▶️ Schema change resumed",
	"Schema change reverting",
	"Revert skipped",
	"Cutover complete",
	"Cutover started",
	"⏭️ Progress comment superseded",
}

// IsFrozenByEarlierRelease reports whether a comment body is a frozen
// rendering written by a release that tracked owed freezes with only the
// superseded comment's ID. Such a release left its marker set when it froze a
// comment but failed to clear the marker, so the current release finishing
// that marker must not fold the body a second time. A frozen body opens with
// one of those releases' prefixes and carries the successor link on that same
// headline; both are required, so a live body that merely opens with the same
// words (e.g. an edited comment starting "Cutover complete") is never mistaken
// for a frozen one.
func IsFrozenByEarlierRelease(body string) bool {
	headline, _, _ := strings.Cut(body, "\n")
	if !strings.Contains(headline, supersededFoldMarker) {
		return false
	}
	for _, prefix := range earlierReleaseFrozenPrefixes {
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
