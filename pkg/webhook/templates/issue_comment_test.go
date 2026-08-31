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
	rendered := RenderUnsupportedAutoConfirm("apply")
	assert.Contains(t, rendered, "The `-y` flag is not supported for `apply`.")
	assert.Contains(t, rendered, "`-y` belongs to the CLI",
		"an operator who copied the flag from a CLI example is told where it works")
	assert.Contains(t, rendered, "reply with the confirm command it posts",
		"the reply points at the two-step confirm rather than a way around it")
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
	assert.NotContains(t, rendered, "-v <")
}

func TestRenderControlMissingApplyIDVolumeShowsRequiredLevelFlag(t *testing.T) {
	rendered := RenderControlMissingApplyID("volume")
	assert.Contains(t, rendered, "Missing Apply ID")
	assert.Contains(t, rendered, "schemabot volume <apply-id> -e <environment> -v <1-11>")
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
	assert.Contains(t, rendered, "a fresh progress comment will track the schema change at the new volume",
		"the acknowledgement points the operator at the new comment that appears once the level takes effect")
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

// TestRenderVolumeSupersededProgressComment verifies the frozen body written
// over an old progress comment after a volume change: the headline names the
// level the frozen comment covered (its own era) with the new level mentioned
// only as what superseded it, links the successor comment, and folds the final
// pre-change progress into a details block so the record stays on the PR
// without looking live.
func TestRenderVolumeSupersededProgressComment(t *testing.T) {
	rendered := RenderVolumeSupersededProgressComment(VolumeSupersededProgressData{
		Volume:         8,
		PreviousVolume: 3,
		Repo:           "acme/testapp",
		PR:             42,
		NewCommentID:   2222222222,
		PreviousBody:   "## Schema Change Progress\n\nVolume: 3/11",
	})
	assert.Contains(t, rendered, "⏩ Progress at volume **3/11**",
		"the headline names the frozen comment's own era level")
	assert.Contains(t, rendered, "superseded by the change to **8/11**",
		"the new level appears only as what superseded the era")
	assert.Contains(t, rendered, "https://github.com/acme/testapp/pull/42#issuecomment-2222222222")
	assert.Contains(t, rendered, "<details>")
	assert.Contains(t, rendered, "<summary>Progress before the volume change</summary>")
	assert.Contains(t, rendered, "Volume: 3/11",
		"the superseded body is preserved inside the fold")
}

// TestRenderVolumeSupersededProgressCommentDefaultVolumeEra verifies the
// frozen body for a comment posted before any explicit level: the headline
// describes the default-volume era instead of rendering a zero level.
func TestRenderVolumeSupersededProgressCommentDefaultVolumeEra(t *testing.T) {
	rendered := RenderVolumeSupersededProgressComment(VolumeSupersededProgressData{
		Volume:         2,
		PreviousVolume: 0,
		Repo:           "acme/testapp",
		PR:             42,
		NewCommentID:   2222222222,
		PreviousBody:   "## Schema Change Progress\n\nCopying",
	})
	assert.Contains(t, rendered, "⏩ Progress at the default volume",
		"a zero recorded level renders as the default-volume era")
	assert.Contains(t, rendered, "superseded by the change to **2/11**")
	assert.NotContains(t, rendered, "**0/11**",
		"a zero level is never rendered as a numeric era")
	assert.Contains(t, rendered, "https://github.com/acme/testapp/pull/42#issuecomment-2222222222")
	assert.Contains(t, rendered, "<summary>Progress before the volume change</summary>")
}

// TestRenderResumeSupersededProgressComment verifies the frozen body written
// over an old progress comment after a resume: the headline names the pre-stop
// era the frozen comment covered with the resume mentioned only as where
// progress continues, links the successor comment, and folds the final
// pre-stop progress into a details block so the record stays on the PR without
// looking live.
func TestRenderResumeSupersededProgressComment(t *testing.T) {
	rendered := RenderResumeSupersededProgressComment(SupersededProgressData{
		Repo:         "acme/testapp",
		PR:           42,
		NewCommentID: 2222222222,
		PreviousBody: "## Schema Change Progress\n\nStopped at 21%",
	})
	assert.Contains(t, rendered, "⏸️ Progress before the stop — the schema change resumed in",
		"the headline names the pre-stop era, with the resume subordinate")
	assert.Contains(t, rendered, "https://github.com/acme/testapp/pull/42#issuecomment-2222222222")
	assert.Contains(t, rendered, "<details>")
	assert.Contains(t, rendered, "<summary>Progress before the stop</summary>")
	assert.Contains(t, rendered, "Stopped at 21%",
		"the superseded body is preserved inside the fold")
}

// TestRenderRevertSupersededProgressComment verifies the frozen body written
// over an old progress comment after a revert: the headline names the
// pre-revert era the frozen comment covered with the revert mentioned only as
// where tracking continues, links the successor comment, and folds the final
// pre-revert progress into a details block.
func TestRenderRevertSupersededProgressComment(t *testing.T) {
	rendered := RenderRevertSupersededProgressComment(SupersededProgressData{
		Repo:         "acme/testapp",
		PR:           42,
		NewCommentID: 2222222222,
		PreviousBody: "## Schema Change Progress\n\nRevert window open",
	})
	assert.Contains(t, rendered, "⏪ Progress before the revert — the revert is tracked in",
		"the headline names the pre-revert era, with the revert subordinate")
	assert.Contains(t, rendered, "https://github.com/acme/testapp/pull/42#issuecomment-2222222222")
	assert.Contains(t, rendered, "<summary>Progress before the revert</summary>")
	assert.Contains(t, rendered, "Revert window open",
		"the superseded body is preserved inside the fold")
}

// TestRenderSkipRevertSupersededProgressComment verifies the frozen body
// written over an old progress comment after a skip-revert: the headline names
// the revert-window era the frozen comment covered with the skip mentioned
// only as where finalization continues, links the successor comment, and folds
// the final revert-window rendering into a details block.
func TestRenderSkipRevertSupersededProgressComment(t *testing.T) {
	rendered := RenderSkipRevertSupersededProgressComment(SupersededProgressData{
		Repo:         "acme/testapp",
		PR:           42,
		NewCommentID: 2222222222,
		PreviousBody: "## Schema Change Progress\n\nRevert window open",
	})
	assert.Contains(t, rendered, "⏭️ Revert window before the skip — the schema change is finalizing in",
		"the headline names the revert-window era, with the skip subordinate")
	assert.Contains(t, rendered, "https://github.com/acme/testapp/pull/42#issuecomment-2222222222")
	assert.Contains(t, rendered, "<summary>Progress before the revert was skipped</summary>")
	assert.Contains(t, rendered, "Revert window open",
		"the superseded body is preserved inside the fold")
}

// TestRenderCutoverSupersededComment verifies the frozen body written over the
// spent cutover prompt: the headline names what the frozen comment was — the
// prompt — with the completed cutover mentioned only as what superseded it,
// links the successor comment, and folds the prompt into a details block.
func TestRenderCutoverSupersededComment(t *testing.T) {
	rendered := RenderCutoverSupersededComment(SupersededProgressData{
		Repo:         "acme/testapp",
		PR:           42,
		NewCommentID: 2222222222,
		PreviousBody: "## Ready for cutover\n\nRun the cutover command.",
	})
	assert.Contains(t, rendered, "⏸️ Cutover prompt — the cutover completed; progress continues in",
		"the headline names the prompt era, with the completed cutover subordinate")
	assert.Contains(t, rendered, "https://github.com/acme/testapp/pull/42#issuecomment-2222222222")
	assert.Contains(t, rendered, "<summary>Cutover prompt</summary>")
	assert.Contains(t, rendered, "Ready for cutover",
		"the prompt body is preserved inside the fold")
}

// TestRenderSupersededProgressComment verifies the frozen body written over a
// progress comment when the superseding rotation is no longer known: it links
// the successor comment and folds the final progress into a details block
// without claiming a specific rotation caused it.
func TestRenderSupersededProgressComment(t *testing.T) {
	rendered := RenderSupersededProgressComment(SupersededProgressData{
		Repo:         "acme/testapp",
		PR:           42,
		NewCommentID: 2222222222,
		PreviousBody: "## Schema Change Progress\n\nStopped at 21%",
	})
	assert.Contains(t, rendered, "Progress comment superseded")
	assert.Contains(t, rendered, "https://github.com/acme/testapp/pull/42#issuecomment-2222222222")
	assert.Contains(t, rendered, "<details>")
	assert.Contains(t, rendered, "<summary>Earlier progress</summary>")
	assert.Contains(t, rendered, "Stopped at 21%",
		"the superseded body is preserved inside the fold")
	assert.NotContains(t, rendered, "superseded by the change to",
		"the generic fold does not claim a volume change caused it")
	assert.NotContains(t, rendered, "resumed",
		"the generic fold does not claim a resume caused it")
}

// TestIsSupersededProgressComment verifies the frozen-body predicate accepts
// every frozen flavor — so a freeze retry never folds a frozen body inside a
// second fold — and rejects live bodies, including ones that open with the
// same words as a flavor prefix, so a freeze retry never skips a comment that
// still needs folding.
func TestIsSupersededProgressComment(t *testing.T) {
	shared := SupersededProgressData{
		Repo: "acme/testapp", PR: 42, NewCommentID: 1, PreviousBody: "old",
	}
	frozen := map[string]string{
		"volume": RenderVolumeSupersededProgressComment(VolumeSupersededProgressData{
			Volume: 8, Repo: "acme/testapp", PR: 42, NewCommentID: 1, PreviousBody: "old",
		}),
		"resume":      RenderResumeSupersededProgressComment(shared),
		"revert":      RenderRevertSupersededProgressComment(shared),
		"skip-revert": RenderSkipRevertSupersededProgressComment(shared),
		"cutover":     RenderCutoverSupersededComment(shared),
		"generic":     RenderSupersededProgressComment(shared),
	}
	for flavor, body := range frozen {
		assert.Truef(t, IsSupersededProgressComment(body),
			"a %s-frozen body is recognized as frozen", flavor)
	}

	// Bodies frozen by earlier server versions carry the headlines those
	// versions rendered. They can still hold a pending-freeze marker (the
	// marker-clear write may have failed), so the retry must recognize them as
	// frozen rather than wrap them in a second fold.
	legacyFold := func(headline string) string {
		return headline + " [a new progress comment](https://github.com/acme/testapp/pull/42#issuecomment-1).\n\n<details>\n<summary>Earlier progress</summary>\n\nold\n\n</details>\n"
	}
	legacyFrozen := map[string]string{
		"volume":      legacyFold("⏩ Volume changed to **8/11** — progress continues in"),
		"resume":      legacyFold("▶️ Schema change resumed — progress continues in"),
		"revert":      legacyFold("Schema change reverting — the revert is tracked in"),
		"skip-revert": legacyFold("Revert skipped — the schema change is finalizing in"),
		"cutover":     legacyFold("Cutover complete — progress continues in"),
	}
	for flavor, body := range legacyFrozen {
		assert.Truef(t, IsSupersededProgressComment(body),
			"a %s body frozen by an earlier version is recognized as frozen", flavor)
	}

	live := map[string]string{
		"progress body":            "## Schema Change Status — Staging",
		"cutover-complete wording": "Cutover complete — the schema change is finalizing.",
		"reverting wording":        "Schema change reverting — see the table below.",
		"skip-revert wording":      "Revert skipped by the operator.",
	}
	for name, body := range live {
		assert.Falsef(t, IsSupersededProgressComment(body),
			"a live %s is not frozen", name)
	}
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
