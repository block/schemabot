package commands

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/checkstate"
)

// The output an operator reads on a pull request whose merge gate is open
// after a successful apply: the Check Run says in progress, the stored row
// says success on an earlier commit, and the two together explain the gate.
// The rendering has to keep the row's own commit visible, or the two records
// read as a contradiction rather than as a result that has not caught up.
func TestRenderChecksInspectionExplainsAStaleSuccessfulApply(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo:        "octo/repo",
		PullRequest: 709,
		HeadSHA:     "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		PRState:     "open",
		CheckRunsOnHead: []apitypes.InspectedCheckRun{{
			Name: "SchemaBot", CheckRunID: 102754133862,
			Status: checkstate.StatusInProgress, StartedAt: "2026-09-10T05:16:44Z",
		}},
		Rows: []apitypes.InspectedCheck{{
			Environment: "production", DatabaseType: "mysql", Database: "widgets",
			RecordedSHA: "e22e4cefaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CoversHead: false,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
			ApplyIdentifier: "apply-abc123", ApplyState: "completed",
			Reason:  checkstate.ReasonAwaitingReplanAfterApply,
			Summary: "An apply succeeded, but its result is recorded for an earlier commit.",
			Remedy:  "Comment `schemabot plan production` on the pull request.",
			// Blocking and self-converging: the operator waits or nudges a
			// plan, and reconciliation is not the answer.
			Blocking: true, SelfConverging: true,
		}},
	}))

	rendered := out.String()
	assert.Contains(t, rendered, "octo/repo#709 is open at 43da12bb.")
	assert.Contains(t, rendered, `Check Run "SchemaBot" on 43da12bb: in_progress (started 2026-09-10T05:16:44Z).`)
	assert.Contains(t, rendered, "e22e4cef (older)")
	assert.Contains(t, rendered, checkstate.ReasonAwaitingReplanAfterApply)
	assert.Contains(t, rendered, "Waiting on SchemaBot:")
	assert.Contains(t, rendered, "production/widgets: An apply succeeded")
	assert.Contains(t, rendered, "apply: apply-abc123 (completed)")
	assert.Contains(t, rendered, "do: Comment `schemabot plan production`")
	assert.NotContains(t, rendered, "Waiting on an operator:")
}

// A block a terminal apply left behind is filed under the operator, because
// waiting is the one response that never clears it.
func TestRenderChecksInspectionSeparatesReconciliationFromWaiting(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
		CheckRunsOnHead: []apitypes.InspectedCheckRun{{Name: "SchemaBot", Status: checkstate.StatusInProgress}},
		Rows: []apitypes.InspectedCheck{
			{
				Environment: "production", Database: "gadgets",
				RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
				Status: checkstate.StatusInProgress, ApplyIdentifier: "apply-running",
				Reason: checkstate.ReasonApplyRunning, Summary: "An apply is running.",
				Remedy: "Follow it with `sq schemabot status`.", Blocking: true, SelfConverging: true,
			},
			{
				Environment: "production", Database: "widgets",
				RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
				Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionActionRequired,
				BlockingReason: "rollback_completed", Reason: checkstate.ReasonReconciliationOwed,
				Summary: "A terminal apply left this check blocking.",
				Remedy:  "Reconcile the target environment.", Blocking: true,
			},
		},
	}))

	rendered := out.String()
	waiting := strings.Index(rendered, "Waiting on SchemaBot:")
	owed := strings.Index(rendered, "Waiting on an operator:")
	require.Positive(t, waiting)
	require.Positive(t, owed)
	assert.Less(t, waiting, owed, "what SchemaBot resolves comes before what needs a person")
	assert.Contains(t, rendered, "blocking reason: rollback_completed")
	assert.Contains(t, rendered, "do: Reconcile the target environment.")
}

// Stored rows with no Check Run on the head is a different problem from a run
// sitting in progress, and it is the one backfill addresses, so the rendering
// says which of the two it is looking at.
func TestRenderChecksInspectionReportsAMissingCheckRun(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
		ChecksEnabled:        true,
		MissingCheckRunNames: []string{"SchemaBot"},
		Rows: []apitypes.InspectedCheck{{
			Environment: "production", Database: "widgets",
			RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
			Reason: checkstate.ReasonResolved, Summary: "The plan concluded successfully.",
			Remedy: "Nothing to do.", SelfConverging: true,
		}},
	}))

	rendered := out.String()
	assert.Contains(t, rendered, "Missing on 43da12bb: SchemaBot.")
	assert.Contains(t, rendered, "sq schemabot checks backfill octo/repo")
	assert.Contains(t, rendered, "a missing Check Run is: branch protection cannot pass without SchemaBot",
		"a settled stored row does not make a gate pass that has no run to pass")
	assert.NotContains(t, rendered, "\nNothing is holding the merge gate open.")
}

// A run another app already answers under is the finding a backfill cannot
// close. Recreating SchemaBot's run leaves the other app's in place, and
// branch protection reads whichever one it picked, so an operator sent to the
// backfill alone runs it and watches the gate stay shut.
func TestRenderChecksInspectionReportsAnUntrustedConflictBesideAMissingRun(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
		ChecksEnabled:          true,
		MissingCheckRunNames:   []string{"SchemaBot (production)"},
		UntrustedConflictNames: []string{"SchemaBot (production)"},
		Rows: []apitypes.InspectedCheck{{
			Environment: "production", Database: "widgets",
			RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
			Reason: checkstate.ReasonResolved, Summary: "The plan concluded successfully.",
			Remedy: "Nothing to do.", SelfConverging: true,
		}},
	}))

	rendered := out.String()
	assert.Contains(t, rendered, "Held by another app on 43da12bb: SchemaBot (production).")
	assert.Contains(t, rendered, "cannot touch the other app's",
		"the operator has to be told the backfill does not reach the conflicting run")
	assert.Contains(t, rendered, "Another app also answers under SchemaBot (production) on 43da12bb",
		"the summary sends the operator to a backfill that leaves the conflict standing")
}

// Two contested names on one pull request need not be contested the same way:
// one can have SchemaBot's own run missing under it and the other have it
// present. The two take opposite advice — a backfill recreates the first and
// has nothing to do for the second — so a single line covering both would be
// wrong for one of them whichever way it was written.
func TestRenderChecksInspectionSplitsConflictsByWhetherItsOwnRunIsThere(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
		ChecksEnabled: true,
		CheckRunsOnHead: []apitypes.InspectedCheckRun{{
			Name: "SchemaBot (staging)", CheckRunID: 102754133862,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
		}},
		MissingCheckRunNames:   []string{"SchemaBot (production)"},
		UntrustedConflictNames: []string{"SchemaBot (production)", "SchemaBot (staging)"},
		Rows:                   []apitypes.InspectedCheck{},
	}))

	rendered := out.String()
	assert.Contains(t, rendered, "Held by another app on 43da12bb: SchemaBot (production).",
		"the name whose own run is absent is the one a backfill recreates")
	assert.NotContains(t, rendered, "Held by another app on 43da12bb: SchemaBot (production), SchemaBot (staging).",
		"the name whose own run is present must not be swept into the recreate line")
	assert.Contains(t, rendered, "Answered by another app too on 43da12bb: SchemaBot (staging).",
		"the name whose own run is present has nothing for a backfill to recreate")
}

// A conflict outlives a trusted run that is present and passing: protection
// may be reading the other app's run instead. Asserting a clear gate over one
// would be this command stating something it cannot see.
func TestRenderChecksInspectionDoesNotCallTheGateClearOverAnUntrustedConflict(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
		ChecksEnabled: true,
		CheckRunsOnHead: []apitypes.InspectedCheckRun{{
			Name: "SchemaBot (production)", CheckRunID: 102754133862,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
		}},
		UntrustedConflictNames: []string{"SchemaBot (production)"},
		Rows: []apitypes.InspectedCheck{{
			Environment: "production", Database: "widgets",
			RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
			Reason: checkstate.ReasonResolved, Summary: "The plan concluded successfully.",
			Remedy: "Nothing to do.", SelfConverging: true,
		}},
	}))

	rendered := out.String()
	assert.Contains(t, rendered, "another app answers under SchemaBot (production)")
	assert.NotContains(t, rendered, "Nothing is holding the merge gate open.")
	assert.Contains(t, rendered, "Answered by another app too on 43da12bb: SchemaBot (production).")
	assert.Contains(t, rendered, "so a backfill has nothing to recreate",
		"SchemaBot's own run is already on the head, so the scan finds nothing missing to recreate")
	assert.NotContains(t, rendered, "A backfill recreates SchemaBot's run",
		"pointing at a backfill that does nothing is the wrong turn this line exists to prevent")
}

// A conflict and SchemaBot's own unfinished run can hold the gate at once.
// Reporting only the conflict would have an operator remove the other app's
// run and find the gate still shut, so the summary names both.
func TestRenderChecksInspectionReportsItsOwnRunHoldingTheGateBesideAConflict(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
		ChecksEnabled: true,
		CheckRunsOnHead: []apitypes.InspectedCheckRun{{
			Name: "SchemaBot (production)", CheckRunID: 102754133862,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionFailure,
		}},
		UntrustedConflictNames: []string{"SchemaBot (production)"},
		Rows: []apitypes.InspectedCheck{{
			Environment: "production", Database: "widgets",
			RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
			Reason: checkstate.ReasonResolved, Summary: "The plan concluded successfully.",
			Remedy: "Nothing to do.", SelfConverging: true,
		}},
	}))

	rendered := out.String()
	assert.Contains(t, rendered, "another app answers under SchemaBot (production)")
	assert.Contains(t, rendered, `Branch protection is also waiting on a run of SchemaBot's own on 43da12bb: "SchemaBot (production)" concluded failure.`,
		"resolving the conflict alone leaves SchemaBot's own failed run holding the gate")
}

// One expected name can be absent while a second sits on the head unconcluded.
// The missing run is what a backfill fixes; the unconcluded one it will not
// touch, so a summary that named only the absence would send an operator to a
// remedy and leave the reason the gate stays shut unaccounted for.
func TestRenderChecksInspectionNamesItsOwnRunHoldingTheGateBesideAMissingOne(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
		ChecksEnabled: true,
		CheckRunsOnHead: []apitypes.InspectedCheckRun{{
			Name: "SchemaBot (staging)", CheckRunID: 102754133862,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionFailure,
		}},
		MissingCheckRunNames: []string{"SchemaBot (production)"},
		Rows: []apitypes.InspectedCheck{{
			Environment: "staging", Database: "widgets",
			RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
			Reason: checkstate.ReasonResolved, Summary: "The plan concluded successfully.",
			Remedy: "Nothing to do.", SelfConverging: true,
		}},
	}))

	rendered := out.String()
	assert.Contains(t, rendered, "branch protection cannot pass without SchemaBot (production)")
	assert.Contains(t, rendered, `Branch protection is also waiting on a run of SchemaBot's own on 43da12bb: "SchemaBot (staging)" concluded failure.`,
		"the backfill recreates the missing run and leaves the failed one holding the gate")
}

// Two of SchemaBot's own runs holding the gate are counted as two.
func TestRenderChecksInspectionCountsTheOwnRunsHoldingTheGate(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
		ChecksEnabled: true,
		CheckRunsOnHead: []apitypes.InspectedCheckRun{
			{Name: "SchemaBot (staging)", CheckRunID: 102754133862, Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionFailure},
			{Name: "SchemaBot (production)", CheckRunID: 102754133863, Status: checkstate.StatusInProgress},
		},
		UntrustedConflictNames: []string{"SchemaBot (staging)"},
		Rows: []apitypes.InspectedCheck{{
			Environment: "staging", Database: "widgets",
			RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
			Reason: checkstate.ReasonResolved, Summary: "The plan concluded successfully.",
			Remedy: "Nothing to do.", SelfConverging: true,
		}},
	}))

	assert.Contains(t, out.String(), "Branch protection is also waiting on runs of SchemaBot's own on 43da12bb:")
}

// A name GitHub could not be read for comes back neither present nor
// missing. Reporting it as missing would recommend recreating a Check Run
// that may well be sitting there, so the inspection says the read failed and
// stops short of the backfill.
func TestRenderChecksInspectionDoesNotCallAnUnreadableCheckRunMissing(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA:                 "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		PRState:                 "open",
		ChecksEnabled:           true,
		UnreadableCheckRunNames: []string{"SchemaBot"},
		Rows: []apitypes.InspectedCheck{{
			Environment: "production", Database: "widgets",
			RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
			Reason: checkstate.ReasonResolved, Summary: "The plan concluded successfully.",
			Remedy: "Nothing to do.", SelfConverging: true,
		}},
	}))

	rendered := out.String()
	assert.Contains(t, rendered, "No Check Run on 43da12bb could be read")
	assert.Contains(t, rendered, "whether the gate is clear is not known",
		"a failed read leaves the gate unanswered, never clear")
	assert.NotContains(t, rendered, "checks backfill",
		"an unreadable name is not evidence a Check Run is gone")
}

// A deployment publishing one check per environment has one required check
// per name, so a present run must not stand in for an absent one: the gate is
// held by the name that is gone, and that is what the operator has to see.
func TestRenderChecksInspectionReportsOneMissingNameBesideAPresentRun(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
		ChecksEnabled: true,
		CheckRunsOnHead: []apitypes.InspectedCheckRun{{
			Name: "SchemaBot (staging)", CheckRunID: 1,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
		}},
		MissingCheckRunNames: []string{"SchemaBot (production)"},
		Rows: []apitypes.InspectedCheck{{
			Environment: "staging", Database: "widgets",
			RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
			Reason: checkstate.ReasonResolved, Summary: "The plan concluded successfully.",
			Remedy: "Nothing to do.", SelfConverging: true,
		}},
	}))

	rendered := out.String()
	assert.Contains(t, rendered, `Check Run "SchemaBot (staging)" on 43da12bb: completed / success.`)
	assert.Contains(t, rendered, "Missing on 43da12bb: SchemaBot (production).")
	assert.Contains(t, rendered, "cannot pass without SchemaBot (production)")
	assert.NotContains(t, rendered, "\nNothing is holding the merge gate open.")
}

// A response narrowed to one environment never read the others, so it cannot
// claim the pull request's gate is clear.
func TestRenderChecksInspectionQualifiesThePassingLineByEnvironment(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
		Environment: "staging", ChecksEnabled: true,
		CheckRunsOnHead: []apitypes.InspectedCheckRun{{
			Name: "SchemaBot (staging)", Status: checkstate.StatusCompleted,
			Conclusion: checkstate.ConclusionSuccess,
		}},
		Rows: []apitypes.InspectedCheck{{
			Environment: "staging", Database: "widgets",
			RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
			Reason: checkstate.ReasonResolved, Summary: "The plan concluded successfully.",
			Remedy: "Nothing to do.", SelfConverging: true,
		}},
	}))

	rendered := out.String()
	assert.Contains(t, rendered, "Nothing in staging is holding the merge gate open. Other environments were not read.")
	assert.NotContains(t, rendered, "\nNothing is holding the merge gate open.")
}

// Settled stored rows behind a run GitHub still shows as unfinished are a
// disagreement, not a passing gate: the run holds branch protection closed on
// its own, so the rendering reports the run rather than the rows.
func TestRenderChecksInspectionReportsAnUnsettledRunOverSettledRows(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
		ChecksEnabled: true,
		CheckRunsOnHead: []apitypes.InspectedCheckRun{{
			Name: "SchemaBot", Status: checkstate.StatusInProgress,
		}},
		Rows: []apitypes.InspectedCheck{{
			Environment: "production", Database: "widgets",
			RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
			Reason: checkstate.ReasonResolved, Summary: "The plan concluded successfully.",
			Remedy: "Nothing to do.", SelfConverging: true,
		}},
	}))

	rendered := out.String()
	assert.Contains(t, rendered, `but branch protection is, on 43da12bb: "SchemaBot" is in_progress`)
	assert.NotContains(t, rendered, "\nNothing is holding the merge gate open.")
}

// A repository this deployment does not publish Check Runs for has no run to
// recreate, so the absence is reported as the configuration choice it is
// rather than as a gap for the backfill.
func TestRenderChecksInspectionReportsChecksDisabled(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
		Rows: []apitypes.InspectedCheck{{
			Environment: "production", Database: "widgets",
			RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
			Reason: checkstate.ReasonResolved, Summary: "The plan concluded successfully.",
			Remedy: "Nothing to do.", SelfConverging: true,
		}},
	}))

	rendered := out.String()
	assert.Contains(t, rendered, "does not publish Check Runs for octo/repo (enable_checks: false)")
	assert.NotContains(t, rendered, "checks backfill")
	assert.Contains(t, rendered, "Nothing is holding the merge gate open.")
}

// A run left on the head of a repository this deployment publishes no Check
// Runs for is still worth seeing, but it is not one SchemaBot maintains, and
// nothing beside it is a gap for the backfill to close.
func TestRenderChecksInspectionReportsAStaleRunOnAChecksDisabledRepo(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
		CheckRunsOnHead: []apitypes.InspectedCheckRun{{
			Name: "SchemaBot (staging)", CheckRunID: 4,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
		}},
		MissingCheckRunNames: []string{"SchemaBot (production)"},
		Rows: []apitypes.InspectedCheck{{
			Environment: "staging", Database: "widgets",
			RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
			Reason: checkstate.ReasonResolved, Summary: "The plan concluded successfully.",
			Remedy: "Nothing to do.", SelfConverging: true,
		}},
	}))

	rendered := out.String()
	assert.Contains(t, rendered, `Check Run "SchemaBot (staging)" on 43da12bb: completed / success.`)
	assert.Contains(t, rendered, "the run above is not one it maintains")
	assert.NotContains(t, rendered, "checks backfill",
		"a deployment that publishes no Check Runs has none to recreate")
	assert.NotContains(t, rendered, "Missing on 43da12bb")
}

// The backfill holds a pull request whose head still carries an uncompleted
// Check Run, so recommending it there would name a command that reports the
// pull request as held and recreates nothing.
func TestRenderChecksInspectionDoesNotRecommendABackfillTheBackfillRefuses(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
		ChecksEnabled: true,
		CheckRunsOnHead: []apitypes.InspectedCheckRun{{
			Name: "SchemaBot (staging)", CheckRunID: 4, Status: checkstate.StatusInProgress,
		}},
		MissingCheckRunNames: []string{"SchemaBot (production)"},
		Rows: []apitypes.InspectedCheck{{
			Environment: "staging", Database: "widgets",
			RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
			Status: checkstate.StatusInProgress,
			Reason: checkstate.ReasonApplyRunning, Summary: "An apply is running.",
			Remedy: "Wait for it to settle.", Blocking: true, SelfConverging: true,
		}},
	}))

	rendered := out.String()
	assert.Contains(t, rendered, "Missing on 43da12bb: SchemaBot (production).")
	assert.Contains(t, rendered, "will not recreate it while \"SchemaBot (staging)\" (in_progress) has not concluded")
	assert.NotContains(t, rendered, "recreates it.",
		"the backfill holds this pull request rather than recreating anything")
}

// A Check Run that concluded without passing holds branch protection closed
// just as an unfinished one does, so settled stored rows beside it are not a
// clear gate.
func TestRenderChecksInspectionReportsAFailedRunOverSettledRows(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
		ChecksEnabled: true,
		CheckRunsOnHead: []apitypes.InspectedCheckRun{{
			Name: "SchemaBot", Status: checkstate.StatusCompleted,
			Conclusion: checkstate.ConclusionActionRequired,
		}},
		Rows: []apitypes.InspectedCheck{{
			Environment: "production", Database: "widgets",
			RecordedSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CoversHead: true,
			Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess,
			Reason: checkstate.ReasonResolved, Summary: "The plan concluded successfully.",
			Remedy: "Nothing to do.", SelfConverging: true,
		}},
	}))

	rendered := out.String()
	assert.Contains(t, rendered, `but branch protection is, on 43da12bb: "SchemaBot" concluded action_required`)
	assert.NotContains(t, rendered, "\nNothing is holding the merge gate open.")
}

// A pull request SchemaBot holds no state for is reported as exactly that,
// rather than as an empty table an operator has to interpret.
func TestRenderChecksInspectionReportsNoStoredState(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, renderChecksInspection(&out, &apitypes.ChecksInspectResponse{
		Repo: "octo/repo", PullRequest: 709,
		HeadSHA: "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PRState: "open",
	}))
	assert.Contains(t, out.String(), "SchemaBot holds no check state for this pull request.")
}

func TestShortSHA(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "43da12bb", shortSHA("43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
	assert.Equal(t, "abc", shortSHA("abc"))
	assert.Equal(t, "an unknown commit", shortSHA(""))
}

// The pull request can be named however the operator already holds it: the
// address bar, the "owner/name#number" form a comment uses, or a repository
// with the number as a second argument.
func TestChecksShowTargetAcceptsEveryWayOfNamingAPullRequest(t *testing.T) {
	accepted := []struct {
		name   string
		cmd    ChecksShowCmd
		repo   string
		number int
	}{
		{"repository and number", ChecksShowCmd{PullRequest: "acme/store", Number: 412}, "acme/store", 412},
		{"pull request URL", ChecksShowCmd{PullRequest: "https://github.com/acme/store/pull/412"}, "acme/store", 412},
		{"URL on the files view", ChecksShowCmd{PullRequest: "https://github.com/acme/store/pull/412/files"}, "acme/store", 412},
		{"comment form", ChecksShowCmd{PullRequest: "acme/store#412"}, "acme/store", 412},
		{"URL agreeing with the number beside it", ChecksShowCmd{PullRequest: "acme/store#412", Number: 412}, "acme/store", 412},
	}
	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			repo, number, err := tc.cmd.target()
			require.NoError(t, err)
			assert.Equal(t, tc.repo, repo)
			assert.Equal(t, tc.number, number)
		})
	}
}

// A reference carrying one pull request number beside an argument naming a
// different one is refused rather than resolved by precedence, and a bare
// repository with no number says which of the two forms to reach for.
func TestChecksShowTargetRefusesAnAmbiguousOrIncompletePullRequest(t *testing.T) {
	t.Run("a number that disagrees with the reference", func(t *testing.T) {
		cmd := ChecksShowCmd{PullRequest: "https://github.com/acme/store/pull/412", Number: 500}
		_, _, err := cmd.target()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "412")
		assert.Contains(t, err.Error(), "500")
	})

	t.Run("a repository with no number", func(t *testing.T) {
		cmd := ChecksShowCmd{PullRequest: "acme/store"}
		_, _, err := cmd.target()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "add the number")
	})

	t.Run("a bare number names no repository", func(t *testing.T) {
		cmd := ChecksShowCmd{PullRequest: "412"}
		_, _, err := cmd.target()
		require.Error(t, err)
	})
}
