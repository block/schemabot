package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// A re-plan after a terminal apply is the PR's only route back to a passing
// gate when its commit moved on: the plan that ran during the apply was refused
// by the ownership guard, so the stored row still names the apply's commit and
// the aggregate holds it as blocking. These are the conditions under which that
// re-plan is owed.
func TestReplanOwedAfterTerminalApply(t *testing.T) {
	t.Run("released check left behind by a moved head", func(t *testing.T) {
		check := &storage.Check{HeadSHA: "apply-sha"}
		assert.True(t, replanOwedAfterTerminalApply(check, openPRAt("newer-sha")))
	})

	// A successful apply keeps its check claimed, and the target holds exactly
	// what it set out to put there. Nothing else will ever write this row, so
	// without the re-plan the gate blocks on an outcome it already has.
	t.Run("successful apply retains ownership and still converges", func(t *testing.T) {
		check := completedCheck("apply-sha", checkConclusionSuccess)
		assert.True(t, replanOwedAfterTerminalApply(check, openPRAt("newer-sha")))
	})

	// A cancelled apply whose completed task history keeps the row claimed owes
	// an operator a reconciliation. Re-planning could replace that block with a
	// clean result computed against a database the apply already changed.
	t.Run("cancelled apply owing a reconciliation", func(t *testing.T) {
		check := completedCheck("apply-sha", checkConclusionFailure)
		assert.False(t, replanOwedAfterTerminalApply(check, openPRAt("newer-sha")))
	})

	// A commit that removes schema the apply already applied leaves the target
	// diverged from the PR, and the block naming that divergence must outlive
	// the head it was recorded for.
	t.Run("apply whose commit removed the schema it applied", func(t *testing.T) {
		check := completedCheck("apply-sha", checkConclusionActionRequired)
		assert.False(t, replanOwedAfterTerminalApply(check, openPRAt("newer-sha")))
	})

	// An apply still holding an in_progress row has not settled. Only its owner
	// releases it, and a plan for a newer commit is not that owner.
	t.Run("apply still holding an in-progress row", func(t *testing.T) {
		check := &storage.Check{HeadSHA: "apply-sha", ApplyID: 42, Status: checkStatusInProgress}
		assert.False(t, replanOwedAfterTerminalApply(check, openPRAt("newer-sha")))
	})

	t.Run("check already covers the PR head", func(t *testing.T) {
		check := &storage.Check{HeadSHA: "same-sha"}
		assert.False(t, replanOwedAfterTerminalApply(check, openPRAt("same-sha")))

		succeeded := completedCheck("same-sha", checkConclusionSuccess)
		assert.False(t, replanOwedAfterTerminalApply(succeeded, openPRAt("same-sha")))
	})

	t.Run("closed PR has no gate left to converge", func(t *testing.T) {
		check := &storage.Check{HeadSHA: "apply-sha"}
		closed := &github.PullRequestInfo{HeadSHA: "newer-sha", State: "closed"}
		assert.False(t, replanOwedAfterTerminalApply(check, closed))

		merged := &github.PullRequestInfo{HeadSHA: "newer-sha", State: "closed", Merged: true}
		assert.False(t, replanOwedAfterTerminalApply(check, merged))
	})

	// An unreadable head is not evidence the head moved. Re-planning against it
	// would compare the PR to nothing.
	t.Run("missing head SHA", func(t *testing.T) {
		check := &storage.Check{HeadSHA: "apply-sha"}
		assert.False(t, replanOwedAfterTerminalApply(check, openPRAt("")))
	})
}

// openPRAt builds the PR metadata for an open PR at the given head.
func openPRAt(headSHA string) *github.PullRequestInfo {
	return &github.PullRequestInfo{HeadSHA: headSHA, State: "open"}
}

// completedCheck builds the stored row a terminal apply leaves behind when the
// write retains its ownership.
func completedCheck(headSHA, conclusion string) *storage.Check {
	return &storage.Check{
		HeadSHA:    headSHA,
		ApplyID:    42,
		Status:     checkStatusCompleted,
		Conclusion: conclusion,
	}
}

// A terminal apply's aggregate belongs on the commit the PR is gated on. The
// stored row names the commit the apply started on, so publishing from the row
// puts the outcome on a Check Run GitHub no longer displays whenever the head
// moved while the apply ran.
func TestAggregatePublishSHA(t *testing.T) {
	t.Run("head moved while the apply ran", func(t *testing.T) {
		check := &storage.Check{HeadSHA: "apply-sha"}
		assert.Equal(t, "newer-sha", aggregatePublishSHA(check, openPRAt("newer-sha")))
	})

	t.Run("head never moved", func(t *testing.T) {
		check := &storage.Check{HeadSHA: "same-sha"}
		assert.Equal(t, "same-sha", aggregatePublishSHA(check, openPRAt("same-sha")))
	})

	// An outcome on the apply's commit still beats no outcome at all.
	t.Run("head GitHub did not report falls back to the stored commit", func(t *testing.T) {
		check := &storage.Check{HeadSHA: "apply-sha"}
		assert.Equal(t, "apply-sha", aggregatePublishSHA(check, openPRAt("")))
	})

	// A cancelled apply keeps its check claimed so an operator reconciles the
	// target, which suppresses the re-plan. The publish is then the only write
	// left, so it has to reach the head even though no plan will follow it.
	t.Run("reconciliation-owed apply still publishes on the head", func(t *testing.T) {
		check := completedCheck("apply-sha", checkConclusionFailure)
		prInfo := openPRAt("newer-sha")
		assert.Equal(t, "newer-sha", aggregatePublishSHA(check, prInfo))
		assert.False(t, replanOwedAfterTerminalApply(check, prInfo),
			"a retained block must still suppress the re-plan")
	})

	// A merged PR's head is a real commit that can still carry the outcome, and
	// a half-applied target is exactly what someone reading it needs to see.
	t.Run("closed PR publishes on the head", func(t *testing.T) {
		check := &storage.Check{HeadSHA: "apply-sha"}
		closed := &github.PullRequestInfo{HeadSHA: "newer-sha", State: "closed", Merged: true}
		assert.Equal(t, "newer-sha", aggregatePublishSHA(check, closed))
	})
}
