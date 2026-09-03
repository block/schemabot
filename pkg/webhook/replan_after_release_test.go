package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// A check released by a terminal apply is the PR's only route back to a
// passing gate when its commit moved on: the plan that ran during the apply
// was refused by the ownership guard, so the stored row still names the
// apply's commit and the aggregate holds it as blocking. These are the
// conditions under which that re-plan is owed.
func TestReplanOwedAfterOwnershipRelease(t *testing.T) {
	t.Run("released check left behind by a moved head", func(t *testing.T) {
		check := &storage.Check{HeadSHA: "apply-sha"}
		assert.True(t, replanOwedAfterOwnershipRelease(check, openPRAt("newer-sha")))
	})

	t.Run("check already covers the PR head", func(t *testing.T) {
		check := &storage.Check{HeadSHA: "same-sha"}
		assert.False(t, replanOwedAfterOwnershipRelease(check, openPRAt("same-sha")))
	})

	// A cancelled apply whose completed task history keeps the row claimed owes
	// an operator a reconciliation. Re-planning could replace that block with a
	// clean result computed against a database the apply already changed.
	t.Run("ownership retained after the terminal outcome", func(t *testing.T) {
		check := &storage.Check{HeadSHA: "apply-sha", ApplyID: 42}
		assert.False(t, replanOwedAfterOwnershipRelease(check, openPRAt("newer-sha")))
	})

	t.Run("closed PR has no gate left to converge", func(t *testing.T) {
		check := &storage.Check{HeadSHA: "apply-sha"}
		closed := &github.PullRequestInfo{HeadSHA: "newer-sha", State: "closed"}
		assert.False(t, replanOwedAfterOwnershipRelease(check, closed))

		merged := &github.PullRequestInfo{HeadSHA: "newer-sha", State: "closed", Merged: true}
		assert.False(t, replanOwedAfterOwnershipRelease(check, merged))
	})

	// An unreadable head is not evidence the head moved. Re-planning against it
	// would compare the PR to nothing.
	t.Run("missing head SHA", func(t *testing.T) {
		check := &storage.Check{HeadSHA: "apply-sha"}
		assert.False(t, replanOwedAfterOwnershipRelease(check, openPRAt("")))
	})
}

// openPRAt builds the PR metadata for an open PR at the given head.
func openPRAt(headSHA string) *github.PullRequestInfo {
	return &github.PullRequestInfo{HeadSHA: headSHA, State: "open"}
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
		check := &storage.Check{HeadSHA: "apply-sha", ApplyID: 42}
		prInfo := openPRAt("newer-sha")
		assert.Equal(t, "newer-sha", aggregatePublishSHA(check, prInfo))
		assert.False(t, replanOwedAfterOwnershipRelease(check, prInfo),
			"retained ownership must still suppress the re-plan")
	})

	// A merged PR's head is a real commit that can still carry the outcome, and
	// a half-applied target is exactly what someone reading it needs to see.
	t.Run("closed PR publishes on the head", func(t *testing.T) {
		check := &storage.Check{HeadSHA: "apply-sha"}
		closed := &github.PullRequestInfo{HeadSHA: "newer-sha", State: "closed", Merged: true}
		assert.Equal(t, "newer-sha", aggregatePublishSHA(check, closed))
	})
}
