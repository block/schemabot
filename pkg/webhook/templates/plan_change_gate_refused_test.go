package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// An automatic apply's comment promises a run. When a change gate refuses the
// apply after that comment is posted, the comment is edited to say nothing ran
// and that the lock is gone, while the plan above stays as it was posted.
func TestRenderPlanComment_ChangeGateRefusedReplacesTheAutomaticApplyPromise(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
		}},
		IsLocked:     true,
		LockOwner:    "acme/shop#42",
		LockAcquired: "2026-09-01 10:00:00 UTC",
	}

	automatic := RenderPlanComment(data)
	assert.Contains(t, automatic, "🔒 **Lock acquired by** `acme/shop#42` at 2026-09-01 10:00:00 UTC")
	assert.Contains(t, automatic, "**Applying automatically**\n")
	assert.NotContains(t, automatic, "Not applied")

	data.RefusedByChangeGate = true
	data.LockReleased = true
	data.RefusalPosted = true
	refused := RenderPlanComment(data)
	assert.Contains(t, refused, "**Not applied** — see the comment below for why.\n")
	assert.Contains(t, refused, "🔓 **Lock released**\n")
	assert.NotContains(t, refused, "Applying automatically", "the comment no longer promises a run that did not happen")
	assert.NotContains(t, refused, "Lock acquired", "the refusal released the lock")
	assert.Contains(t, refused, "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", "the plan stays as it was posted")
	assert.Contains(t, refused, "## Schema Change Apply")
}

// The lock line is a claim about the lock, so the refused comment makes it only
// when this refusal released the lock. A lock the release failed to free, or one
// re-pinned to a newer intent, is still held, and the comment says nothing of it
// rather than claim the lock is gone or still acquired.
func TestRenderPlanComment_ChangeGateRefusedClaimsTheLockReleasedOnlyWhenItWas(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
		}},
		IsLocked:            true,
		LockOwner:           "acme/shop#42",
		LockAcquired:        "2026-09-01 10:00:00 UTC",
		RefusedByChangeGate: true,
		RefusalPosted:       true,
	}

	data.LockReleased = true
	assert.Contains(t, RenderPlanComment(data), "🔓 **Lock released**\n")

	data.LockReleased = false
	held := RenderPlanComment(data)
	assert.NotContains(t, held, "Lock released")
	assert.NotContains(t, held, "Lock acquired")
	assert.Contains(t, held, "**Not applied** — see the comment below for why.\n")
}

// The footer points at the comment below only when the refusal posted. When it
// did not, the footer carries the next step itself: the apply to re-run, scoped
// the way the other re-run hints are.
func TestRenderPlanComment_ChangeGateRefusedNamesTheNextStepWhenTheRefusalDidNotPost(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
		}},
		IsLocked:            true,
		LockOwner:           "acme/shop#42",
		RefusedByChangeGate: true,
		LockReleased:        true,
	}

	unposted := RenderPlanComment(data)
	assert.Contains(t, unposted, "**Not applied** — a change gate refused this apply, and SchemaBot could not post why. Comment `schemabot apply -e staging` again to see the reason.\n")
	assert.NotContains(t, unposted, "see the comment below", "no comment below exists to point at")
	assert.NotContains(t, unposted, "Applying automatically")

	data.ScopedDatabase = "testapp"
	data.Tenant = "acme"
	data.Target = "us"
	scoped := RenderPlanComment(data)
	assert.Contains(t, scoped, "Comment `schemabot apply -e staging -d testapp --target us --tenant acme` again to see the reason.")

	data.RefusalPosted = true
	assert.Contains(t, RenderPlanComment(data), "**Not applied** — see the comment below for why.\n")
}

// The refused footer belongs to a locked comment only: a plan comment never
// announced an apply, so the field changes nothing on it.
func TestRenderPlanComment_ChangeGateRefusedLeavesAPlanCommentAlone(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
		}},
	}
	want := RenderPlanComment(data)
	data.RefusedByChangeGate = true
	assert.Equal(t, want, RenderPlanComment(data))
}
