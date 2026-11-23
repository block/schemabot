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
	refused := RenderPlanComment(data)
	assert.Contains(t, refused, "**Not applied** — see the comment below for why.\n")
	assert.Contains(t, refused, "🔓 **Lock released**\n")
	assert.NotContains(t, refused, "Applying automatically", "the comment no longer promises a run that did not happen")
	assert.NotContains(t, refused, "Lock acquired", "the refusal released the lock")
	assert.Contains(t, refused, "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", "the plan stays as it was posted")
	assert.Contains(t, refused, "## Schema Change Apply")
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
