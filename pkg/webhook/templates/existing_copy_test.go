package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/engine"
)

// A copy the apply will throw away is disclosed in its own 🗑️ section, naming
// the tables, the namespace, how long the copy has been going, and why it
// cannot be resumed. The section also renders on the locked apply comment,
// because confirming against that comment is what destroys the copy.
func TestRenderPlanComment_DiscardedCopyShownOnPlanAndApply(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
		}},
		DiscardedCopies: []ExistingCopyData{
			{Namespace: "testapp", Tables: []string{"orders"}, Reason: engine.DiscardStatementDiffers, Age: "3h 12m"},
		},
	}

	plan := RenderPlanComment(data)
	assert.Contains(t, plan, "🗑️ **Discarding work in progress**: **1** unfinished copy on the target will be thrown away")
	assert.Contains(t, plan, "- `orders` in `testapp` (last progress 3h 12m ago): the schema change differs from the one that started it")
	assert.Contains(t, plan, "the work already done is lost")
	assert.Contains(t, plan, "apply the same schema change that started it")

	data.IsLocked = true
	apply := RenderPlanComment(data)
	assert.Contains(t, apply, "🗑️ **Discarding work in progress**",
		"the locked apply comment keeps the disclosure the confirmation acts on")
}

// A copy the apply will resume is disclosed as a continuation rather than a
// warning: nothing is destroyed, so an operator seeing the apply reappear
// knows it is not starting over.
func TestRenderPlanComment_AdoptedCopyReadsAsContinuation(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
		}},
		AdoptedCopies: []ExistingCopyData{
			{Namespace: "testapp", Tables: []string{"orders", "products"}, Age: "3h 12m"},
		},
	})

	assert.Contains(t, out, "♻️ **Resuming work in progress**: **1** unfinished copy on the target will be continued")
	assert.Contains(t, out, "- `orders`, `products` in `testapp` (last progress 3h 12m ago)")
	assert.Contains(t, out, "picks up where the existing copy stopped")
	assert.NotContains(t, out, "🗑️", "an adopted copy is not a discard warning")
}

// An expired checkpoint is a different cause from a changed statement, and the
// remedy differs, so the section names which one applies.
func TestRenderPlanComment_DiscardedCopyNamesExpiryCause(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"}}},
		DiscardedCopies: []ExistingCopyData{
			{Namespace: "testapp", Tables: []string{"orders"}, Reason: engine.DiscardCheckpointExpired, Age: "9d 4h"},
		},
	})

	assert.Contains(t, out, "- `orders` in `testapp` (last progress 9d 4h ago): it is too old to resume")
}

// A copy with no recorded progress to date it by still names the tables it
// covers; the age is omitted rather than rendered as a bare zero, which would
// read as "started just now" and understate what is being thrown away.
func TestRenderPlanComment_DiscardedCopyWithoutAge(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"}}},
		DiscardedCopies: []ExistingCopyData{
			{Namespace: "testapp", Tables: []string{"orders"}, Reason: engine.DiscardStatementDiffers},
		},
	})

	assert.Contains(t, out, "- `orders` in `testapp`: the schema change differs from the one that started it")
	assert.NotContains(t, out, "last progress")
}

// A plan against a clean target renders exactly as it always has: no copy
// disclosure at all, which is the ordinary case.
func TestRenderPlanComment_NoCopySectionOnCleanTarget(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"}}},
	})

	assert.NotContains(t, out, "work in progress")
}
