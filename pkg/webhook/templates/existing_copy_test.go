package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/engine"
)

// A copy the apply will throw away is disclosed as a warning that leads with
// what applying costs — how long the copy has been running — then names the
// tables, the namespace, why it cannot be resumed, and the remedy. This is the
// rendering that reaches an operator who still has the decision.
func TestRenderPlanComment_DiscardedCopyWarnsWhileTheDecisionIsTheOperators(t *testing.T) {
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
	assert.Contains(t, plan, "⚠️ **Applying destroys work in progress**: **1** unfinished copy on the target, 3h 12m of copying")
	assert.Contains(t, plan, "- `orders` in `testapp`: the schema change differs from the one that started it")
	assert.NotContains(t, plan, "(last progress 3h 12m ago)",
		"the headline already carries the age, so the entry does not repeat it")
	assert.Contains(t, plan, "the 3h 12m already spent is lost and cannot be recovered")
	assert.Contains(t, plan, "apply the same schema change that started it")

	// A locked apply that stopped for confirmation is the same situation: the
	// copy is still there and confirming is what destroys it, so the warning
	// and its remedy belong on the comment the confirmation acts on.
	data.IsLocked = true
	data.AutoConfirmDowngradeReason = "Applying destroys work in progress on the target"
	paused := RenderPlanComment(data)
	assert.Contains(t, paused, "⚠️ **Applying destroys work in progress**")
	assert.Contains(t, paused, "apply the same schema change that started it")
}

// An apply that is already running has nothing to ask and no remedy to offer:
// the copy is destroyed as the comment is posted. So the section records what
// the apply threw away instead of coaching a recovery that is already out of
// reach — still under ⚠️, since work destroyed is a warning whether or not the
// reader could have stopped it.
func TestRenderPlanComment_DiscardedCopyReadsAsARecordOnceApplying(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true, IsLocked: true,
		LockOwner: "block/schemabot#42",
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
		}},
		DiscardedCopies: []ExistingCopyData{
			{Namespace: "testapp", Tables: []string{"orders"}, Reason: engine.DiscardStatementDiffers, Age: "3h 12m"},
		},
	})

	assert.Contains(t, out, "**Applying automatically**",
		"the fixture is the automatic path, which is what makes the section a record")
	assert.Contains(t, out, "⚠️ **This apply destroys work in progress**: **1** unfinished copy on the target, 3h 12m of copying")
	assert.Contains(t, out, "- `orders` in `testapp`: the schema change differs from the one that started it")
	assert.Contains(t, out, "the 3h 12m already spent is gone")
	assert.NotContains(t, out, "**Applying destroys work in progress**",
		"the hypothetical subject belongs to a comment where applying is still a choice")
	assert.NotContains(t, out, "apply the same schema change that started it",
		"the remedy is out of reach once the copy is being destroyed")
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
	assert.NotContains(t, out, "destroys work in progress", "an adopted copy is not a discard warning")
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

	assert.Contains(t, out, "⚠️ **Applying destroys work in progress**: **1** unfinished copy on the target, 9d 4h of copying")
	assert.Contains(t, out, "- `orders` in `testapp`: it is too old to resume")
}

// A copy covering only part of the schema change is discarded for a cause the
// operator did not create and cannot undo by restoring the batch, so the
// section says so plainly rather than implying the schema change drifted.
func TestRenderPlanComment_DiscardedCopyNamesPartialCoverage(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"}}},
		DiscardedCopies: []ExistingCopyData{
			{Namespace: "testapp", Tables: []string{"orders"}, Reason: engine.DiscardCopyIncomplete, Age: "3h 12m"},
		},
	})

	assert.Contains(t, out, "- `orders` in `testapp`: it covers only some of the tables this schema change alters")
}

// Several discarded copies have several ages, and no one of them describes what
// applying costs, so each keeps its own age on its own entry rather than one
// standing in for the rest in the headline.
func TestRenderPlanComment_SeveralDiscardedCopiesKeepTheirOwnAges(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"}}},
		DiscardedCopies: []ExistingCopyData{
			{Namespace: "orders_a", Tables: []string{"orders"}, Reason: engine.DiscardStatementDiffers, Age: "3h 12m"},
			{Namespace: "orders_b", Tables: []string{"orders"}, Reason: engine.DiscardCheckpointExpired, Age: "9d 4h"},
		},
	})

	assert.Contains(t, out, "⚠️ **Applying destroys work in progress**: **2** unfinished copies on the target\n",
		"no single age speaks for both copies, so the headline carries none")
	assert.Contains(t, out, "- `orders` in `orders_a` (last progress 3h 12m ago): the schema change differs from the one that started it")
	assert.Contains(t, out, "- `orders` in `orders_b` (last progress 9d 4h ago): it is too old to resume")
	assert.Contains(t, out, "the work already done is lost and cannot be recovered")
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

	assert.Contains(t, out, "⚠️ **Applying destroys work in progress**: **1** unfinished copy on the target\n",
		"an unknown age is omitted rather than rendered as a bare zero in the headline")
	assert.Contains(t, out, "- `orders` in `testapp`: the schema change differs from the one that started it")
	assert.Contains(t, out, "the work already done is lost and cannot be recovered")
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
