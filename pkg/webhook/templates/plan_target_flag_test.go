package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func narrowedPlan() PlanCommentData {
	data := planWithChanges()
	data.Environment = "production"
	data.Target = "payments-002"
	return data
}

// A plan narrowed to one target says so where the reader starts: the
// metadata line names the target, and the note under it says the other
// targets were not planned and the schema check was not updated, with the
// whole-environment plan that does update it. Its apply carries the same
// --target, because an apply of a narrowed plan has to name its target.
func TestPlanCommentNarrowedToOneTarget(t *testing.T) {
	data := narrowedPlan()
	data.ScopedDatabase = "orders"
	rendered := RenderPlanComment(data)

	assert.Contains(t, rendered, "**Database**: `orders` | **Type**: `MySQL` | **Target**: `payments-002`")
	assert.Contains(t, rendered, "**Target `payments-002` only.** Other targets in `production` are not planned, and this plan does not update the schema check.")
	assert.Contains(t, rendered, "Once every target has the change, run `schemabot plan -e production -d orders` to update the check.")
	assert.Contains(t, rendered, "schemabot apply -e production -d orders --target payments-002")
}

// The locked comment of a narrowed apply says the other targets are left as
// they are and the check blocks merge until a plan of every target shows the
// change is complete. apply-confirm takes no --target: the pending plan
// already names its target.
func TestApplyCommentNarrowedToOneTarget(t *testing.T) {
	data := narrowedPlan()
	data.Tenant = "alpha"
	data.IsLocked = true
	data.PendingManualConfirmation = true
	rendered := RenderPlanComment(data)

	assert.Contains(t, rendered, "**Target `payments-002` only.** Other targets in `production` are left as they are, and the schema check blocks merge until a plan of every target shows the change is complete.")
	assert.Contains(t, rendered, "run `schemabot plan -e production --tenant alpha` to update the check")
	assert.Contains(t, rendered, "schemabot apply-confirm -e production --tenant alpha\n")
	assert.NotContains(t, rendered, "apply-confirm -e production --target")
}

// The unsafe-change refusal of a narrowed apply coaches a re-run of the same
// narrowed apply, not one of the whole rollout.
func TestUnsafeRefusalNarrowedToOneTarget(t *testing.T) {
	data := narrowedPlan()
	data.HasUnsafeChanges = true
	data.UnsafeChanges = []UnsafeChangeData{{Table: "users", Reason: "drops a column", ChangeType: "drop"}}
	rendered := RenderUnsafeChangesBlocked(data)

	assert.Contains(t, rendered, "schemabot apply -e production --target payments-002 --allow-unsafe")
}

// A comment that covers the whole rollout carries no target anywhere.
func TestPlanCommentWithoutTargetNamesNone(t *testing.T) {
	rendered := RenderPlanComment(planWithChanges())

	assert.NotContains(t, rendered, "**Target")
	assert.NotContains(t, rendered, "--target")
}

func TestRenderTargetFlagUsage(t *testing.T) {
	assert.Equal(t, "The `--target` flag is not supported for `rollback`. Only `plan` and `apply` take it.",
		RenderUnsupportedTargetFlag("rollback"))
	assert.Equal(t, "`--target` picks a target inside one environment, so it needs `-e` too.\n\n"+
		"**Usage**: `schemabot plan -e <environment> --target prod/payments-002`",
		RenderTargetMissingEnv("plan", "prod/payments-002"))
}
