package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/glyph"
)

func narrowedPlan() PlanCommentData {
	data := planWithChanges()
	data.Environment = "production"
	data.Target = "payments-002"
	return data
}

// Rolling a change out one target at a time is an ordinary rollout, so a plan
// narrowed to one target reads like any other plan: the metadata line names
// the target beside the database, with no note or glyph calling it out. Its
// apply carries the same --target, because an apply of a narrowed plan has to
// name its target.
func TestPlanCommentNarrowedToOneTarget(t *testing.T) {
	data := narrowedPlan()
	data.ScopedDatabase = "orders"
	rendered := RenderPlanComment(data)
	wholeEnvironment := data
	wholeEnvironment.Target = ""
	whole := RenderPlanComment(wholeEnvironment)

	assert.Contains(t, rendered, "**Database**: `orders` | **Type**: `MySQL` | **Target**: `payments-002`")
	assert.Contains(t, rendered, "schemabot apply -e production -d orders --target payments-002")
	assert.Equal(t, strings.Count(whole, "\n"), strings.Count(rendered, "\n"), "narrowing adds no lines to the comment")
	assert.NotContains(t, rendered, glyph.Attention)
	assert.NotContains(t, rendered, glyph.Info)
}

// apply-confirm on a narrowed apply takes no --target: the pending plan
// already names its target.
func TestApplyCommentNarrowedToOneTarget(t *testing.T) {
	data := narrowedPlan()
	data.Tenant = "alpha"
	data.IsLocked = true
	data.PendingManualConfirmation = true
	rendered := RenderPlanComment(data)

	assert.Contains(t, rendered, "**Target**: `payments-002` | **Tenant**: `alpha`")
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

// A narrowed comment echoes the target inside code spans it cannot break
// out of. Target names are opaque, and the missing-environment reply echoes
// one the server has not yet matched, so a backtick in the name must not end
// the span and turn the rest of the comment into markdown.
func TestNarrowedTargetStaysInsideItsCodeSpan(t *testing.T) {
	data := narrowedPlan()
	data.Target = "pay`ments"
	rendered := RenderPlanComment(data)

	assert.Contains(t, rendered, "**Target**: `` pay`ments ``")
	assert.Equal(t, "`--target` picks a target inside one environment, so it needs `-e` too.\n\n"+
		"**Usage**: `` schemabot plan -e <environment> --target **x`y** ``",
		RenderTargetMissingEnv("plan", "**x`y**"))
}

func TestRenderTargetFlagUsage(t *testing.T) {
	assert.Equal(t, "The `--target` flag is not supported for `rollback`. Only `plan` and `apply` take it.",
		RenderUnsupportedTargetFlag("rollback"))
	assert.Equal(t, "`--target` picks a target inside one environment, so it needs `-e` too.\n\n"+
		"**Usage**: `schemabot plan -e <environment> --target prod/payments-002`",
		RenderTargetMissingEnv("plan", "prod/payments-002"))
}
