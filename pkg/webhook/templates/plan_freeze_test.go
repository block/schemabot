package templates

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var planFreezeEnd = time.Date(2026, 12, 2, 0, 0, 0, 0, time.UTC)

func frozenPlan(env string, notice *PlanFreezeNotice) PlanCommentData {
	plan := baseSchemaPlan(env, nil)
	plan.Freezes = notice
	return plan
}

// A freeze in effect is named in the plan, before the DDL, with the
// override that gets an urgent change through under the default policy, so
// the author knows the apply would be refused before they comment it.
func TestRenderPlanCommentShowsAFreezeInEffect(t *testing.T) {
	out := RenderPlanComment(frozenPlan("production", &PlanFreezeNotice{
		InEffect:         []RefusingFreeze{{ID: "freeze-3f9a1c2e7b4d8a06", Reason: "Holiday code freeze", EndsAt: &planFreezeEnd}},
		OverrideOnReason: true,
	}))

	notice := "❄️ **`production` is frozen, so applying this plan would be refused now.**"
	require.Contains(t, out, notice)
	assert.Contains(t, out, "| `freeze-3f9a1c2e7b4d8a06` | until 2026-12-02T00:00:00Z | Holiday code freeze |")
	assert.Contains(t, out, "Wait for the freeze to end before you apply. If this change can't wait, apply with `--override` and your reason, as the command below does: SchemaBot lets it through and records your reason.")
	assert.Less(t, strings.Index(out, notice), strings.Index(out, "CREATE TABLE"), "the notice leads the DDL")
	assert.Contains(t, out, "```\nschemabot apply -e production --override \"<why it can't wait>\"\n```", "the apply is offered with the override")
}

// Under the approval policy the notice says an override needs an approver,
// and names the teams one comes from.
func TestRenderPlanCommentFreezeUnderTheApprovalPolicy(t *testing.T) {
	out := RenderPlanComment(frozenPlan("production", &PlanFreezeNotice{
		InEffect:  []RefusingFreeze{{ID: "freeze-3f9a1c2e7b4d8a06", Reason: "Holiday code freeze"}},
		Approvers: []string{"acme/freeze-approvers"},
	}))

	assert.Contains(t, out, "| `freeze-3f9a1c2e7b4d8a06` | until lifted | Holiday code freeze |")
	assert.Contains(t, out, "A freeze approver other than you and the PR author, a member of `acme/freeze-approvers`, must approve it, and then you run the command again.")
	assert.Contains(t, out, `schemabot apply -e production --override "<why it can't wait>"`)
	assert.NotContains(t, out, "records your reason")
}

// Under the approval policy with no approver team, nobody could approve an
// override from the pull request, so neither the notice nor the command
// offers one, as the refusal does not.
func TestRenderPlanCommentFreezeWithNoApproverOffersNoOverride(t *testing.T) {
	out := RenderPlanComment(frozenPlan("production", &PlanFreezeNotice{
		InEffect: []RefusingFreeze{{ID: "freeze-3f9a1c2e7b4d8a06"}},
	}))

	assert.Contains(t, out, "Wait for the freeze to end before you apply.\n")
	assert.Contains(t, out, "`production` is frozen")
	assert.NotContains(t, out, "--override")
}

// A scheduled freeze, such as a planned code freeze, is named with when it
// starts, so the author can apply before it does.
func TestRenderPlanCommentShowsAScheduledFreeze(t *testing.T) {
	out := RenderPlanComment(frozenPlan("production", &PlanFreezeNotice{
		Scheduled: []ScheduledFreeze{{
			ID: "freeze-8c41d2e9a7f03b56", Reason: "Launch week code freeze",
			StartsAt: time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC), EndsAt: &planFreezeEnd,
		}},
		OverrideOnReason: true,
	}))

	assert.Contains(t, out, "❄️ **A freeze is scheduled in `production`.** Apply before it starts, and let the change finish: while it holds, the apply is refused, and so is a `start` or `cutover` the change still needs.")
	assert.Contains(t, out, "| `freeze-8c41d2e9a7f03b56` | 2026-12-01T00:00:00Z | until 2026-12-02T00:00:00Z | Launch week code freeze |")
	assert.NotContains(t, out, "is frozen", "nothing refuses the apply yet")
	assert.NotContains(t, out, "--override", "an apply before the freeze starts needs no override")
}

// Freezes in effect and scheduled are both shown, each counted beyond the
// listed ones.
func TestRenderPlanCommentShowsInEffectAndScheduledFreezes(t *testing.T) {
	notice := &PlanFreezeNotice{OverrideOnReason: true}
	for i := range maxListedFreezes + 2 {
		notice.InEffect = append(notice.InEffect, RefusingFreeze{ID: fmt.Sprintf("freeze-a%015d", i)})
		notice.Scheduled = append(notice.Scheduled, ScheduledFreeze{ID: fmt.Sprintf("freeze-b%015d", i), StartsAt: planFreezeEnd})
	}

	out := RenderPlanComment(frozenPlan("staging", notice))

	assert.Contains(t, out, "Wait for the freezes to end before you apply.")
	assert.Contains(t, out, "2 more freezes also hold it.")
	assert.Contains(t, out, "❄️ **Freezes are scheduled in `staging`.**")
	assert.Contains(t, out, "2 more freezes are scheduled.")
	assert.Less(t, strings.Index(out, "is frozen"), strings.Index(out, "are scheduled"))
}

// A freeze reason is a person's text: a mention in it notifies nobody and
// markup in it cannot break the table.
func TestRenderPlanCommentFreezeReasonIsDefused(t *testing.T) {
	out := RenderPlanComment(frozenPlan("staging", &PlanFreezeNotice{
		Scheduled: []ScheduledFreeze{{ID: "freeze-8c41d2e9a7f03b56", Reason: "ping @octocat | </details>", StartsAt: planFreezeEnd}},
	}))

	assert.Contains(t, out, "ping `@octocat`", "the mention is a code span, which notifies nobody")
	assert.Contains(t, out, `\| \</details> |`, "the pipe and the tag are escaped")
}

// No notice without freezes.
func TestRenderPlanCommentWithoutFreezesHasNoNotice(t *testing.T) {
	for name, notice := range map[string]*PlanFreezeNotice{"nil": nil, "empty": {OverrideOnReason: true}} {
		t.Run(name, func(t *testing.T) {
			assert.NotContains(t, RenderPlanComment(frozenPlan("staging", notice)), "❄️")
		})
	}
}

// In a comment for several environments, the notice renders in the frozen
// environment's section, and that environment is never merged with one that
// is not frozen.
func TestRenderMultiEnvPlanCommentScopesFreezeNoticeToItsEnvironment(t *testing.T) {
	staging := frozenPlan("staging", nil)
	production := frozenPlan("production", &PlanFreezeNotice{
		InEffect: []RefusingFreeze{{ID: "freeze-3f9a1c2e7b4d8a06", Reason: "Holiday code freeze"}}, OverrideOnReason: true,
	})
	require.False(t, plansIdentical(&staging, &production), "a frozen environment must not dedupe with one that is not")

	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "testapp",
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})

	assert.Equal(t, 1, strings.Count(out, "is frozen"))
	productionAt := strings.Index(out, "Production")
	require.NotEqual(t, -1, productionAt)
	assert.Less(t, productionAt, strings.Index(out, "`production` is frozen"), "the notice renders in the production section")
}

// A frozen plan with an unsafe change asks for both consents on one command:
// the --allow-unsafe instruction leads into the command that already carries
// --override, so the author is not refused once for each.
func TestRenderPlanCommentFrozenUnsafeChangeAsksForBothFlags(t *testing.T) {
	plan := frozenPlan("production", &PlanFreezeNotice{
		InEffect: []RefusingFreeze{{ID: "freeze-3f9a1c2e7b4d8a06", Reason: "Holiday code freeze"}}, OverrideOnReason: true,
	})
	plan.HasUnsafeChanges = true
	plan.UnsafeChanges = []UnsafeChangeData{{Table: "users", Reason: "drops column email"}}

	out := RenderPlanComment(plan)

	assert.Contains(t, out, "add `--allow-unsafe` to confirm 1 unsafe change on `users`")
	assert.Contains(t, out, "schemabot apply -e production --override \"<why it can't wait>\"")
}

// In a comment for several environments, only the frozen environment's apply
// carries --override.
func TestRenderMultiEnvPlanCommentOverridesOnlyTheFrozenEnvironment(t *testing.T) {
	staging := frozenPlan("staging", nil)
	production := frozenPlan("production", &PlanFreezeNotice{
		InEffect: []RefusingFreeze{{ID: "freeze-3f9a1c2e7b4d8a06"}}, OverrideOnReason: true,
	})

	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "testapp",
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})

	assert.Contains(t, out, "```\nschemabot apply -e staging\n```")
	assert.Contains(t, out, "```\nschemabot apply -e production --override \"<why it can't wait>\"\n```")
}

// An apply refused for its unsafe change meets the freeze next, so the
// refusal names the freeze and its retry carries --override. The author types
// the reason again, as they type --allow-unsafe again.
func TestRenderUnsafeChangesBlockedOffersTheOverride(t *testing.T) {
	plan := frozenPlan("production", &PlanFreezeNotice{
		InEffect: []RefusingFreeze{{ID: "freeze-3f9a1c2e7b4d8a06", Reason: "Holiday code freeze"}}, OverrideOnReason: true,
	})
	plan.HasUnsafeChanges = true
	plan.UnsafeChanges = []UnsafeChangeData{{Table: "users", Reason: "drops column email"}}

	out := RenderUnsafeChangesBlocked(plan)

	assert.Contains(t, out, "`production` is frozen")
	assert.Contains(t, out, "```\nschemabot apply -e production --allow-unsafe --override \"<why it can't wait>\"\n```")
}

// Without a freeze in effect, the retry carries no override: nothing is left
// for it to let through.
func TestRenderUnsafeChangesBlockedWithoutAFreezeOffersNoOverride(t *testing.T) {
	plan := frozenPlan("production", nil)
	plan.HasUnsafeChanges = true
	plan.UnsafeChanges = []UnsafeChangeData{{Table: "users", Reason: "drops column email"}}

	assert.NotContains(t, RenderUnsafeChangesBlocked(plan), "--override")
}

// A freeze notice points at the apply command below it, so a plan whose apply
// the footer withholds shows none: behind an earlier environment's refused
// apply, or under a member apply refusal.
func TestRenderPlanCommentFreezeNoticeNeedsAnOfferedApply(t *testing.T) {
	notice := func() *PlanFreezeNotice {
		return &PlanFreezeNotice{InEffect: []RefusingFreeze{{ID: "freeze-3f9a1c2e7b4d8a06"}}, OverrideOnReason: true}
	}
	t.Run("behind a refused environment", func(t *testing.T) {
		staging := frozenPlan("staging", nil)
		staging.BlockedChanges = []BlockedChangeData{{Table: "users", Reason: "the engine refuses it"}}
		production := frozenPlan("production", notice())

		out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
			Database:     "testapp",
			Environments: []string{"staging", "production"},
			Plans:        map[string]*PlanCommentData{"staging": &staging, "production": &production},
		})

		require.Contains(t, out, "applies only after staging")
		assert.NotContains(t, out, "is frozen")
		assert.NotContains(t, out, "--override")
	})
	t.Run("member apply refusal", func(t *testing.T) {
		plan := frozenPlan("production", notice())
		plan.MemberApplyRefusalTarget = "eu"
		plan.MemberApplyRefusal = "its plan is stale"

		out := RenderPlanComment(plan)

		require.Contains(t, out, "This PR cannot apply every target's plan")
		assert.NotContains(t, out, "is frozen")
		assert.NotContains(t, out, "--override")
	})
}
