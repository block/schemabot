package webhook

import (
	"context"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// annotatePlanFreezes adds the freezes that cover this environment's change to
// a plan comment that offers an apply, so its author learns before applying
// that a freeze refuses the apply now, or will once it starts. A plan narrowed
// to one target passes narrowed, as its apply does. database is the PR's
// database, which the apply gates on: the plan response may leave it empty.
//
// The notice only informs. The apply's change gate reads the freezes again and
// is the one that refuses (AZ-10), so a lookup that fails leaves the notice off
// and never fails the plan.
func (h *Handler) annotatePlanFreezes(ctx context.Context, data *templates.PlanCommentData, planResp *apitypes.PlanResponse, database, environment string, narrowed bool, repo string, pr int) {
	if h.service == nil || planResp == nil || !data.OffersApply() {
		return
	}
	freezes, err := h.service.FreezesCoveringPlan(ctx, environment, database, planResp.Deployment, narrowed)
	if err != nil {
		h.logger.Warn("could not read the freezes for the plan comment; it shows no freeze notice, and the apply checks the freezes again",
			"repo", repo, "pr", pr, "database", database, "environment", environment, "error", err)
		return
	}
	if len(freezes.InEffect) == 0 && len(freezes.Scheduled) == 0 {
		return
	}
	notice := &templates.PlanFreezeNotice{OverrideOnReason: true} // the default policy
	if config, ok := h.serverConfig(); ok {
		notice.OverrideOnReason = config.Freeze.OverrideOnReason()
		notice.Approvers = config.Freeze.GitHubTeams
	}
	for _, freeze := range freezes.InEffect {
		notice.InEffect = append(notice.InEffect, templates.RefusingFreeze{
			ID: freeze.Identifier, Reason: freeze.Reason, EndsAt: freeze.EndsAt,
		})
	}
	for _, freeze := range freezes.Scheduled {
		notice.Scheduled = append(notice.Scheduled, templates.ScheduledFreeze{
			ID: freeze.Identifier, Reason: freeze.Reason, StartsAt: freeze.StartsAt, EndsAt: freeze.EndsAt,
		})
	}
	data.Freezes = notice
}
