package webhook

import (
	"context"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// filterFailingNonSchemaBotChecks returns checks that are failing, excluding
// SchemaBot's own checks and checks with conclusion "neutral", "skipped", or "success".
// Only checks with completed status and conclusion "failure", "error", or "timed_out"
// are considered failing.
func filterFailingNonSchemaBotChecks(statuses []ghclient.PRCheckStatus, config *api.ServerConfig) []templates.BlockingCheck {
	var failing []templates.BlockingCheck
	filterRequiredChecks := statusRollupContainsRequiredCheck(statuses, config)
	for _, s := range statuses {
		if s.IsSchemaBot {
			continue
		}
		if filterRequiredChecks && !config.IsCheckRequired(s.Name) {
			continue
		}
		if s.Status != "completed" {
			continue
		}
		switch s.Conclusion {
		case "failure", "error", "timed_out":
			failing = append(failing, templates.BlockingCheck{
				Name:  s.Name,
				State: s.Conclusion,
			})
		}
	}
	return failing
}

// enforcePassingChecks verifies that all non-SchemaBot PR checks are passing.
// Returns true if apply was blocked (caller should return), false if it may proceed.
// Blocks on both failing checks and in-progress checks with distinct messages.
func (h *Handler) enforcePassingChecks(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, headSHA, environment string) bool {
	config := h.service.Config()
	if !config.ShouldRequirePassingChecks() {
		h.logger.Debug("passing checks gate disabled", "repo", repo, "pr", pr)
		return false
	}

	statuses, err := client.GetPRCheckStatuses(ctx, repo, headSHA)
	if err != nil {
		h.logger.Error("failed to fetch PR check statuses, blocking apply",
			"repo", repo, "pr", pr, "environment", environment, "error", err)
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByCheckStatusError(environment, err))
		return true
	}

	failing := filterFailingNonSchemaBotChecks(statuses, config)
	inProgress := filterInProgressNonSchemaBotChecks(statuses, config)

	if len(failing) > 0 {
		h.logger.Info("apply blocked by failing PR checks",
			"repo", repo, "pr", pr, "environment", environment,
			"failing_count", len(failing))
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByFailingChecks(environment, failing))
		return true
	}

	if len(inProgress) > 0 {
		h.logger.Info("apply blocked by in-progress PR checks",
			"repo", repo, "pr", pr, "environment", environment,
			"in_progress_count", len(inProgress))
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByInProgressChecks(environment, inProgress))
		return true
	}

	return false
}

// filterInProgressNonSchemaBotChecks returns checks that are still running,
// excluding SchemaBot's own checks.
func filterInProgressNonSchemaBotChecks(statuses []ghclient.PRCheckStatus, config *api.ServerConfig) []templates.BlockingCheck {
	var inProgress []templates.BlockingCheck
	filterRequiredChecks := statusRollupContainsRequiredCheck(statuses, config)
	for _, s := range statuses {
		if s.IsSchemaBot {
			continue
		}
		if filterRequiredChecks && !config.IsCheckRequired(s.Name) {
			continue
		}
		switch s.Status {
		case "in_progress", "queued", "pending":
			inProgress = append(inProgress, templates.BlockingCheck{
				Name:  s.Name,
				State: s.Status,
			})
		}
	}
	return inProgress
}

func statusRollupContainsRequiredCheck(statuses []ghclient.PRCheckStatus, config *api.ServerConfig) bool {
	if config == nil || len(config.RequiredChecks) == 0 {
		return false
	}
	for _, s := range statuses {
		if s.IsSchemaBot {
			continue
		}
		if config.IsCheckRequired(s.Name) {
			return true
		}
	}
	return false
}
