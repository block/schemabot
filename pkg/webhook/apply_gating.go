package webhook

import (
	"context"
	"strings"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// filterNonPassingNonSchemaBotChecks returns completed checks that block apply,
// excluding SchemaBot's own checks. A completed check is ignored only when its
// conclusion is "success", "neutral", or "skipped"; every other conclusion
// (such as "failure", "timed_out", "cancelled", "action_required", "stale",
// or "startup_failure") blocks apply, so unrecognized conclusions fail closed.
func filterNonPassingNonSchemaBotChecks(statuses []ghclient.PRCheckStatus, config *api.ServerConfig) []templates.BlockingCheck {
	var notPassing []templates.BlockingCheck
	filterRequiredChecks := statusesContainRequiredCheck(statuses, config)
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
		if isPassingCheckConclusion(s.Conclusion) {
			continue
		}
		notPassing = append(notPassing, templates.BlockingCheck{
			Name:  s.Name,
			State: s.Conclusion,
		})
	}
	return notPassing
}

// isPassingCheckConclusion reports whether a completed check's conclusion
// allows apply to proceed. Only "success", "neutral", and "skipped" pass;
// every other conclusion blocks apply.
func isPassingCheckConclusion(conclusion string) bool {
	switch conclusion {
	case "success", "neutral", "skipped":
		return true
	default:
		return false
	}
}

// enforcePassingChecks verifies that all non-SchemaBot PR checks are passing.
// Returns true if apply was blocked (caller should return), false if it may proceed.
// Blocks on both non-passing completed checks and in-progress checks with
// distinct messages.
func (h *Handler) enforcePassingChecks(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, headSHA, environment string) bool {
	config := h.service.Config()
	if !config.ShouldRequirePassingChecks() {
		h.logger.Debug("passing checks gate disabled", "repo", repo, "pr", pr)
		return false
	}

	statuses, err := client.GetPRCheckStatuses(ctx, repo, headSHA, config.RequiredChecks)
	if err != nil {
		details := &templates.CheckStatusAccessDetails{
			GitHubApp: h.githubAppDisplayNameForRepo(repo, client),
		}
		diagnostic := ghclient.CheckStatusAccessDiagnostic{}
		diagnosticRan := checkStatusReadLooksPermissionDenied(err)
		if diagnosticRan {
			diagnostic = client.DiagnoseCheckStatusAccess(ctx, repo, headSHA)
			details.MissingPermissions = diagnostic.MissingPermissions
			details.ChecksReadable = diagnostic.ChecksReadable
			details.CommitStatusesReadable = diagnostic.CommitStatusesReadable
		}
		h.logger.Error("failed to fetch PR check statuses, blocking apply",
			"repo", repo,
			"pr", pr,
			"environment", environment,
			"head_sha", headSHA,
			"installation_id", installationID,
			"github_operation", "read_pr_check_statuses",
			"github_app", details.GitHubApp,
			"diagnostic_ran", diagnosticRan,
			"checks_readable", diagnostic.ChecksReadable,
			"commit_statuses_readable", diagnostic.CommitStatusesReadable,
			"missing_permissions", diagnostic.MissingPermissions,
			"checks_error", diagnostic.ChecksError,
			"commit_statuses_error", diagnostic.CommitStatusesError,
			"error", err)
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByCheckStatusError(environment, err, details))
		return true
	}

	notPassing := filterNonPassingNonSchemaBotChecks(statuses, config)
	inProgress := filterInProgressNonSchemaBotChecks(statuses, config)

	if len(notPassing) > 0 {
		h.logger.Info("apply blocked by non-passing PR checks",
			"repo", repo, "pr", pr, "environment", environment,
			"not_passing_count", len(notPassing))
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByNonPassingChecks(environment, notPassing))
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

func checkStatusReadLooksPermissionDenied(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Resource not accessible")
}

func (h *Handler) githubAppDisplayNameForRepo(repo string, client *ghclient.InstallationClient) string {
	if client != nil {
		if slug := client.AppSlug(); slug != "" {
			return slug
		}
	}
	if cfg := h.config(); cfg != nil {
		if resolved, err := cfg.ResolveGitHubAppForRepo(repo); err == nil && resolved.Name != defaultAppName {
			return resolved.Name
		}
	}
	return ""
}

// filterInProgressNonSchemaBotChecks returns checks that are still running,
// excluding SchemaBot's own checks.
func filterInProgressNonSchemaBotChecks(statuses []ghclient.PRCheckStatus, config *api.ServerConfig) []templates.BlockingCheck {
	var inProgress []templates.BlockingCheck
	filterRequiredChecks := statusesContainRequiredCheck(statuses, config)
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

func statusesContainRequiredCheck(statuses []ghclient.PRCheckStatus, config *api.ServerConfig) bool {
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
