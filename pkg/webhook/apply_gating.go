package webhook

import (
	"context"
	"strings"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// isAggregateCheckName reports whether a check name matches this deployment's
// configured aggregate Check Run name: the base name itself or the
// environment-scoped form "base (<env>)". Aggregate identity is decided by
// the creating GitHub App, never by name — this predicate exists only to
// flag aggregate-named checks from untrusted apps for operator triage.
func isAggregateCheckName(name, aggregateBase string) bool {
	if name == aggregateBase {
		return true
	}
	return strings.HasPrefix(name, aggregateBase+" (") && strings.HasSuffix(name, ")")
}

// filterNonPassingNonSchemaBotChecks returns completed checks that block apply,
// excluding checks created by trusted SchemaBot GitHub Apps. A completed check
// is ignored only when its conclusion is "success", "neutral", or "skipped";
// every other conclusion (such as "failure", "timed_out", "cancelled",
// "action_required", "stale", or "startup_failure") blocks apply, so
// unrecognized conclusions fail closed.
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
	notReported := missingRequiredChecks(statuses, config)
	h.flagUntrustedAggregateNamedChecks(ctx, statuses, config, repo, pr, headSHA, environment)

	if len(notPassing) > 0 {
		h.logger.Info("apply blocked by non-passing PR checks",
			"repo", repo, "pr", pr, "environment", environment,
			"not_passing_count", len(notPassing))
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByNonPassingChecks(environment, notPassing))
		return true
	}

	if len(inProgress) > 0 || len(notReported) > 0 {
		h.logger.Info("apply blocked: PR checks have not finished verifying this commit",
			"repo", repo, "pr", pr, "environment", environment,
			"in_progress_count", len(inProgress),
			"missing_required_count", len(notReported),
			"missing_required_checks", blockingCheckNames(notReported))
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByInProgressChecks(environment, inProgress, notReported))
		return true
	}

	return false
}

// blockingCheckNames extracts the check names from a slice of blocking checks
// so triage logs can name the checks rather than only count them.
func blockingCheckNames(checks []templates.BlockingCheck) []string {
	names := make([]string, 0, len(checks))
	for _, c := range checks {
		names = append(names, c.Name)
	}
	return names
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

// flagUntrustedAggregateNamedChecks surfaces PR checks that carry this
// deployment's aggregate Check Run name but were not created by a trusted
// SchemaBot GitHub App. Such a check is either a spoof attempt or a sibling
// deployment whose App slug is missing from trusted-check-app-slugs; the
// signal fires in both cases because operators need the app identity to tell
// the two apart, independent of whether the check currently gates applies.
// The log states the check's actual gating impact: it is treated as ordinary
// external CI, but when required_checks narrowing is active a non-required
// check does not block.
func (h *Handler) flagUntrustedAggregateNamedChecks(ctx context.Context, statuses []ghclient.PRCheckStatus, config *api.ServerConfig, repo string, pr int, headSHA, environment string) {
	aggregateBase := h.aggregateCheckNameForRepo(repo)
	filterRequiredChecks := statusesContainRequiredCheck(statuses, config)
	for _, s := range statuses {
		if s.IsSchemaBot {
			continue
		}
		if !isAggregateCheckName(s.Name, aggregateBase) {
			continue
		}
		if filterRequiredChecks && !config.IsCheckRequired(s.Name) {
			h.logger.Warn("aggregate-named check from untrusted GitHub App is present; required_checks narrowing keeps it from gating applies, but it may be impersonating SchemaBot",
				"repo", repo, "pr", pr, "head_sha", headSHA, "environment", environment,
				"check_name", s.Name, "app_slug", s.AppSlug,
				"check_status", s.Status, "check_conclusion", s.Conclusion)
		} else {
			h.logger.Warn("aggregate-named check from untrusted GitHub App is treated as external CI and will block applies unless passing",
				"repo", repo, "pr", pr, "head_sha", headSHA, "environment", environment,
				"check_name", s.Name, "app_slug", s.AppSlug,
				"check_status", s.Status, "check_conclusion", s.Conclusion)
		}
		metrics.RecordUntrustedAggregateNamedCheck(ctx, repo, environment, s.AppSlug, metrics.CheckTrustGatePassingChecks)
	}
}

// filterInProgressNonSchemaBotChecks returns checks that are still running,
// excluding checks created by trusted SchemaBot GitHub Apps.
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

// missingRequiredStateNotReported is the State surfaced for a configured
// required check that GitHub has not yet reported on the PR commit. It mirrors
// how a legacy commit status in the EXPECTED state maps to in_progress: GitHub
// knows the check is owed but has not delivered a result, so apply must wait.
const missingRequiredStateNotReported = "not reported"

// missingRequiredChecks returns the configured required checks that GitHub has
// not reported on the commit, by name regardless of which app owns the status.
// An absent required check is treated as a blocking in-progress check so the
// apply gate fails closed: a required check that has not reported must never
// let an apply through. Returns nil when no required checks are configured,
// since the gate then has no named checks to demand.
func missingRequiredChecks(statuses []ghclient.PRCheckStatus, config *api.ServerConfig) []templates.BlockingCheck {
	if config == nil || len(config.RequiredChecks) == 0 {
		return nil
	}
	reported := make(map[string]struct{}, len(statuses))
	for _, s := range statuses {
		reported[s.Name] = struct{}{}
	}
	var missing []templates.BlockingCheck
	for _, name := range config.RequiredChecks {
		if _, ok := reported[name]; ok {
			continue
		}
		missing = append(missing, templates.BlockingCheck{
			Name:  name,
			State: missingRequiredStateNotReported,
		})
	}
	return missing
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
