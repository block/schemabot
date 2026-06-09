package webhook

import (
	"context"
	"fmt"
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// checkPriorEnvironments enforces server-owned environment ordering: all enabled
// environments before the current one in the server promotion order must have a
// successful SchemaBot check.
// Returns true if the apply is blocked (caller should return).
//
// For environments: [sandbox, staging, production]
//   - applying to sandbox: no prior envs, always allowed
//   - applying to staging: sandbox must be success
//   - applying to production: both sandbox and staging must be success
//
// Server config owns the configured environments and their promotion order.
// When allowed_environments is configured, this instance only owns a subset of
// environments. For prior environments owned by this instance, local storage is
// checked. For prior environments owned by another instance, the GitHub Checks
// API is queried for the per-environment aggregate check run.
func (h *Handler) checkPriorEnvironments(
	ctx context.Context, repo string, pr int,
	database, dbType, environment string,
	environments []string,
	installationID int64, requestedBy string,
) bool {
	config := h.service.Config()
	environments = config.OrderedEnvironments(environments)

	// Find the index of the current environment
	currentIdx := -1
	for i, env := range environments {
		if env == environment {
			currentIdx = i
			break
		}
	}

	// First environment or not in list — no prior environments to check
	if currentIdx <= 0 {
		return false
	}

	// Check all prior environments
	for i := 0; i < currentIdx; i++ {
		priorEnv := environments[i]

		if config.IsEnvironmentAllowed(priorEnv) {
			// This instance owns the prior environment — check local database
			if blocked := h.checkPriorEnvViaLocal(ctx, repo, pr, database, dbType, environment, priorEnv, installationID); blocked {
				return true
			}
		} else {
			// Another instance owns this environment — check GitHub Checks API
			if blocked := h.checkPriorEnvViaGitHub(ctx, repo, pr, database, environment, priorEnv, installationID); blocked {
				return true
			}
		}
	}

	return false
}

// checkPriorEnvViaLocal checks the prior environment status using the local database.
func (h *Handler) checkPriorEnvViaLocal(
	ctx context.Context, repo string, pr int,
	database, dbType, environment, priorEnv string,
	installationID int64,
) bool {
	check, err := h.waitForLocalPriorEnvCheck(ctx, repo, pr, database, dbType, environment, priorEnv)
	if err != nil {
		h.logger.Error("failed to look up prior environment check",
			"repo", repo, "pr", pr,
			"database", database, "database_type", dbType,
			"environment", environment, "prior_environment", priorEnv,
			"error", err)
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnvCheckError(priorEnv, "read SchemaBot storage", err))
		return true
	}

	if check == nil {
		h.logger.Warn("prior environment check is missing, blocking apply",
			"repo", repo, "pr", pr,
			"database", database, "database_type", dbType,
			"environment", environment, "prior_environment", priorEnv,
			"attempts", h.priorEnvCheckMaxAttemptCount())
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByMissingPriorEnvCheck(priorEnv))
		return true
	}

	switch {
	case check.Conclusion == checkConclusionSuccess:
		h.logger.Debug("prior environment check passed, allowing apply",
			"repo", repo, "pr", pr,
			"database", database, "database_type", dbType,
			"environment", environment, "prior_environment", priorEnv,
			"check_status", check.Status, "check_conclusion", check.Conclusion)
		return false
	case check.Status == checkStatusInProgress:
		h.logger.Warn("prior environment check is still in progress after retries, blocking apply",
			"repo", repo, "pr", pr,
			"database", database, "database_type", dbType,
			"environment", environment, "prior_environment", priorEnv,
			"check_status", check.Status, "check_conclusion", check.Conclusion,
			"attempts", h.priorEnvCheckMaxAttemptCount())
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnvInProgress(database, environment, priorEnv))
		return true
	default:
		status := "has pending changes"
		action := fmt.Sprintf("Apply %s first", priorEnv)
		if check.Conclusion == checkConclusionFailure {
			status = "failed"
			action = fmt.Sprintf("Fix the issue and re-apply %s", priorEnv)
		}
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnv(database, environment, priorEnv, status, action))
		return true
	}
}

func (h *Handler) waitForLocalPriorEnvCheck(
	ctx context.Context, repo string, pr int,
	database, dbType, environment, priorEnv string,
) (*storage.Check, error) {
	// A later-environment apply can race a prior-environment plan/apply webhook
	// that has not persisted its check state yet. Retry briefly, then preserve
	// the fail-closed behavior if the prior environment still cannot be proven
	// safe.
	attempts := h.priorEnvCheckMaxAttemptCount()
	for attempt := 1; attempt <= attempts; attempt++ {
		check, err := h.service.Storage().Checks().Get(ctx, repo, pr, priorEnv, dbType, database)
		if err != nil {
			return nil, err
		}
		if !shouldRetryStoredPriorEnvCheck(check) || attempt == attempts {
			return check, nil
		}

		h.logger.Debug("prior environment check state not ready, retrying",
			"repo", repo, "pr", pr,
			"database", database, "database_type", dbType,
			"environment", environment, "prior_environment", priorEnv,
			"check_status", storedPriorEnvCheckStatus(check),
			"attempt", attempt, "max_attempts", attempts)
		if err := h.waitBeforePriorEnvCheckRetry(ctx); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func shouldRetryStoredPriorEnvCheck(check *storage.Check) bool {
	return check == nil || check.Status == checkStatusInProgress
}

func storedPriorEnvCheckStatus(check *storage.Check) string {
	if check == nil {
		return "missing"
	}
	return check.Status
}

// checkPriorEnvViaGitHub checks the prior environment status by querying the
// GitHub Checks API for the per-environment aggregate check run created by the
// other SchemaBot instance that owns that environment.
//
// The remote check uses the prior environment's aggregate Check Run, which rolls
// up ALL databases in the prior environment. This is stricter than per-database
// checking: production apply for any database is blocked until ALL databases in
// staging are applied. This is the correct behavior for the remote case — we
// cannot query per-database check state from another instance, and it is safer to
// require the entire environment to be healthy before promoting.
func (h *Handler) checkPriorEnvViaGitHub(
	ctx context.Context, repo string, pr int,
	database, environment, priorEnv string,
	installationID int64,
) bool {
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for prior env check, blocking apply",
			"prior_env", priorEnv, "error", err)
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnvCheckError(priorEnv, "create GitHub client", err))
		return true
	}

	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to fetch PR for prior env check, blocking apply",
			"prior_env", priorEnv, "error", err)
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnvCheckError(priorEnv, "fetch PR details", err))
		return true
	}

	checkName := aggregateCheckNameForEnv(h.aggregateCheckNameForRepo(repo), priorEnv)
	checkResult, err := h.waitForGitHubPriorEnvCheck(ctx, client, repo, pr, database, environment, priorEnv, prInfo.HeadSHA, checkName)
	if err != nil {
		h.logger.Error("failed to query GitHub check for prior environment, blocking apply",
			"prior_env", priorEnv, "check_name", checkName, "error", err)
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnvCheckError(priorEnv, "query check runs", err))
		return true
	}

	if checkResult == nil {
		h.logger.Warn("no GitHub check found for prior environment, blocking apply",
			"repo", repo, "pr", pr,
			"database", database,
			"environment", environment, "prior_environment", priorEnv,
			"check_name", checkName,
			"attempts", h.priorEnvCheckMaxAttemptCount())
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByMissingPriorEnvCheck(priorEnv))
		return true
	}

	switch {
	case checkResult.Status == checkStatusCompleted && checkResult.Conclusion == checkConclusionSuccess:
		h.logger.Debug("prior environment verified via GitHub check",
			"repo", repo, "pr", pr,
			"database", database,
			"environment", environment, "prior_environment", priorEnv,
			"check_name", checkName, "conclusion", checkResult.Conclusion)
		return false
	case checkResult.Status == checkStatusInProgress || checkResult.Status == checkStatusQueued:
		h.logger.Warn("prior environment GitHub check is still non-terminal after retries, blocking apply",
			"repo", repo, "pr", pr,
			"database", database,
			"environment", environment, "prior_environment", priorEnv,
			"check_name", checkName,
			"check_status", checkResult.Status, "check_conclusion", checkResult.Conclusion,
			"attempts", h.priorEnvCheckMaxAttemptCount())
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnvInProgress(database, environment, priorEnv))
		return true
	default:
		status := "has pending changes"
		action := fmt.Sprintf("Apply %s first", priorEnv)
		if checkResult.Conclusion == checkConclusionFailure {
			status = "failed"
			action = fmt.Sprintf("Fix the issue and re-apply %s", priorEnv)
		}
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnv(database, environment, priorEnv, status, action))
		return true
	}
}

func (h *Handler) waitForGitHubPriorEnvCheck(
	ctx context.Context, client *ghclient.InstallationClient,
	repo string, pr int,
	database, environment, priorEnv, headSHA, checkName string,
) (*ghclient.CheckRunResult, error) {
	// Cross-instance prior environment checks depend on GitHub Check Run
	// visibility. Retry briefly so ordering jitter does not cause an avoidable
	// block, then fail closed if the required check is still missing or running.
	attempts := h.priorEnvCheckMaxAttemptCount()
	for attempt := 1; attempt <= attempts; attempt++ {
		checkResult, err := client.FindCheckRunByName(ctx, repo, headSHA, checkName)
		if err != nil {
			return nil, err
		}
		if !shouldRetryGitHubPriorEnvCheck(checkResult) || attempt == attempts {
			return checkResult, nil
		}

		h.logger.Debug("prior environment GitHub check not ready, retrying",
			"repo", repo, "pr", pr,
			"database", database,
			"environment", environment, "prior_environment", priorEnv,
			"head_sha", headSHA,
			"check_name", checkName,
			"check_status", githubPriorEnvCheckStatus(checkResult),
			"attempt", attempt, "max_attempts", attempts)
		if err := h.waitBeforePriorEnvCheckRetry(ctx); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func shouldRetryGitHubPriorEnvCheck(check *ghclient.CheckRunResult) bool {
	return check == nil || check.Status == checkStatusQueued || check.Status == checkStatusInProgress
}

func githubPriorEnvCheckStatus(check *ghclient.CheckRunResult) string {
	if check == nil {
		return "missing"
	}
	return check.Status
}

func (h *Handler) priorEnvCheckMaxAttemptCount() int {
	if h == nil || h.priorEnvCheckMaxAttempts <= 0 {
		return defaultPriorEnvCheckMaxAttempts
	}
	return h.priorEnvCheckMaxAttempts
}

func (h *Handler) priorEnvCheckRetryDelay() time.Duration {
	if h == nil || h.priorEnvCheckRetryInterval <= 0 {
		return defaultPriorEnvCheckRetryInterval
	}
	return h.priorEnvCheckRetryInterval
}

func (h *Handler) waitBeforePriorEnvCheckRetry(ctx context.Context) error {
	timer := time.NewTimer(h.priorEnvCheckRetryDelay())
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
