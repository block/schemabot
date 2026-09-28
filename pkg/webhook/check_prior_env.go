package webhook

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// checkPriorEnvironments enforces the database's effective environment
// promotion order: all enabled environments before the current one in that
// order must have a successful SchemaBot check.
// Returns blocked=true when a prior environment verifiably fails the gate
// (caller should return). A failure to read a prior environment's state — a
// storage read for a locally owned environment or a GitHub Check Run lookup
// for a remotely owned one — stops the command (fail closed) and is returned
// as an error, not a block: the ordering could not be verified, so the outcome
// is not the command's answer and a durable driver may re-drive it.
// Deployment-shape errors in that class (for example an own-App slug the
// Check Run trust check cannot verify) may not clear on a re-drive alone, but
// they are bounded by the driver's retry budget, so the gate does not
// maintain a separate taxonomy for them. suppressRetryComments silences the
// read-failure comments on durable attempts, where the driver retries and
// posts the single terminal answer instead; merit blocks always comment.
//
// For environments: [sandbox, staging, production]
//   - applying to sandbox: no prior envs, always allowed
//   - applying to staging: sandbox must be success
//   - applying to production: both sandbox and staging must be success
//
// Server config owns the configured environments and their promotion order.
// The effective order is the database's environment_order override when
// configured, otherwise the server-wide environment_order — databases promote
// through different environment sequences, so the gate resolves the order per
// database. When allowed_environments is configured, this instance only owns a
// subset of environments. For prior environments owned by this instance, local
// storage is checked. For prior environments owned by another instance, the
// GitHub Checks API is queried for the per-environment aggregate check run.
// Either way, only a result recorded on the PR's current head commit satisfies
// the gate: headSHA is the head the caller read, and stored check state for
// any other commit does not count for it.
func (h *Handler) checkPriorEnvironments(
	ctx context.Context, repo string, pr int, headSHA string,
	database, dbType, environment string,
	environments []string,
	installationID int64,
	suppressRetryComments bool,
) (blocked bool, err error) {
	config := h.service.Config()

	// On a scoped instance the database's effective promotion order is the only
	// authoritative source for which environments precede the target. If the
	// target is absent from that order we cannot identify its prior
	// environments, so staging-first ordering cannot be enforced. Fail closed:
	// block the apply with a config error rather than fall back to a local-only
	// ordering that omits prior environments owned by other instances.
	if scopedTargetMissingFromPromotionOrder(config, database, environment) {
		order := config.PromotionOrderForDatabase(database)
		h.logger.Warn("target environment is absent from the configured promotion order, blocking apply",
			"repo", repo, "pr", pr,
			"database", database, "database_type", dbType,
			"environment", environment,
			"promotion_order", order)
		metrics.RecordPromotionConfigErrorBlock(ctx, repo, database, environment)
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByUnlistedEnvironment(environment, order))
		return true, nil
	}

	environments = promotionGateEnvironments(config, database, environment, environments)

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
		return false, nil
	}

	// Check all prior environments
	for i := 0; i < currentIdx; i++ {
		priorEnv := environments[i]

		if config.IsEnvironmentAllowed(priorEnv) {
			// This instance owns the prior environment — check local database
			blocked, err := h.checkPriorEnvViaLocal(ctx, repo, pr, headSHA, database, dbType, environment, priorEnv, installationID, suppressRetryComments)
			if err != nil {
				return false, err
			}
			if blocked {
				return true, nil
			}
		} else {
			// Another instance owns this environment — check GitHub Checks API
			blocked, err := h.checkPriorEnvViaGitHub(ctx, repo, pr, database, environment, priorEnv, installationID, suppressRetryComments)
			if err != nil {
				return false, err
			}
			if blocked {
				return true, nil
			}
		}
	}

	return false, nil
}

// scopedTargetMissingFromPromotionOrder reports whether this instance is scoped
// to a subset of environments (allowed_environments is configured) while the
// target environment is absent from the database's effective promotion order.
// In that state the prior environments cannot be derived from the order, and
// the staging-first gate cannot be enforced — the caller must fail closed.
func scopedTargetMissingFromPromotionOrder(config *api.ServerConfig, database, environment string) bool {
	if config == nil || len(config.AllowedEnvironments) == 0 {
		return false
	}
	return !slices.Contains(config.PromotionOrderForDatabase(database), environment)
}

func promotionGateEnvironments(config *api.ServerConfig, database, environment string, environments []string) []string {
	if config == nil {
		return environments
	}
	if len(config.AllowedEnvironments) > 0 {
		order := config.PromotionOrderForDatabase(database)
		if slices.Contains(order, environment) {
			return order
		}
	}
	return config.OrderedEnvironmentsForDatabase(database, environments)
}

// promotionCheckNameForRepo returns the aggregate Check Run base name the
// promotion gate queries when a prior environment is owned by another
// deployment: the owning App's promotion-check-name override when configured,
// otherwise this deployment's own aggregate base.
func (h *Handler) promotionCheckNameForRepo(repo string) string {
	config, ok := h.serverConfig()
	if !ok {
		return aggregateCheckName
	}
	return config.PromotionCheckNameBaseForRepo(repo)
}

// checkPriorEnvViaLocal checks the prior environment status using the local
// database. A storage read failure stops the command (fail closed) and is
// returned as an error rather than a block.
//
// The stored check state is one row per target, not one per commit, so a
// success recorded on an earlier commit is still in the row after a push. The
// gate only accepts a success whose commit is headSHA, the PR's current head:
// a row for any other commit blocks, since the prior environment has not been
// verified against what would now be applied.
func (h *Handler) checkPriorEnvViaLocal(
	ctx context.Context, repo string, pr int, headSHA string,
	database, dbType, environment, priorEnv string,
	installationID int64,
	suppressRetryComments bool,
) (blocked bool, err error) {
	if headSHA == "" {
		h.logger.Error("PR head commit is unknown, cannot verify prior environment check, stopping apply",
			"repo", repo, "pr", pr,
			"database", database, "database_type", dbType,
			"environment", environment, "prior_environment", priorEnv)
		if !suppressRetryComments {
			h.postComment(repo, pr, installationID,
				templates.RenderApplyBlockedByPriorEnvCheckError(priorEnv, "resolve the PR head commit"))
		}
		return false, fmt.Errorf("prior environment gate for %s: PR %s#%d head commit is empty", priorEnv, repo, pr)
	}

	check, err := h.waitForLocalPriorEnvCheck(ctx, repo, pr, headSHA, database, dbType, environment, priorEnv)
	if err != nil {
		h.logger.Error("failed to look up prior environment check",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", database, "database_type", dbType,
			"environment", environment, "prior_environment", priorEnv,
			"error", err)
		if !suppressRetryComments {
			h.postComment(repo, pr, installationID,
				templates.RenderApplyBlockedByPriorEnvCheckError(priorEnv, "read SchemaBot storage"))
		}
		return false, fmt.Errorf("prior environment gate read stored check for %s: %w", priorEnv, err)
	}

	if check == nil {
		h.logger.Warn("prior environment check is missing, blocking apply",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", database, "database_type", dbType,
			"environment", environment, "prior_environment", priorEnv,
			"attempts", h.priorEnvCheckMaxAttemptCount())
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByMissingPriorEnvCheck(priorEnv))
		return true, nil
	}

	switch {
	case check.Conclusion == checkConclusionSuccess && storedPriorEnvCheckIsForHead(check, headSHA):
		h.logger.Debug("prior environment check passed on the PR head commit, allowing apply",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", database, "database_type", dbType,
			"environment", environment, "prior_environment", priorEnv,
			"check_id", check.ID, "check_status", check.Status, "check_conclusion", check.Conclusion)
		return false, nil
	case check.Status == checkStatusInProgress:
		h.logger.Warn("prior environment check is still in progress after retries, blocking apply",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", database, "database_type", dbType,
			"environment", environment, "prior_environment", priorEnv,
			"check_id", check.ID, "check_head_sha", check.HeadSHA,
			"check_status", check.Status, "check_conclusion", check.Conclusion,
			"attempts", h.priorEnvCheckMaxAttemptCount())
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnvInProgress(database, environment, priorEnv))
		return true, nil
	case !storedPriorEnvCheckIsForHead(check, headSHA):
		h.logger.Warn("prior environment check was recorded on a commit other than the PR head, blocking apply",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", database, "database_type", dbType,
			"environment", environment, "prior_environment", priorEnv,
			"check_id", check.ID, "check_head_sha", check.HeadSHA,
			"check_status", check.Status, "check_conclusion", check.Conclusion,
			"attempts", h.priorEnvCheckMaxAttemptCount())
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByStalePriorEnvCheck(priorEnv, check.HeadSHA, headSHA))
		return true, nil
	default:
		status := "has pending changes"
		action := fmt.Sprintf("Apply %s first", priorEnv)
		if check.Conclusion == checkConclusionFailure {
			status = "failed"
			action = fmt.Sprintf("Fix the issue and re-apply %s", priorEnv)
		}
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnv(database, environment, priorEnv, status, action))
		return true, nil
	}
}

func (h *Handler) waitForLocalPriorEnvCheck(
	ctx context.Context, repo string, pr int, headSHA string,
	database, dbType, environment, priorEnv string,
) (*storage.Check, error) {
	// A later-environment apply can race a prior-environment plan/apply webhook
	// that has not persisted its check state yet — including the plan a push
	// triggers for the new head, which runs asynchronously and leaves the row
	// naming the previous commit until it lands. Retry briefly, then preserve
	// the fail-closed behavior if the prior environment still cannot be proven
	// safe.
	attempts := h.priorEnvCheckMaxAttemptCount()
	for attempt := 1; attempt <= attempts; attempt++ {
		check, err := h.service.Storage().Checks().Get(ctx, repo, pr, priorEnv, dbType, database)
		if err != nil {
			return nil, err
		}
		if !shouldRetryStoredPriorEnvCheck(check, headSHA) || attempt == attempts {
			return check, nil
		}

		h.logger.Debug("prior environment check state not ready, retrying",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", database, "database_type", dbType,
			"environment", environment, "prior_environment", priorEnv,
			"check_status", storedPriorEnvCheckStatus(check),
			"check_head_sha", storedPriorEnvCheckHeadSHA(check),
			"attempt", attempt, "max_attempts", attempts)
		if err := h.waitBeforePriorEnvCheckRetry(ctx); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// shouldRetryStoredPriorEnvCheck reports whether the stored check state may
// still be about to change into an answer for headSHA: it is missing, still
// running, or names a commit other than the PR head.
func shouldRetryStoredPriorEnvCheck(check *storage.Check, headSHA string) bool {
	if check == nil {
		return true
	}
	if check.Status == checkStatusInProgress {
		return true
	}
	return !storedPriorEnvCheckIsForHead(check, headSHA)
}

// storedPriorEnvCheckIsForHead reports whether the stored check state was
// recorded on headSHA. A result for any other commit says nothing about what
// the PR would apply now.
func storedPriorEnvCheckIsForHead(check *storage.Check, headSHA string) bool {
	return headSHA != "" && check.HeadSHA == headSHA
}

func storedPriorEnvCheckStatus(check *storage.Check) string {
	if check == nil {
		return "missing"
	}
	return check.Status
}

func storedPriorEnvCheckHeadSHA(check *storage.Check) string {
	if check == nil {
		return ""
	}
	return check.HeadSHA
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
//
// Only Check Runs created by a trusted SchemaBot GitHub App are accepted:
// this deployment's own App or a configured trusted sibling deployment App
// (github.trusted-check-app-slugs). A same-named check run from any other app
// (e.g. a GitHub Actions job) cannot satisfy the gate, and when ownership
// cannot be verified the gate blocks the apply.
//
// A GitHub read failure (client creation, PR fetch, or the Check Run query)
// stops the command (fail closed) and is returned as an error rather than a
// block.
func (h *Handler) checkPriorEnvViaGitHub(
	ctx context.Context, repo string, pr int,
	database, environment, priorEnv string,
	installationID int64,
	suppressRetryComments bool,
) (blocked bool, err error) {
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for prior env check, stopping apply",
			"prior_env", priorEnv, "error", err)
		if !suppressRetryComments {
			h.postComment(repo, pr, installationID,
				templates.RenderApplyBlockedByPriorEnvCheckError(priorEnv, "create GitHub client"))
		}
		return false, fmt.Errorf("prior environment gate create GitHub client for %s: %w", priorEnv, err)
	}

	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to fetch PR for prior env check, stopping apply",
			"prior_env", priorEnv, "error", err)
		if !suppressRetryComments {
			h.postComment(repo, pr, installationID,
				templates.RenderApplyBlockedByPriorEnvCheckError(priorEnv, "fetch PR details"))
		}
		return false, fmt.Errorf("prior environment gate fetch PR %s#%d for %s: %w", repo, pr, priorEnv, err)
	}

	checkName := aggregateCheckNameForEnv(h.promotionCheckNameForRepo(repo), priorEnv)
	checkResult, untrustedApps, err := h.waitForGitHubPriorEnvCheck(ctx, client, repo, pr, database, environment, priorEnv, prInfo.HeadSHA, checkName)
	if err != nil {
		h.logger.Error("failed to query GitHub check for prior environment, stopping apply",
			"repo", repo, "pr", pr,
			"database", database,
			"environment", environment, "prior_environment", priorEnv,
			"head_sha", prInfo.HeadSHA,
			"check_name", checkName, "error", err)
		if !suppressRetryComments {
			h.postComment(repo, pr, installationID,
				templates.RenderApplyBlockedByPriorEnvCheckError(priorEnv, "query check runs"))
		}
		return false, fmt.Errorf("prior environment gate query check run %q for %s: %w", checkName, priorEnv, err)
	}

	if checkResult == nil && len(untrustedApps) > 0 {
		// The prior environment's check exists on this commit but none of the
		// creating apps are trusted — re-running plan/apply on the prior
		// environment cannot fix this; only trusting the owning deployment's
		// App (or removing a spoofed check) can.
		h.logger.Warn("prior environment check exists only from untrusted GitHub Apps, blocking apply",
			"repo", repo, "pr", pr,
			"database", database,
			"environment", environment, "prior_environment", priorEnv,
			"head_sha", prInfo.HeadSHA,
			"check_name", checkName,
			"untrusted_app_slugs", untrustedApps)
		for _, appSlug := range untrustedApps {
			metrics.RecordUntrustedAggregateNamedCheck(ctx, repo, environment, appSlug, metrics.CheckTrustGatePromotion)
		}
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByUntrustedPriorEnvCheck(priorEnv, checkName, untrustedApps))
		return true, nil
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
		return true, nil
	}

	switch {
	case checkResult.Status == checkStatusCompleted && checkResult.Conclusion == checkConclusionSuccess:
		h.logger.Debug("prior environment verified via GitHub check",
			"repo", repo, "pr", pr,
			"database", database,
			"environment", environment, "prior_environment", priorEnv,
			"check_name", checkName, "conclusion", checkResult.Conclusion)
		return false, nil
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
		return true, nil
	default:
		status := "has pending changes"
		action := fmt.Sprintf("Apply %s first", priorEnv)
		if checkResult.Conclusion == checkConclusionFailure {
			status = "failed"
			action = fmt.Sprintf("Fix the issue and re-apply %s", priorEnv)
		}
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnv(database, environment, priorEnv, status, action))
		return true, nil
	}
}

func (h *Handler) waitForGitHubPriorEnvCheck(
	ctx context.Context, client *ghclient.InstallationClient,
	repo string, pr int,
	database, environment, priorEnv, headSHA, checkName string,
) (*ghclient.CheckRunResult, []string, error) {
	// Cross-instance prior environment checks depend on GitHub Check Run
	// visibility. Retry briefly so ordering jitter does not cause an avoidable
	// block, then fail closed if the required check is still missing or running.
	attempts := h.priorEnvCheckMaxAttemptCount()
	var untrustedApps []string
	for attempt := 1; attempt <= attempts; attempt++ {
		checkResult, untrusted, err := client.FindCheckRunByName(ctx, repo, headSHA, checkName)
		if err != nil {
			return nil, nil, err
		}
		untrustedApps = untrusted
		if !shouldRetryGitHubPriorEnvCheck(checkResult) || attempt == attempts {
			return checkResult, untrustedApps, nil
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
			return nil, nil, err
		}
	}

	return nil, untrustedApps, nil
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
