package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// pullRequestPayload represents the relevant fields from a GitHub pull_request webhook.
type pullRequestPayload struct {
	Action      string `json:"action"`
	PullRequest struct {
		Number int `json:"number"`
		Head   struct {
			SHA string `json:"sha"`
			Ref string `json:"ref"`
		} `json:"head"`
		User struct {
			Login string `json:"login"`
		} `json:"user"`
	} `json:"pull_request"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
}

// handlePullRequest processes GitHub pull_request webhook events.
// On PR open/synchronize/reopen, it auto-plans all databases with schema changes.
func (h *Handler) handlePullRequest(w http.ResponseWriter, body []byte) {
	var payload pullRequestPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid pull_request payload")
		return
	}

	// Route PR actions
	switch payload.Action {
	case "opened", "synchronize", "reopened":
		// proceed to auto-plan below
	case "closed":
		h.goSafe(payload.Repository.FullName, payload.PullRequest.Number, payload.Installation.ID, func() {
			h.handlePRClosed(payload.Repository.FullName, payload.PullRequest.Number, payload.Installation.ID)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "PR close cleanup started"})
		return
	default:
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "pull_request action ignored",
		})
		return
	}

	installationID := payload.Installation.ID
	if installationID == 0 {
		h.writeError(w, http.StatusBadRequest, "missing installation ID in webhook payload")
		return
	}

	repo := payload.Repository.FullName
	pr := payload.PullRequest.Number
	headSHA := payload.PullRequest.Head.SHA

	// Reject webhooks from repositories not in the configured allowlist
	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("webhook from unregistered repository", "repo", repo)
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "repository not registered",
		})
		return
	}

	h.logger.Info("auto-plan triggered",
		"action", payload.Action,
		"repo", repo,
		"pr", pr,
		"head_sha", headSHA,
	)

	// Discover all configs matching changed schema files in this PR
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := h.ghClient.ForInstallation(installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client", "error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to initialize GitHub client")
		return
	}

	configs, err := client.FindAllConfigsForPR(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to discover configs for PR", "repo", repo, "pr", pr, "error", err)
		h.postConfigDiscoveryFailure(ctx, client, repo, pr, headSHA, err)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "config discovery failed"})
		return
	}

	// Collect database names from discovered configs
	affectedDatabases := make(map[string]bool)
	for _, cfg := range configs {
		affectedDatabases[cfg.Config.Database] = true
	}

	// Clean up stale checks from databases no longer in the PR.
	// Pass the new HEAD SHA so cleanup can create new check runs on the correct commit.
	h.goSafe(repo, pr, installationID, func() {
		h.cleanupStaleChecks(repo, pr, headSHA, installationID, affectedDatabases)
	})

	if len(configs) == 0 {
		h.logger.Info("no schema files in PR, skipping auto-plan", "repo", repo, "pr", pr)
		// Post passing aggregates on the current HEAD SHA so branch protection
		// isn't blocked on PRs that don't touch schema files. Always post —
		// on synchronize events the HEAD SHA changes, so the aggregate must be
		// recreated on the new commit. If stale per-database check records exist,
		// cleanupStaleChecks (above) also updates the aggregate — both converge
		// to the same result (passing aggregate on new SHA) so the overlap is safe.
		h.goSafe(repo, pr, installationID, func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			c, err := h.ghClient.ForInstallation(installationID)
			if err != nil {
				h.logger.Error("failed to create GitHub client for passing aggregate", "error", err)
				return
			}
			h.postPassingAggregates(ctx, c, repo, pr, headSHA,
				"No managed schema changes",
				"This PR does not contain schema changes managed by SchemaBot.")
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "no schema files in PR"})
		return
	}

	// Launch auto-plan for each discovered config
	for _, cfg := range configs {
		database := cfg.Config.Database
		h.goSafe(repo, pr, installationID, func() {
			h.handleMultiEnvPlan(repo, pr, database, installationID, "", true)
		})
	}

	h.writeJSON(w, http.StatusOK, map[string]string{"message": "auto-plan started"})
}

func (h *Handler) postConfigDiscoveryFailure(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, discoveryErr error) {
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:  "schema_config_discovery",
		Repository: repo,
		Status:     "error",
	})

	block := configDiscoveryFailedBlock
	if ghclient.IsUnavailableError(discoveryErr) {
		block = githubConfigDiscoveryUnavailableBlock
	}
	h.logger.Info("posting failing aggregate for config discovery failure",
		"repo", repo, "pr", pr, "head_sha", headSHA,
		"blocking_reason", block.blockingReason)
	h.postFailingAggregatesWithBlock(ctx, client, repo, pr, headSHA,
		h.aggregateMessagesForAllEnvironments(block.message), block)
}

func (h *Handler) aggregateMessagesForAllEnvironments(message string) map[string]string {
	allowed := h.service.Config().AllowedEnvironments
	if len(allowed) == 0 {
		return map[string]string{aggregateSentinel: message}
	}

	messages := make(map[string]string, len(allowed))
	for _, env := range allowed {
		messages[env] = message
	}
	return messages
}

// handlePRClosed cleans up resources when a PR is closed (merged or unmerged).
// Releases any locks held by this PR and deletes stored check records.
func (h *Handler) handlePRClosed(repo string, pr int, _ int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Release all locks held by this PR
	locks, err := h.service.Storage().Locks().GetByPR(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to look up locks for closed PR", "repo", repo, "pr", pr, "error", err)
	} else {
		for _, lock := range locks {
			if err := h.service.Storage().Locks().Release(ctx, lock.DatabaseName, lock.DatabaseType, lock.Owner); err != nil {
				h.logger.Error("failed to release lock on PR close",
					"database", lock.DatabaseName, "error", err)
			} else {
				h.logger.Info("released lock on PR close",
					"repo", repo, "pr", pr, "database", lock.DatabaseName)
			}
		}
	}

	// Delete all check records for this PR
	if err := h.service.Storage().Checks().DeleteByPR(ctx, repo, pr); err != nil {
		h.logger.Error("failed to delete checks for closed PR", "repo", repo, "pr", pr, "error", err)
	} else {
		h.logger.Info("deleted checks for closed PR", "repo", repo, "pr", pr)
	}
}

// cleanupStaleChecks updates checks for databases no longer in the PR.
// Plan-only checks can be marked "success" because the current PR no longer asks
// SchemaBot to apply anything. Checks that represent a started apply remain
// blocking because the live database may already have changed or may still change.
//
// On synchronize events, headSHA is the new commit SHA. Stale checks must be created
// as new check runs on this SHA (not updated on the old SHA) so GitHub shows them
// on the current commit.
func (h *Handler) cleanupStaleChecks(repo string, pr int, headSHA string, installationID int64, affectedDatabases map[string]bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if h.service == nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("cannot clean up stale checks without service", "repo", repo, "pr", pr, "head_sha", headSHA)
		return
	}
	if h.service.Storage() == nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("cannot clean up stale checks without storage", "repo", repo, "pr", pr, "head_sha", headSHA)
		return
	}

	client, clientErr := h.ghClient.ForInstallation(installationID)
	if clientErr != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to create GitHub client for stale cleanup", "repo", repo, "pr", pr, "head_sha", headSHA, "error", clientErr)
		return
	}
	if !h.verifyHeadSHAStillCurrentForPR(ctx, client, repo, pr, headSHA, "stale_check_cleanup") {
		return
	}

	checks, err := h.service.Storage().Checks().GetByPR(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to get checks for stale cleanup", "repo", repo, "pr", pr, "error", err)
		return
	}

	cleaned := false

	for _, check := range checks {
		if isAggregateCheck(check) {
			h.logger.Debug("skipping aggregate check during stale cleanup",
				"repo", repo, "pr", pr, "head_sha", headSHA,
				"environment", check.Environment, "check_id", check.ID)
			continue
		}

		if affectedDatabases[check.DatabaseName] {
			h.logger.Debug("skipping check during stale cleanup because database is still affected",
				"repo", repo, "pr", pr, "head_sha", headSHA,
				"database", check.DatabaseName, "database_type", check.DatabaseType,
				"environment", check.Environment, "check_id", check.ID)
			continue
		}

		// This check's database is no longer in the PR.
		h.logger.Info("cleaning up stale check",
			"repo", repo, "pr", pr,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "head_sha", headSHA,
			"previous_status", check.Status, "previous_conclusion", check.Conclusion,
			"previous_blocking_reason", check.BlockingReason, "apply_id", check.ApplyID)

		if checkHasStartedApply(check) {
			if h.blockStaleStartedApplyCheckState(ctx, repo, pr, headSHA, check) {
				cleaned = true
			}
			continue
		}

		if h.markStalePlanOnlyCheckStateSuccessful(ctx, repo, pr, headSHA, check) {
			cleaned = true
		}
	}

	// Recompute aggregate on the new HEAD SHA after cleaning up stale checks
	if cleaned {
		h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
	} else {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "noop",
		})
	}
}

func (h *Handler) blockStaleStartedApplyCheckState(ctx context.Context, repo string, pr int, headSHA string, check *storage.Check) bool {
	check.HeadSHA = headSHA
	check.HasChanges = true
	check.BlockingReason = schemaRemovedAfterApplyBlock.blockingReason
	check.ErrorMessage = schemaRemovedAfterApplyBlock.message
	if check.Status == checkStatusInProgress {
		check.Conclusion = ""
	} else {
		check.Status = checkStatusCompleted
		check.Conclusion = checkConclusionActionRequired
	}
	if err := h.service.Storage().Checks().Upsert(ctx, check); err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "stale_check_cleanup",
			Repository:   repo,
			Database:     check.DatabaseName,
			DatabaseType: check.DatabaseType,
			Environment:  check.Environment,
			Status:       "error",
		})
		h.logger.Error("failed to block stale check with started apply",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "check_id", check.ID,
			"apply_id", check.ApplyID, "error", err)
		return false
	}
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:    "stale_check_cleanup",
		Repository:   repo,
		Database:     check.DatabaseName,
		DatabaseType: check.DatabaseType,
		Environment:  check.Environment,
		Status:       "blocked",
	})
	return true
}

func (h *Handler) markStalePlanOnlyCheckStateSuccessful(ctx context.Context, repo string, pr int, headSHA string, check *storage.Check) bool {
	check.HeadSHA = headSHA
	check.Conclusion = checkConclusionSuccess
	check.HasChanges = false
	check.Status = checkStatusCompleted
	check.BlockingReason = ""
	check.ErrorMessage = ""
	if err := h.service.Storage().Checks().Upsert(ctx, check); err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "stale_check_cleanup",
			Repository:   repo,
			Database:     check.DatabaseName,
			DatabaseType: check.DatabaseType,
			Environment:  check.Environment,
			Status:       "error",
		})
		h.logger.Error("failed to mark stale plan check successful",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "check_id", check.ID,
			"error", err)
		return false
	}
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:    "stale_check_cleanup",
		Repository:   repo,
		Database:     check.DatabaseName,
		DatabaseType: check.DatabaseType,
		Environment:  check.Environment,
		Status:       "success",
	})
	return true
}
