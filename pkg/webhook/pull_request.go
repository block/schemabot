package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
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

	h.logger.Info("auto-plan triggered",
		"action", payload.Action,
		"repo", repo,
		"pr", pr,
		"head_sha", payload.PullRequest.Head.SHA,
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
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "config discovery failed"})
		return
	}

	// Collect database names from discovered configs
	affectedDatabases := make(map[string]bool)
	for _, cfg := range configs {
		affectedDatabases[cfg.Config.Database] = true
	}

	// Clean up stale checks from databases no longer in the PR
	h.goSafe(repo, pr, installationID, func() {
		h.cleanupStaleChecks(repo, pr, installationID, affectedDatabases)
	})

	if len(configs) == 0 {
		h.logger.Debug("no schema files in PR, skipping auto-plan", "repo", repo, "pr", pr)
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

// handlePRClosed cleans up resources when a PR is closed (merged or unmerged).
// Releases any locks held by this PR and deletes stored check records.
func (h *Handler) handlePRClosed(repo string, pr int, _ int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if h.service == nil {
		return
	}

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

// cleanupStaleChecks marks checks for databases no longer in the PR as "success".
// This handles the case where a user removes a schema change from the PR — the old
// check would otherwise stay at "action_required" and block merge.
func (h *Handler) cleanupStaleChecks(repo string, pr int, installationID int64, affectedDatabases map[string]bool) {
	if h.service == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	checks, err := h.service.Storage().Checks().GetByPR(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to get checks for stale cleanup", "repo", repo, "pr", pr, "error", err)
		return
	}

	for _, check := range checks {
		if affectedDatabases[check.DatabaseName] {
			continue // still affected, not stale
		}

		// This check's database is no longer in the PR — mark as success
		h.logger.Info("cleaning up stale check",
			"repo", repo, "pr", pr,
			"database", check.DatabaseName, "environment", check.Environment,
			"previous_conclusion", check.Conclusion)

		if check.CheckRunID > 0 {
			client, err := h.ghClient.ForInstallation(installationID)
			if err != nil {
				h.logger.Error("failed to create GitHub client for stale check cleanup", "error", err)
				continue
			}

			checkName := checkRunName(check.Environment, check.DatabaseType, check.DatabaseName)
			opts := ghclient.CheckRunOptions{
				Name:       checkName,
				Status:     "completed",
				Conclusion: "success",
				Output: &ghclient.CheckRunOutput{
					Title:   "No schema changes",
					Summary: "Schema files for this database were removed from the PR.",
				},
			}
			if err := client.UpdateCheckRun(ctx, repo, check.CheckRunID, opts); err != nil {
				h.logger.Error("failed to update stale check run", "checkRunID", check.CheckRunID, "error", err)
			}
		}

		// Update stored check record to reflect success
		check.Conclusion = "success"
		check.HasChanges = false
		check.Status = "completed"
		if err := h.service.Storage().Checks().Upsert(ctx, check); err != nil {
			h.logger.Error("failed to update stale check record", "checkID", check.ID, "error", err)
		}
	}
}
