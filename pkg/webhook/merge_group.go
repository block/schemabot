package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
)

// mergeGroupPayload is the subset of the GitHub merge_group webhook payload
// SchemaBot needs. GitHub fires this event when a pull request enters or leaves
// a repository's merge queue.
type mergeGroupPayload struct {
	Action     string `json:"action"`
	MergeGroup struct {
		HeadSHA string `json:"head_sha"`
	} `json:"merge_group"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
}

// handleMergeGroup responds to merge_group webhook events so SchemaBot's
// required Check Runs do not wedge a repository's merge queue.
//
// A merge queue tests queued pull requests combined, on a synthetic head
// commit, before they land on the base branch. Branch protection re-evaluates
// the same required checks against that merge-group head SHA — but SchemaBot
// only ever publishes its checks on PR head SHAs, so without this handler the
// required SchemaBot check would never appear on the merge-group commit and the
// queue entry would block indefinitely.
//
// Posting a passing check is correct: SchemaBot applies schema changes before a
// PR merges, and branch protection already required the PR-head check to pass
// before the PR could enter the queue. The merge group sits strictly downstream
// of an already-completed, already-gated apply, so there is nothing left to
// verify on the combined commit.
func (h *Handler) handleMergeGroup(ctx context.Context, metricApp string, w http.ResponseWriter, body []byte) {
	var payload mergeGroupPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid merge_group payload")
		return
	}

	// GitHub sends "checks_requested" when a PR joins the queue and "destroyed"
	// when it leaves. Only checks_requested needs a check run on the new SHA.
	if payload.Action != "checks_requested" {
		h.logger.Debug("merge_group action ignored",
			"action", payload.Action, "repo", payload.Repository.FullName)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "merge_group action ignored"})
		return
	}

	repo := payload.Repository.FullName
	headSHA := payload.MergeGroup.HeadSHA
	installationID := h.effectiveInstallationID(ctx, payload.Installation.ID)
	if installationID == 0 {
		h.writeError(w, http.StatusBadRequest, "missing installation ID in merge_group payload")
		return
	}
	if headSHA == "" {
		h.writeError(w, http.StatusBadRequest, "missing merge_group head_sha in merge_group payload")
		return
	}

	// A repo SchemaBot does not manage gets no check — its check is not required
	// on that repo, so there is nothing to unblock.
	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("merge_group webhook from unregistered repository",
			"repo", repo, "head_sha", headSHA, "installation_id", installationID)
		metrics.RecordUnregisteredRepositoryWebhook(ctx, metricApp, "merge_group", payload.Action, repo)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "repository not registered"})
		return
	}

	// An aggregate participant's checks are never required — the leader owns
	// the required aggregate and posts it on the merge-group commit — so a
	// participant posting here would only re-add the per-tenant rows the
	// aggregate removes. A silent participant cannot wedge the queue: only
	// required checks gate a merge-group entry.
	if h.isAggregateParticipant(repo) {
		h.logger.Info("aggregate participant staying silent on merge_group; the leader posts the required checks",
			"repo", repo, "head_sha", headSHA, "installation_id", installationID)
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "merge_group_check",
			Repository: repo,
			Status:     "skipped",
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "merge_group ignored (aggregate participant, staying silent)"})
		return
	}

	// When check publishing is disabled for this repo, SchemaBot's check is not
	// required either, so skipping the merge-group check is correct.
	if !h.shouldPublishChecks(ctx, repo, "merge_group_check") {
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check publishing disabled"})
		return
	}

	postCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for merge_group check",
			"repo", repo, "head_sha", headSHA, "installation_id", installationID, "error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to initialize GitHub client")
		return
	}

	if err := h.postPassingAggregateChecks(postCtx, client, repo, headSHA, passingAggregateCheckContent{
		operation: "merge_group_check",
		title:     "Schema changes verified before merge queue",
		summary: "Schema changes in queued pull requests are applied and verified by SchemaBot before " +
			"they enter the merge queue, so no additional verification is required for this merge group.",
	}); err != nil {
		// Return 500 so the delivery is recorded as failed and shows up in the
		// App's delivery log for redelivery. The merge queue blocks until the
		// check is posted, so the failure must be visible for retry, not
		// silently dropped.
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "merge_group_check",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to post merge_group checks",
			"repo", repo, "head_sha", headSHA, "error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to post merge_group checks")
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]string{"message": "merge_group checks posted"})
}

// passingAggregateCheckContent carries the operation name and Check Run output
// for a passing aggregate published outside the PR-head path (merge-queue
// heads, default-branch pushes).
type passingAggregateCheckContent struct {
	operation string
	title     string
	summary   string
}

// postPassingAggregateChecks publishes a passing aggregate Check Run on
// headSHA for each environment this instance gates, reusing the same check
// names as the PR-head aggregates so branch protection's required checks
// always match.
func (h *Handler) postPassingAggregateChecks(ctx context.Context, client *ghclient.InstallationClient, repo, headSHA string, content passingAggregateCheckContent) error {
	for _, target := range h.aggregateCheckTargetsForRepo(repo) {
		opts := ghclient.CheckRunOptions{
			Name:       target.name,
			Status:     checkStatusCompleted,
			Conclusion: checkConclusionSuccess,
			Output: &ghclient.CheckRunOutput{
				Title:   content.title,
				Summary: content.summary,
			},
		}
		// Reuse an existing run for this name on the SHA so a webhook
		// redelivery updates it rather than creating a duplicate Check Run.
		// The lookup errors when the App slug is unknown; on any lookup error
		// fall back to creating a new run — a duplicate Check Run is the safe
		// outcome, a missing one is not.
		existing, _, findErr := client.FindCheckRunByName(ctx, repo, headSHA, target.name)
		if findErr != nil {
			h.logger.Warn("could not look up existing check; creating a new one",
				"repo", repo, "head_sha", headSHA, "check_name", target.name,
				"operation", content.operation, "error", findErr)
		}
		switch {
		case findErr == nil && existing != nil:
			if err := client.UpdateCheckRun(ctx, repo, existing.ID, opts); err != nil {
				return fmt.Errorf("update %s check %q on %s@%s: %w", content.operation, target.name, repo, headSHA, err)
			}
		default:
			if _, err := client.CreateCheckRun(ctx, repo, headSHA, opts); err != nil {
				return fmt.Errorf("create %s check %q on %s@%s: %w", content.operation, target.name, repo, headSHA, err)
			}
		}
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   content.operation,
			Repository:  repo,
			Environment: target.environment,
			Status:      "success",
		})
		h.logger.Info("passing aggregate check posted",
			"repo", repo, "head_sha", headSHA, "check_name", target.name,
			"environment", target.environment, "operation", content.operation)
	}
	return nil
}
