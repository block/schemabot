package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// pushPayload is the subset of the GitHub push webhook payload SchemaBot
// needs. GitHub fires this event for every branch or tag push on repositories
// the App is installed on.
type pushPayload struct {
	Ref        string `json:"ref"`
	After      string `json:"after"`
	Deleted    bool   `json:"deleted"`
	Repository struct {
		FullName      string `json:"full_name"`
		DefaultBranch string `json:"default_branch"`
	} `json:"repository"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
}

// handlePush responds to push webhook events on the default branch by
// publishing SchemaBot's passing aggregate Check Run on the pushed commit.
//
// Every other SchemaBot check runs against a PR head or a merge-queue head, so
// its check suites never record the default branch as their head branch. Branch
// rulesets index required-check sources by exactly that field: an App that has
// never completed a check suite on the target branch cannot be selected as a
// check source, which forces repository admins into unpinned "any source"
// requirements that a same-named status from any writer can satisfy. Publishing
// the aggregate on default-branch pushes keeps the App selectable as a pinned
// check source.
//
// Posting a passing check is correct for the same reason as the merge-queue
// check: SchemaBot gates schema changes on pull requests before they reach the
// default branch, so a commit already on the default branch has nothing left to
// verify. Rulesets evaluate required checks on PR and merge-queue commits, never
// on commits already landed, so this check can never satisfy a merge gate.
func (h *Handler) handlePush(ctx context.Context, metricApp string, w http.ResponseWriter, body []byte, deliveryID string) {
	var payload pushPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid push payload")
		return
	}

	repo := payload.Repository.FullName
	headSHA := payload.After

	// Only default-branch pushes need the check; pushes to feature branches,
	// merge-queue branches, and tags are covered by the PR and merge-queue
	// check paths.
	if payload.Repository.DefaultBranch == "" || payload.Ref != "refs/heads/"+payload.Repository.DefaultBranch {
		h.logger.Debug("push ignored: not the default branch",
			"repo", repo, "ref", payload.Ref, "default_branch", payload.Repository.DefaultBranch)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "push ignored: not the default branch"})
		return
	}

	// A branch deletion has no commit to publish a check on.
	if payload.Deleted || headSHA == "" || strings.Trim(headSHA, "0") == "" {
		h.logger.Debug("push ignored: branch deletion",
			"repo", repo, "ref", payload.Ref)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "push ignored: branch deletion"})
		return
	}

	installationID := h.effectiveInstallationID(ctx, payload.Installation.ID)
	if installationID == 0 {
		h.writeError(w, http.StatusBadRequest, "missing installation ID in push payload")
		return
	}

	// A repo SchemaBot does not manage gets no check — its check is not
	// required on that repo, so there is no check source to keep selectable.
	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Debug("push webhook from unregistered repository",
			"repo", repo, "head_sha", headSHA, "installation_id", installationID)
		metrics.RecordUnregisteredRepositoryWebhook(ctx, metricApp, "push", "", repo)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "repository not registered"})
		return
	}

	// An aggregate participant's checks are never required — the leader owns
	// the required aggregate, so only the leader needs to stay selectable as a
	// ruleset check source. A participant seeding its informational check on
	// every landed commit would only add noise.
	if h.isAggregateParticipant(repo) {
		h.logger.Info("aggregate participant staying silent on default-branch push; the leader maintains the check source",
			"repo", repo, "head_sha", headSHA, "installation_id", installationID)
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "default_branch_check",
			Repository: repo,
			Status:     "skipped",
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "push ignored (aggregate participant, staying silent)"})
		return
	}

	// When check publishing is disabled for this repo, SchemaBot's check is not
	// required either, so there is no check source to maintain.
	if !h.shouldPublishChecks(ctx, repo, "default_branch_check") {
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check publishing disabled"})
		return
	}

	if h.durableWebhookDispatch {
		// Enqueue and ACK fast; a leased driver posts the check with retries so
		// a process restart mid-post cannot drop the delivery. Enqueue failure
		// is a deliberate 500 with no in-process fallback — it fails loudly (a
		// red delivery in GitHub's webhook UI) so an operator can Redeliver.
		inserted, err := h.enqueueDurablePush(ctx, payload, body, deliveryID, installationID)
		if err != nil {
			h.logger.Error("failed to enqueue durable default-branch check",
				"repo", repo, "head_sha", headSHA, "installation_id", installationID,
				"delivery_id", deliveryID, "error", err)
			metrics.RecordWebhookEvent(ctx, metricApp, "push", "", repo, "durable_enqueue_failed")
			h.writeError(w, http.StatusInternalServerError, "failed to enqueue webhook delivery")
			return
		}
		if !inserted {
			h.logger.Info("durable push delivery already queued",
				"repo", repo, "head_sha", headSHA, "delivery_id", deliveryID)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "default-branch check already queued"})
			return
		}
		h.logger.Info("durable default-branch check queued",
			"repo", repo, "head_sha", headSHA, "delivery_id", deliveryID)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "default-branch check queued"})
		return
	}

	postCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for default-branch check",
			"repo", repo, "head_sha", headSHA, "installation_id", installationID, "error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to initialize GitHub client")
		return
	}

	if err := h.postPassingAggregateChecks(postCtx, client, repo, headSHA, defaultBranchCheckContent()); err != nil {
		// Return 500 so the delivery is recorded as failed and shows up in the
		// App's delivery log for redelivery. A missed post only ages the
		// ruleset source index — the next default-branch push refreshes it —
		// and reposting is idempotent.
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "default_branch_check",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to post default-branch checks",
			"repo", repo, "head_sha", headSHA, "error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to post default-branch checks")
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]string{"message": "default-branch checks posted"})
}

// defaultBranchCheckContent is the passing Check Run content published on a
// default-branch commit. Both the request path and the durable driver use it so
// a redelivery updates the same check with identical output.
func defaultBranchCheckContent() passingAggregateCheckContent {
	return passingAggregateCheckContent{
		operation: "default_branch_check",
		title:     "Schema changes are verified before merge",
		summary: "SchemaBot gates schema changes on pull requests before they reach the default branch, " +
			"so commits on the default branch require no additional verification. " +
			"This check also keeps SchemaBot selectable as a required-check source in branch rulesets.",
	}
}

// enqueueDurablePush persists a default-branch push delivery in the inbox. The
// stored TenantID is the resolved installation ID so the driver, which runs
// outside any HTTP request, does not have to re-resolve a repo-level install.
func (h *Handler) enqueueDurablePush(ctx context.Context, payload pushPayload, body []byte, deliveryID string, installationID int64) (bool, error) {
	return h.enqueueDurableWebhookEvent(ctx, &storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: deliveryID,
		Event:      "push",
		Repository: payload.Repository.FullName,
		HeadSHA:    payload.After,
		TenantID:   strconv.FormatInt(installationID, 10),
		Payload:    body,
	})
}

// processDurablePush posts the passing default-branch check for a claimed push
// delivery. It re-validates every enqueue-time guard fail-closed — config can
// change between enqueue and drive, and rows can arrive via replay — so a
// delivery the current config would ignore (not the default branch, a deletion,
// an unregistered repo, a participant, or publishing disabled) completes as a
// no-op rather than posting. GitHub client and post failures are retryable;
// posting is idempotent (the check is looked up by name and updated in place),
// so a retry after a partial post reconciles rather than duplicates.
func (h *Handler) processDurablePush(ctx context.Context, event *storage.WebhookEvent) (retry bool, err error) {
	var payload pushPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return false, fmt.Errorf("decode durable push delivery %s: %w", event.DeliveryID, err)
	}

	repo := payload.Repository.FullName
	headSHA := payload.After

	if payload.Repository.DefaultBranch == "" || payload.Ref != "refs/heads/"+payload.Repository.DefaultBranch {
		h.logger.Info("durable push delivery ignored because it is not the default branch",
			"delivery_id", event.DeliveryID, "repo", repo, "ref", payload.Ref,
			"default_branch", payload.Repository.DefaultBranch)
		return false, nil
	}

	if payload.Deleted || headSHA == "" || strings.Trim(headSHA, "0") == "" {
		h.logger.Info("durable push delivery ignored because it is a branch deletion",
			"delivery_id", event.DeliveryID, "repo", repo, "ref", payload.Ref)
		return false, nil
	}

	installationID := h.durableInstallationID(ctx, event, payload.Installation.ID)
	if installationID == 0 {
		return false, fmt.Errorf("durable push delivery %s missing installation ID", event.DeliveryID)
	}

	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("durable push delivery from unregistered repository",
			"delivery_id", event.DeliveryID, "repo", repo, "head_sha", headSHA, "installation_id", installationID)
		metrics.RecordUnregisteredRepositoryWebhook(ctx, h.metricAppForRepo(repo), "push", "", repo)
		return false, nil
	}

	if h.isAggregateParticipant(repo) {
		h.logger.Info("durable push delivery skipped: aggregate participant stays silent; the leader maintains the check source",
			"delivery_id", event.DeliveryID, "repo", repo, "head_sha", headSHA, "installation_id", installationID)
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "default_branch_check",
			Repository: repo,
			Status:     "skipped",
		})
		return false, nil
	}

	if !h.shouldPublishChecks(ctx, repo, "default_branch_check") {
		// shouldPublishChecks logs and records the skip.
		return false, nil
	}

	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		return true, fmt.Errorf("create GitHub client for durable push %s@%s: %w", repo, headSHA, err)
	}

	if err := h.postPassingAggregateChecks(ctx, client, repo, headSHA, defaultBranchCheckContent()); err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "default_branch_check",
			Repository: repo,
			Status:     "error",
		})
		return true, fmt.Errorf("post durable default-branch checks for %s@%s: %w", repo, headSHA, err)
	}

	h.logger.Info("durable default-branch checks posted",
		"delivery_id", event.DeliveryID, "repo", repo, "head_sha", headSHA)
	return false, nil
}
