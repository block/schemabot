package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/metrics"
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
func (h *Handler) handlePush(ctx context.Context, metricApp string, w http.ResponseWriter, body []byte) {
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

	postCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for default-branch check",
			"repo", repo, "head_sha", headSHA, "installation_id", installationID, "error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to initialize GitHub client")
		return
	}

	if err := h.postPassingAggregateChecks(postCtx, client, repo, headSHA, passingAggregateCheckContent{
		operation: "default_branch_check",
		title:     "Schema changes are verified before merge",
		summary: "SchemaBot gates schema changes on pull requests before they reach the default branch, " +
			"so commits on the default branch require no additional verification. " +
			"This check also keeps SchemaBot selectable as a required-check source in branch rulesets.",
	}); err != nil {
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
