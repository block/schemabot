// check_suite.go feeds GitHub's check_suite.requested delivery into the
// durable inbox as a redundant convergence signal. For every push to a PR
// branch, a GitHub App with check permissions receives two independent
// deliveries seconds apart: pull_request (what auto-plan acts on) and
// check_suite.requested. When the pull_request delivery is lost upstream of
// the inbox, the check suite sits queued with zero check runs and the PR is
// blocked with no recourse until the reconciler's next scan. The check_suite
// delivery is GitHub's purpose-built "populate your check runs for this SHA
// now" signal — handling it converges a lost auto-plan in minutes instead of
// waiting for the reconcile interval.
//
// The signal is deliberately deferred, not raced: the ingress enqueues the
// delivery with a not-before time (the recovery grace), so the organic
// pull_request delivery — which almost always arrives and plans within
// seconds — wins. When the grace passes, processing re-resolves the suite
// head against GitHub's *current* PR state and synthesizes a recovery
// delivery only for open PRs still at that head whose auto-plan coverage is
// missing from the inbox. Recovery rows use the same deterministic GUID as
// the reconciler's missing-head synthesis, so the two recovery producers
// dedupe against each other naturally.
package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// checkSuiteRecoveryAction is the only check_suite action the recovery path
// consumes. GitHub sends "requested" on every push; "rerequested" (a human
// clicked Re-run on the suite) already re-plans through check_run.rerequested,
// and "completed" carries no work.
const checkSuiteRecoveryAction = "requested"

// defaultCheckSuiteRecoveryGrace is how long a check_suite delivery waits in
// the inbox before it becomes claimable. The organic pull_request delivery
// for the same push normally arrives within seconds, plans the head, and the
// recovery pass then no-ops on coverage. The grace only delays recovery of a
// genuinely lost delivery, so it trades a few minutes of convergence lag for
// never racing the primary signal.
const defaultCheckSuiteRecoveryGrace = 2 * time.Minute

// checkSuitePayload represents the relevant fields from a GitHub check_suite
// webhook. HeadBranch is null (decoded as empty) when the suite head lives in
// a fork, and PullRequests is empty in the same cases, so processing falls
// back to resolving PRs by head SHA via the API.
type checkSuitePayload struct {
	Action     string `json:"action"`
	CheckSuite struct {
		HeadSHA      string `json:"head_sha"`
		HeadBranch   string `json:"head_branch"`
		PullRequests []struct {
			Number int `json:"number"`
		} `json:"pull_requests"`
	} `json:"check_suite"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
}

// handleCheckSuite enqueues a check_suite.requested delivery into the durable
// inbox with a not-before time and ACKs fast. All resolution work — which PRs
// the suite head belongs to, whether their auto-plan coverage exists — runs
// later under a driver lease, after the grace has given the organic
// pull_request delivery time to win.
func (h *Handler) handleCheckSuite(ctx context.Context, metricApp string, w http.ResponseWriter, body []byte, deliveryID string) {
	var payload checkSuitePayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid check_suite payload")
		return
	}
	payload.Repository.FullName = storage.CanonicalKey(payload.Repository.FullName)
	repo := payload.Repository.FullName
	headSHA := payload.CheckSuite.HeadSHA

	if payload.Action != checkSuiteRecoveryAction {
		h.logger.Debug("check_suite delivery ignored because action carries no recovery work",
			"action", payload.Action, "repo", repo, "delivery_id", deliveryID)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_suite action ignored"})
		return
	}
	if !h.checkSuiteRecovery {
		h.logger.Debug("check_suite delivery ignored because check-suite recovery is disabled",
			"repo", repo, "head_sha", headSHA, "delivery_id", deliveryID)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_suite recovery disabled"})
		return
	}
	if !h.durableWebhookDispatch {
		// Recovery rides the durable inbox: without dispatch there is no
		// driver to claim the synthesized row, so the delivery is ignored.
		h.logger.Debug("check_suite delivery ignored because durable webhook dispatch is disabled",
			"repo", repo, "head_sha", headSHA, "delivery_id", deliveryID)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "durable webhook dispatch disabled"})
		return
	}
	if repo == "" || headSHA == "" {
		h.writeError(w, http.StatusBadRequest, "check_suite payload missing repository or head SHA")
		return
	}
	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("webhook from unregistered repository",
			"event", "check_suite", "action", payload.Action, "repo", repo,
			"delivery_id", deliveryID)
		metrics.RecordUnregisteredRepositoryWebhook(ctx, metricApp, "check_suite", payload.Action, repo)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "repository not registered"})
		return
	}
	// check_suite.requested fires for every push to every branch, but GitHub
	// names the open PRs at a same-repository head in the payload when it
	// sends the delivery, so a non-fork suite with an empty list means no
	// open PR existed at delivery time and there is nothing to recover.
	// Skipping it here keeps the dominant non-PR traffic — pushes to branches
	// (including the default branch) without an open PR — from occupying
	// inbox rows and grace-delayed processing passes. The cost of the skip:
	// for a same-repository PR, the only check_suite.requested at its initial
	// head fires at push time, before the PR exists, so it is dropped here —
	// a lost pull_request.opened on a same-repository PR is not recovered by
	// this mechanism at all; the reconciler's missing-head scan owns that
	// gap. Fork heads are the mirror image: their suite is created when the
	// PR opens, so they arrive with an empty head_branch and an empty list
	// and pass through to processing's API-resolution fallback.
	if payload.CheckSuite.HeadBranch != "" && len(payload.CheckSuite.PullRequests) == 0 {
		h.logger.Debug("check_suite delivery skipped because no open PR existed at the suite head",
			"repo", repo, "head_sha", headSHA, "branch", payload.CheckSuite.HeadBranch,
			"delivery_id", deliveryID)
		metrics.RecordWebhookCheckSuiteRecovery(ctx, repo, "no_pr_at_ingress")
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_suite without an open PR skipped"})
		return
	}

	installationID := h.effectiveInstallationID(ctx, payload.Installation.ID)
	if installationID == 0 {
		h.writeError(w, http.StatusBadRequest, "missing installation ID in webhook payload")
		return
	}

	retryAfter := time.Now().Add(h.checkSuiteRecoveryGrace)
	event := &storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: deliveryID,
		Event:      "check_suite",
		Action:     payload.Action,
		Repository: repo,
		HeadSHA:    headSHA,
		TenantID:   strconv.FormatInt(installationID, 10),
		Payload:    body,
		RetryAfter: &retryAfter,
	}
	inserted, err := h.enqueueDurableWebhookEvent(ctx, event)
	if err != nil {
		h.logger.Error("failed to enqueue durable check_suite recovery",
			"repo", repo, "head_sha", headSHA, "delivery_id", deliveryID, "error", err)
		metrics.RecordWebhookEvent(ctx, metricApp, "check_suite", payload.Action, repo, "durable_enqueue_failed")
		h.writeError(w, http.StatusInternalServerError, "failed to enqueue webhook delivery")
		return
	}
	if !inserted {
		h.logger.Info("durable check_suite delivery already queued",
			"repo", repo, "head_sha", headSHA, "delivery_id", deliveryID)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_suite recovery already queued"})
		return
	}
	if event.ID == 0 {
		// A zero ID after inserted=true means Create reopened a redelivered
		// terminal row rather than inserting a fresh one, and a reopen clears
		// the not-before time: Redeliver is an operator recovery lever, so
		// the reopened row is claimable immediately.
		h.logger.Info("durable check_suite recovery reopened by redelivery and claimable immediately",
			"repo", repo, "head_sha", headSHA, "delivery_id", deliveryID)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_suite recovery queued"})
		return
	}
	h.logger.Info("durable check_suite recovery queued",
		"repo", repo, "head_sha", headSHA, "delivery_id", deliveryID,
		"not_before", retryAfter.UTC().Format(time.RFC3339))
	h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_suite recovery queued"})
}

// processDurableCheckSuite drives a claimed check_suite delivery: it resolves
// which open PRs currently sit at the suite's head SHA and synthesizes a
// recovery delivery for each one whose auto-plan coverage is missing from the
// inbox. In the steady state — the organic pull_request delivery arrived and
// planned during the grace — every candidate is covered and the pass no-ops.
func (h *Handler) processDurableCheckSuite(ctx context.Context, event *storage.WebhookEvent) (retry bool, err error) {
	var payload checkSuitePayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return false, fmt.Errorf("decode durable check_suite delivery %s: %w", event.DeliveryID, err)
	}
	// Re-validate the action fail-closed: rows can arrive via replay or a
	// future producer, and a non-requested action carries no recovery work.
	if payload.Action != checkSuiteRecoveryAction {
		h.logger.Info("durable check_suite delivery ignored because action needs no work",
			"delivery_id", event.DeliveryID, "action", payload.Action, "repo", event.Repository)
		return false, nil
	}
	// The kill switch must stop synthesis with a restart, including rows
	// enqueued before the operator flipped it, so re-validate the flag here
	// and not just at ingress.
	if !h.checkSuiteRecovery {
		h.logger.Info("durable check_suite delivery ignored because check-suite recovery is disabled",
			"delivery_id", event.DeliveryID, "repo", event.Repository, "head_sha", event.HeadSHA)
		return false, nil
	}
	repo := storage.CanonicalKey(event.Repository)
	headSHA := event.HeadSHA
	if repo == "" || headSHA == "" {
		return false, fmt.Errorf("durable check_suite delivery %s missing repo or head SHA", event.DeliveryID)
	}
	// The allowlist can change between enqueue and claim; re-validate so a
	// deregistered repository cannot have work synthesized for it.
	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("durable check_suite delivery from unregistered repository",
			"delivery_id", event.DeliveryID, "repo", repo, "head_sha", headSHA)
		metrics.RecordUnregisteredRepositoryWebhook(ctx, h.metricAppForRepo(repo), "check_suite", payload.Action, repo)
		return false, nil
	}
	installationID, err := durableInstallationID(event)
	if err != nil {
		return false, err
	}
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		return true, fmt.Errorf("create GitHub client for durable check_suite delivery %s (%s@%s): %w",
			event.DeliveryID, repo, headSHA, err)
	}
	store := h.webhookEventStore()
	if store == nil {
		return true, fmt.Errorf("webhook event storage is unavailable for durable check_suite delivery %s", event.DeliveryID)
	}

	candidates, truncated, retryable, err := h.resolveCheckSuitePRs(ctx, client, repo, headSHA, payload)
	if err != nil {
		return retryable, err
	}
	if truncated {
		// PR coverage for this head is incomplete, not absent — a matching
		// PR beyond the page budget goes unresolved this delivery, and the
		// reconciler's missing-head scan backstops it. Recorded distinctly
		// from no_open_pr so a dashboard shows truncation instead of a
		// benign no-match.
		metrics.RecordWebhookCheckSuiteRecovery(ctx, repo, "truncated")
	}
	if len(candidates) == 0 {
		if !truncated {
			h.logger.Info("durable check_suite delivery matched no open PR at the suite head",
				"delivery_id", event.DeliveryID, "repo", repo, "head_sha", headSHA)
			metrics.RecordWebhookCheckSuiteRecovery(ctx, repo, "no_open_pr")
		}
		return false, nil
	}

	var synthesized int
	for _, pr := range candidates {
		covered, err := store.HasEventForHead(ctx, storage.WebhookProviderGitHub, repo, pr, headSHA)
		if err != nil {
			return true, fmt.Errorf("query inbox coverage for %s#%d@%s (durable check_suite delivery %s): %w",
				repo, pr, headSHA, event.DeliveryID, err)
		}
		if covered {
			h.logger.Debug("check_suite recovery skipped PR head with auto-plan coverage",
				"delivery_id", event.DeliveryID, "repo", repo, "pr", pr, "head_sha", headSHA)
			metrics.RecordWebhookCheckSuiteRecovery(ctx, repo, "covered")
			continue
		}
		inserted, resynthesized, err := h.synthesizeMissingHeadDelivery(ctx, repo, pr, headSHA, installationID)
		if err != nil {
			return true, fmt.Errorf("synthesize recovery delivery for %s#%d@%s (durable check_suite delivery %s): %w",
				repo, pr, headSHA, event.DeliveryID, err)
		}
		if !inserted {
			// A live recovery row for this head already exists — the
			// reconciler (or an earlier check_suite delivery) got there
			// first and the dispatcher will plan it.
			h.logger.Debug("check_suite recovery found an already-queued recovery delivery",
				"delivery_id", event.DeliveryID, "repo", repo, "pr", pr, "head_sha", headSHA)
			metrics.RecordWebhookCheckSuiteRecovery(ctx, repo, "already_queued")
			continue
		}
		synthesized++
		outcome := "synthesized"
		if resynthesized {
			outcome = "resynthesized"
		}
		h.logger.Warn("check_suite recovery synthesized inbox delivery for open PR head missing its auto-plan delivery",
			"delivery_id", event.DeliveryID, "repo", repo, "pr", pr, "head_sha", headSHA,
			"resynthesized", resynthesized)
		metrics.RecordWebhookCheckSuiteRecovery(ctx, repo, outcome)
	}
	h.logger.Info("durable check_suite recovery pass completed",
		"delivery_id", event.DeliveryID, "repo", repo, "head_sha", headSHA,
		"candidate_prs", len(candidates), "synthesized", synthesized)
	return false, nil
}

// resolveCheckSuitePRs resolves the open PRs whose *current* head is the
// check suite's head SHA. The payload's pull_requests entries are
// point-in-time snapshots taken when GitHub sent the delivery — and the
// recovery grace means minutes have passed — so each is re-fetched and kept
// only when the PR is still open at the same head; a moved or closed PR is
// skipped because a fresher signal owns its new state. Only a fork head — an
// empty head_branch, whose payload cannot name PRs — falls back to a bounded
// walk of the repository's open PRs matched by head SHA, whose listing is
// current at call time. truncated reports that the walk exhausted its page
// budget before the listing did, so the returned PRs are incomplete rather
// than a genuine no-match.
func (h *Handler) resolveCheckSuitePRs(ctx context.Context, client *ghclient.InstallationClient, repo, headSHA string, payload checkSuitePayload) (prs []int, truncated bool, retry bool, err error) {
	if len(payload.CheckSuite.PullRequests) > 0 {
		for _, candidate := range payload.CheckSuite.PullRequests {
			if candidate.Number <= 0 {
				h.logger.Debug("check_suite recovery skipped payload PR entry without a number",
					"repo", repo, "head_sha", headSHA)
				continue
			}
			info, err := client.FetchPullRequestNoCache(ctx, repo, candidate.Number)
			if err != nil {
				return nil, false, true, fmt.Errorf("fetch pull request %s#%d for check_suite head %s: %w",
					repo, candidate.Number, headSHA, err)
			}
			if info.IsClosed() {
				h.logger.Debug("check_suite recovery skipped closed PR",
					"repo", repo, "pr", candidate.Number, "head_sha", headSHA)
				continue
			}
			if info.HeadSHA != headSHA {
				h.logger.Debug("check_suite recovery skipped PR whose head moved past the suite head",
					"repo", repo, "pr", candidate.Number, "suite_head_sha", headSHA,
					"current_head_sha", info.HeadSHA)
				continue
			}
			prs = append(prs, candidate.Number)
		}
		return prs, false, false, nil
	}
	// GitHub names the open PRs for a same-repository head in the payload, so
	// an empty list on a non-fork suite (a non-empty head_branch) means no
	// open PR existed when the delivery was sent. A same-repository PR opened
	// after the push never produces another check_suite.requested at this
	// head, so its recovery belongs to the reconciler's missing-head scan,
	// not this path. The API walk below exists only for fork heads, whose
	// payload cannot name PRs.
	if payload.CheckSuite.HeadBranch != "" {
		h.logger.Debug("check_suite recovery resolved no PRs because the same-repository suite head had no open PR at delivery time",
			"repo", repo, "head_sha", headSHA, "head_branch", payload.CheckSuite.HeadBranch)
		return nil, false, false, nil
	}
	page := 1
	for range h.webhookReconcileMaxPages {
		open, nextPage, _, err := client.ListOpenPullRequestsPage(ctx, repo, page, webhookReconcilePageSize)
		if err != nil {
			return nil, false, true, fmt.Errorf("list open pull requests for %s while resolving check_suite head %s: %w",
				repo, headSHA, err)
		}
		for _, pr := range open {
			if pr.HeadSHA == headSHA {
				prs = append(prs, pr.Number)
			}
		}
		if nextPage == 0 {
			return prs, false, false, nil
		}
		page = nextPage
	}
	// The page budget ran out before the listing was exhausted, so a matching
	// PR beyond the budget goes unresolved this delivery. The reconciler's
	// missing-head scan remains the backstop for anything missed here.
	h.logger.Warn("check_suite recovery exhausted its page budget while resolving the suite head; open PR coverage for this head is truncated",
		"repo", repo, "head_sha", headSHA, "max_pages", h.webhookReconcileMaxPages,
		"page_size", webhookReconcilePageSize)
	return prs, true, false, nil
}
