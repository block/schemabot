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
//
// check_suite.rerequested is a different signal: an operator pressed Re-run
// all checks on the commit. It carries operator intent, so it is queued for
// a driver at once and re-plans every open PR still at the suite head, the
// same way check_run.rerequested re-plans one PR when its single check is
// re-run.
package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// checkSuiteRecoveryAction is the only check_suite action the recovery path
// consumes. GitHub sends "requested" on every push.
const checkSuiteRecoveryAction = "requested"

// checkSuiteRerunAction is the check_suite action GitHub sends when someone
// presses Re-run all checks. Without this path the button does nothing.
// "completed" carries no work.
const checkSuiteRerunAction = "rerequested"

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

	if payload.Action == checkSuiteRerunAction {
		h.handleCheckSuiteRerequest(ctx, metricApp, w, payload, body, deliveryID)
		return
	}
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
		metrics.RecordUnregisteredRepositoryWebhook(ctx, metricApp, "check_suite", payload.Action)
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
	ctx = ghclient.WithPRInfoCache(ctx)
	var payload checkSuitePayload
	if err := decodeDurablePayload(event, &payload); err != nil {
		return false, err
	}
	if payload.Action == checkSuiteRerunAction {
		return h.processDurableCheckSuiteRerequest(ctx, event, payload)
	}
	// Re-validate the action fail-closed: rows can arrive via replay or a
	// future producer, and any other action carries no recovery work.
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
		metrics.RecordUnregisteredRepositoryWebhook(ctx, h.metricAppForRepo(repo), "check_suite", payload.Action)
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
	for _, candidate := range candidates {
		pr, info := candidate.number, candidate.info
		decision, err := h.evaluatePRTarget(ctx, client, repo, info)
		if err != nil {
			return true, fmt.Errorf("evaluate check_suite PR target %s#%d: %w", repo, pr, err)
		}
		observation := prTargetObservation(info, decision)
		covered, err := h.recoveryTargetCovered(ctx, store, repo, pr, headSHA, observation)
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
		inserted, resynthesized, err := h.synthesizeMissingHeadDelivery(ctx, repo, pr, headSHA, installationID, observation)
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

type checkSuitePR struct {
	number int
	info   *ghclient.PullRequestInfo
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
func (h *Handler) resolveCheckSuitePRs(ctx context.Context, client *ghclient.InstallationClient, repo, headSHA string, payload checkSuitePayload) (prs []checkSuitePR, truncated bool, retry bool, err error) {
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
			prs = append(prs, checkSuitePR{number: candidate.Number, info: info})
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
				prs = append(prs, checkSuitePR{number: pr.Number, info: &ghclient.PullRequestInfo{HeadSHA: pr.HeadSHA, BaseRef: pr.BaseRef, DefaultBranch: pr.DefaultBranch, NativeStack: pr.NativeStack, StackBaseRef: pr.StackBaseRef, State: "open"}})
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

// handleCheckSuiteRerequest queues an operator's Re-run all checks. Unlike
// recovery it is not held back by a grace period and not gated on the
// recovery kill switch: it is a direct request, and nothing else will answer
// it. Enqueue failure is a deliberate 500, as for check_run.rerequested:
// GitHub never retries the delivery, so a red delivery in GitHub's webhook
// UI beats losing the re-run silently.
func (h *Handler) handleCheckSuiteRerequest(ctx context.Context, metricApp string, w http.ResponseWriter, payload checkSuitePayload, body []byte, deliveryID string) {
	repo := payload.Repository.FullName
	headSHA := payload.CheckSuite.HeadSHA
	if repo == "" || headSHA == "" {
		h.writeError(w, http.StatusBadRequest, "check_suite payload missing repository or head SHA")
		return
	}
	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("webhook from unregistered repository",
			"event", "check_suite", "action", payload.Action, "repo", repo,
			"delivery_id", deliveryID)
		metrics.RecordUnregisteredRepositoryWebhook(ctx, metricApp, "check_suite", payload.Action)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "repository not registered"})
		return
	}
	// GitHub names the open PRs at a same-repository head in the payload, so
	// an empty list there means the commit has no open PR to plan. A fork
	// head (empty head_branch) cannot name its PRs and is resolved later.
	if payload.CheckSuite.HeadBranch != "" && len(payload.CheckSuite.PullRequests) == 0 {
		h.logger.Info("check_suite re-run ignored because no open PR is at the suite head",
			"repo", repo, "head_sha", headSHA, "branch", payload.CheckSuite.HeadBranch,
			"delivery_id", deliveryID)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_suite re-run ignored without an open PR"})
		return
	}
	installationID := h.effectiveInstallationID(ctx, payload.Installation.ID)
	if installationID == 0 {
		h.writeError(w, http.StatusBadRequest, "missing installation ID in webhook payload")
		return
	}

	if !h.durableWebhookDispatch {
		retry, err := h.rerunCheckSuite(context.Background(), repo, headSHA, installationID, payload, deliveryID)
		if err != nil {
			h.logger.Error("check_suite re-run failed",
				"repo", repo, "head_sha", headSHA, "delivery_id", deliveryID,
				"retryable", retry, "error", err)
			h.writeError(w, http.StatusInternalServerError, "failed to re-run checks")
			return
		}
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_suite re-run processed"})
		return
	}

	event := &storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: deliveryID,
		Event:      "check_suite",
		Action:     payload.Action,
		Repository: repo,
		HeadSHA:    headSHA,
		TenantID:   strconv.FormatInt(installationID, 10),
		Payload:    body,
	}
	inserted, err := h.enqueueDurableWebhookEvent(ctx, event)
	if err != nil {
		h.logger.Error("failed to enqueue durable check_suite re-run",
			"repo", repo, "head_sha", headSHA, "delivery_id", deliveryID, "error", err)
		metrics.RecordWebhookEvent(ctx, metricApp, "check_suite", payload.Action, repo, "durable_enqueue_failed")
		h.writeError(w, http.StatusInternalServerError, "failed to enqueue webhook delivery")
		return
	}
	if !inserted {
		h.logger.Info("durable check_suite re-run already queued",
			"repo", repo, "head_sha", headSHA, "delivery_id", deliveryID)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_suite re-run already queued"})
		return
	}
	h.logger.Info("durable check_suite re-run queued",
		"repo", repo, "head_sha", headSHA, "delivery_id", deliveryID)
	h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_suite re-run queued"})
}

// processDurableCheckSuiteRerequest drives a claimed Re-run all checks
// delivery under the driver lease.
func (h *Handler) processDurableCheckSuiteRerequest(ctx context.Context, event *storage.WebhookEvent, payload checkSuitePayload) (retry bool, err error) {
	repo := storage.CanonicalKey(event.Repository)
	headSHA := event.HeadSHA
	if repo == "" || headSHA == "" {
		return false, fmt.Errorf("durable check_suite re-run %s missing repo or head SHA", event.DeliveryID)
	}
	// The allowlist can change between enqueue and claim.
	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("durable check_suite re-run from unregistered repository",
			"delivery_id", event.DeliveryID, "repo", repo, "head_sha", headSHA)
		metrics.RecordUnregisteredRepositoryWebhook(ctx, h.metricAppForRepo(repo), "check_suite", payload.Action)
		return false, nil
	}
	installationID, err := durableInstallationID(event)
	if err != nil {
		return false, err
	}
	return h.rerunCheckSuite(ctx, repo, headSHA, installationID, payload, event.DeliveryID)
}

// rerunCheckSuite plans every PR the suite head belongs to again, through the
// same per-PR path as a single check's Re-run, so a closed PR or a PR whose
// head moved is skipped there. A failure on any PR keeps the whole delivery
// retryable; planning a PR again at the same head is safe, so a retry that
// re-plans a PR which already succeeded costs only the repeated work.
func (h *Handler) rerunCheckSuite(ctx context.Context, repo, headSHA string, installationID int64, payload checkSuitePayload, deliveryID string) (retry bool, err error) {
	runCtx := ctx
	lookupCtx, cancel, client, err := h.autoPlanBootstrap(ctx, repo, installationID)
	if err != nil {
		metrics.RecordWebhookEvent(runCtx, h.metricAppForRepo(repo), "check_suite", payload.Action, repo, "auto_plan_bootstrap_failed")
		return true, fmt.Errorf("bootstrap check_suite re-run %s for %s@%s: %w", deliveryID, repo, headSHA, err)
	}
	prs, err := h.checkSuiteRerunPRs(lookupCtx, client, repo, headSHA, payload)
	cancel()
	if err != nil {
		return true, err
	}
	if len(prs) == 0 {
		h.logger.Info("check_suite re-run found no open PR at the suite head; nothing is planned",
			"delivery_id", deliveryID, "repo", repo, "head_sha", headSHA)
		return false, nil
	}
	return h.replanCheckSuitePRs(runCtx, client, repo, headSHA, installationID, deliveryID, prs)
}

// replanCheckSuitePRs re-plans each PR a Re-run all checks names. The PRs are
// independent, so each gets its own auto-plan deadline, and one PR's failure
// does not stop the rest from being planned. The delivery is retried when any
// failure is retryable, and dead-lettered only when every failure is
// permanent.
func (h *Handler) replanCheckSuitePRs(runCtx context.Context, client *ghclient.InstallationClient, repo, headSHA string, installationID int64, deliveryID string, prs []int) (retry bool, err error) {
	var errs []error
	for _, pr := range prs {
		if runCtx.Err() != nil {
			h.logger.Warn("check_suite re-run stopped before planning every PR because its run was cancelled; the delivery will be retried",
				"delivery_id", deliveryID, "repo", repo, "head_sha", headSHA, "next_pr", pr)
			errs = append(errs, fmt.Errorf("check_suite re-run %s for %s@%s cancelled before PR #%d: %w", deliveryID, repo, headSHA, pr, runCtx.Err()))
			retry = true
			break
		}
		prCtx, prCancel := autoPlanContext(runCtx)
		prRetry, prErr := h.replanRerunRequest(prCtx, runCtx, client, rerunRequest{
			kind:           checkSuiteRerun,
			deliveryID:     deliveryID,
			repo:           repo,
			pr:             pr,
			headSHA:        headSHA,
			installationID: installationID,
		})
		prCancel()
		if prErr != nil {
			h.logger.Warn("check_suite re-run could not plan a PR; the remaining PRs are still planned",
				"delivery_id", deliveryID, "repo", repo, "pr", pr, "head_sha", headSHA, "retryable", prRetry, "error", prErr)
			errs = append(errs, prErr)
			retry = retry || prRetry
		}
	}
	return retry, errors.Join(errs...)
}

// checkSuiteRerunPRs lists the PR numbers a Re-run all checks names. A
// same-repository suite names them in the payload. A fork head cannot, so
// its PRs are found by walking the open PRs for the suite head, as recovery
// does.
func (h *Handler) checkSuiteRerunPRs(ctx context.Context, client *ghclient.InstallationClient, repo, headSHA string, payload checkSuitePayload) ([]int, error) {
	if len(payload.CheckSuite.PullRequests) > 0 {
		prs := make([]int, 0, len(payload.CheckSuite.PullRequests))
		for _, candidate := range payload.CheckSuite.PullRequests {
			if candidate.Number <= 0 {
				h.logger.Warn("check_suite re-run skipped a payload PR entry without a number",
					"repo", repo, "head_sha", headSHA)
				continue
			}
			prs = append(prs, candidate.Number)
		}
		return prs, nil
	}
	candidates, truncated, _, err := h.resolveCheckSuitePRs(ctx, client, repo, headSHA, payload)
	if err != nil {
		return nil, err
	}
	if truncated {
		h.logger.Warn("check_suite re-run plans only the open PRs found before the page budget ran out",
			"repo", repo, "head_sha", headSHA, "found", len(candidates))
	}
	prs := make([]int, 0, len(candidates))
	for _, candidate := range candidates {
		prs = append(prs, candidate.number)
	}
	return prs, nil
}
