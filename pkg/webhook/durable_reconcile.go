// durable_reconcile.go implements the webhook reconciliation loop: the
// correctness backstop for deliveries the durable inbox cannot recover on its
// own. Each pass does two things:
//
//   - Actively terminates inbox rows wedged in processing (driver hard-killed on
//     its final attempt, lease expired, attempts at the cap) that FindNext never
//     reclaims, emitting each as a durable failure so it surfaces in
//     metrics/alerting and drains the stuck-processing gauge. GitHub Redeliver
//     can also reopen such a row on demand; this sweep is the automatic
//     complement that recovers rows nobody redelivered.
//   - Detects any recently updated open PR head in a registered repository that
//     has no corresponding webhook_events row, surfacing deliveries lost
//     upstream of the inbox (edge auth failures, GitHub-side send failures) —
//     or heads whose only rows were discarded by claim-time coalescing, since
//     superseded rows do not attest coverage of their head.
//     With synthesis enabled (WithWebhookReconcileSynthesis) it also recovers
//     each miss by enqueueing a pull_request-equivalent inbox row (see
//     synthesizedDeliveryGUID; naturally deduped per head) that the durable
//     dispatcher plans through the ordinary auto-plan flow; otherwise the scan
//     is report-only. A dead-lettered head (failed_permanent) is not a miss —
//     HasEventForHead reports it covered — so synthesis cannot resurrect a
//     delivery the driver proved can never succeed for that head.
package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

const (
	defaultWebhookReconcileInterval = 30 * time.Minute

	// defaultWebhookReconcileLookback bounds how far back the updated-descending
	// PR listing is walked. It also suppresses a permanent false-positive class:
	// open PRs whose last activity predates the inbox feature never have rows.
	defaultWebhookReconcileLookback = 48 * time.Hour

	// defaultWebhookReconcileGrace skips PRs updated moments ago, whose webhook
	// delivery may legitimately still be in flight to the inbox.
	defaultWebhookReconcileGrace = 15 * time.Minute

	defaultWebhookReconcileMaxPages = 5
	webhookReconcilePageSize        = 100
)

// startWebhookReconciler launches the reconcile loop on the durable-dispatch
// lifecycle: it shares the dispatch stop channel, context, and wait group, so
// StopDurableWebhookDispatch also stops the reconciler. The first pass runs
// after one full interval — deliberately not at startup — so a rolling deploy
// does not fan a GitHub list scan out across every starting replica.
func (h *Handler) startWebhookReconciler(ctx context.Context, stop <-chan struct{}) {
	h.durableWebhookWg.Go(func() {
		ticker := time.NewTicker(h.webhookReconcileInterval)
		defer ticker.Stop()
		h.logger.Info("webhook reconciler started",
			"interval", h.webhookReconcileInterval,
			"lookback", h.webhookReconcileLookback,
			"grace", h.webhookReconcileGrace)
		// The stuck-processing sweep is a single cheap DB UPDATE, not a GitHub
		// list scan, so it runs once at startup rather than waiting a full
		// interval. A fleet that crash-loops faster than the reconcile interval
		// would otherwise never reclaim the rows those very crashes wedged. The
		// missing-delivery scan still waits for the first tick (see the
		// startup-delay rationale below) to avoid fanning GitHub list calls out
		// across every starting replica on a rolling deploy.
		if ctx.Err() == nil {
			if store := h.webhookEventStore(); store != nil {
				h.terminateStuckWebhookEvents(ctx, store)
			} else {
				h.logger.Warn("webhook reconciler startup stuck-processing sweep skipped because webhook event storage is unavailable")
			}
		}
		for {
			select {
			case <-stop:
				h.logger.Debug("webhook reconciler stopping")
				return
			case <-ctx.Done():
				h.logger.Debug("webhook reconciler context cancelled")
				return
			case <-ticker.C:
				h.reconcileWebhookInbox(ctx)
			}
		}
	})
}

// reconcileWebhookInbox runs one reconciliation pass in two stages: an active
// stuck-processing sweep that terminalizes inbox rows wedged past the attempt
// cap, followed by a missing-delivery scan over registered repositories. The
// scan reports every open PR head with no inbox delivery and, when synthesis
// is enabled, recovers each miss by enqueueing a synthesized inbox row.
func (h *Handler) reconcileWebhookInbox(ctx context.Context) {
	store := h.webhookEventStore()
	if store == nil {
		h.logger.Warn("webhook reconciler skipped because webhook event storage is unavailable")
		return
	}
	// The stuck-processing sweep scans the whole inbox by state, not by repo, so
	// it runs before the registry check — it must reclaim crashed deliveries
	// even when the registry is allow-all and the missing-delivery scan below
	// cannot enumerate repos.
	h.terminateStuckWebhookEvents(ctx, store)

	cfg := h.service.Config()
	if cfg == nil || len(cfg.Repos) == 0 {
		// An empty repo registry means "allow all", which is not an enumerable
		// set; the missing-delivery scan needs an explicit registry to know what
		// to scan.
		h.logger.Debug("webhook reconciler missing-delivery scan skipped because the repo registry is empty (allow-all)")
		return
	}

	repos := make([]string, 0, len(cfg.Repos))
	for repo := range cfg.Repos {
		repos = append(repos, repo)
	}
	sort.Strings(repos)

	start := time.Now()
	var scanned, missing, synthesized int
	for _, repo := range repos {
		if ctx.Err() != nil {
			h.logger.Debug("webhook reconcile missing-delivery scan stopped early because the context was cancelled",
				"repos_scanned", scanned, "error", ctx.Err())
			return
		}
		repoScanned, repoMissing, repoSynthesized := h.reconcileRepoWebhookInbox(ctx, store, repo)
		scanned += repoScanned
		missing += repoMissing
		synthesized += repoSynthesized
	}
	h.logger.Info("webhook reconcile missing-delivery scan finished",
		"repos", len(repos), "prs_scanned", scanned, "missing_inbox_rows", missing,
		"synthesized", synthesized, "synthesis_enabled", h.webhookReconcileSynthesis,
		"duration", time.Since(start))
}

// webhookReconcileStuckReason is the terminal last_error recorded on rows the
// reconciler sweeps out of a wedged processing state. The row reached the
// attempt cap and its lease expired without a terminal write — usually a driver
// hard-killed mid-attempt, but it can also be a dispatch that completed its work
// yet died before recording completion. The reason stays agnostic between those.
const webhookReconcileStuckReason = "terminated by reconciler: processing lease expired at attempt cap without a terminal write"

// terminateStuckWebhookEvents marks failed every inbox row parked in processing
// with an expired lease at the attempt cap — the driver reached the cap and its
// lease expired without a terminal write. FindNext stops reclaiming a processing row once attempts reach the
// cap, so without this sweep such a row stays unclaimable forever and its
// delivery GUID deduplicates every redelivery. Terminalizing it emits the row
// as a failure and makes it eligible for the redeliver-reopen path.
func (h *Handler) terminateStuckWebhookEvents(ctx context.Context, store storage.WebhookEventStore) {
	terminated, err := store.TerminateStuckProcessing(ctx, webhookReconcileStuckReason)
	if err != nil {
		h.logger.Warn("webhook reconciler failed to terminate stuck processing events", "error", err)
		return
	}
	if terminated == 0 {
		return
	}
	h.logger.Warn("webhook reconciler terminated stuck processing events", "terminated", terminated)
	metrics.RecordWebhookReconcileStuckTerminated(ctx, terminated)
}

// reconcileRepoWebhookInbox scans one repository's recently updated open PRs
// for heads with no inbox delivery, synthesizing a recovery row per miss when
// synthesis is enabled and reporting otherwise. Returns how many PRs were
// checked, how many were missing rows, and how many recovery rows were
// enqueued.
func (h *Handler) reconcileRepoWebhookInbox(ctx context.Context, store storage.WebhookEventStore, repo string) (scanned, missing, synthesized int) {
	installationID, err := h.resolveRepoWebhookInstallation(ctx, repo)
	if err != nil {
		h.logger.Warn("webhook reconciler could not resolve installation for repository", "repo", repo, "error", err)
		return 0, 0, 0
	}
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Warn("webhook reconciler could not build client for repository", "repo", repo, "error", err)
		return 0, 0, 0
	}

	now := time.Now()
	cutoff := now.Add(-h.webhookReconcileLookback)
	grace := now.Add(-h.webhookReconcileGrace)
	page := 1
	coverageComplete := false
pages:
	for range h.webhookReconcileMaxPages {
		prs, nextPage, _, err := client.ListOpenPullRequestsPage(ctx, repo, page, webhookReconcilePageSize)
		if err != nil {
			h.logger.Warn("webhook reconciler failed to list open pull requests", "repo", repo, "page", page, "error", err)
			return scanned, missing, synthesized
		}
		for _, pr := range prs {
			if pr.UpdatedAt.Before(cutoff) {
				// The listing is newest-updated first; everything after this is
				// older than the lookback window.
				coverageComplete = true
				break pages
			}
			if pr.HeadSHA == "" {
				// A PR listing without a head SHA can't be matched to an inbox
				// delivery; skip rather than emit a spurious missing-row report.
				h.logger.Debug("webhook reconciler skipped open PR with no head SHA",
					"repo", repo, "pr", pr.Number)
				continue
			}
			if pr.UpdatedAt.After(grace) {
				// Updated within the grace window; its webhook delivery may still
				// be in flight to the inbox, so a missing row here is expected.
				h.logger.Debug("webhook reconciler skipped recently updated open PR within grace window",
					"repo", repo, "pr", pr.Number, "updated_at", pr.UpdatedAt)
				continue
			}
			scanned++
			found, err := store.HasEventForHead(ctx, storage.WebhookProviderGitHub, repo, pr.Number, pr.HeadSHA)
			if err != nil {
				h.logger.Warn("webhook reconciler failed to query inbox for PR head",
					"repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA, "error", err)
				continue
			}
			if found {
				continue
			}
			missing++
			metrics.RecordWebhookReconcileMissingEvent(ctx, repo)
			if !h.webhookReconcileSynthesis {
				h.logger.Warn("webhook reconciler found open PR head with no inbox delivery (report-only; synthesis disabled)",
					"repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA, "updated_at", pr.UpdatedAt)
				continue
			}
			h.logger.Warn("webhook reconciler found open PR head with no inbox delivery; synthesizing recovery delivery",
				"repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA, "updated_at", pr.UpdatedAt)
			inserted, resynthesized, err := h.synthesizeMissingHeadDelivery(ctx, repo, pr.Number, pr.HeadSHA, installationID)
			if err != nil {
				// Each head recovers independently; the next pass retries this one.
				h.logger.Warn("webhook reconciler failed to synthesize recovery delivery for open PR head",
					"repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA, "error", err)
				continue
			}
			if !inserted {
				// Another pod's reconciler won the enqueue race on the same
				// synthesized GUID and its row is still live; the head is
				// covered. (An organic delivery can never collide here — its
				// GUID is GitHub's, not the synthesized form — it is caught by
				// the HasEventForHead check upstream instead.)
				h.logger.Debug("webhook reconciler skipped synthesizing recovery delivery because one is already queued",
					"repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA)
				continue
			}
			synthesized++
			metrics.RecordWebhookReconcileSynthesizedEvent(ctx, repo, resynthesized)
		}
		if nextPage == 0 {
			coverageComplete = true
			break
		}
		page = nextPage
	}
	if !coverageComplete {
		// The page budget ran out before the walk reached the lookback cutoff,
		// so open PR heads older than the last page scanned went unchecked this
		// pass. Surface the truncated coverage rather than silently capping it.
		h.logger.Warn("webhook reconciler exhausted its page budget before reaching the lookback cutoff; open PR coverage is truncated this pass",
			"repo", repo, "max_pages", h.webhookReconcileMaxPages, "page_size", webhookReconcilePageSize)
	}
	return scanned, missing, synthesized
}

// webhookReconcileSynthesizedAction is the pull_request action stamped on
// synthesized recovery deliveries. The lost organic delivery could have been
// any auto-plannable action; synchronize routes the row through the same
// auto-plan flow, and its empty before SHA makes the comment gate decide from
// tracked plan comment freshness — posting the plan for a genuinely unplanned
// head while a head already covered by a current plan is not commented twice.
const webhookReconcileSynthesizedAction = "synchronize"

// synthesizedDeliveryGUID is the deterministic dedup key for a synthesized
// recovery delivery. It must fit the webhook_events delivery_id column, and
// the repository full name is the only unbounded component, so the repo is
// folded into a short digest while the PR number and a truncated head SHA
// stay readable for triage. The GUID is a dedup key only — the inbox row's
// repository, pull_request, and head_sha columns carry the full values.
// Re-scans of the same head produce the same GUID and dedupe naturally, and
// every new push mints a fresh recovery candidate.
func synthesizedDeliveryGUID(repo string, pr int, headSHA string) string {
	repoDigest := sha256.Sum256([]byte(repo))
	if len(headSHA) > 12 {
		headSHA = headSHA[:12]
	}
	return fmt.Sprintf("%s%x:%d@%s", storage.SynthesizedWebhookDeliveryIDPrefix, repoDigest[:6], pr, headSHA)
}

// synthesizeMissingHeadDelivery enqueues a pull_request-equivalent inbox row
// for an open PR head whose organic webhook delivery never reached the inbox.
// The payload is the minimal pull_request shape the durable dispatcher
// decodes, and the resolved installation is persisted as the tenant because a
// synthesized delivery has no payload installation to resolve from.
//
// resynthesized reports whether a row for this head's synthesized GUID
// already existed — meaning a previous recovery attempt terminally failed
// and this enqueue reopens it — so the caller can separate first-time
// recovery from a head that keeps failing after recovery. The pre-check and
// the enqueue are not atomic; a concurrent pod inserting between them can at
// worst mislabel one metric increment, never affect the row itself.
func (h *Handler) synthesizeMissingHeadDelivery(ctx context.Context, repo string, pr int, headSHA string, installationID int64) (inserted, resynthesized bool, err error) {
	guid := synthesizedDeliveryGUID(repo, pr, headSHA)
	if store := h.webhookEventStore(); store != nil {
		prior, err := store.GetByDeliveryID(ctx, storage.WebhookProviderGitHub, guid)
		if err != nil {
			return false, false, fmt.Errorf("check for prior synthesized delivery %s for %s#%d@%s: %w", guid, repo, pr, headSHA, err)
		}
		resynthesized = prior != nil
	}
	var payload pullRequestPayload
	payload.Action = webhookReconcileSynthesizedAction
	payload.PullRequest.Number = pr
	payload.PullRequest.Head.SHA = headSHA
	payload.Repository.FullName = repo
	body, err := json.Marshal(payload)
	if err != nil {
		return false, false, fmt.Errorf("encode synthesized pull_request payload for %s#%d@%s: %w", repo, pr, headSHA, err)
	}
	inserted, err = h.enqueueDurableWebhookEvent(ctx, &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
		DeliveryID:  guid,
		Event:       "pull_request",
		Action:      webhookReconcileSynthesizedAction,
		Repository:  repo,
		PullRequest: pr,
		HeadSHA:     headSHA,
		TenantID:    strconv.FormatInt(installationID, 10),
		Payload:     body,
	})
	return inserted, resynthesized, err
}
