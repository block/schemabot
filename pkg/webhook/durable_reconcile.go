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
//   - Reports (report-only) any recently updated open PR head in a registered
//     repository that has no corresponding webhook_events row, surfacing
//     deliveries lost upstream of the inbox (edge auth failures, GitHub-side
//     send failures). A later phase will synthesize pull_request-equivalent
//     inbox rows (delivery_id "recon:<repo>#<pr>@<sha>", naturally deduped) once
//     report-only output has been tuned.
package webhook

import (
	"context"
	"sort"
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
// cap, followed by a report-only missing-delivery scan over registered
// repositories. Only the missing-delivery scan is report-only; the sweep
// mutates rows.
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
	var scanned, missing int
	for _, repo := range repos {
		if ctx.Err() != nil {
			h.logger.Debug("webhook reconcile missing-delivery scan stopped early because the context was cancelled",
				"repos_scanned", scanned, "error", ctx.Err())
			return
		}
		repoScanned, repoMissing := h.reconcileRepoWebhookInbox(ctx, store, repo)
		scanned += repoScanned
		missing += repoMissing
	}
	h.logger.Info("webhook reconcile missing-delivery scan finished (report-only)",
		"repos", len(repos), "prs_scanned", scanned, "missing_inbox_rows", missing,
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
// and reports heads with no inbox delivery. Returns how many PRs were checked
// and how many were missing rows.
func (h *Handler) reconcileRepoWebhookInbox(ctx context.Context, store storage.WebhookEventStore, repo string) (scanned, missing int) {
	installationID, err := h.resolveRepoWebhookInstallation(ctx, repo)
	if err != nil {
		h.logger.Warn("webhook reconciler could not resolve installation for repository", "repo", repo, "error", err)
		return 0, 0
	}
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Warn("webhook reconciler could not build client for repository", "repo", repo, "error", err)
		return 0, 0
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
			return scanned, missing
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
			h.logger.Warn("webhook reconciler found open PR head with no inbox delivery (report-only)",
				"repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA, "updated_at", pr.UpdatedAt)
			metrics.RecordWebhookReconcileMissingEvent(ctx, repo)
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
	return scanned, missing
}
