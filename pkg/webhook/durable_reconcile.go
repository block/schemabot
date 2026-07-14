// durable_reconcile.go implements the report-only webhook reconciliation loop:
// the correctness backstop for deliveries lost upstream of the durable inbox
// (edge auth failures, GitHub-side send failures). It periodically lists
// recently updated open PRs in registered repositories and reports any PR head
// that has no corresponding webhook_events row. This phase only logs and emits
// metrics; a later phase will synthesize pull_request-equivalent inbox rows
// (delivery_id "recon:<repo>#<pr>@<sha>", naturally deduped) once report-only
// output has been tuned.
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
		h.logger.Info("webhook reconciler started (report-only)",
			"interval", h.webhookReconcileInterval,
			"lookback", h.webhookReconcileLookback,
			"grace", h.webhookReconcileGrace)
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

// reconcileWebhookInbox runs one report-only pass over all registered
// repositories.
func (h *Handler) reconcileWebhookInbox(ctx context.Context) {
	store := h.webhookEventStore()
	if store == nil {
		h.logger.Warn("webhook reconciler skipped because webhook event storage is unavailable")
		return
	}
	cfg := h.service.Config()
	if cfg == nil || len(cfg.Repos) == 0 {
		// An empty repo registry means "allow all", which is not an enumerable
		// set; reconciliation needs an explicit registry to know what to scan.
		h.logger.Debug("webhook reconciler skipped because the repo registry is empty (allow-all)")
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
			return
		}
		repoScanned, repoMissing := h.reconcileRepoWebhookInbox(ctx, store, repo)
		scanned += repoScanned
		missing += repoMissing
	}
	h.logger.Info("webhook reconcile pass finished (report-only)",
		"repos", len(repos), "prs_scanned", scanned, "missing_inbox_rows", missing,
		"duration", time.Since(start))
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
				break pages
			}
			if pr.HeadSHA == "" || pr.UpdatedAt.After(grace) {
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
			break
		}
		page = nextPage
	}
	return scanned, missing
}
