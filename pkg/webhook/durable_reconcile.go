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
//
// The missing-delivery scan pages each repository's updated-descending open
// PR listing under a per-pass page budget, in two phases. A fresh-window walk
// from the newest page examines every head updated since the last fresh walk
// that reached its floor, so new activity is always covered promptly. A
// resumed scan then continues from a persisted per-repository cursor toward
// the lookback cutoff, with a reserved share of the budget so sustained fresh
// traffic cannot starve it. When the budget runs out before the cutoff, the
// cursor records where to resume — the next page, and a watermark naming the
// oldest head examined so heads that shifted onto that page since are not
// examined twice — guaranteeing the full lookback window is examined across
// a bounded number of passes instead of restarting from the newest PRs every
// pass. Each pass claims a repository's cursor before walking, with a
// compare-and-set, so replicas ticking together take turns rather than
// overwriting each other's progress.
package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/github"
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

	// MinWebhookReconcileMaxPages is the smallest page budget a pass can be
	// given. The budget is shared between the fresh-window walk and the
	// resumed scan's reserve, and each needs a page of its own: with one page
	// the reserve takes it and the newest page, where new activity lands,
	// would go unexamined until the cycle completes.
	MinWebhookReconcileMaxPages = 2

	// defaultWebhookReconcileScanClaim is how long one pass's claim on a
	// repository's scan excludes other replicas. It outlasts any single pass
	// yet expires before the claimant's own next tick, so a replica that dies
	// mid-pass costs the repository at most one interval of coverage.
	defaultWebhookReconcileScanClaim = defaultWebhookReconcileInterval / 2

	// webhookReconcileScanCursorKeyPrefix namespaces the per-repository
	// settings row that persists the missing-delivery scan's resume point.
	webhookReconcileScanCursorKeyPrefix = "webhook_reconcile_scan_cursor:"
)

// webhookReconcileScanCursor is the persisted resume point for one
// repository's missing-delivery scan. When a pass exhausts its page budget
// before reaching the lookback cutoff, the cursor records where the following
// pass continues the walk instead of restarting from the newest PRs — a
// restart would let sustained PR traffic starve older heads forever.
//
// Page numbers shift as the listing reorders: an updated PR moves to the
// front and pushes everything behind it one slot deeper, a closed PR pulls
// later entries shallower. Page is therefore only where the resumed walk
// starts fetching; Watermark decides what on those pages still needs
// examining. A head updated more recently than the watermark sits above the
// resume point because it moved there since the walk passed, and the fresh
// walk owns it; only heads at or below the watermark count as the deep walk's
// progress. Entries pulled shallower by a closed PR can slip above the resume
// point unexamined, which the next cycle's restart from the newest page
// recovers. Page fetches, not examinations, are the budget, so the resumed
// scan advances each pass by its page share less the pages of new activity
// that entered the listing since: it converges while the reserved share
// outpaces front insertion, and the truncation warning's examined count shows
// when it does not. The invariant is eventual coverage of the full lookback
// window across a bounded number of passes, not exact per-pass coverage.
type webhookReconcileScanCursor struct {
	// Page is the next 1-based listing page the resumed scan fetches.
	Page int `json:"page"`
	// Watermark is the update time of the oldest head the current cycle's
	// resumed scan has examined; zero until it examines its first head.
	Watermark time.Time `json:"watermark,omitzero"`
	// FreshCoveredAt is the instant from which every later head update is
	// known to have been examined by a fresh-window walk. The next fresh walk
	// descends to it (widened by the grace window) and advances it only when
	// it gets there, so a fresh walk cut short by the page budget widens the
	// next one instead of leaving a gap.
	FreshCoveredAt time.Time `json:"fresh_covered_at,omitzero"`
	// CycleStartedAt is when the current scan cycle began at the newest page.
	CycleStartedAt time.Time `json:"cycle_started_at"`
	// CyclePasses counts the reconcile passes the current cycle has spent
	// without reaching the lookback cutoff.
	CyclePasses int `json:"cycle_passes"`
	// ClaimedUntil serializes the repository's scan across replicas: a pass
	// that finds a live claim skips the repository, and a pass that takes the
	// claim does so with a compare-and-set on the stored cursor, so two
	// replicas ticking together cannot both walk the listing and overwrite
	// each other's progress.
	ClaimedUntil time.Time `json:"claimed_until,omitzero"`
}

func webhookReconcileScanCursorKey(repo string) string {
	return webhookReconcileScanCursorKeyPrefix + repo
}

func (h *Handler) settingsStore() storage.SettingsStore {
	if h.service == nil || h.service.Storage() == nil {
		return nil
	}
	return h.service.Storage().Settings()
}

// loadedScanCursor is a repository's scan cursor together with what a pass
// needs to write it back: the stored setting it was decoded from, and whether
// writing back is possible at all.
type loadedScanCursor struct {
	cursor webhookReconcileScanCursor
	// stored is the settings row the cursor was decoded from, nil when the
	// repository has none yet. Every write compares against it.
	stored *storage.Setting
	// persistable is false when settings storage is unavailable or the read
	// failed. The pass then scans from a fresh cursor and writes nothing back,
	// so a stored cursor survives a transient read failure intact instead of
	// being overwritten by a pass that never saw it.
	persistable bool
}

// freshWebhookReconcileScanCursor starts a cycle at the newest page. Its fresh
// window reaches back one interval, the span the previous pass would have
// covered had it run.
func (h *Handler) freshWebhookReconcileScanCursor(now time.Time) webhookReconcileScanCursor {
	return webhookReconcileScanCursor{
		Page:           1,
		CycleStartedAt: now,
		FreshCoveredAt: now.Add(-h.webhookReconcileInterval),
	}
}

// loadWebhookReconcileScanCursor returns the repository's persisted scan
// cursor, or a fresh newest-page cursor when none is stored, it cannot be
// decoded, or storage is unavailable — the scan then degrades to
// restart-from-newest rather than skipping the pass.
func (h *Handler) loadWebhookReconcileScanCursor(ctx context.Context, repo string, now time.Time) loadedScanCursor {
	fresh := h.freshWebhookReconcileScanCursor(now)
	store := h.settingsStore()
	if store == nil {
		h.logger.Debug("webhook reconciler scan cursor unavailable because settings storage is unavailable; scanning from the newest page without persisting progress", "repo", repo)
		return loadedScanCursor{cursor: fresh}
	}
	setting, err := store.Get(ctx, webhookReconcileScanCursorKey(repo))
	if err != nil {
		h.logger.Warn("webhook reconciler failed to load scan cursor; scanning from the newest page without persisting progress so the stored cursor survives", "repo", repo, "error", err)
		return loadedScanCursor{cursor: fresh}
	}
	if setting == nil {
		return loadedScanCursor{cursor: fresh, persistable: true}
	}
	var cursor webhookReconcileScanCursor
	if err := json.Unmarshal([]byte(setting.Value), &cursor); err != nil {
		h.logger.Warn("webhook reconciler could not decode scan cursor; scanning from the newest page and replacing it", "repo", repo, "error", err)
		return loadedScanCursor{cursor: fresh, stored: setting, persistable: true}
	}
	if cursor.Page < 1 {
		cursor.Page = 1
	}
	if cursor.CycleStartedAt.IsZero() {
		cursor.CycleStartedAt = fresh.CycleStartedAt
	}
	if cursor.FreshCoveredAt.IsZero() {
		cursor.FreshCoveredAt = fresh.FreshCoveredAt
	}
	return loadedScanCursor{cursor: cursor, stored: setting, persistable: true}
}

// claimWebhookReconcileScan takes this pass's claim on the repository's scan,
// reporting whether the pass may proceed. A live claim held by another pass
// skips the repository until it expires. The claim is written with a
// compare-and-set against the loaded cursor, so of two replicas that load the
// same cursor only one proceeds. When the cursor cannot be persisted at all
// the pass proceeds unserialized, as it would without a settings store.
func (h *Handler) claimWebhookReconcileScan(ctx context.Context, repo string, loaded *loadedScanCursor, now time.Time) bool {
	if !loaded.persistable {
		return true
	}
	if loaded.cursor.ClaimedUntil.After(now) {
		h.logger.Debug("webhook reconciler skipped repository because another pass holds its scan claim",
			"repo", repo, "claimed_until", loaded.cursor.ClaimedUntil)
		return false
	}
	claimed := loaded.cursor
	claimed.ClaimedUntil = now.Add(h.webhookReconcileScanClaim)
	encoded, err := json.Marshal(claimed)
	if err != nil {
		h.logger.Warn("webhook reconciler failed to encode scan cursor claim; scanning without persisting progress", "repo", repo, "error", err)
		loaded.persistable = false
		return true
	}
	swapped, err := h.settingsStore().CompareAndSet(ctx, webhookReconcileScanCursorKey(repo), loaded.stored, string(encoded))
	if err != nil {
		h.logger.Warn("webhook reconciler failed to claim scan cursor; scanning without persisting progress", "repo", repo, "error", err)
		loaded.persistable = false
		return true
	}
	if !swapped {
		h.logger.Debug("webhook reconciler skipped repository because another pass claimed its scan first", "repo", repo)
		return false
	}
	loaded.cursor = claimed
	loaded.stored = &storage.Setting{Key: webhookReconcileScanCursorKey(repo), Value: string(encoded)}
	return true
}

// saveWebhookReconcileScanCursor persists the repository's scan cursor with a
// compare-and-set against the value this pass claimed, so a pass whose claim
// was superseded cannot overwrite the newer progress. Persistence failures are
// logged and tolerated: the next pass resumes from the previously stored
// cursor and re-examines already-covered heads, which is idempotent.
func (h *Handler) saveWebhookReconcileScanCursor(ctx context.Context, repo string, loaded loadedScanCursor, cursor webhookReconcileScanCursor) {
	if !loaded.persistable {
		h.logger.Debug("webhook reconciler scan cursor not persisted because this pass could not load or claim it", "repo", repo)
		return
	}
	encoded, err := json.Marshal(cursor)
	if err != nil {
		h.logger.Warn("webhook reconciler failed to encode scan cursor", "repo", repo, "error", err)
		return
	}
	swapped, err := h.settingsStore().CompareAndSet(ctx, webhookReconcileScanCursorKey(repo), loaded.stored, string(encoded))
	if err != nil {
		h.logger.Warn("webhook reconciler failed to persist scan cursor; the next pass resumes from the previously stored cursor", "repo", repo, "error", err)
		return
	}
	if !swapped {
		h.logger.Warn("webhook reconciler discarded this pass's scan progress because another pass advanced the repository's cursor concurrently", "repo", repo)
	}
}

// deleteOrphanedWebhookReconcileScanCursors removes the scan cursors of
// repositories no longer in the registry, so a deregistered repository does
// not leave a settings row behind forever.
func (h *Handler) deleteOrphanedWebhookReconcileScanCursors(ctx context.Context, registered map[string]api.RepoConfig) {
	store := h.settingsStore()
	if store == nil {
		h.logger.Debug("webhook reconciler orphaned scan cursor cleanup skipped because settings storage is unavailable")
		return
	}
	settings, err := store.List(ctx)
	if err != nil {
		h.logger.Warn("webhook reconciler failed to list settings for orphaned scan cursor cleanup", "error", err)
		return
	}
	for _, setting := range settings {
		repo, isCursor := strings.CutPrefix(setting.Key, webhookReconcileScanCursorKeyPrefix)
		if !isCursor {
			continue
		}
		if _, ok := registered[repo]; ok {
			continue
		}
		if err := store.Delete(ctx, setting.Key); err != nil {
			if errors.Is(err, storage.ErrSettingNotFound) {
				h.logger.Debug("webhook reconciler orphaned scan cursor was already deleted", "repo", repo)
				continue
			}
			h.logger.Warn("webhook reconciler failed to delete orphaned scan cursor", "repo", repo, "error", err)
			continue
		}
		h.logger.Info("webhook reconciler deleted the scan cursor of a repository that is no longer registered", "repo", repo)
	}
}

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
	h.deleteOrphanedWebhookReconcileScanCursors(ctx, cfg.Repos)

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

	loaded := h.loadWebhookReconcileScanCursor(ctx, repo, now)
	if !h.claimWebhookReconcileScan(ctx, repo, &loaded, now) {
		return 0, 0, 0
	}
	cursor := loaded.cursor

	// freshFloor bounds the fresh-window walk: every head updated since the
	// fresh coverage point — widened by the grace window so a head skipped as
	// too-fresh last pass cannot age past the boundary between passes — is
	// examined every pass, wherever the resumed scan's cursor is.
	freshFloor := cursor.FreshCoveredAt.Add(-h.webhookReconcileGrace)
	if freshFloor.Before(cutoff) {
		freshFloor = cutoff
	}

	budget := h.webhookReconcileMaxPages
	// The resumed scan keeps a reserved share of the page budget so sustained
	// fresh PR traffic cannot starve it: without the reserve, a repository
	// with more fresh updates per pass than the budget covers would never
	// advance the cursor, and heads deeper in the listing would never be
	// examined.
	scanReserve := max(1, h.webhookReconcileMaxPages/2)

	freshWalk := h.walkOpenPRPages(ctx, store, client, repo, installationID, webhookReconcileWalkBounds{
		startPage: 1,
		floor:     freshFloor,
		cutoff:    cutoff,
		grace:     grace,
		budget:    &budget,
		reserve:   scanReserve,
	})
	scanned += freshWalk.scanned
	missing += freshWalk.missing
	synthesized += freshWalk.synthesized
	if freshWalk.listFailed {
		return scanned, missing, synthesized
	}
	if freshWalk.reachedFloor() {
		// Every head updated between the previous coverage point and the
		// start of this pass has now been examined; anything updated during
		// the pass falls inside the next pass's window.
		cursor.FreshCoveredAt = now
	} else {
		h.logger.Debug("webhook reconciler fresh-window walk stopped at the page reserve before reaching its floor; the next pass widens its fresh window to cover the remainder",
			"repo", repo, "last_page", freshWalk.lastPage, "fresh_floor", freshFloor)
	}

	cycleComplete := freshWalk.crossedCutoff || freshWalk.listingEnded
	if !cycleComplete {
		// Resume the deep scan past both the pages the fresh walk just
		// examined and the pages earlier passes of this cycle covered.
		resume := max(cursor.Page, freshWalk.lastPage+1)
		scanWalk := h.walkOpenPRPages(ctx, store, client, repo, installationID, webhookReconcileWalkBounds{
			startPage: resume,
			floor:     cutoff,
			cutoff:    cutoff,
			grace:     grace,
			budget:    &budget,
			watermark: cursor.Watermark,
		})
		scanned += scanWalk.scanned
		missing += scanWalk.missing
		synthesized += scanWalk.synthesized
		if scanWalk.listFailed {
			return scanned, missing, synthesized
		}
		cycleComplete = scanWalk.crossedCutoff || scanWalk.listingEnded
		if !cycleComplete {
			cursor.Page = scanWalk.nextPage
			if !scanWalk.watermark.IsZero() {
				cursor.Watermark = scanWalk.watermark
			}
			cursor.CyclePasses++
			h.saveWebhookReconcileScanCursor(ctx, repo, loaded, cursor)
			metrics.RecordWebhookReconcileScanTruncated(ctx, repo)
			h.logger.Warn("webhook reconciler exhausted its page budget before reaching the lookback cutoff; the resumed scan continues from the cursor next pass",
				"repo", repo, "max_pages", h.webhookReconcileMaxPages, "page_size", webhookReconcilePageSize,
				"resume_page", cursor.Page, "watermark", cursor.Watermark, "examined", scanWalk.examined,
				"cycle_passes", cursor.CyclePasses)
			return scanned, missing, synthesized
		}
	}

	passes := cursor.CyclePasses + 1
	metrics.RecordWebhookReconcileScanCycleCompleted(ctx, repo, int64(passes))
	if passes > 1 {
		h.logger.Info("webhook reconciler missing-delivery scan cycle reached the lookback cutoff",
			"repo", repo, "cycle_passes", passes, "cycle_age", now.Sub(cursor.CycleStartedAt))
	}
	// A completed cycle has examined every head that was in the window when
	// it started, so the fresh coverage point need never sit before then.
	freshCoveredAt := cursor.FreshCoveredAt
	if cycleCovered := cursor.CycleStartedAt.Add(-h.webhookReconcileGrace); cycleCovered.After(freshCoveredAt) {
		freshCoveredAt = cycleCovered
	}
	h.saveWebhookReconcileScanCursor(ctx, repo, loaded, webhookReconcileScanCursor{
		Page:           1,
		CycleStartedAt: now,
		FreshCoveredAt: freshCoveredAt,
		ClaimedUntil:   cursor.ClaimedUntil,
	})
	return scanned, missing, synthesized
}

// webhookReconcileWalkBounds bounds one walk over the updated-descending open
// PR listing. The walk starts at startPage and stops paging once a listed PR
// is older than floor, the listing ends, or fetching another page would drop
// the shared budget below reserve. Every fetched page is examined in full
// down to cutoff, so a later walk may safely resume at lastPage+1. A non-zero
// watermark confines examination to heads updated at or before it: heads
// newer than the watermark on a resumed page moved there since the walk
// passed, and belong to the fresh walk.
type webhookReconcileWalkBounds struct {
	startPage int
	floor     time.Time
	cutoff    time.Time
	grace     time.Time
	budget    *int
	reserve   int
	watermark time.Time
}

// webhookReconcileWalkResult reports how one listing walk ended alongside its
// examination counts.
type webhookReconcileWalkResult struct {
	scanned, missing, synthesized int
	// examined counts the listed heads the walk examined, excluding heads
	// passed over as newer than the watermark.
	examined int
	// lastPage is the last page fetched (0 when the budget allowed none).
	lastPage int
	// nextPage is GitHub's next page after lastPage (0 when the listing ended).
	nextPage int
	// watermark is the update time of the oldest head examined (zero when the
	// walk examined none).
	watermark time.Time
	// crossedFloor means a listed PR was older than the walk's floor, so the
	// walk fetched everything it was bounded to.
	crossedFloor bool
	// crossedCutoff means a listed PR was older than the lookback cutoff:
	// everything deeper is out of the coverage window.
	crossedCutoff bool
	// listingEnded means the walk consumed the final page of the listing.
	listingEnded bool
	// listFailed means a list call failed and the walk stopped early.
	listFailed bool
}

// reachedFloor reports whether the walk covered every head down to its floor
// rather than being cut short by the page budget.
func (r webhookReconcileWalkResult) reachedFloor() bool {
	return r.crossedFloor || r.crossedCutoff || r.listingEnded
}

// walkOpenPRPages pages through the repository's open PR listing within
// bounds, examining every listed head for inbox coverage.
func (h *Handler) walkOpenPRPages(ctx context.Context, store storage.WebhookEventStore, client *github.InstallationClient, repo string, installationID int64, bounds webhookReconcileWalkBounds) webhookReconcileWalkResult {
	var result webhookReconcileWalkResult
	page := bounds.startPage
	for {
		if *bounds.budget <= bounds.reserve {
			h.logger.Debug("webhook reconciler listing walk stopped before fetching the next page because the remaining page budget is reserved",
				"repo", repo, "next_page", page, "remaining_budget", *bounds.budget, "reserve", bounds.reserve)
			return result
		}
		prs, nextPage, _, err := client.ListOpenPullRequestsPage(ctx, repo, page, webhookReconcilePageSize)
		if err != nil {
			h.logger.Warn("webhook reconciler failed to list open pull requests", "repo", repo, "page", page, "error", err)
			result.listFailed = true
			return result
		}
		*bounds.budget--
		result.lastPage = page
		result.nextPage = nextPage
		examinedOnPage := 0
		for _, pr := range prs {
			if pr.UpdatedAt.Before(bounds.cutoff) {
				// The listing is newest-updated first; everything after this is
				// older than the lookback window.
				result.crossedCutoff = true
				break
			}
			if pr.UpdatedAt.Before(bounds.floor) {
				// Past the walk's floor; the page is already fetched, so keep
				// examining it in full — that lets a later walk resume at the
				// next page without a coverage gap — but stop paging after it.
				result.crossedFloor = true
			}
			if !bounds.watermark.IsZero() && pr.UpdatedAt.After(bounds.watermark) {
				// Newer than anything this walk has examined so far: the head
				// moved above the resume point since the walk passed, and the
				// fresh walk covers it.
				continue
			}
			s, m, syn := h.examineOpenPRHead(ctx, store, repo, pr, bounds.grace, installationID)
			result.scanned += s
			result.missing += m
			result.synthesized += syn
			result.examined++
			examinedOnPage++
			if result.watermark.IsZero() || pr.UpdatedAt.Before(result.watermark) {
				result.watermark = pr.UpdatedAt
			}
		}
		if !bounds.watermark.IsZero() && examinedOnPage == 0 && len(prs) > 0 && !result.crossedCutoff {
			h.logger.Debug("webhook reconciler passed over a resumed listing page because every head on it was updated after the scan watermark",
				"repo", repo, "page", page, "watermark", bounds.watermark)
		}
		if result.nextPage == 0 {
			result.listingEnded = true
			return result
		}
		if result.crossedCutoff || result.crossedFloor {
			return result
		}
		page = result.nextPage
	}
}

// examineOpenPRHead checks one listed open PR head for inbox coverage,
// reporting a miss and — when synthesis is enabled — enqueueing a recovery
// delivery for it. The returned counts are 0-or-1 increments for the pass
// totals.
func (h *Handler) examineOpenPRHead(ctx context.Context, store storage.WebhookEventStore, repo string, pr github.OpenPullRequest, grace time.Time, installationID int64) (scanned, missing, synthesized int) {
	if pr.HeadSHA == "" {
		// A PR listing without a head SHA can't be matched to an inbox
		// delivery; skip rather than emit a spurious missing-row report.
		h.logger.Debug("webhook reconciler skipped open PR with no head SHA",
			"repo", repo, "pr", pr.Number)
		return 0, 0, 0
	}
	if pr.UpdatedAt.After(grace) {
		// Updated within the grace window; its webhook delivery may still
		// be in flight to the inbox, so a missing row here is expected.
		h.logger.Debug("webhook reconciler skipped recently updated open PR within grace window",
			"repo", repo, "pr", pr.Number, "updated_at", pr.UpdatedAt)
		return 0, 0, 0
	}
	found, err := store.HasEventForHead(ctx, storage.WebhookProviderGitHub, repo, pr.Number, pr.HeadSHA)
	if err != nil {
		h.logger.Warn("webhook reconciler failed to query inbox for PR head",
			"repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA, "error", err)
		return 1, 0, 0
	}
	if found {
		return 1, 0, 0
	}
	metrics.RecordWebhookReconcileMissingEvent(ctx, repo)
	if !h.webhookReconcileSynthesis {
		// Report-only mode re-reports the same missing head on every
		// pass until synthesis is enabled or an organic delivery
		// arrives, so the per-head line is info; the metric and the
		// per-pass summary carry the operator signal.
		h.logger.Info("webhook reconciler found open PR head with no inbox delivery (report-only; synthesis disabled)",
			"repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA, "updated_at", pr.UpdatedAt)
		return 1, 1, 0
	}
	h.logger.Warn("webhook reconciler found open PR head with no inbox delivery; synthesizing recovery delivery",
		"repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA, "updated_at", pr.UpdatedAt)
	inserted, resynthesized, err := h.synthesizeMissingHeadDelivery(ctx, repo, pr.Number, pr.HeadSHA, installationID)
	if err != nil {
		// Each head recovers independently; the next pass retries this one.
		h.logger.Warn("webhook reconciler failed to synthesize recovery delivery for open PR head",
			"repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA, "error", err)
		return 1, 1, 0
	}
	if !inserted {
		// Another pod's reconciler won the enqueue race on the same
		// synthesized GUID and its row is still live; the head is
		// covered. (An organic delivery can never collide here — its
		// GUID is GitHub's, not the synthesized form — it is caught by
		// the HasEventForHead check upstream instead.)
		h.logger.Debug("webhook reconciler skipped synthesizing recovery delivery because one is already queued",
			"repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA)
		return 1, 1, 0
	}
	metrics.RecordWebhookReconcileSynthesizedEvent(ctx, repo, resynthesized)
	return 1, 1, 1
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
	repo = storage.CanonicalKey(repo)
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
