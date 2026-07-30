// check_refresh.go drives the check refresh guardrail: when an apply reaches
// terminal success on a (environment, database type, database) target, every
// other open PR with stored plan check state against that target planned
// against a schema that no longer exists. The operator drive tail (and a
// backstop sweep here) records a durable check_refresh_requests row, and this
// processor consumes it: it finds the sibling PRs' stored check state through
// the checks reverse index, re-plans each PR against the live schema, and —
// when a re-plan fails — fails the stored check closed so a stale plan can
// never keep passing. CLI/gRPC applies carry no PR surface at all, so this
// processor is the only path that keeps their targets' sibling PR checks
// honest.
package webhook

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

const (
	defaultCheckRefreshPollInterval  = 30 * time.Second
	defaultCheckRefreshLeaseDuration = 2 * time.Minute

	// defaultCheckRefreshSweepLookback bounds the backstop sweep over completed
	// applies missing a refresh request. The sweep exists for the crash window
	// between an apply's terminal write and the drive tail's recording, plus
	// process downtime; requests are unique per apply, so re-sweeping the same
	// window is a no-op.
	defaultCheckRefreshSweepLookback = 6 * time.Hour

	// checkRefreshRetryDelay is the pause before a failed fan-out is reclaimed.
	checkRefreshRetryDelay = time.Minute

	// checkRefreshPRConcurrency bounds how many sibling PRs one fan-out
	// re-plans at once, so a hot target with many open PRs cannot saturate the
	// GitHub API or the plan engine.
	checkRefreshPRConcurrency = 3

	// maxCheckRefreshAttempts aliases the store's claim ceiling so the drive
	// records a retryable failure as terminal on the same attempt at which
	// ClaimNext would stop handing the request out.
	maxCheckRefreshAttempts = storage.MaxCheckRefreshAttempts
)

// Per-PR fan-out outcomes recorded via metrics.RecordCheckRefreshPROutcome.
const (
	checkRefreshOutcomeRefreshed         = "refreshed"
	checkRefreshOutcomeBlockedReplan     = "blocked_replan_failed"
	checkRefreshOutcomeSkippedInFlight   = "skipped_in_flight"
	checkRefreshOutcomeSkippedPRClosed   = "skipped_pr_closed"
	checkRefreshOutcomeSkippedNotManaged = "skipped_not_managed"
	checkRefreshOutcomeSkippedSuperseded = "skipped_superseded"
)

// StartCheckRefreshProcessor starts the background driver that consumes
// durable check refresh requests. Idempotent; StopCheckRefreshProcessor stops
// it and waits for the in-flight pass to finish.
func (h *Handler) StartCheckRefreshProcessor(ctx context.Context) {
	if h.checkRefreshStore() == nil {
		h.logger.Warn("check refresh processor not started: storage is unavailable; sibling PR checks will go stale after applies until it recovers")
		return
	}

	h.checkRefreshMu.Lock()
	if h.checkRefreshStop != nil {
		h.checkRefreshMu.Unlock()
		h.logger.Info("check refresh processor already running")
		return
	}
	stop := make(chan struct{})
	driverCtx, cancel := context.WithCancel(ctx)
	h.checkRefreshStop = stop
	h.checkRefreshCancel = cancel
	// Register on the WaitGroup while the mutex is held so Start cannot race a
	// concurrent Stop's Wait.
	h.checkRefreshWg.Go(func() {
		h.checkRefreshDriver(driverCtx, stop)
	})
	h.checkRefreshMu.Unlock()

	h.logger.Info("check refresh processor started",
		"interval", h.checkRefreshPollInterval,
		"lease_duration", h.checkRefreshLeaseDuration,
		"sweep_lookback", h.checkRefreshSweepLookback)
}

// StopCheckRefreshProcessor stops the check refresh driver and waits for the
// in-flight pass to finish its current drive.
func (h *Handler) StopCheckRefreshProcessor() {
	h.checkRefreshMu.Lock()
	if h.checkRefreshStop == nil {
		h.checkRefreshMu.Unlock()
		h.logger.Debug("check refresh processor stop requested but it is not running")
		return
	}
	stop := h.checkRefreshStop
	cancel := h.checkRefreshCancel
	h.checkRefreshStop = nil
	h.checkRefreshCancel = nil
	h.checkRefreshMu.Unlock()

	close(stop)
	if cancel != nil {
		cancel()
	}
	h.checkRefreshWg.Wait()
	h.logger.Info("check refresh processor stopped")
}

// KickCheckRefresh wakes the check refresh driver to run a pass now instead
// of waiting for the next poll tick. Non-blocking: when a kick is already
// pending, the coming pass drains the new request too. The durable request
// row is the source of truth — a kick that lands with no driver running, or
// on a different pod than the one that will claim the request, only costs
// poll latency, never the refresh.
func (h *Handler) KickCheckRefresh() {
	select {
	case h.checkRefreshKick <- struct{}{}:
	default:
	}
}

func (h *Handler) checkRefreshDriver(ctx context.Context, stop <-chan struct{}) {
	owner := checkRefreshLeaseOwner()
	ticker := time.NewTicker(h.checkRefreshPollInterval)
	defer ticker.Stop()

	h.logger.Debug("check refresh driver started", "lease_owner", owner)
	h.runCheckRefreshPass(ctx, owner)

	for {
		select {
		case <-stop:
			h.logger.Debug("check refresh driver stopping")
			return
		case <-ctx.Done():
			h.logger.Debug("check refresh driver context cancelled")
			return
		case <-h.checkRefreshKick:
			h.logger.Debug("check refresh driver woken by recorded-request kick")
			h.runCheckRefreshPass(ctx, owner)
		case <-ticker.C:
			h.runCheckRefreshPass(ctx, owner)
		}
	}
}

// runCheckRefreshPass runs one full processor pass: backfill requests the
// drive tails missed, terminalize requests wedged past the attempt cap, then
// claim and drive requests until none remain claimable.
func (h *Handler) runCheckRefreshPass(ctx context.Context, owner string) {
	h.sweepCheckRefreshRequests(ctx)
	h.terminateStuckCheckRefreshRequests(ctx)
	h.drainCheckRefreshRequests(ctx, owner)
}

// sweepCheckRefreshRequests backfills refresh requests for completed applies
// that have none — the applies table is the outbox, so a pod crash between an
// apply's terminal write and its drive-tail recording cannot lose the fan-out.
func (h *Handler) sweepCheckRefreshRequests(ctx context.Context) {
	store := h.checkRefreshStore()
	applies, err := store.FindCompletedAppliesMissingRequest(ctx, h.checkRefreshSweepLookback)
	if err != nil {
		h.logger.Error("check refresh sweep failed to find completed applies missing a refresh request; drive-tail gaps stay unbackfilled until the next pass", "error", err)
		return
	}
	for _, apply := range applies {
		recorded, err := store.Record(ctx, &storage.CheckRefreshRequest{
			ApplyID:         apply.ID,
			ApplyIdentifier: apply.ApplyIdentifier,
			Environment:     apply.Environment,
			DatabaseType:    apply.DatabaseType,
			DatabaseName:    apply.Database,
			Repository:      apply.Repository,
			PullRequest:     apply.PullRequest,
			RequestedBy:     apply.Caller,
		})
		if err != nil {
			// Each apply's backfill is independent; a failed one is retried on
			// the next sweep pass.
			h.logger.Error("check refresh sweep failed to backfill a refresh request for a completed apply",
				append(apply.LogAttrs(), "error", err)...)
			metrics.RecordCheckRefreshRecordFailure(ctx, apply.Database, apply.Environment)
			continue
		}
		if !recorded {
			// A drive tail recorded it between the sweep query and this insert.
			h.logger.Debug("check refresh sweep found the refresh request already recorded",
				apply.LogAttrs()...)
			continue
		}
		h.logger.Info("check refresh sweep backfilled a refresh request the drive tail did not record",
			apply.LogAttrs()...)
		metrics.RecordCheckRefreshRecorded(ctx, apply.Database, apply.Environment, metrics.CheckRefreshSourceSweep)
	}
}

// terminateStuckCheckRefreshRequests terminalizes requests wedged in
// processing past the attempt cap with an expired lease, so a poison request
// cannot be reclaimed forever. Each terminated request means sibling PR
// stored checks for its target may remain stale until their PRs re-plan.
func (h *Handler) terminateStuckCheckRefreshRequests(ctx context.Context) {
	terminated, err := h.checkRefreshStore().TerminateStuckProcessing(ctx, "check refresh request exceeded its attempt cap with an expired lease")
	if err != nil {
		h.logger.Error("check refresh stuck-processing sweep failed; wedged requests stay unterminalized until the next pass", "error", err)
		return
	}
	if terminated == 0 {
		return
	}
	h.logger.Error("check refresh requests terminated after exceeding their attempt cap; sibling PR stored checks for their targets may remain stale until those PRs re-plan",
		"terminated", terminated)
	metrics.RecordCheckRefreshTerminatedStuck(ctx, terminated)
}

// drainCheckRefreshRequests claims and drives requests until none remain
// claimable, so a backlog is worked down within a single tick. It stops on the
// first empty claim or claim error — a storage error must not spin a tight
// loop — and on context cancellation.
func (h *Handler) drainCheckRefreshRequests(ctx context.Context, owner string) {
	for {
		if ctx.Err() != nil {
			return
		}
		if !h.driveNextCheckRefresh(ctx, owner) {
			return
		}
	}
}

// driveNextCheckRefresh claims and drives at most one request. It reports
// whether a request was claimed, so the drain loop knows whether to continue.
func (h *Handler) driveNextCheckRefresh(ctx context.Context, owner string) (claimed bool) {
	store := h.checkRefreshStore()
	req, err := store.ClaimNext(ctx, owner, h.checkRefreshLeaseDuration)
	if err != nil {
		h.logger.Error("check refresh driver failed to claim a request", "lease_owner", owner, "error", err)
		return false
	}
	if req == nil {
		h.logger.Debug("check refresh driver found no request to claim")
		return false
	}

	h.logger.Info("check refresh driver claimed a request",
		"lease_owner", owner,
		"apply_id", req.ApplyIdentifier,
		"environment", req.Environment,
		"database_type", req.DatabaseType,
		"database", req.DatabaseName,
		"origin_repo", req.Repository,
		"origin_pr", req.PullRequest,
		"requested_by", req.RequestedBy,
		"attempts", req.Attempts)

	h.driveClaimedCheckRefresh(ctx, store, req)
	return true
}

// driveClaimedCheckRefresh runs the fan-out → heartbeat → finish lifecycle for
// a freshly claimed request, coalescing pending sibling requests for the same
// target once the fan-out succeeds.
func (h *Handler) driveClaimedCheckRefresh(ctx context.Context, store storage.CheckRefreshRequestStore, req *storage.CheckRefreshRequest) {
	// Capture the pending siblings before the fan-out starts: the fan-out
	// re-plans against the live schema, so it covers every schema change
	// recorded before it began. A request recorded mid-fan-out is not covered
	// and stays pending for the next drain.
	siblings, err := store.PendingForTarget(ctx, req.Environment, req.DatabaseType, req.DatabaseName, req.ID)
	if err != nil {
		// Coalescing is an optimization: without the sibling list each pending
		// request runs its own fan-out, which re-plans the same PRs again —
		// wasteful but safe.
		h.logger.Warn("check refresh driver could not list pending sibling requests; siblings will fan out on their own",
			"apply_id", req.ApplyIdentifier, "environment", req.Environment,
			"database_type", req.DatabaseType, "database", req.DatabaseName, "error", err)
		siblings = nil
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	stopHeartbeat := h.startCheckRefreshHeartbeat(runCtx, req, cancelRun)
	fanErr := h.safeFanOutCheckRefresh(runCtx, req)
	heartbeatErr := stopHeartbeat()
	cancelRun()

	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if fanErr != nil {
		retryAfter := (*time.Time)(nil)
		if req.Attempts < maxCheckRefreshAttempts {
			due := time.Now().Add(checkRefreshRetryDelay)
			retryAfter = &due
		} else {
			h.logger.Error("check refresh request exhausted its retry budget and is now terminal; sibling PR stored checks for the target may remain stale until those PRs re-plan",
				"apply_id", req.ApplyIdentifier, "environment", req.Environment,
				"database_type", req.DatabaseType, "database", req.DatabaseName,
				"attempts", req.Attempts, "error", fanErr)
		}
		if err := store.MarkFailed(finishCtx, req.ID, req.LeaseToken, fanErr.Error(), retryAfter); err != nil {
			if errors.Is(err, storage.ErrCheckRefreshLeaseLost) || errors.Is(err, storage.ErrCheckRefreshNotFound) {
				h.logger.Warn("check refresh driver lost the request lease before recording failure; another driver owns the request",
					"apply_id", req.ApplyIdentifier, "environment", req.Environment,
					"database_type", req.DatabaseType, "database", req.DatabaseName)
				return
			}
			h.logger.Error("check refresh driver failed to record the fan-out failure",
				"apply_id", req.ApplyIdentifier, "environment", req.Environment,
				"database_type", req.DatabaseType, "database", req.DatabaseName, "error", err)
			return
		}
		outcome := "failed_terminal"
		if retryAfter != nil {
			outcome = "failed_retrying"
		}
		metrics.RecordCheckRefreshEventOutcome(finishCtx, req.DatabaseName, req.Environment, outcome)
		h.logger.Warn("check refresh driver recorded a fan-out failure",
			"apply_id", req.ApplyIdentifier, "environment", req.Environment,
			"database_type", req.DatabaseType, "database", req.DatabaseName,
			"retry", retryAfter != nil, "error", fanErr)
		return
	}
	if heartbeatErr != nil {
		// The fan-out reported success but the lease heartbeat failed, so
		// ownership is uncertain. Do not mark it completed — leave the row
		// processing so lease expiry hands it to another driver. Re-planning
		// the same PRs again is safe.
		h.logger.Warn("check refresh driver skipped completion because the request lease heartbeat failed; leaving the request for reclaim",
			"apply_id", req.ApplyIdentifier, "environment", req.Environment,
			"database_type", req.DatabaseType, "database", req.DatabaseName, "error", heartbeatErr)
		metrics.RecordCheckRefreshEventOutcome(finishCtx, req.DatabaseName, req.Environment, "lease_lost")
		return
	}
	if err := store.MarkCompleted(finishCtx, req.ID, req.LeaseToken); err != nil {
		if errors.Is(err, storage.ErrCheckRefreshLeaseLost) || errors.Is(err, storage.ErrCheckRefreshNotFound) {
			h.logger.Warn("check refresh driver lost the request lease before recording completion; another driver owns the request",
				"apply_id", req.ApplyIdentifier, "environment", req.Environment,
				"database_type", req.DatabaseType, "database", req.DatabaseName)
			return
		}
		h.logger.Error("check refresh driver failed to mark the request completed",
			"apply_id", req.ApplyIdentifier, "environment", req.Environment,
			"database_type", req.DatabaseType, "database", req.DatabaseName, "error", err)
		return
	}
	metrics.RecordCheckRefreshEventOutcome(finishCtx, req.DatabaseName, req.Environment, "completed")
	h.logger.Info("check refresh driver completed the request",
		"apply_id", req.ApplyIdentifier, "environment", req.Environment,
		"database_type", req.DatabaseType, "database", req.DatabaseName)

	for _, sibling := range siblings {
		coalesced, err := store.CompletePendingCoalesced(finishCtx, sibling.ID)
		if err != nil {
			// Each sibling completes independently; a failed coalesce leaves
			// the sibling pending, so its own fan-out (a redundant but safe
			// re-plan) finishes it.
			h.logger.Warn("check refresh driver failed to coalesce a covered sibling request; it will run its own fan-out",
				"apply_id", req.ApplyIdentifier, "sibling_apply_id", sibling.ApplyIdentifier,
				"environment", req.Environment, "database_type", req.DatabaseType,
				"database", req.DatabaseName, "error", err)
			continue
		}
		if !coalesced {
			h.logger.Debug("check refresh sibling request no longer pending; its own lifecycle finishes it",
				"apply_id", req.ApplyIdentifier, "sibling_apply_id", sibling.ApplyIdentifier,
				"environment", req.Environment, "database_type", req.DatabaseType,
				"database", req.DatabaseName)
			continue
		}
		h.logger.Info("check refresh driver coalesced a sibling request covered by this fan-out",
			"apply_id", req.ApplyIdentifier, "sibling_apply_id", sibling.ApplyIdentifier,
			"environment", req.Environment, "database_type", req.DatabaseType,
			"database", req.DatabaseName)
	}
}

// safeFanOutCheckRefresh runs the fan-out with panic recovery: the row stays
// processing until its lease expires and is then claimable again, so a driver
// panic would otherwise crash-loop every replica on the same poison request. A
// recovered panic is a retryable failure, so the attempt cap makes a
// deterministic panic terminal instead.
func (h *Handler) safeFanOutCheckRefresh(ctx context.Context, req *storage.CheckRefreshRequest) (err error) {
	defer func() {
		if r := recover(); r != nil {
			h.logger.Error("check refresh driver recovered from panic during fan-out",
				"apply_id", req.ApplyIdentifier, "environment", req.Environment,
				"database_type", req.DatabaseType, "database", req.DatabaseName,
				"panic", fmt.Sprintf("%v", r), "stack", string(debug.Stack()))
			err = fmt.Errorf("panic during check refresh fan-out for apply %s: %v", req.ApplyIdentifier, r)
		}
	}()
	return h.fanOutCheckRefresh(ctx, req)
}

// fanOutCheckRefresh re-plans every sibling PR whose stored check state
// targets the request's (environment, database type, database). A returned
// error means at least one PR was neither refreshed nor safely failed closed,
// so the request must be retried; re-planning already-refreshed PRs on that
// retry is safe.
func (h *Handler) fanOutCheckRefresh(ctx context.Context, req *storage.CheckRefreshRequest) error {
	// Aggregate rows never match: their database type and name are the
	// aggregate sentinel, not a real target.
	checks, err := h.service.Storage().Checks().GetByTarget(ctx, req.Environment, req.DatabaseType, req.DatabaseName)
	if err != nil {
		return fmt.Errorf("list stored check state for target %s/%s in %s: %w",
			req.DatabaseType, req.DatabaseName, req.Environment, err)
	}

	var targets []*storage.Check
	for _, check := range checks {
		if check.Repository == req.Repository && check.PullRequest == req.PullRequest {
			// The originating PR's own apply lifecycle already updated its
			// stored check state; re-planning it here would be redundant.
			h.logger.Debug("check refresh skipping the originating PR",
				"apply_id", req.ApplyIdentifier, "repo", check.Repository, "pr", check.PullRequest,
				"environment", req.Environment, "database_type", req.DatabaseType,
				"database", req.DatabaseName)
			continue
		}
		targets = append(targets, check)
	}
	if len(targets) == 0 {
		h.logger.Info("check refresh found no sibling PR check state for the target",
			"apply_id", req.ApplyIdentifier, "environment", req.Environment,
			"database_type", req.DatabaseType, "database", req.DatabaseName)
		return nil
	}

	h.logger.Info("check refresh fanning out to sibling PRs whose plans predate the schema change",
		"apply_id", req.ApplyIdentifier, "environment", req.Environment,
		"database_type", req.DatabaseType, "database", req.DatabaseName,
		"requested_by", req.RequestedBy, "sibling_prs", len(targets))

	sem := make(chan struct{}, checkRefreshPRConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for _, check := range targets {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := h.refreshPRPlanForTarget(ctx, req, check); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// refreshPRPlanForTarget re-plans one sibling PR's stored check state against
// the target's live schema. It returns nil when the PR was refreshed or safely
// skipped (closed, in-flight-owned, no longer managed, or superseded by a
// racing write), and nil when a re-plan failure was durably failed closed. A
// returned error means the PR was neither refreshed nor failed closed, so the
// request must be retried.
func (h *Handler) refreshPRPlanForTarget(ctx context.Context, req *storage.CheckRefreshRequest, check *storage.Check) error {
	repo, pr := check.Repository, check.PullRequest

	if check.Status == checkStatusInProgress {
		// A started apply remains authoritative for its stored check state; a
		// re-plan here would fight the in-flight apply's own lifecycle. The
		// apply's terminal update (or stale-check reconciliation) refreshes it.
		h.logger.Info("check refresh leaving in-flight apply-owned check state untouched",
			"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
			"environment", req.Environment, "database_type", req.DatabaseType,
			"database", req.DatabaseName, "check_apply_id", check.ApplyID,
			"check_head_sha", check.HeadSHA)
		metrics.RecordCheckRefreshPROutcome(ctx, repo, req.DatabaseName, req.Environment, checkRefreshOutcomeSkippedInFlight)
		return nil
	}

	// Driver work runs outside any HTTP request, so resolve the App
	// installation from config the same way repo-level webhook dispatch does.
	installationID, err := h.resolveRepoWebhookInstallation(ctx, repo)
	if err != nil {
		return fmt.Errorf("resolve installation for check refresh of %s#%d (target %s/%s in %s, apply %s): %w",
			repo, pr, req.DatabaseType, req.DatabaseName, req.Environment, req.ApplyIdentifier, err)
	}
	prCtx, cancel, client, err := h.commandBootstrap(repo, installationID)
	defer cancel()
	if err != nil {
		return fmt.Errorf("bootstrap check refresh of %s#%d (target %s/%s in %s, apply %s): %w",
			repo, pr, req.DatabaseType, req.DatabaseName, req.Environment, req.ApplyIdentifier, err)
	}

	// A GitHub failure here is uncertainty, not staleness: keep the request
	// retryable rather than guessing at the PR's state.
	prInfo, err := client.FetchPullRequestNoCache(prCtx, repo, pr)
	if err != nil {
		return fmt.Errorf("verify PR state for check refresh of %s#%d (target %s/%s in %s, apply %s): %w",
			repo, pr, req.DatabaseType, req.DatabaseName, req.Environment, req.ApplyIdentifier, err)
	}
	if prInfo.IsClosed() {
		h.logger.Info("check refresh skipping closed PR; its stored checks no longer gate a merge",
			"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
			"environment", req.Environment, "database_type", req.DatabaseType,
			"database", req.DatabaseName, "merged", prInfo.Merged)
		metrics.RecordCheckRefreshPROutcome(ctx, repo, req.DatabaseName, req.Environment, checkRefreshOutcomeSkippedPRClosed)
		return nil
	}

	schemaResult, err := h.createManagedSchemaRequestFromPR(prCtx, client, repo, pr, req.Environment, req.DatabaseName, action.Plan)
	if err != nil {
		if isCheckRefreshNotManagedError(err) {
			// A determinate answer, not uncertainty: the PR's current head no
			// longer manages this target here, so there is nothing to re-plan.
			h.logger.Info("check refresh skipping PR that no longer manages the target",
				"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
				"environment", req.Environment, "database_type", req.DatabaseType,
				"database", req.DatabaseName, "reason", err)
			metrics.RecordCheckRefreshPROutcome(ctx, repo, req.DatabaseName, req.Environment, checkRefreshOutcomeSkippedNotManaged)
			return nil
		}
		return h.blockCheckForFailedRefresh(prCtx, client, req, check,
			fmt.Errorf("discover schema config: %w", err))
	}
	if err := h.attachServerEnvironments(schemaResult, req.Environment); err != nil {
		if isCheckRefreshNotManagedError(err) {
			h.logger.Info("check refresh skipping PR whose target environment is no longer configured",
				"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
				"environment", req.Environment, "database_type", req.DatabaseType,
				"database", req.DatabaseName, "reason", err)
			metrics.RecordCheckRefreshPROutcome(ctx, repo, req.DatabaseName, req.Environment, checkRefreshOutcomeSkippedNotManaged)
			return nil
		}
		return h.blockCheckForFailedRefresh(prCtx, client, req, check,
			fmt.Errorf("validate schema environments: %w", err))
	}

	prNumber := int32(pr)
	planReq := api.PlanRequest{
		Database:      schemaResult.Database,
		Environment:   req.Environment,
		Type:          schemaResult.Type,
		SchemaFiles:   schemaResult.SchemaFiles,
		Repository:    repo,
		PullRequest:   &prNumber,
		HeadSHA:       &schemaResult.HeadSHA,
		SchemaPath:    schemaResult.SchemaPath,
		SourceTrusted: true,
	}
	planProto, planResp, err := h.executePlanProtoWithTransientRetry(prCtx, planReq, repo, pr)
	if err != nil {
		return h.blockCheckForFailedRefresh(prCtx, client, req, check,
			fmt.Errorf("re-plan against changed schema: %w", err))
	}

	// Roll up every deployment's diff against the refreshed plan so drift on a
	// non-primary deployment fails the check closed, exactly as at review time.
	drift := h.reviewTimeDrift(prCtx, planReq, planProto, planResp.Deployment, repo, pr)

	sha, _, err := h.upsertPlanCheckRecord(prCtx, client, repo, pr, schemaResult, planResp, req.Environment, drift, checkRefreshNote(req))
	if err != nil {
		if errors.Is(err, errPlanCheckHeadStale) {
			// A racing synchronize replaced the PR head mid-refresh; its own
			// auto-plan against the new head is authoritative.
			h.logger.Info("check refresh superseded by a newer PR head; the newer head's plan is authoritative",
				"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
				"environment", req.Environment, "database_type", req.DatabaseType,
				"database", req.DatabaseName, "planned_head_sha", schemaResult.HeadSHA)
			metrics.RecordCheckRefreshPROutcome(ctx, repo, req.DatabaseName, req.Environment, checkRefreshOutcomeSkippedSuperseded)
			return nil
		}
		return h.blockCheckForFailedRefresh(prCtx, client, req, check,
			fmt.Errorf("store refreshed plan check record: %w", err))
	}
	h.updateAggregateCheck(prCtx, client, repo, pr, sha)

	h.logger.Info("check refresh re-planned sibling PR against the changed schema",
		"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr, "head_sha", sha,
		"environment", req.Environment, "database_type", req.DatabaseType,
		"database", req.DatabaseName, "requested_by", req.RequestedBy,
		"has_changes", planResp.HasChanges())
	metrics.RecordCheckRefreshPROutcome(ctx, repo, req.DatabaseName, req.Environment, checkRefreshOutcomeRefreshed)
	return nil
}

// blockCheckForFailedRefresh fails a sibling PR's stored check state closed
// after its refresh re-plan failed: the plan on record was computed against a
// schema that no longer exists, so it must not keep passing. The raw cause is
// logged server-side with full identifiers; the stored check carries only the
// fixed sanitized block message. Returns nil once the block is durable (or a
// racing write superseded it) and an error when the flip itself failed, so the
// request is retried.
func (h *Handler) blockCheckForFailedRefresh(ctx context.Context, client *ghclient.InstallationClient, req *storage.CheckRefreshRequest, check *storage.Check, cause error) error {
	h.logger.Error("check refresh re-plan failed; failing the stored check closed",
		"repo", check.Repository, "pr", check.PullRequest, "check_head_sha", check.HeadSHA,
		"environment", req.Environment, "database_type", req.DatabaseType,
		"database", req.DatabaseName, "apply_id", req.ApplyIdentifier,
		"requested_by", req.RequestedBy, "error", cause)

	blocked := *check
	blocked.Status = checkStatusCompleted
	blocked.Conclusion = checkConclusionActionRequired
	blocked.HasChanges = true
	blocked.BlockingReason = schemaChangedReplanFailedBlock.blockingReason
	blocked.ErrorMessage = schemaChangedReplanFailedBlock.message
	blocked.ChangeSummary = clampDriftSummary(fmt.Sprintf(
		"schema for %s in %s changed (apply %s); re-plan failed — see server logs",
		req.DatabaseName, req.Environment, req.ApplyIdentifier))
	flipped, err := h.service.Storage().Checks().MarkBlockedForFailedRefresh(ctx, &blocked)
	if err != nil {
		return fmt.Errorf("fail stored check closed for %s#%d (target %s/%s in %s, apply %s) after re-plan failure: %w",
			check.Repository, check.PullRequest, req.DatabaseType, req.DatabaseName,
			req.Environment, req.ApplyIdentifier, err)
	}
	if !flipped {
		// The head-SHA condition (or the in-flight apply-owned guard) refused
		// the write: a racing synchronize re-planned a newer head, or an apply
		// claimed the row. Either way the newer write is authoritative.
		h.logger.Info("check refresh fail-closed flip superseded by a racing write; the newer stored check state is authoritative",
			"apply_id", req.ApplyIdentifier, "repo", check.Repository, "pr", check.PullRequest,
			"environment", req.Environment, "database_type", req.DatabaseType,
			"database", req.DatabaseName, "check_head_sha", check.HeadSHA)
		metrics.RecordCheckRefreshPROutcome(ctx, check.Repository, req.DatabaseName, req.Environment, checkRefreshOutcomeSkippedSuperseded)
		return nil
	}
	h.updateAggregateCheck(ctx, client, check.Repository, check.PullRequest, check.HeadSHA)
	metrics.RecordCheckRefreshPROutcome(ctx, check.Repository, req.DatabaseName, req.Environment, checkRefreshOutcomeBlockedReplan)
	return nil
}

// startCheckRefreshHeartbeat extends the request lease on a fixed cadence
// while the fan-out runs, so a fan-out spanning many PRs is not reclaimed
// mid-flight. On lease loss it cancels the run context so in-flight work
// stops. The returned join function stops the heartbeat and reports the
// heartbeat failure (nil when the lease was held for the whole run).
func (h *Handler) startCheckRefreshHeartbeat(ctx context.Context, req *storage.CheckRefreshRequest, cancelRun context.CancelFunc) func() error {
	hbCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	var heartbeatErr error // written once before close(done)
	interval := h.checkRefreshLeaseDuration / 3
	if interval <= 0 {
		interval = 10 * time.Second
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if err := h.checkRefreshStore().Heartbeat(hbCtx, req.ID, req.LeaseToken, h.checkRefreshLeaseDuration); err != nil {
					if hbCtx.Err() != nil {
						// The heartbeat was stopped intentionally mid-call; the
						// interrupted call is not a lease failure.
						h.logger.Debug("check refresh heartbeat interrupted by intentional stop; not a lease failure",
							"apply_id", req.ApplyIdentifier)
						return
					}
					if errors.Is(err, storage.ErrCheckRefreshLeaseLost) || errors.Is(err, storage.ErrCheckRefreshNotFound) {
						// Lease loss and a deleted row are both terminal for this
						// run: the result has nowhere to land, so stop instead of
						// finishing work that cannot be recorded.
						h.logger.Warn("check refresh heartbeat lost the request lease; driver will stop",
							"apply_id", req.ApplyIdentifier, "environment", req.Environment,
							"database_type", req.DatabaseType, "database", req.DatabaseName, "error", err)
						heartbeatErr = err
						cancelRun()
						return
					}
					// A transient store error is not lease loss: the lease is
					// still ours until it expires, so keep working and retry on
					// the next tick.
					h.logger.Warn("check refresh heartbeat failed; will retry",
						"apply_id", req.ApplyIdentifier, "environment", req.Environment,
						"database_type", req.DatabaseType, "database", req.DatabaseName, "error", err)
				}
			}
		}
	}()
	return func() error {
		stop()
		<-done
		return heartbeatErr
	}
}

// isCheckRefreshNotManagedError reports whether a schema discovery error is a
// determinate "this PR does not manage the target here" answer — safe to skip
// — as opposed to uncertainty (GitHub unavailability, storage failure), which
// must fail closed or retry.
func isCheckRefreshNotManagedError(err error) bool {
	var dbNotFound *ghclient.DatabaseNotFoundError
	var outsideAllowedDirs *schemaConfigOutsideAllowedDirsError
	var dbNotConfigured *api.DatabaseNotConfiguredError
	var envNotConfigured *environmentNotConfiguredError
	return errors.Is(err, ghclient.ErrNoConfig) ||
		errors.As(err, &dbNotFound) ||
		errors.As(err, &outsideAllowedDirs) ||
		errors.As(err, &dbNotConfigured) ||
		errors.As(err, &envNotConfigured)
}

// checkRefreshNote renders the attribution line appended to a refreshed
// check's stored change summary, so the aggregate's Change column says why an
// unchanged PR was re-planned. RequestedBy is caller-influenced text, so the
// note is sanitized for markdown-table rendering and clamped to the column
// width.
func checkRefreshNote(req *storage.CheckRefreshRequest) string {
	return clampDriftSummary(fmt.Sprintf("re-planned: schema for %s in %s changed (apply %s by %s)",
		req.DatabaseName, req.Environment, req.ApplyIdentifier, req.RequestedBy))
}

// appendRefreshNote joins a plan's own change summary with the refresh
// attribution note, keeping the combined value within the stored column width.
func appendRefreshNote(changeSummary, note string) string {
	if changeSummary == "" {
		return note
	}
	return clampDriftSummary(changeSummary + " · " + note)
}

func (h *Handler) checkRefreshStore() storage.CheckRefreshRequestStore {
	if h.service == nil || h.service.Storage() == nil {
		return nil
	}
	return h.service.Storage().CheckRefreshRequests()
}

func checkRefreshLeaseOwner() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s/%d/check-refresh", hostname, os.Getpid())
}
