// merge_gate.go drives the merge gate guardrail around applies on a
// (environment, database type, database) target. Before an apply's engine
// work starts, the operator gate records a durable preflight request; this
// processor consumes it by holding every sibling PR's stored check on the
// target action-required (with a PR comment explaining the hold), so a merge
// cannot land on a verdict the apply is about to invalidate — the gate blocks
// the apply's start until the holds are confirmed. Once the apply settles
// terminally, the drive tail (and backstop sweeps here) records a settle
// request; this processor consumes it by re-planning each sibling PR against
// the live schema, which refreshes stale verdicts and releases the holds. A
// re-plan that fails leaves the stored check failed closed so a stale plan
// can never keep passing. CLI/gRPC applies carry no PR surface at all, so
// this processor is the only path that keeps their targets' sibling PR
// checks honest.
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
	"github.com/block/schemabot/pkg/webhook/templates"
)

const (
	defaultMergeGatePollInterval  = 30 * time.Second
	defaultMergeGateLeaseDuration = 2 * time.Minute

	// defaultMergeGateSweepLookback bounds the backstop sweep over completed
	// applies missing a merge gate request. The sweep exists for the crash window
	// between an apply's terminal write and the drive tail's recording, plus
	// process downtime; requests are unique per apply, so re-sweeping the same
	// window is a no-op.
	defaultMergeGateSweepLookback = 6 * time.Hour

	// mergeGateRetryDelay is the pause before a failed fan-out is reclaimed.
	mergeGateRetryDelay = time.Minute

	// mergeGatePRConcurrency bounds how many sibling PRs one fan-out
	// re-plans at once, so a hot target with many open PRs cannot saturate the
	// GitHub API or the plan engine.
	mergeGatePRConcurrency = 3

	// maxMergeGateAttempts aliases the store's claim ceiling so the drive
	// records a retryable failure as terminal on the same attempt at which
	// ClaimNext would stop handing the request out.
	maxMergeGateAttempts = storage.MaxMergeGateAttempts
)

// Per-PR fan-out outcomes recorded via metrics.RecordMergeGatePROutcome.
const (
	mergeGateOutcomeRefreshed         = "refreshed"
	mergeGateOutcomeBlockedReplan     = "blocked_replan_failed"
	mergeGateOutcomeSkippedInFlight   = "skipped_in_flight"
	mergeGateOutcomeSkippedPRClosed   = "skipped_pr_closed"
	mergeGateOutcomeSkippedNotManaged = "skipped_not_managed"
	mergeGateOutcomeSkippedSuperseded = "skipped_superseded"
	mergeGateOutcomeHeld              = "held"
	mergeGateOutcomeHoldSuperseded    = "hold_superseded"
)

// StartMergeGateProcessor starts the background driver that consumes
// durable merge gate requests, and registers this handler as the
// service's merge gate consumer. Registration lives here rather than at
// handler construction because the consumer callback is what tells the
// operator a processor exists: the drive tail records settles and the apply
// gate records (and waits on) preflights only when something will drain
// them. Idempotent; StopMergeGateProcessor stops the driver and waits for
// the in-flight pass to finish.
func (h *Handler) StartMergeGateProcessor(ctx context.Context) {
	if h.mergeGateStore() == nil {
		h.logger.Warn("merge gate processor not started: storage is unavailable; sibling PR checks will go stale after applies until it recovers")
		return
	}

	// Wake the driver as soon as a drive tail or apply gate records a
	// request, so it is consumed without waiting for the next poll tick. The
	// durable request row stays the source of truth: a kick lost to a pod
	// boundary only costs poll latency.
	h.service.OnMergeGateRecorded = h.KickMergeGate

	h.mergeGateMu.Lock()
	if h.mergeGateStop != nil {
		h.mergeGateMu.Unlock()
		h.logger.Info("merge gate processor already running")
		return
	}
	stop := make(chan struct{})
	driverCtx, cancel := context.WithCancel(ctx)
	h.mergeGateStop = stop
	h.mergeGateCancel = cancel
	// Register on the WaitGroup while the mutex is held so Start cannot race a
	// concurrent Stop's Wait.
	h.mergeGateWg.Go(func() {
		h.mergeGateDriver(driverCtx, stop)
	})
	h.mergeGateMu.Unlock()

	h.logger.Info("merge gate processor started",
		"interval", h.mergeGatePollInterval,
		"lease_duration", h.mergeGateLeaseDuration,
		"sweep_lookback", h.mergeGateSweepLookback)
}

// StopMergeGateProcessor stops the merge gate driver and waits for the
// in-flight pass to finish its current drive.
func (h *Handler) StopMergeGateProcessor() {
	h.mergeGateMu.Lock()
	if h.mergeGateStop == nil {
		h.mergeGateMu.Unlock()
		h.logger.Debug("merge gate processor stop requested but it is not running")
		return
	}
	stop := h.mergeGateStop
	cancel := h.mergeGateCancel
	h.mergeGateStop = nil
	h.mergeGateCancel = nil
	h.mergeGateMu.Unlock()

	close(stop)
	if cancel != nil {
		cancel()
	}
	h.mergeGateWg.Wait()
	h.logger.Info("merge gate processor stopped")
}

// KickMergeGate wakes the merge gate driver to run a pass now instead
// of waiting for the next poll tick. Non-blocking: when a kick is already
// pending, the coming pass drains the new request too. The durable request
// row is the source of truth — a kick that lands with no driver running, or
// on a different pod than the one that will claim the request, only costs
// poll latency, never the refresh.
func (h *Handler) KickMergeGate() {
	select {
	case h.mergeGateKick <- struct{}{}:
	default:
	}
}

func (h *Handler) mergeGateDriver(ctx context.Context, stop <-chan struct{}) {
	owner := mergeGateLeaseOwner()
	ticker := time.NewTicker(h.mergeGatePollInterval)
	defer ticker.Stop()

	h.logger.Debug("merge gate driver started", "lease_owner", owner)
	h.runMergeGatePass(ctx, owner)

	for {
		select {
		case <-stop:
			h.logger.Debug("merge gate driver stopping")
			return
		case <-ctx.Done():
			h.logger.Debug("merge gate driver context cancelled")
			return
		case <-h.mergeGateKick:
			h.logger.Debug("merge gate driver woken by recorded-request kick")
			h.runMergeGatePass(ctx, owner)
		case <-ticker.C:
			h.runMergeGatePass(ctx, owner)
		}
	}
}

// runMergeGatePass runs one full processor pass: backfill requests the
// drive tails missed, terminalize requests wedged past the attempt cap, then
// claim and drive requests until none remain claimable.
func (h *Handler) runMergeGatePass(ctx context.Context, owner string) {
	h.sweepMergeGateRequests(ctx)
	h.sweepPreflightedAppliesMissingSettle(ctx)
	h.terminateStuckMergeGateRequests(ctx)
	h.rearmPreflightRendersForActiveApplies(ctx)
	h.drainMergeGateRequests(ctx, owner)
}

// rearmPreflightRendersForActiveApplies re-arms terminally failed preflight
// requests whose apply is still active. The operator gate starts applies on
// stored holds, so a code-host rendering that exhausted its retries (for
// example through a code-host outage) has nothing else retrying it until the
// apply settles — and once the code host recovers, sibling PRs' visible
// Check Runs would sit stale-green for the rest of the apply. Re-arming
// keeps the render retrying while the apply runs; the settle re-plan
// supersedes it after that.
func (h *Handler) rearmPreflightRendersForActiveApplies(ctx context.Context) {
	reopened, err := h.mergeGateStore().ReopenTerminalPreflightsForActiveApplies(ctx)
	if err != nil {
		h.logger.Error("merge gate re-arm sweep failed; terminally failed preflight renders stay unretried until the next pass", "error", err)
		return
	}
	if reopened == 0 {
		return
	}
	h.logger.Warn("merge gate re-armed terminally failed preflight renders for still-active applies; their sibling PRs' visible checks stay unrendered until a render succeeds",
		"reopened", reopened)
	metrics.RecordMergeGatePreflightRearmed(ctx, reopened)
}

// sweepMergeGateRequests backfills merge gate requests for completed applies
// that have none — the applies table is the outbox, so a pod crash between an
// apply's terminal write and its drive-tail recording cannot lose the fan-out.
func (h *Handler) sweepMergeGateRequests(ctx context.Context) {
	store := h.mergeGateStore()
	applies, err := store.FindCompletedAppliesMissingRequest(ctx, h.mergeGateSweepLookback)
	if err != nil {
		h.logger.Error("merge gate sweep failed to find completed applies missing a merge gate request; drive-tail gaps stay unbackfilled until the next pass", "error", err)
		return
	}
	for _, apply := range applies {
		recorded, err := store.Record(ctx, &storage.MergeGateRequest{
			ApplyID:         apply.ID,
			Kind:            storage.MergeGateKindSettle,
			ApplyIdentifier: apply.ApplyIdentifier,
			Environment:     apply.Environment,
			DatabaseType:    apply.DatabaseType,
			DatabaseName:    apply.Database,
			Repository:      apply.Repository,
			ChangeKey:       storage.ChangeKeyForPullRequest(apply.PullRequest),
			RequestedBy:     apply.Caller,
		})
		if err != nil {
			// Each apply's backfill is independent; a failed one is retried on
			// the next sweep pass.
			h.logger.Error("merge gate sweep failed to backfill a settle request for a completed apply",
				append(apply.LogAttrs(), "error", err)...)
			metrics.RecordMergeGateRecordFailure(ctx, apply.Database, apply.Environment)
			continue
		}
		if !recorded {
			// A drive tail recorded it between the sweep query and this insert.
			h.logger.Debug("merge gate sweep found the settle request already recorded",
				apply.LogAttrs()...)
			continue
		}
		h.logger.Info("merge gate sweep backfilled a settle request the drive tail did not record",
			apply.LogAttrs()...)
		metrics.RecordMergeGateRecorded(ctx, apply.Database, apply.Environment, metrics.MergeGateSourceSweep)
	}
}

// sweepPreflightedAppliesMissingSettle backfills settle requests for applies
// that settled terminally — in any state — after a preflight held sibling PR
// checks, but whose settle was never recorded. The completed-applies sweep
// only covers terminal success; this one exists because a preflight hold must
// always be released, even when the apply failed, was stopped, or was
// cancelled before changing the schema. Without it, an apply cancelled while
// queued (whose drive tail never ran) would leave sibling checks
// action-required forever.
func (h *Handler) sweepPreflightedAppliesMissingSettle(ctx context.Context) {
	store := h.mergeGateStore()
	applies, err := store.FindTerminalAppliesWithPreflightMissingSettle(ctx, h.mergeGateSweepLookback)
	if err != nil {
		h.logger.Error("merge gate release sweep failed to find preflighted terminal applies missing a settle; held sibling PR checks stay blocked until the next pass", "error", err)
		return
	}
	for _, apply := range applies {
		recorded, err := store.Record(ctx, &storage.MergeGateRequest{
			ApplyID:         apply.ID,
			Kind:            storage.MergeGateKindSettle,
			ApplyIdentifier: apply.ApplyIdentifier,
			Environment:     apply.Environment,
			DatabaseType:    apply.DatabaseType,
			DatabaseName:    apply.Database,
			Repository:      apply.Repository,
			ChangeKey:       storage.ChangeKeyForPullRequest(apply.PullRequest),
			RequestedBy:     apply.Caller,
		})
		if err != nil {
			h.logger.Error("merge gate release sweep failed to backfill a settle request for a preflighted terminal apply; its held sibling PR checks stay blocked until the next pass",
				append(apply.LogAttrs(), "error", err)...)
			metrics.RecordMergeGateRecordFailure(ctx, apply.Database, apply.Environment)
			continue
		}
		if !recorded {
			h.logger.Debug("merge gate release sweep found the settle request already recorded",
				apply.LogAttrs()...)
			continue
		}
		h.logger.Info("merge gate release sweep backfilled a settle request for a preflighted terminal apply; its held sibling PR checks will be re-planned",
			apply.LogAttrs()...)
		metrics.RecordMergeGateRecorded(ctx, apply.Database, apply.Environment, metrics.MergeGateSourceReleaseSweep)
	}
}

// terminateStuckMergeGateRequests terminalizes requests wedged in
// processing past the attempt cap with an expired lease, so a poison request
// cannot be reclaimed forever. Each terminated request means sibling PR
// stored checks for its target may remain stale until their PRs re-plan.
func (h *Handler) terminateStuckMergeGateRequests(ctx context.Context) {
	terminated, err := h.mergeGateStore().TerminateStuckProcessing(ctx, "merge gate request exceeded its attempt cap with an expired lease")
	if err != nil {
		h.logger.Error("merge gate stuck-processing sweep failed; wedged requests stay unterminalized until the next pass", "error", err)
		return
	}
	if terminated == 0 {
		return
	}
	h.logger.Error("merge gate requests terminated after exceeding their attempt cap; sibling PR stored checks for their targets may remain stale until those PRs re-plan",
		"terminated", terminated)
	metrics.RecordMergeGateTerminatedStuck(ctx, terminated)
}

// drainMergeGateRequests claims and drives requests until none remain
// claimable, so a backlog is worked down within a single tick. It stops on the
// first empty claim or claim error — a storage error must not spin a tight
// loop — and on context cancellation.
func (h *Handler) drainMergeGateRequests(ctx context.Context, owner string) {
	for {
		if ctx.Err() != nil {
			return
		}
		if !h.driveNextMergeGate(ctx, owner) {
			return
		}
	}
}

// driveNextMergeGate claims and drives at most one request. It reports
// whether a request was claimed, so the drain loop knows whether to continue.
func (h *Handler) driveNextMergeGate(ctx context.Context, owner string) (claimed bool) {
	store := h.mergeGateStore()
	req, err := store.ClaimNext(ctx, owner, h.mergeGateLeaseDuration)
	if err != nil {
		h.logger.Error("merge gate driver failed to claim a request", "lease_owner", owner, "error", err)
		return false
	}
	if req == nil {
		h.logger.Debug("merge gate driver found no request to claim")
		return false
	}

	h.logger.Info("merge gate driver claimed a request",
		"lease_owner", owner,
		"kind", req.Kind,
		"apply_id", req.ApplyIdentifier,
		"environment", req.Environment,
		"database_type", req.DatabaseType,
		"database", req.DatabaseName,
		"origin_repo", req.Repository,
		"origin_change", req.ChangeKey,
		"requested_by", req.RequestedBy,
		"attempts", req.Attempts)

	h.driveClaimedMergeGate(ctx, store, req)
	return true
}

// driveClaimedMergeGate runs the fan-out → heartbeat → finish lifecycle for
// a freshly claimed request, coalescing pending sibling requests for the same
// target once the fan-out succeeds.
func (h *Handler) driveClaimedMergeGate(ctx context.Context, store storage.MergeGateRequestStore, req *storage.MergeGateRequest) {
	// Capture the pending same-kind siblings before the fan-out starts: the
	// fan-out acts on the target's current state, so it covers every request
	// recorded before it began. A request recorded mid-fan-out is not covered
	// and stays pending for the next drain.
	siblings, err := store.PendingForTarget(ctx, req.Environment, req.DatabaseType, req.DatabaseName, req.Kind, req.ID)
	if err != nil {
		// Coalescing is an optimization: without the sibling list each pending
		// request runs its own fan-out, which re-plans the same PRs again —
		// wasteful but safe.
		h.logger.Warn("merge gate driver could not list pending sibling requests; siblings will fan out on their own",
			"apply_id", req.ApplyIdentifier, "environment", req.Environment,
			"database_type", req.DatabaseType, "database", req.DatabaseName, "error", err)
		siblings = nil
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	stopHeartbeat := h.startMergeGateHeartbeat(runCtx, req, cancelRun)
	fanErr := h.safeFanOutMergeGate(runCtx, req)
	heartbeatErr := stopHeartbeat()
	cancelRun()

	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if fanErr != nil {
		retryAfter := (*time.Time)(nil)
		if req.Attempts < maxMergeGateAttempts {
			due := time.Now().Add(mergeGateRetryDelay)
			retryAfter = &due
		} else {
			h.logger.Error("merge gate request exhausted its retry budget and is now terminal; sibling PR stored checks for the target may remain stale until those PRs re-plan",
				"apply_id", req.ApplyIdentifier, "environment", req.Environment,
				"database_type", req.DatabaseType, "database", req.DatabaseName,
				"attempts", req.Attempts, "error", fanErr)
		}
		if err := store.MarkFailed(finishCtx, req.ID, req.LeaseToken, fanErr.Error(), retryAfter); err != nil {
			if errors.Is(err, storage.ErrMergeGateLeaseLost) || errors.Is(err, storage.ErrMergeGateNotFound) {
				h.logger.Warn("merge gate driver lost the request lease before recording failure; another driver owns the request",
					"apply_id", req.ApplyIdentifier, "environment", req.Environment,
					"database_type", req.DatabaseType, "database", req.DatabaseName)
				return
			}
			h.logger.Error("merge gate driver failed to record the fan-out failure",
				"apply_id", req.ApplyIdentifier, "environment", req.Environment,
				"database_type", req.DatabaseType, "database", req.DatabaseName, "error", err)
			return
		}
		outcome := "failed_terminal"
		if retryAfter != nil {
			outcome = "failed_retrying"
		}
		metrics.RecordMergeGateEventOutcome(finishCtx, req.DatabaseName, req.Environment, outcome)
		h.logger.Warn("merge gate driver recorded a fan-out failure",
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
		h.logger.Warn("merge gate driver skipped completion because the request lease heartbeat failed; leaving the request for reclaim",
			"apply_id", req.ApplyIdentifier, "environment", req.Environment,
			"database_type", req.DatabaseType, "database", req.DatabaseName, "error", heartbeatErr)
		metrics.RecordMergeGateEventOutcome(finishCtx, req.DatabaseName, req.Environment, "lease_lost")
		return
	}
	if err := store.MarkCompleted(finishCtx, req.ID, req.LeaseToken); err != nil {
		if errors.Is(err, storage.ErrMergeGateLeaseLost) || errors.Is(err, storage.ErrMergeGateNotFound) {
			h.logger.Warn("merge gate driver lost the request lease before recording completion; another driver owns the request",
				"apply_id", req.ApplyIdentifier, "environment", req.Environment,
				"database_type", req.DatabaseType, "database", req.DatabaseName)
			return
		}
		h.logger.Error("merge gate driver failed to mark the request completed",
			"apply_id", req.ApplyIdentifier, "environment", req.Environment,
			"database_type", req.DatabaseType, "database", req.DatabaseName, "error", err)
		return
	}
	metrics.RecordMergeGateEventOutcome(finishCtx, req.DatabaseName, req.Environment, "completed")
	h.logger.Info("merge gate driver completed the request",
		"apply_id", req.ApplyIdentifier, "environment", req.Environment,
		"database_type", req.DatabaseType, "database", req.DatabaseName)

	for _, sibling := range siblings {
		coalesced, err := store.CompletePendingCoalesced(finishCtx, sibling.ID)
		if err != nil {
			// Each sibling completes independently; a failed coalesce leaves
			// the sibling pending, so its own fan-out (a redundant but safe
			// re-plan) finishes it.
			h.logger.Warn("merge gate driver failed to coalesce a covered sibling request; it will run its own fan-out",
				"apply_id", req.ApplyIdentifier, "sibling_apply_id", sibling.ApplyIdentifier,
				"environment", req.Environment, "database_type", req.DatabaseType,
				"database", req.DatabaseName, "error", err)
			continue
		}
		if !coalesced {
			h.logger.Debug("merge gate sibling request no longer pending; its own lifecycle finishes it",
				"apply_id", req.ApplyIdentifier, "sibling_apply_id", sibling.ApplyIdentifier,
				"environment", req.Environment, "database_type", req.DatabaseType,
				"database", req.DatabaseName)
			continue
		}
		h.logger.Info("merge gate driver coalesced a sibling request covered by this fan-out",
			"apply_id", req.ApplyIdentifier, "sibling_apply_id", sibling.ApplyIdentifier,
			"environment", req.Environment, "database_type", req.DatabaseType,
			"database", req.DatabaseName)
	}
}

// safeFanOutMergeGate runs the fan-out with panic recovery: the row stays
// processing until its lease expires and is then claimable again, so a driver
// panic would otherwise crash-loop every replica on the same poison request. A
// recovered panic is a retryable failure, so the attempt cap makes a
// deterministic panic terminal instead.
func (h *Handler) safeFanOutMergeGate(ctx context.Context, req *storage.MergeGateRequest) (err error) {
	defer func() {
		if r := recover(); r != nil {
			h.logger.Error("merge gate driver recovered from panic during fan-out",
				"apply_id", req.ApplyIdentifier, "environment", req.Environment,
				"database_type", req.DatabaseType, "database", req.DatabaseName,
				"panic", fmt.Sprintf("%v", r), "stack", string(debug.Stack()))
			err = fmt.Errorf("panic during merge gate fan-out for apply %s: %v", req.ApplyIdentifier, r)
		}
	}()
	return h.fanOutMergeGate(ctx, req)
}

// fanOutMergeGate dispatches a claimed request to the fan-out its kind
// selects: a preflight holds sibling PR checks before the apply starts, a
// settle re-plans them after it finishes.
func (h *Handler) fanOutMergeGate(ctx context.Context, req *storage.MergeGateRequest) error {
	switch req.Kind {
	case storage.MergeGateKindPreflight:
		return h.fanOutCheckPreflight(ctx, req)
	case storage.MergeGateKindSettle:
		return h.fanOutCheckSettle(ctx, req)
	default:
		// Fail the request rather than guess: an unknown kind means a newer
		// (or corrupted) writer recorded something this build cannot drive.
		return fmt.Errorf("unknown merge gate request kind %q for apply %s", req.Kind, req.ApplyIdentifier)
	}
}

// siblingChecksForTarget lists the stored check state the request fans out
// to: every check on the request's (environment, database type, database)
// except the originating PR's own rows — its own apply lifecycle keeps those
// current. Aggregate rows never match the target query: their database type
// and name are the aggregate sentinel, not a real target.
func (h *Handler) siblingChecksForTarget(ctx context.Context, req *storage.MergeGateRequest) ([]*storage.Check, error) {
	checks, err := h.service.Storage().Checks().GetByTarget(ctx, req.Environment, req.DatabaseType, req.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("list stored check state for target %s/%s in %s: %w",
			req.DatabaseType, req.DatabaseName, req.Environment, err)
	}
	var targets []*storage.Check
	for _, check := range checks {
		if isOriginatingChange(check, req) {
			h.logger.Debug("merge gate skipping the originating PR",
				"apply_id", req.ApplyIdentifier, "kind", req.Kind,
				"repo", check.Repository, "pr", check.PullRequest,
				"environment", req.Environment, "database_type", req.DatabaseType,
				"database", req.DatabaseName)
			continue
		}
		targets = append(targets, check)
	}
	return targets, nil
}

// fanOutCheckSettle re-plans every sibling PR whose stored check state
// targets the request's (environment, database type, database). A returned
// error means at least one PR was neither refreshed nor safely failed closed,
// so the request must be retried; re-planning already-refreshed PRs on that
// retry is safe.
func (h *Handler) fanOutCheckSettle(ctx context.Context, req *storage.MergeGateRequest) error {
	// While a later preflighted apply on the same target is still active, its
	// holds must survive: re-planning now would overwrite them with verdicts
	// computed against a schema that apply is about to change. Defer to that
	// apply's own settle — every preflighted apply records one when it settles
	// (drive tail or release sweep) — which re-plans the target's siblings
	// against the final schema.
	activeHold, err := h.mergeGateStore().HasActivePreflightedApplyOnTarget(ctx, req.Environment, req.DatabaseType, req.DatabaseName)
	if err != nil {
		return fmt.Errorf("check for active preflighted applies on target %s/%s in %s: %w",
			req.DatabaseType, req.DatabaseName, req.Environment, err)
	}
	if activeHold {
		h.logger.Info("merge gate settle deferred: a preflighted apply on the target is still active, so sibling holds stay in place until its own settle re-plans them",
			"apply_id", req.ApplyIdentifier, "environment", req.Environment,
			"database_type", req.DatabaseType, "database", req.DatabaseName)
		return nil
	}

	targets, err := h.siblingChecksForTarget(ctx, req)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		h.logger.Info("merge gate found no sibling PR check state for the target",
			"apply_id", req.ApplyIdentifier, "environment", req.Environment,
			"database_type", req.DatabaseType, "database", req.DatabaseName)
		return nil
	}

	h.logger.Info("merge gate fanning out to sibling PRs whose plans predate the schema change",
		"apply_id", req.ApplyIdentifier, "environment", req.Environment,
		"database_type", req.DatabaseType, "database", req.DatabaseName,
		"requested_by", req.RequestedBy, "sibling_prs", len(targets))

	sem := make(chan struct{}, mergeGatePRConcurrency)
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

// prTargetKey identifies one PR within a fan-out.
type prTargetKey struct {
	repo string
	pr   int
}

// fanOutCheckPreflight holds every sibling PR's stored check on the target
// action-required before the originating apply's engine work starts, in two
// phases. The hold phase flips the stored checks and stamps
// holds_recorded_at — storage-only writes, which is what the operator gate
// waits on: the code host being unreachable must never block an apply on the
// rendering of its own holds. The render phase then surfaces the holds on
// the code host (the aggregate Check Run recompute and one hold comment per
// PR); a render failure keeps the request retryable without re-blocking the
// apply, and while the code host is fully down its merge surface is down
// with it. Every step is idempotent (conditional flips, a set-once stamp, an
// aggregate recompute, and a marker-deduplicated comment), so retries
// converge without duplicate comments.
func (h *Handler) fanOutCheckPreflight(ctx context.Context, req *storage.MergeGateRequest) error {
	targets, err := h.siblingChecksForTarget(ctx, req)
	if err != nil {
		return err
	}

	byPR := make(map[prTargetKey][]*storage.Check)
	for _, check := range targets {
		key := prTargetKey{repo: check.Repository, pr: check.PullRequest}
		byPR[key] = append(byPR[key], check)
	}

	if req.HoldsRecordedAt == nil {
		if err := h.recordPreflightHolds(ctx, req, byPR); err != nil {
			return err
		}
	} else {
		h.logger.Debug("check preflight stored holds already recorded; resuming the code-host rendering",
			"apply_id", req.ApplyIdentifier, "environment", req.Environment,
			"database_type", req.DatabaseType, "database", req.DatabaseName)
	}

	if len(byPR) == 0 {
		h.logger.Info("check preflight found no sibling PR check state to hold for the target",
			"apply_id", req.ApplyIdentifier, "environment", req.Environment,
			"database_type", req.DatabaseType, "database", req.DatabaseName)
		return nil
	}

	sem := make(chan struct{}, mergeGatePRConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for key, checks := range byPR {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := h.renderCheckHoldOnPR(ctx, req, key.repo, key.pr, checks); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// recordPreflightHolds is the storage-only hold phase: it flips every
// actionable sibling stored check on the target action-required, then stamps
// holds_recorded_at on the request. No code-host call happens here, so the
// phase — and the operator gate waiting on the stamp — succeeds or fails on
// storage alone.
func (h *Handler) recordPreflightHolds(ctx context.Context, req *storage.MergeGateRequest, byPR map[prTargetKey][]*storage.Check) error {
	h.logger.Info("merge gate preflight holding sibling PR stored checks before the apply starts",
		"apply_id", req.ApplyIdentifier, "environment", req.Environment,
		"database_type", req.DatabaseType, "database", req.DatabaseName,
		"requested_by", req.RequestedBy, "sibling_prs", len(byPR))

	for key, checks := range byPR {
		if err := h.holdStoredPRChecks(ctx, req, key.repo, key.pr, checks); err != nil {
			return err
		}
	}
	if err := h.mergeGateStore().MarkPreflightHoldsRecorded(ctx, req.ID, req.LeaseToken); err != nil {
		return fmt.Errorf("record preflight holds for apply %s (target %s/%s in %s): %w",
			req.ApplyIdentifier, req.DatabaseType, req.DatabaseName, req.Environment, err)
	}
	now := time.Now()
	req.HoldsRecordedAt = &now
	h.logger.Info("check preflight stored holds recorded; the apply may start while the code-host rendering completes",
		"apply_id", req.ApplyIdentifier, "environment", req.Environment,
		"database_type", req.DatabaseType, "database", req.DatabaseName,
		"sibling_prs", len(byPR))
	return nil
}

// preflightActionableChecks filters one PR's checks to the rows a preflight
// acts on. In-flight apply-owned rows are excluded — they already block the
// aggregate and their apply's lifecycle stays authoritative — and logged
// only when the exclusion decides the hold (logSkips true, the hold phase),
// not again during the render.
func (h *Handler) preflightActionableChecks(ctx context.Context, req *storage.MergeGateRequest, repo string, pr int, checks []*storage.Check, logSkips bool) []*storage.Check {
	actionable := make([]*storage.Check, 0, len(checks))
	for _, check := range checks {
		if check.Status == checkStatusInProgress {
			if logSkips {
				h.logger.Info("check preflight leaving in-flight apply-owned check state untouched; it already blocks the PR",
					"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
					"environment", req.Environment, "database_type", req.DatabaseType,
					"database", req.DatabaseName, "check_apply_id", check.ApplyID,
					"check_head_sha", check.HeadSHA)
				metrics.RecordMergeGatePROutcome(ctx, repo, req.DatabaseName, req.Environment, mergeGateOutcomeSkippedInFlight)
			}
			continue
		}
		actionable = append(actionable, check)
	}
	return actionable
}

// holdStoredPRChecks flips one sibling PR's stored checks on the target
// action-required. Storage-only — the code-host rendering happens separately
// in renderCheckHoldOnPR. A flip refused by the head-SHA condition means a
// racing synchronize re-planned a newer head against the pre-apply schema;
// the hold yields to it (logged and counted — the settle re-plans that head
// when the apply finishes).
func (h *Handler) holdStoredPRChecks(ctx context.Context, req *storage.MergeGateRequest, repo string, pr int, checks []*storage.Check) error {
	actionable := h.preflightActionableChecks(ctx, req, repo, pr, checks, true)
	if len(actionable) == 0 {
		return nil
	}

	held := 0
	for _, check := range actionable {
		hold := *check
		hold.Status = checkStatusCompleted
		hold.Conclusion = checkConclusionActionRequired
		hold.BlockingReason = applyInFlightBlock.blockingReason
		hold.ErrorMessage = applyInFlightBlock.message
		hold.ChangeSummary = clampDriftSummary(fmt.Sprintf("held: apply %s by %s is changing %s in %s",
			req.ApplyIdentifier, req.RequestedBy, req.DatabaseName, req.Environment))
		flipped, err := h.service.Storage().Checks().MarkBlockedForApplyInFlight(ctx, &hold)
		if err != nil {
			return fmt.Errorf("hold stored check for %s#%d (target %s/%s in %s, apply %s): %w",
				repo, pr, req.DatabaseType, req.DatabaseName, req.Environment, req.ApplyIdentifier, err)
		}
		if !flipped {
			// Already held (an idempotent retry or an overlapping apply's
			// preflight), or superseded by a racing write on a newer head. The
			// distinction does not change this fan-out's behavior — the row is
			// either blocking already or owned by a newer verdict the settle
			// will re-plan — but it matters for triage, so log and count it.
			h.logger.Warn("check preflight did not flip a stored check; it is already held or a racing write on a newer head superseded the hold",
				"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
				"environment", req.Environment, "database_type", req.DatabaseType,
				"database", req.DatabaseName, "check_head_sha", check.HeadSHA,
				"check_blocking_reason", check.BlockingReason)
			metrics.RecordMergeGatePROutcome(ctx, repo, req.DatabaseName, req.Environment, mergeGateOutcomeHoldSuperseded)
			continue
		}
		held++
		metrics.RecordMergeGatePROutcome(ctx, repo, req.DatabaseName, req.Environment, mergeGateOutcomeHeld)
	}

	h.logger.Info("merge gate preflight held sibling PR stored checks for the target",
		"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
		"environment", req.Environment, "database_type", req.DatabaseType,
		"database", req.DatabaseName, "requested_by", req.RequestedBy,
		"held", held, "rows", len(actionable))
	return nil
}

// renderCheckHoldOnPR surfaces one sibling PR's already-durable hold on the
// code host: it recomputes the visible aggregate Check Run from the stored
// rows so the hold blocks the merge button, and posts the
// marker-deduplicated comment explaining the hold. A failure here keeps the
// request retryable but never re-blocks the apply — the stored holds are in
// place, and while the code host is fully down its merge surface is down
// with it; the re-arm sweep keeps the render retrying for as long as the
// apply runs.
func (h *Handler) renderCheckHoldOnPR(ctx context.Context, req *storage.MergeGateRequest, repo string, pr int, checks []*storage.Check) error {
	actionable := h.preflightActionableChecks(ctx, req, repo, pr, checks, false)
	if len(actionable) == 0 {
		h.logger.Debug("check preflight has nothing to render for the PR; only in-flight apply-owned rows, which already block it",
			"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
			"environment", req.Environment, "database_type", req.DatabaseType,
			"database", req.DatabaseName)
		return nil
	}

	installationID, err := h.resolveRepoWebhookInstallation(ctx, repo)
	if err != nil {
		return fmt.Errorf("resolve installation for check preflight of %s#%d (target %s/%s in %s, apply %s): %w",
			repo, pr, req.DatabaseType, req.DatabaseName, req.Environment, req.ApplyIdentifier, err)
	}
	prCtx, cancel, client, err := h.commandBootstrap(repo, installationID)
	defer cancel()
	if err != nil {
		return fmt.Errorf("bootstrap check preflight of %s#%d (target %s/%s in %s, apply %s): %w",
			repo, pr, req.DatabaseType, req.DatabaseName, req.Environment, req.ApplyIdentifier, err)
	}

	// A GitHub failure here is uncertainty, not staleness: keep the request
	// retryable rather than guessing at the PR's state.
	prInfo, err := client.FetchPullRequestNoCache(prCtx, repo, pr)
	if err != nil {
		return fmt.Errorf("verify PR state for check preflight of %s#%d (target %s/%s in %s, apply %s): %w",
			repo, pr, req.DatabaseType, req.DatabaseName, req.Environment, req.ApplyIdentifier, err)
	}
	if prInfo.IsClosed() {
		h.logger.Info("check preflight skipping closed PR; its held stored checks no longer gate a merge, so the hold needs no rendering",
			"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
			"environment", req.Environment, "database_type", req.DatabaseType,
			"database", req.DatabaseName, "merged", prInfo.Merged)
		metrics.RecordMergeGatePROutcome(prCtx, repo, req.DatabaseName, req.Environment, mergeGateOutcomeSkippedPRClosed)
		return nil
	}

	// Recompute the PR's visible aggregate Check Run from the stored rows so
	// the hold blocks the merge button, not just the database record.
	// Idempotent, so retries converge the Check Run.
	h.updateAggregateCheck(prCtx, client, repo, pr, actionable[0].HeadSHA)

	if err := h.ensureCheckHoldComment(prCtx, client, req, repo, pr); err != nil {
		return err
	}

	if err := h.flipQueuedSiblingAdmission(prCtx, client, req, repo, pr, actionable); err != nil {
		return err
	}

	h.logger.Info("merge gate preflight rendered the hold on the sibling PR",
		"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
		"environment", req.Environment, "database_type", req.DatabaseType,
		"database", req.DatabaseName, "requested_by", req.RequestedBy)
	return nil
}

// flipQueuedSiblingAdmission ejects a queued sibling PR from the merge queue
// by flipping SchemaBot's admission check on its merge-group commit. The
// admission verdict recorded when the PR entered the queue said nothing
// blocked; this apply's hold makes that verdict stale — and the queue watches
// only the merge-group commit, so the PR-head flip alone cannot stop the
// queued merge. Turning the recorded admission check action-required makes
// GitHub remove the PR from the queue mid-test, and the ejection guidance
// comment tells its author to queue again once the checks settle green. A
// sibling that is not queued needs nothing here; a GitHub failure keeps the
// render retryable — while GitHub is fully down its merge queue cannot merge
// the sibling either, so the window this flip closes stays closed.
func (h *Handler) flipQueuedSiblingAdmission(ctx context.Context, client *ghclient.InstallationClient, req *storage.MergeGateRequest, repo string, pr int, actionable []*storage.Check) error {
	entry, err := client.FetchMergeQueueEntry(ctx, repo, pr)
	if err != nil {
		return fmt.Errorf("fetch merge queue entry for check preflight of %s#%d (target %s/%s in %s, apply %s): %w",
			repo, pr, req.DatabaseType, req.DatabaseName, req.Environment, req.ApplyIdentifier, err)
	}
	if entry == nil {
		h.logger.Debug("check preflight sibling PR is not in the merge queue; no admission check to flip",
			"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
			"environment", req.Environment, "database_type", req.DatabaseType,
			"database", req.DatabaseName)
		return nil
	}
	if entry.HeadSHA == "" {
		// Queued, but the queue has not built this entry's merge group yet, so
		// there is no commit to flip a check on. The stored holds are already in
		// place, so the admission re-fold will block the group when the queue
		// requests checks for it.
		h.logger.Info("check preflight sibling PR is queued without a merge group yet; the admission check will block it when the queue requests checks",
			"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
			"environment", req.Environment, "database_type", req.DatabaseType,
			"database", req.DatabaseName)
		return nil
	}

	flipped := 0
	for _, target := range h.aggregateCheckTargetsForRepo(repo) {
		if target.environment != aggregateSentinel && target.environment != req.Environment {
			continue
		}
		if err := h.postAggregateCheck(ctx, client, repo, entry.HeadSHA, target, passingAggregateCheckContent{
			operation: "merge_group_hold_flip",
			title:     mergeGroupBlockedTitle,
			summary:   mergeGroupBlockedSummary,
		}, checkConclusionActionRequired); err != nil {
			return err
		}
		flipped++
		metrics.RecordMergeGroupRenderFlip(ctx, repo, target.environment)
	}
	if flipped == 0 {
		// The hold's environment has no aggregate target on this repo, so no
		// admission check gates it and there is nothing to flip. The stored hold
		// still blocks the PR-head aggregate for the environments that are gated.
		h.logger.Warn("check preflight found no aggregate target for the held environment; the queued sibling's merge group has no admission check to flip",
			"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
			"environment", req.Environment, "database_type", req.DatabaseType,
			"database", req.DatabaseName, "merge_group_head_sha", entry.HeadSHA)
		return nil
	}

	h.logger.Info("check preflight flipped the queued sibling's merge-group admission check; the queue will remove the PR until its checks settle green",
		"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
		"environment", req.Environment, "database_type", req.DatabaseType,
		"database", req.DatabaseName, "merge_group_head_sha", entry.HeadSHA,
		"flipped_checks", flipped)
	return h.ensureMergeQueueEjectedComment(ctx, client, repo, pr, entry.HeadSHA, actionable)
}

// checkHoldCommentMarker is the hidden marker that makes the hold comment
// idempotent per (PR, apply): a retried preflight fan-out searches recent PR
// comments for it before posting. ApplyIdentifier is server-generated, so the
// marker needs no sanitization.
func checkHoldCommentMarker(req *storage.MergeGateRequest) string {
	return fmt.Sprintf("<!-- schemabot:check-hold:%s -->", req.ApplyIdentifier)
}

// ensureCheckHoldComment posts the comment explaining the hold, exactly once
// per PR and originating apply: the hidden marker in the body deduplicates
// retries after partial failures. Comment failures fail the fan-out — the
// hold's explanation is part of the preflight contract, so the apply does not
// start until the operator-facing surface is complete.
func (h *Handler) ensureCheckHoldComment(ctx context.Context, client *ghclient.InstallationClient, req *storage.MergeGateRequest, repo string, pr int) error {
	marker := checkHoldCommentMarker(req)
	exists, err := client.HasIssueCommentWithMarker(ctx, repo, pr, marker)
	if err != nil {
		return fmt.Errorf("search for existing hold comment on %s#%d (apply %s): %w",
			repo, pr, req.ApplyIdentifier, err)
	}
	if exists {
		h.logger.Debug("check preflight hold comment already posted",
			"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr)
		return nil
	}
	body := templates.RenderCheckHold(templates.CheckHoldData{
		ApplyIdentifier: req.ApplyIdentifier,
		RequestedBy:     req.RequestedBy,
		Database:        req.DatabaseName,
		Environment:     req.Environment,
	})
	body = h.renderPRComment(body) + "\n" + marker
	if _, _, err := client.CreateIssueComment(ctx, repo, pr, body); err != nil {
		return fmt.Errorf("post hold comment on %s#%d (target %s/%s in %s, apply %s): %w",
			repo, pr, req.DatabaseType, req.DatabaseName, req.Environment, req.ApplyIdentifier, err)
	}
	h.logger.Info("check preflight posted the hold comment",
		"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
		"environment", req.Environment, "database_type", req.DatabaseType,
		"database", req.DatabaseName)
	return nil
}

// refreshPRPlanForTarget re-plans one sibling PR's stored check state against
// the target's live schema. It returns nil when the PR was refreshed or safely
// skipped (closed, in-flight-owned, no longer managed, or superseded by a
// racing write), and nil when a re-plan failure was durably failed closed. A
// returned error means the PR was neither refreshed nor failed closed, so the
// request must be retried.
func (h *Handler) refreshPRPlanForTarget(ctx context.Context, req *storage.MergeGateRequest, check *storage.Check) error {
	repo, pr := check.Repository, check.PullRequest

	if check.Status == checkStatusInProgress {
		// A started apply remains authoritative for its stored check state; a
		// re-plan here would fight the in-flight apply's own lifecycle. The
		// apply's terminal update (or stale-check reconciliation) refreshes it.
		h.logger.Info("merge gate leaving in-flight apply-owned check state untouched",
			"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
			"environment", req.Environment, "database_type", req.DatabaseType,
			"database", req.DatabaseName, "check_apply_id", check.ApplyID,
			"check_head_sha", check.HeadSHA)
		metrics.RecordMergeGatePROutcome(ctx, repo, req.DatabaseName, req.Environment, mergeGateOutcomeSkippedInFlight)
		return nil
	}

	// Driver work runs outside any HTTP request, so resolve the App
	// installation from config the same way repo-level webhook dispatch does.
	installationID, err := h.resolveRepoWebhookInstallation(ctx, repo)
	if err != nil {
		return fmt.Errorf("resolve installation for merge gate of %s#%d (target %s/%s in %s, apply %s): %w",
			repo, pr, req.DatabaseType, req.DatabaseName, req.Environment, req.ApplyIdentifier, err)
	}
	prCtx, cancel, client, err := h.commandBootstrap(repo, installationID)
	defer cancel()
	if err != nil {
		return fmt.Errorf("bootstrap merge gate of %s#%d (target %s/%s in %s, apply %s): %w",
			repo, pr, req.DatabaseType, req.DatabaseName, req.Environment, req.ApplyIdentifier, err)
	}

	// A GitHub failure here is uncertainty, not staleness: keep the request
	// retryable rather than guessing at the PR's state.
	prInfo, err := client.FetchPullRequestNoCache(prCtx, repo, pr)
	if err != nil {
		return fmt.Errorf("verify PR state for merge gate of %s#%d (target %s/%s in %s, apply %s): %w",
			repo, pr, req.DatabaseType, req.DatabaseName, req.Environment, req.ApplyIdentifier, err)
	}
	if prInfo.IsClosed() {
		h.logger.Info("merge gate skipping closed PR; its stored checks no longer gate a merge",
			"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
			"environment", req.Environment, "database_type", req.DatabaseType,
			"database", req.DatabaseName, "merged", prInfo.Merged)
		metrics.RecordMergeGatePROutcome(ctx, repo, req.DatabaseName, req.Environment, mergeGateOutcomeSkippedPRClosed)
		return nil
	}

	schemaResult, err := h.createManagedSchemaRequestFromPR(prCtx, client, repo, pr, req.Environment, req.DatabaseName, action.Plan)
	if err != nil {
		if isMergeGateNotManagedError(err) {
			// A determinate answer, not uncertainty: the PR's current head no
			// longer manages this target here, so there is nothing to re-plan.
			h.logger.Info("merge gate skipping PR that no longer manages the target",
				"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
				"environment", req.Environment, "database_type", req.DatabaseType,
				"database", req.DatabaseName, "reason", err)
			metrics.RecordMergeGatePROutcome(ctx, repo, req.DatabaseName, req.Environment, mergeGateOutcomeSkippedNotManaged)
			return nil
		}
		return h.blockCheckForFailedRefresh(prCtx, client, req, check,
			fmt.Errorf("discover schema config: %w", err))
	}
	if err := h.attachServerEnvironments(schemaResult, req.Environment); err != nil {
		if isMergeGateNotManagedError(err) {
			h.logger.Info("merge gate skipping PR whose target environment is no longer configured",
				"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
				"environment", req.Environment, "database_type", req.DatabaseType,
				"database", req.DatabaseName, "reason", err)
			metrics.RecordMergeGatePROutcome(ctx, repo, req.DatabaseName, req.Environment, mergeGateOutcomeSkippedNotManaged)
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

	sha, _, err := h.upsertPlanCheckRecord(prCtx, client, repo, pr, schemaResult, planResp, req.Environment, drift, mergeGateNote(req))
	if err != nil {
		if errors.Is(err, errPlanCheckHeadStale) {
			// A racing synchronize replaced the PR head mid-refresh; its own
			// auto-plan against the new head is authoritative.
			h.logger.Info("merge gate superseded by a newer PR head; the newer head's plan is authoritative",
				"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr,
				"environment", req.Environment, "database_type", req.DatabaseType,
				"database", req.DatabaseName, "planned_head_sha", schemaResult.HeadSHA)
			metrics.RecordMergeGatePROutcome(ctx, repo, req.DatabaseName, req.Environment, mergeGateOutcomeSkippedSuperseded)
			return nil
		}
		return h.blockCheckForFailedRefresh(prCtx, client, req, check,
			fmt.Errorf("store refreshed plan check record: %w", err))
	}
	h.updateAggregateCheck(prCtx, client, repo, pr, sha)

	h.logger.Info("merge gate re-planned sibling PR against the changed schema",
		"apply_id", req.ApplyIdentifier, "repo", repo, "pr", pr, "head_sha", sha,
		"environment", req.Environment, "database_type", req.DatabaseType,
		"database", req.DatabaseName, "requested_by", req.RequestedBy,
		"has_changes", planResp.HasChanges())
	metrics.RecordMergeGatePROutcome(ctx, repo, req.DatabaseName, req.Environment, mergeGateOutcomeRefreshed)
	return nil
}

// blockCheckForFailedRefresh fails a sibling PR's stored check state closed
// after its refresh re-plan failed: the plan on record was computed against a
// schema that no longer exists, so it must not keep passing. The raw cause is
// logged server-side with full identifiers; the stored check carries only the
// fixed sanitized block message. Returns nil once the block is durable (or a
// racing write superseded it) and an error when the flip itself failed, so the
// request is retried.
func (h *Handler) blockCheckForFailedRefresh(ctx context.Context, client *ghclient.InstallationClient, req *storage.MergeGateRequest, check *storage.Check, cause error) error {
	h.logger.Error("merge gate re-plan failed; failing the stored check closed",
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
		h.logger.Info("merge gate fail-closed flip superseded by a racing write; the newer stored check state is authoritative",
			"apply_id", req.ApplyIdentifier, "repo", check.Repository, "pr", check.PullRequest,
			"environment", req.Environment, "database_type", req.DatabaseType,
			"database", req.DatabaseName, "check_head_sha", check.HeadSHA)
		metrics.RecordMergeGatePROutcome(ctx, check.Repository, req.DatabaseName, req.Environment, mergeGateOutcomeSkippedSuperseded)
		return nil
	}
	h.updateAggregateCheck(ctx, client, check.Repository, check.PullRequest, check.HeadSHA)
	metrics.RecordMergeGatePROutcome(ctx, check.Repository, req.DatabaseName, req.Environment, mergeGateOutcomeBlockedReplan)
	return nil
}

// startMergeGateHeartbeat extends the request lease on a fixed cadence
// while the fan-out runs, so a fan-out spanning many PRs is not reclaimed
// mid-flight. On lease loss it cancels the run context so in-flight work
// stops. The returned join function stops the heartbeat and reports the
// heartbeat failure (nil when the lease was held for the whole run).
func (h *Handler) startMergeGateHeartbeat(ctx context.Context, req *storage.MergeGateRequest, cancelRun context.CancelFunc) func() error {
	hbCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	var heartbeatErr error // written once before close(done)
	interval := h.mergeGateLeaseDuration / 3
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
				if err := h.mergeGateStore().Heartbeat(hbCtx, req.ID, req.LeaseToken, h.mergeGateLeaseDuration); err != nil {
					if hbCtx.Err() != nil {
						// The heartbeat was stopped intentionally mid-call; the
						// interrupted call is not a lease failure.
						h.logger.Debug("merge gate heartbeat interrupted by intentional stop; not a lease failure",
							"apply_id", req.ApplyIdentifier)
						return
					}
					if errors.Is(err, storage.ErrMergeGateLeaseLost) || errors.Is(err, storage.ErrMergeGateNotFound) {
						// Lease loss and a deleted row are both terminal for this
						// run: the result has nowhere to land, so stop instead of
						// finishing work that cannot be recorded.
						h.logger.Warn("merge gate heartbeat lost the request lease; driver will stop",
							"apply_id", req.ApplyIdentifier, "environment", req.Environment,
							"database_type", req.DatabaseType, "database", req.DatabaseName, "error", err)
						heartbeatErr = err
						cancelRun()
						return
					}
					// A transient store error is not lease loss: the lease is
					// still ours until it expires, so keep working and retry on
					// the next tick.
					h.logger.Warn("merge gate heartbeat failed; will retry",
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

// isMergeGateNotManagedError reports whether a schema discovery error is a
// determinate "this PR does not manage the target here" answer — safe to skip
// — as opposed to uncertainty (GitHub unavailability, storage failure), which
// must fail closed or retry.
func isMergeGateNotManagedError(err error) bool {
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

// isOriginatingChange reports whether a stored check belongs to the change
// that originated the merge gate request. CLI/gRPC applies carry no
// originating change (empty ChangeKey) and therefore match nothing.
func isOriginatingChange(check *storage.Check, req *storage.MergeGateRequest) bool {
	if req.ChangeKey == "" {
		return false
	}
	return check.Repository == req.Repository &&
		storage.ChangeKeyForPullRequest(check.PullRequest) == req.ChangeKey
}

// mergeGateNote renders the attribution line appended to a refreshed
// check's stored change summary, so the aggregate's Change column says why an
// unchanged PR was re-planned. RequestedBy is caller-influenced text, so the
// note is sanitized for markdown-table rendering and clamped to the column
// width.
func mergeGateNote(req *storage.MergeGateRequest) string {
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

func (h *Handler) mergeGateStore() storage.MergeGateRequestStore {
	if h.service == nil || h.service.Storage() == nil {
		return nil
	}
	return h.service.Storage().MergeGateRequests()
}

func mergeGateLeaseOwner() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s/%d/check-refresh", hostname, os.Getpid())
}
