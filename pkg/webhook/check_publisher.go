package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

func (h *Handler) shouldPublishChecks(ctx context.Context, repo string, operation string) bool {
	if h.service == nil || h.service.Config().AreChecksEnabled(repo) {
		return true
	}
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:  operation,
		Repository: repo,
		Status:     "skipped",
	})
	h.logger.Info("status check publishing disabled for repository", "repo", repo, "operation", operation)
	return false
}

// verifyHeadSHAStillCurrentForPR returns false when writing status check state
// for headSHA would be unsafe because the PR now points at a different commit
// SHA. It records a metric and logs the reason before every false return so
// callers can stop without adding duplicate log noise.
func (h *Handler) verifyHeadSHAStillCurrentForPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, operation string) bool {
	current, _ := h.verifyHeadSHACurrency(ctx, client, repo, pr, headSHA, operation)
	return current
}

// verifyHeadSHACurrency is the lossless form of verifyHeadSHAStillCurrentForPR:
// it returns the same current bool plus the underlying error so a caller with a
// retry mechanism can tell a transient failure (empty head SHA or a PR-fetch
// error, both non-nil) apart from a genuinely superseded head (current=false,
// err=nil), which must never be retried as if GitHub had failed. It records the
// same metrics and logs as before. A superseded head is not an operational
// failure, so it returns a nil error.
func (h *Handler) verifyHeadSHACurrency(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, operation string) (bool, error) {
	if headSHA == "" {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  operation,
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("refusing to update status check without head SHA", "repo", repo, "pr", pr, "operation", operation)
		return false, fmt.Errorf("verify head SHA for %s#%d (%s): missing head SHA", repo, pr, operation)
	}

	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  operation,
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to verify status check head SHA before update",
			"repo", repo, "pr", pr, "head_sha", headSHA, "operation", operation, "error", err)
		return false, fmt.Errorf("verify head SHA for %s#%d (%s): %w", repo, pr, operation, err)
	}
	if prInfo.HeadSHA != headSHA {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  operation,
			Repository: repo,
			Status:     "stale",
		})
		h.logger.Info("skipping stale status check update because head SHA is no longer current for PR",
			"repo", repo, "pr", pr, "operation", operation,
			"stale_head_sha", headSHA, "current_head_sha", prInfo.HeadSHA)
		return false, nil
	}
	return true, nil
}

// aggregateFoldFollowUp names the post-fold side effect a caller must apply
// after updateAggregateCheckOnce returns. The one-shot fold never schedules a
// timer or mutates the re-fold budget itself, so the owner of retries (the
// updateAggregateCheck wrapper today; the durable dispatch path later) decides
// how to act on the disposition. A ScheduleParticipantRefold follow-up with a
// nil error means the aggregate was published but at least one expected
// participant has not yet converged (not reported, not terminal, or its Check
// Run read failed).
//
// Participant Check Run *read* failures are deliberately conveyed only through
// this retriable disposition, not the error return: participant convergence is
// owned by the re-fold budget, and folding those reads into the error would make
// a retry-owning caller (the durable path) and the re-fold timer double-retry
// the same condition. So a nil error here does NOT assert that no operational
// GitHub read failed during the fold — only that the fold itself completed and
// the aggregate was written. Operational failures the caller must react to (head
// verification, PR-files fetch, per-environment upserts) do return a non-nil
// error alongside their disposition.
type aggregateFoldFollowUp uint8

const (
	aggregateFoldNoFollowUp aggregateFoldFollowUp = iota
	aggregateFoldClearParticipantRefoldBudget
	aggregateFoldScheduleLeaderRefold
	aggregateFoldScheduleParticipantRefold
)

// updateAggregateCheck is the fire-and-forget wrapper: it folds the aggregate
// and applies the returned follow-up (timer scheduling / budget clearing) that
// the callers with no way to react to a failed fold rely on. Callers that own a
// retry mechanism call updateAggregateCheckOnce directly and act on its error.
func (h *Handler) updateAggregateCheck(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string) {
	followUp, _ := h.updateAggregateCheckOnce(ctx, client, repo, pr, headSHA)
	h.applyAggregateFoldFollowUp(ctx, client, repo, pr, headSHA, followUp)
}

// applyAggregateFoldFollowUp applies the in-memory post-fold side effect
// updateAggregateCheckOnce returns: scheduling a bounded re-fold timer or
// clearing the participant re-fold budget. It is shared by the fire-and-forget
// wrapper and the durable dispatch path so both react to a disposition the same
// way; the fold core never mutates timers or the budget itself.
func (h *Handler) applyAggregateFoldFollowUp(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, followUp aggregateFoldFollowUp) {
	switch followUp {
	case aggregateFoldNoFollowUp:
	case aggregateFoldClearParticipantRefoldBudget:
		h.clearParticipantRefoldBudget(repo, pr)
	case aggregateFoldScheduleLeaderRefold:
		h.scheduleLeaderRefoldIfConfigured(ctx, repo, pr, client.InstallationID())
	case aggregateFoldScheduleParticipantRefold:
		h.scheduleParticipantRefold(ctx, repo, pr, client.InstallationID())
	default:
		// A follow-up the fold core returns but this switch doesn't apply would
		// silently drop the post-fold side effect (a re-fold that never gets
		// scheduled, a budget that never clears). Fail loud so a new disposition
		// can't no-op here unnoticed.
		h.logger.Error("unhandled aggregate fold follow-up, post-fold side effect dropped",
			"repo", repo, "pr", pr, "head_sha", headSHA, "follow_up", followUp)
	}
}

// updateAggregateCheckOnce recomputes and creates/updates aggregate check runs
// that roll up per-database checks for a PR. It does the fold exactly once and
// returns the follow-up its caller must apply plus the underlying error, so a
// caller with a retry mechanism can react to a failed fold. It never schedules a
// timer or mutates the re-fold budget itself.
//
// When allowed_environments is configured, per-environment aggregates are created
// (e.g., "SchemaBot (staging)") that only roll up checks for that environment.
// Deployments can customize the base name when independent SchemaBot gates share
// a repository.
//
// When allowed_environments is NOT configured, a single aggregate is created
// that rolls up all per-database checks.
//
// Aggregate logic (first match wins):
//   - ANY check "in_progress"     → aggregate status "in_progress"
//   - ANY check "failure"         → aggregate "failure"
//   - ANY check "action_required" → aggregate "action_required"
//   - ALL checks "success"        → aggregate "success"
//   - NO per-database checks      → no aggregate (PR doesn't touch schema)
func (h *Handler) updateAggregateCheckOnce(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string) (aggregateFoldFollowUp, error) {
	if !h.shouldPublishChecks(ctx, repo, "aggregate_check_sync") {
		return aggregateFoldNoFollowUp, nil
	}

	current, err := h.verifyHeadSHACurrency(ctx, client, repo, pr, headSHA, "aggregate_check_sync")
	if !current {
		// A non-current head covers a transient PR-fetch failure (non-nil err)
		// as well as a genuinely newer head (nil err); either way a leader's
		// next re-fold fetches the then-current head, so arm a bounded re-fold
		// instead of stranding an aggregate waiting on a participant re-read.
		// verifyHeadSHACurrency already logged the reason.
		return aggregateFoldScheduleLeaderRefold, err
	}

	checks, err := h.service.Storage().Checks().GetByPR(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "aggregate_check_sync",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to fetch checks for aggregate", "repo", repo, "pr", pr, "error", err)
		// A storage blip must not end a leader's retry chain while its
		// aggregate renders unresolved participants as in_progress; the
		// bounded re-fold retries the read.
		return aggregateFoldScheduleLeaderRefold, fmt.Errorf("fetch checks for aggregate %s#%d: %w", repo, pr, err)
	}

	// Filter out aggregate checks — only per-database checks contribute
	var dbChecks []*storage.Check
	for _, c := range checks {
		if !isAggregateCheck(c) {
			dbChecks = append(dbChecks, c)
		}
	}

	config := h.service.Config()

	// The aggregate-check leader gates its per-environment aggregate on the
	// Check Runs published by participant deployments, so it must fold those in
	// even when it has no per-database checks of its own. Non-leaders (and repos
	// with no aggregate config) never fetch files or fold — their behavior is
	// unchanged.
	var expectedParticipants []api.ExpectedTenant
	if config.IsAggregateLeaderForRepo(repo) {
		files, err := client.FetchPRFiles(ctx, repo, pr)
		if err != nil {
			// The leader cannot determine which participants a PR requires without
			// the changed files. Fail closed: do not publish an aggregate at all,
			// which leaves any existing (still-blocking) aggregate in place rather
			// than downgrading it to a passing success we cannot justify. The read
			// failure is as transient as a failed participant Checks read, so arm
			// a bounded re-fold — otherwise one API blip strands the aggregate
			// until an external event re-folds it.
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:  "aggregate_check_sync",
				Repository: repo,
				Status:     "error",
			})
			h.logger.Error("aggregate leader cannot fetch PR files, not publishing aggregate",
				"repo", repo, "pr", pr, "head_sha", headSHA, "error", err)
			return aggregateFoldScheduleParticipantRefold, fmt.Errorf("fetch PR files for aggregate leader %s#%d: %w", repo, pr, err)
		}
		expectedParticipants = config.ExpectedParticipantChecksForPR(repo, prFilePaths(files))
	}

	// No per-database checks means the PR doesn't touch schema files (or all check
	// records were already deleted by PR close cleanup). With no expected
	// participants to fold either, there is no aggregate to create.
	if len(dbChecks) == 0 && len(expectedParticipants) == 0 {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "aggregate_check_sync",
			Repository: repo,
			Status:     "noop",
		})
		h.logger.Debug("no per-database checks or expected participants for aggregate", "repo", repo, "pr", pr)
		return aggregateFoldNoFollowUp, nil
	}

	checkNameBase := h.aggregateCheckNameForRepo(repo)

	// A retriable participant outcome (not reported yet, or a failed Checks
	// API read) can newly resolve on a later re-read. Participants with no
	// schema work publish their check but never comment, so no nudge will
	// arrive — the leader must re-fold on its own schedule or the aggregate
	// stays blocked on a participant that already reported green. While the
	// re-fold budget lasts, unresolved participants render as in_progress
	// (equally merge-blocking) so the wait is not mislabeled as pending
	// applies.
	participantsRetriable := false
	retryPending := h.participantRefoldBudgetRemaining(repo, pr)

	// Every configured environment is attempted even if an earlier upsert fails,
	// and the failures are joined so the participant follow-up below is still
	// decided from the full fold — a single environment's write failure must not
	// skip the others or drop the re-fold decision.
	var upsertErrs []error

	if len(config.AllowedEnvironments) > 0 {
		// Per-environment aggregates: create one aggregate per allowed environment.
		// Each uses the real environment name in the storage key to avoid collisions
		// between environments (e.g., staging vs production aggregates).
		for _, env := range config.AllowedEnvironments {
			envChecks := filterChecksByEnvironment(dbChecks, env)
			participantChecks, retriable := h.participantCheckOutcomes(ctx, client, repo, pr, env, headSHA, expectedParticipants, retryPending)
			participantsRetriable = participantsRetriable || retriable
			envChecks = append(envChecks, participantChecks...)
			if len(envChecks) == 0 {
				h.logger.Debug("no checks or expected participants for aggregate environment",
					"repo", repo, "pr", pr, "environment", env)
				continue
			}
			checkName := aggregateCheckNameForEnv(checkNameBase, env)
			if err := h.upsertAggregateCheckRunOnce(ctx, client, repo, pr, headSHA, envChecks, checkName, env); err != nil {
				upsertErrs = append(upsertErrs, err)
			}
		}
	} else {
		// Single aggregate. Uses aggregateSentinel for the environment field
		// since there is no per-environment scoping.
		participantChecks, retriable := h.participantCheckOutcomes(ctx, client, repo, pr, aggregateSentinel, headSHA, expectedParticipants, retryPending)
		participantsRetriable = retriable
		aggChecks := make([]*storage.Check, 0, len(dbChecks)+len(participantChecks))
		aggChecks = append(aggChecks, dbChecks...)
		aggChecks = append(aggChecks, participantChecks...)
		if err := h.upsertAggregateCheckRunOnce(ctx, client, repo, pr, headSHA, aggChecks, checkNameBase, aggregateSentinel); err != nil {
			upsertErrs = append(upsertErrs, err)
		}
	}

	if participantsRetriable {
		return aggregateFoldScheduleParticipantRefold, errors.Join(upsertErrs...)
	}
	if len(upsertErrs) > 0 {
		// A failed aggregate publish (the GitHub Check Run write or the storage
		// write recording it) left at least one environment's gate not
		// reflecting this fold. Callers that own a durable retry act on the
		// returned error, but the fire-and-forget entry points (apply and
		// rollback claims, webhook events) have none — arm the bounded re-fold
		// so one transient write failure cannot strand the gate on its previous
		// state, such as a passing aggregate persisting over a just-claimed
		// rollback.
		return aggregateFoldScheduleParticipantRefold, errors.Join(upsertErrs...)
	}
	return aggregateFoldClearParticipantRefoldBudget, nil
}

// anyApplyOwnedInProgress reports whether any contribution is an in-progress
// row an apply owns — the only kind of row a stopped apply can be holding open.
func anyApplyOwnedInProgress(checks []*storage.Check) bool {
	for _, c := range checks {
		if c.Status == checkStatusInProgress && c.ApplyID != 0 {
			return true
		}
	}
	return false
}

// stoppedAppliesForPR resolves which of a PR's applies are stopped, so the fold
// can name a paused apply rather than report it as work in progress.
//
// Folds run on every webhook event and re-fold, and the set is consulted only
// for in-progress apply-owned rows, so a fold with none — the steady state once
// a PR's contributions have all completed — skips the read entirely rather than
// paying for a result it would discard.
//
// A lookup failure returns no set rather than an error: the aggregate's status
// and conclusion do not depend on it, so failing the whole fold would trade a
// less precise title for a check that does not get published at all. The title
// falls back to the state-agnostic wording and the error is logged.
func (h *Handler) stoppedAppliesForPR(ctx context.Context, repo string, pr int, checks []*storage.Check) stoppedApplyIDs {
	if !anyApplyOwnedInProgress(checks) {
		return nil
	}
	applies, err := h.service.Storage().Applies().GetByPR(ctx, repo, pr)
	if err != nil {
		h.logger.Warn("aggregate title will not distinguish stopped applies; failed to load the PR's applies",
			"repo", repo, "pr", pr, "error", err)
		return nil
	}
	var stopped stoppedApplyIDs
	for _, a := range applies {
		if !state.IsState(a.State, state.Apply.Stopped) {
			continue
		}
		if stopped == nil {
			stopped = stoppedApplyIDs{}
		}
		stopped[a.ID] = true
	}
	return stopped
}

// prFilePaths extracts the changed-file paths from a PR file listing for
// expected-participant matching.
func prFilePaths(files []ghclient.PRFile) []string {
	paths := make([]string, 0, len(files))
	for _, f := range files {
		paths = append(paths, f.Filename)
	}
	return paths
}

// upsertAggregateCheckRunOnce computes the aggregate conclusion from the given
// checks and creates or updates a GitHub check run with the specified name.
//
// The environment parameter controls the storage key: for per-environment aggregates
// it is the real environment name (e.g., "staging"), for the global aggregate it is
// aggregateSentinel. DatabaseType and DatabaseName always use aggregateSentinel.
//
// Two fail-closed rules shape the fold:
//   - Stored aggregate state carrying a blocking reason is never replaced by a
//     recompute. Blocking reasons record PR-level guard failures (config
//     discovery, managed-directory coverage, environment coverage) and are
//     released only after the auto-plan guards re-verify the PR
//     (clearAggregateBlocksForVerifiedPR).
//   - Per-database rows recorded for a different commit contribute a blocking
//     in-progress placeholder instead of their stored conclusion, so results
//     computed for a previous commit can never pass the aggregate on the
//     current one.
//
// It records the same operation metrics and logs as before and additionally
// returns the underlying error so a caller with a retry mechanism can react to a
// failed GitHub or storage write. A stored blocking reason is an intentional
// fail-closed skip, not an operational failure, so it returns nil.
func (h *Handler) upsertAggregateCheckRunOnce(
	ctx context.Context, client *ghclient.InstallationClient,
	repo string, pr int, headSHA string,
	dbChecks []*storage.Check, checkName string, environment string,
) error {
	// Look up existing aggregate check state using the environment-specific key.
	existing, err := h.service.Storage().Checks().Get(ctx, repo, pr, environment, aggregateSentinel, aggregateSentinel)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   "aggregate_check_sync",
			Repository:  repo,
			Environment: environment,
			Status:      "error",
		})
		h.logger.Error("failed to look up aggregate check", "repo", repo, "pr", pr, "environment", environment, "error", err)
		return fmt.Errorf("look up aggregate check for %s#%d (env %s): %w", repo, pr, environment, err)
	}

	// A stored blocking reason means a PR-level guard failed the aggregate
	// closed. Recompute paths (apply completion, participant Check Run events,
	// manual plans, stale cleanup) do not re-verify the guard condition, so
	// they must leave both the stored state and the failing Check Run in place.
	if existing != nil && existing.BlockingReason != "" {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   "aggregate_check_sync",
			Repository:  repo,
			Environment: environment,
			Status:      "blocked",
		})
		h.logger.Info("aggregate recompute skipped: stored aggregate state is blocked; the aggregate check will keep blocking merge until auto-plan guards re-verify the PR",
			"repo", repo, "pr", pr, "check_name", checkName,
			"environment", environment, "head_sha", headSHA,
			"blocked_head_sha", existing.HeadSHA,
			"blocking_reason", existing.BlockingReason,
			"check_run_id", existing.CheckRunID)
		return nil
	}

	contributions, staleCount := normalizeStaleContributions(dbChecks, headSHA)
	conclusion, status := computeAggregate(contributions)
	title, summary := aggregateSummary(contributions, conclusion, h.stoppedAppliesForPR(ctx, repo, pr, contributions))
	if staleCount > 0 {
		h.logger.Info("aggregate fold holds rows recorded for another commit as blocking until results land for the current commit",
			"repo", repo, "pr", pr, "check_name", checkName,
			"environment", environment, "head_sha", headSHA, "stale_rows", staleCount)
		if status == checkStatusInProgress && !anyInProgressOnCommit(dbChecks, headSHA) {
			title = awaitingCurrentCommitTitle
		}
	}

	opts := ghclient.CheckRunOptions{
		Name:   checkName,
		Status: status,
		Output: &ghclient.CheckRunOutput{
			Title:   title,
			Summary: summary,
		},
	}
	// GitHub requires conclusion only when status is "completed"
	if status == checkStatusCompleted {
		opts.Conclusion = conclusion
	}

	// Create a new check run if no existing record, or if the HEAD SHA changed
	// (new commit pushed). Updating an old check run tied to a previous SHA is
	// invisible on the PR — GitHub only shows checks for the HEAD commit.
	reuseExistingRun := existing != nil && existing.CheckRunID != 0 && existing.HeadSHA == headSHA

	// Branch protection gates on the newest Check Run for this name on this
	// commit, and stored state can skew from it: the GitHub write and the
	// storage write are not atomic, and concurrent folds can each publish a
	// run. Decide reuse against that newest run's real status, not the stored
	// one — updating the run stored state names can leave a newer, concluded
	// run gating the PR with a conclusion this fold never intended.
	var liveRun *ghclient.CheckRunResult
	if reuseExistingRun {
		run, _, err := client.FindCheckRunByName(ctx, repo, headSHA, checkName)
		if err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: environment,
				Status:      "error",
			})
			h.logger.Error("failed to resolve the newest aggregate check run, not publishing aggregate",
				"repo", repo, "pr", pr, "check_name", checkName,
				"environment", environment, "head_sha", headSHA,
				"stored_check_run_id", existing.CheckRunID, "error", err)
			return fmt.Errorf("resolve newest aggregate check run %q for %s#%d@%s (env %s): %w", checkName, repo, pr, headSHA, environment, err)
		}
		if run == nil {
			// Stored state names a run for this commit that no trusted App run
			// matches on GitHub. Publish a fresh run instead of updating a run
			// this deployment cannot see.
			h.logger.Warn("stored aggregate check run not found on the commit, publishing a fresh aggregate check run",
				"repo", repo, "pr", pr, "check_name", checkName,
				"environment", environment, "head_sha", headSHA,
				"stored_check_run_id", existing.CheckRunID)
			reuseExistingRun = false
		}
		liveRun = run
	}

	// A concluded Check Run cannot be rewound: GitHub does not move a
	// completed run back to in_progress — it applies the new output but keeps
	// the stored conclusion, leaving a passing Check Run whose text says an
	// apply is running. Publish the rewind as a fresh Check Run on the same
	// name and SHA instead; the new in-progress run replaces the concluded one
	// on the PR and in branch protection.
	rewound := reuseExistingRun && rewindsConcludedCheckRun(liveRun, status)
	if rewound {
		h.logger.Info("re-creating aggregate check run: the newest run already concluded and GitHub cannot move it back out of completed",
			"repo", repo, "pr", pr, "check_name", checkName,
			"environment", environment, "head_sha", headSHA,
			"concluded_check_run_id", liveRun.ID,
			"concluded_conclusion", liveRun.Conclusion, "check_status", status)
		reuseExistingRun = false
	}
	var checkRunID int64
	if reuseExistingRun {
		if liveRun.ID != existing.CheckRunID {
			// The newest run is not the one stored state recorded — a previous
			// fold published a run but failed the storage write, or another
			// fold raced this one. Adopt the newest run: it is what gates the
			// PR, and recording it below re-converges stored state with GitHub.
			h.logger.Warn("adopting a newer aggregate check run than the stored one",
				"repo", repo, "pr", pr, "check_name", checkName,
				"environment", environment, "head_sha", headSHA,
				"stored_check_run_id", existing.CheckRunID, "check_run_id", liveRun.ID)
		}
		if err := client.UpdateCheckRun(ctx, repo, liveRun.ID, opts); err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: environment,
				Status:      "error",
			})
			h.logger.Error("failed to update aggregate check run",
				"repo", repo, "pr", pr, "check_name", checkName,
				"environment", environment, "check_run_id", liveRun.ID,
				"head_sha", headSHA, "check_status", status,
				"conclusion", conclusion, "error", err)
			return fmt.Errorf("update aggregate check run %d for %s#%d (env %s): %w", liveRun.ID, repo, pr, environment, err)
		}
		checkRunID = liveRun.ID
	} else {
		if existing != nil && existing.HeadSHA != headSHA {
			h.logger.Info("re-creating aggregate check on new HEAD SHA",
				"repo", repo, "pr", pr,
				"old_sha", existing.HeadSHA, "new_sha", headSHA)
		}
		id, err := client.CreateCheckRun(ctx, repo, headSHA, opts)
		if err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: environment,
				Status:      "error",
			})
			h.logger.Error("failed to create aggregate check run",
				"repo", repo, "pr", pr, "check_name", checkName,
				"environment", environment, "head_sha", headSHA,
				"check_status", status, "conclusion", conclusion, "error", err)
			return fmt.Errorf("create aggregate check run for %s#%d@%s (env %s): %w", repo, pr, headSHA, environment, err)
		}
		if rewound {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: environment,
				Status:      "recreated",
			})
		}
		checkRunID = id
	}

	aggCheck := &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      headSHA,
		Environment:  environment,
		DatabaseType: aggregateSentinel,
		DatabaseName: aggregateSentinel,
		CheckRunID:   checkRunID,
		HasChanges:   conclusion != checkConclusionSuccess,
		Status:       status,
		Conclusion:   conclusion,
	}
	if err := h.service.Storage().Checks().Upsert(ctx, aggCheck); err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   "aggregate_check_sync",
			Repository:  repo,
			Environment: environment,
			Status:      "error",
		})
		h.logger.Error("failed to store aggregate check state",
			"repo", repo, "pr", pr, "check_name", checkName,
			"environment", environment, "check_run_id", checkRunID,
			"head_sha", headSHA, "check_status", status,
			"conclusion", conclusion, "error", err)
		return fmt.Errorf("store aggregate check state for %s#%d (env %s): %w", repo, pr, environment, err)
	}

	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:   "aggregate_check_sync",
		Repository:  repo,
		Environment: environment,
		Status:      "success",
	})
	h.logger.Info("aggregate check updated",
		"repo", repo, "pr", pr, "check_name", checkName,
		"environment", environment, "check_run_id", checkRunID,
		"check_status", status, "conclusion", conclusion,
		"per_database_checks", len(dbChecks))
	return nil
}

// rewindsConcludedCheckRun reports whether publishing the recomputed status to
// the given live Check Run would move an already-concluded run out of
// "completed". The GitHub Checks API does not allow that transition on an
// existing run, so callers must publish the rewind as a fresh Check Run
// instead.
func rewindsConcludedCheckRun(run *ghclient.CheckRunResult, status string) bool {
	return run.Status == checkStatusCompleted && status != checkStatusCompleted
}

// postPassingAggregates posts a passing aggregate check for each allowed environment.
// Called when this instance has no work to do for a PR — either because the PR doesn't
// touch schema files, or because the databases don't have environments this instance
// manages. Without this, branch protection would block indefinitely waiting for a
// check that would never come. It does not publish success over existing
// per-database state that still needs operator attention.
func (h *Handler) postPassingAggregates(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string) {
	const (
		title   = "No managed schema changes"
		summary = "This PR does not contain schema changes managed by SchemaBot."
	)
	if !h.shouldPublishChecks(ctx, repo, "aggregate_check_sync") {
		return
	}

	if !h.verifyHeadSHAStillCurrentForPR(ctx, client, repo, pr, headSHA, "aggregate_check_sync") {
		return
	}

	storedChecks, err := h.service.Storage().Checks().GetByPR(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "aggregate_check_sync",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to fetch checks before passing aggregate", "repo", repo, "pr", pr, "error", err)
		return
	}

	checks := h.aggregateCheckTargetsForRepo(repo)

	h.logger.Debug("posting passing aggregates", "repo", repo, "pr", pr, "head_sha", headSHA, "count", len(checks))

	for _, ec := range checks {
		checkName := ec.name

		if hasBlockingCheckForEnvironment(storedChecks, ec.environment) {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "blocked",
			})
			h.logger.Info("skipping passing aggregate because stored checks still block",
				"repo", repo, "pr", pr, "check_name", checkName, "environment", ec.environment)
			continue
		}

		opts := ghclient.CheckRunOptions{
			Name:       checkName,
			Status:     checkStatusCompleted,
			Conclusion: checkConclusionSuccess,
			Output: &ghclient.CheckRunOutput{
				Title:   title,
				Summary: summary,
			},
		}

		existing, err := h.service.Storage().Checks().Get(ctx, repo, pr, ec.environment, aggregateSentinel, aggregateSentinel)
		if err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "error",
			})
			h.logger.Error("failed to look up aggregate check", "repo", repo, "pr", pr, "env", ec.environment, "error", err)
			continue
		}

		// Skip if already passing for this SHA
		if existing != nil && existing.HeadSHA == headSHA && existing.Conclusion == checkConclusionSuccess {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "noop",
			})
			h.logger.Debug("passing aggregate already exists", "repo", repo, "pr", pr, "check_name", checkName)
			continue
		}

		var checkRunID int64
		if existing != nil && existing.CheckRunID != 0 && existing.HeadSHA == headSHA {
			if err := client.UpdateCheckRun(ctx, repo, existing.CheckRunID, opts); err != nil {
				metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
					Operation:   "aggregate_check_sync",
					Repository:  repo,
					Environment: ec.environment,
					Status:      "error",
				})
				h.logger.Error("failed to update passing aggregate",
					"repo", repo, "pr", pr, "check_name", checkName,
					"environment", ec.environment, "check_run_id", existing.CheckRunID,
					"head_sha", headSHA, "error", err)
				continue
			}
			checkRunID = existing.CheckRunID
		} else {
			id, err := client.CreateCheckRun(ctx, repo, headSHA, opts)
			if err != nil {
				metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
					Operation:   "aggregate_check_sync",
					Repository:  repo,
					Environment: ec.environment,
					Status:      "error",
				})
				h.logger.Error("failed to create passing aggregate",
					"repo", repo, "pr", pr, "check_name", checkName,
					"environment", ec.environment, "head_sha", headSHA, "error", err)
				continue
			}
			checkRunID = id
		}

		aggCheck := &storage.Check{
			Repository:   repo,
			PullRequest:  pr,
			HeadSHA:      headSHA,
			Environment:  ec.environment,
			DatabaseType: aggregateSentinel,
			DatabaseName: aggregateSentinel,
			CheckRunID:   checkRunID,
			HasChanges:   false,
			Status:       checkStatusCompleted,
			Conclusion:   checkConclusionSuccess,
		}
		if err := h.service.Storage().Checks().Upsert(ctx, aggCheck); err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "error",
			})
			h.logger.Error("failed to store passing aggregate check",
				"repo", repo, "pr", pr, "check_name", checkName,
				"environment", ec.environment, "check_run_id", checkRunID,
				"head_sha", headSHA, "error", err)
			continue
		}

		action := "created"
		if existing != nil && existing.CheckRunID != 0 && existing.HeadSHA == headSHA {
			action = "updated"
		}
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   "aggregate_check_sync",
			Repository:  repo,
			Environment: ec.environment,
			Status:      "success",
		})
		h.logger.Info("posted passing aggregate",
			"repo", repo, "pr", pr, "head_sha", headSHA, "check_name", checkName,
			"environment", ec.environment, "action", action)
	}
}

// postFailingAggregates posts a failing aggregate check for each allowed environment
// that has errors. Called when all environments fail during planning so branch
// protection shows a clear failure instead of waiting indefinitely.
func (h *Handler) postFailingAggregates(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, errors map[string]string) {
	h.postFailingAggregatesWithBlock(ctx, client, repo, pr, headSHA, errors, checkBlockReason{})
}

// postFailingAggregatesWithBlock stores a blocking reason only for callers with
// a stable failure class. Generic plan errors should use postFailingAggregates.
func (h *Handler) postFailingAggregatesWithBlock(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, errors map[string]string, block checkBlockReason) {
	if !h.shouldPublishChecks(ctx, repo, "aggregate_check_sync") {
		return
	}

	if !h.verifyHeadSHAStillCurrentForPR(ctx, client, repo, pr, headSHA, "aggregate_check_sync") {
		return
	}

	config := h.service.Config()
	checkNameBase := h.aggregateCheckNameForRepo(repo)

	type envCheck struct {
		name        string
		environment string
	}

	var checks []envCheck
	if len(config.AllowedEnvironments) > 0 {
		for _, env := range config.AllowedEnvironments {
			if _, hasError := errors[env]; hasError {
				checks = append(checks, envCheck{
					name:        aggregateCheckNameForEnv(checkNameBase, env),
					environment: env,
				})
			}
		}
	} else {
		checks = append(checks, envCheck{
			name:        checkNameBase,
			environment: aggregateSentinel,
		})
	}

	if len(checks) == 0 {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "aggregate_check_sync",
			Repository: repo,
			Status:     "noop",
		})
		h.logger.Debug("no failing aggregate checks to post", "repo", repo, "pr", pr)
		return
	}

	for _, ec := range checks {
		// Build summary from the error for this environment
		summary := planFailedCheckText
		if errMsg, ok := errors[ec.environment]; ok {
			summary = errMsg
		} else if len(errors) > 0 {
			// Single-instance mode: use first error
			for _, msg := range errors {
				summary = msg
				break
			}
		}

		blockingReason := block.blockingReason
		errorMessage := ""
		if blockingReason != "" {
			errorMessage = summary
		} else {
			existing, err := h.service.Storage().Checks().Get(ctx, repo, pr, ec.environment, aggregateSentinel, aggregateSentinel)
			if err != nil {
				metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
					Operation:   "aggregate_check_sync",
					Repository:  repo,
					Environment: ec.environment,
					Status:      "error",
				})
				h.logger.Error("failed to look up stored aggregate state before failing aggregate; not publishing so any stored block stays authoritative",
					"repo", repo, "pr", pr, "check_name", ec.name,
					"environment", ec.environment, "head_sha", headSHA, "error", err)
				continue
			}
			if existing != nil && existing.BlockingReason != "" {
				// A generic failure must not replace a stored blocking reason —
				// carry it forward so the block keeps gating until the auto-plan
				// guards re-verify the PR.
				blockingReason = existing.BlockingReason
				errorMessage = existing.ErrorMessage
				h.logger.Info("failing aggregate carries forward stored blocking reason",
					"repo", repo, "pr", pr, "check_name", ec.name,
					"environment", ec.environment, "head_sha", headSHA,
					"blocking_reason", blockingReason)
			}
		}
		summary = sanitizeCheckRunErrorSummary(summary)

		opts := ghclient.CheckRunOptions{
			Name:       ec.name,
			Status:     checkStatusCompleted,
			Conclusion: checkConclusionFailure,
			Output: &ghclient.CheckRunOutput{
				Title:   planFailedCheckText,
				Summary: summary,
			},
		}

		id, err := client.CreateCheckRun(ctx, repo, headSHA, opts)
		if err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "error",
			})
			h.logger.Error("failed to create failing aggregate", "repo", repo, "pr", pr, "error", err)
			continue
		}

		aggCheck := &storage.Check{
			Repository:     repo,
			PullRequest:    pr,
			HeadSHA:        headSHA,
			Environment:    ec.environment,
			DatabaseType:   aggregateSentinel,
			DatabaseName:   aggregateSentinel,
			CheckRunID:     id,
			HasChanges:     false,
			Status:         checkStatusCompleted,
			Conclusion:     checkConclusionFailure,
			BlockingReason: blockingReason,
			ErrorMessage:   errorMessage,
		}
		if err := h.service.Storage().Checks().Upsert(ctx, aggCheck); err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "error",
			})
			h.logger.Error("failed to store failing aggregate check", "error", err)
			continue
		}

		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   "aggregate_check_sync",
			Repository:  repo,
			Environment: ec.environment,
			Status:      "success",
		})
		h.logger.Info("posted failing aggregate",
			"repo", repo, "pr", pr, "check_name", ec.name, "env", ec.environment)
	}
}

// planFailedCheckText is the fixed title and fallback summary a failing plan
// aggregate Check Run shows when no more specific error text is available.
const planFailedCheckText = "Plan failed"

// checkRunSummaryEscaper neutralizes the markup that renders in a Check Run
// summary: HTML tags, plus the Markdown constructs that can carry a payload —
// links and images (brackets), which would let error text phish or fire an
// outbound request from every viewer, and code spans (backticks). Entity
// references decode on render but cannot form Markdown structure, so the
// displayed text is unchanged. Quotes are left alone — unlike
// html.EscapeString — as a source-readability choice: they need no
// neutralization here and operator-facing prose stays byte-verbatim.
// Emphasis characters are also left alone; they can restyle text but cannot
// inject content or reach out.
var checkRunSummaryEscaper = strings.NewReplacer(
	"&", "&amp;", "<", "&lt;", ">", "&gt;",
	"[", "&#91;", "]", "&#93;", "`", "&#96;",
)

// sanitizeCheckRunErrorSummary makes error text safe for a Check Run summary:
// single-line sanitization (endpoint redaction, control stripping, clamping)
// plus markup escaping. Text that sanitizes to nothing falls back to a fixed
// summary so the Check Run never publishes with an empty summary.
func sanitizeCheckRunErrorSummary(summary string) string {
	sanitized := templates.SanitizeInlineError(summary)
	if sanitized == "" {
		return planFailedCheckText
	}
	return checkRunSummaryEscaper.Replace(sanitized)
}

// clearAggregateBlocksForVerifiedPR releases stored aggregate blocking reasons
// after the auto-plan guards re-verified the PR at headSHA: config discovery
// succeeded, every schema change under a server-managed directory resolved a
// config, and — re-checked here — every discovered database resolves to at
// least one allowed environment. Recompute paths never release blocks, so this
// is the only way a blocked aggregate can start passing again; the auto-plan
// that follows publishes the fresh aggregate state.
//
// The storage clear is conditional on the head SHA and reason of the row that
// was read, so a block recorded concurrently (for example by a newer commit's
// guards on another pod) stays authoritative.
func (h *Handler) clearAggregateBlocksForVerifiedPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, configs []ghclient.DiscoveredConfig) {
	for _, cfg := range configs {
		environments, err := h.allowedDatabaseEnvironments(cfg.Config.Database)
		if err != nil {
			h.logger.Warn("stored aggregate blocks retained: cannot re-verify allowed environments for database; the aggregate check will keep blocking merge",
				"repo", repo, "pr", pr, "head_sha", headSHA,
				"database", cfg.Config.Database, "error", err)
			return
		}
		if len(environments) == 0 {
			h.logger.Info("stored aggregate blocks retained: database has no allowed configured environments; the aggregate check will keep blocking merge",
				"repo", repo, "pr", pr, "head_sha", headSHA, "database", cfg.Config.Database)
			return
		}
	}

	if !h.verifyHeadSHAStillCurrentForPR(ctx, client, repo, pr, headSHA, "aggregate_block_clear") {
		return
	}

	for _, target := range h.aggregateCheckTargetsForRepo(repo) {
		existing, err := h.service.Storage().Checks().Get(ctx, repo, pr, target.environment, aggregateSentinel, aggregateSentinel)
		if err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_block_clear",
				Repository:  repo,
				Environment: target.environment,
				Status:      "error",
			})
			h.logger.Error("failed to look up aggregate check for block clear; any stored block stays in place",
				"repo", repo, "pr", pr, "environment", target.environment,
				"head_sha", headSHA, "error", err)
			continue
		}
		if existing == nil || existing.BlockingReason == "" {
			h.logger.Debug("no stored aggregate block to clear",
				"repo", repo, "pr", pr, "environment", target.environment, "head_sha", headSHA)
			continue
		}
		cleared, err := h.service.Storage().Checks().ClearAggregateBlock(ctx, existing)
		if err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_block_clear",
				Repository:  repo,
				Environment: target.environment,
				Status:      "error",
			})
			h.logger.Error("failed to clear aggregate block after guard re-verification; the aggregate check will keep blocking merge",
				"repo", repo, "pr", pr, "environment", target.environment,
				"head_sha", headSHA, "blocking_reason", existing.BlockingReason, "error", err)
			continue
		}
		if !cleared {
			// The row changed between the read and the conditional write —
			// another writer re-blocked or moved the aggregate. That newer
			// stored state stays authoritative.
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_block_clear",
				Repository:  repo,
				Environment: target.environment,
				Status:      "noop",
			})
			h.logger.Info("aggregate block not cleared because stored state changed concurrently; the newer stored state stays authoritative",
				"repo", repo, "pr", pr, "environment", target.environment,
				"head_sha", headSHA, "blocked_head_sha", existing.HeadSHA,
				"blocking_reason", existing.BlockingReason)
			continue
		}
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   "aggregate_block_clear",
			Repository:  repo,
			Environment: target.environment,
			Status:      "success",
		})
		h.logger.Info("cleared aggregate block after guards re-verified the PR",
			"repo", repo, "pr", pr, "environment", target.environment,
			"head_sha", headSHA, "blocked_head_sha", existing.HeadSHA,
			"blocking_reason", existing.BlockingReason)
	}
}
