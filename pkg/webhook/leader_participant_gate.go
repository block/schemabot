package webhook

import (
	"context"
	"fmt"
	"slices"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/checkstate"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// checkRunFinder is the narrow slice of the GitHub client the aggregate leader
// needs to fold participant deployments' Check Runs into its own aggregate. It
// returns the newest trusted-app Check Run by name, the slugs of any same-named
// but untrusted apps seen, and an error when ownership cannot be verified.
// *ghclient.InstallationClient satisfies it; tests use a fake.
type checkRunFinder interface {
	FindCheckRunByName(ctx context.Context, repo, headSHA, checkName string) (*ghclient.CheckRunResult, []string, error)
}

// participantCheckOutcome is the resolved state of one participant's Check Run
// for a single environment, ready to fold into the aggregate.
type participantCheckOutcome struct {
	tenant     string
	checkName  string
	status     string // checkStatus*
	conclusion string // checkConclusion* (empty when in_progress)
	// retriable marks an outcome that a later re-read can improve without any
	// config change: the participant has not reported yet, the Checks API
	// read failed, or the participant's run has not reached a terminal state.
	// Untrusted-only and unresolvable-name outcomes are not retriable — only
	// a config fix can clear those.
	retriable bool
}

// unresolved reports whether the outcome blocks only because the participant's
// Check Run could not be read (not reported yet, or a failed Checks API read).
// A trusted non-terminal run is retriable too — a later re-read can observe it
// finish — but it is not unresolved: its live in_progress status is already
// the right thing to render, so it must not be relabeled as a reporting gap.
func (o participantCheckOutcome) unresolved() bool {
	return o.retriable && o.status == checkStatusCompleted
}

// participantUnresolvedBlockingReason marks a synthesized participant row whose
// Check Run could not be read yet (not reported, or a failed Checks API read).
// It drives the aggregate's title and the tenant table's status label so an
// unresolved participant reads as "waiting to report" / "not reported" rather
// than as a pending or failed apply. Synthesized rows are never persisted, so
// the marker never reaches stored check state.
const participantUnresolvedBlockingReason = checkstate.BlockParticipantUnresolved

// synthesizeParticipantCheck maps a resolved participant outcome to a stored
// Check shape so computeAggregate folds it exactly like a per-database check.
// DatabaseType is the aggregate sentinel and DatabaseName is the tenant so the
// synthesized row is distinguishable in the aggregate table while still
// contributing its status/conclusion to the fold.
//
// An unresolved outcome with re-fold budget remaining renders as in_progress:
// a scheduled re-fold will re-read the participant within seconds, and
// in_progress blocks merge exactly as hard as action_required without
// mislabeling the wait as pending applies. Once the budget is spent the row
// lands action_required — the participant is genuinely not reporting and an
// operator needs to look. A retriable outcome that did read a live run (a
// trusted non-terminal check) keeps that run's own status.
func synthesizeParticipantCheck(outcome participantCheckOutcome, repo string, pr int, headSHA, env string, retryPending bool) *storage.Check {
	check := &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      headSHA,
		Environment:  env,
		DatabaseType: aggregateSentinel,
		DatabaseName: outcome.tenant,
		HasChanges:   outcome.conclusion != checkConclusionSuccess,
		Status:       outcome.status,
		Conclusion:   outcome.conclusion,
	}
	if outcome.unresolved() {
		check.BlockingReason = participantUnresolvedBlockingReason
		if retryPending {
			check.Status = checkStatusInProgress
			check.Conclusion = ""
		}
	}
	return check
}

// blockedParticipantOutcome builds a fail-closed outcome that folds to
// action_required so any resolution uncertainty blocks the aggregate.
// retriable distinguishes uncertainty a later re-read can resolve (participant
// not reported yet, transient API failure) from uncertainty only a config
// change can (untrusted App, unresolvable name).
func blockedParticipantOutcome(tenant, checkName string, retriable bool) participantCheckOutcome {
	return participantCheckOutcome{
		tenant:     tenant,
		checkName:  checkName,
		status:     checkStatusCompleted,
		conclusion: checkConclusionActionRequired,
		retriable:  retriable,
	}
}

// participantCheckName resolves the Check Run name a participant publishes for
// env. It mirrors how the aggregate publisher names its own check: when the
// leader has no allowed environments (env is the aggregate sentinel) the
// participant's Check Run uses the unscoped base name; otherwise it is
// env-scoped, exactly as aggregateCheckNameForEnv builds it.
func participantCheckName(base, env string) string {
	if env == aggregateSentinel {
		return base
	}
	return aggregateCheckNameForEnv(base, env)
}

// participantCheckOutcomes resolves every expected participant's per-environment
// Check Run for headSHA and returns synthesized stored Check rows to fold into
// the aggregate leader's check for env, plus whether any outcome is retriable —
// blocked only because the participant has not reported yet, the read failed,
// or its run is not yet terminal — so a scheduled re-fold can converge without
// any external event. It fails closed: anything other than a trusted,
// completed, success participant check yields a blocking row.
//
// resolveParticipantOutcome is the fail-closed truth table:
//   - unresolvable check name           → action_required (blocks)
//   - Checks API error                  → action_required (blocks, retriable)
//   - only untrusted same-named runs    → action_required (blocks)
//   - no trusted run on the commit      → action_required (blocks, retriable)
//   - trusted run, non-terminal         → in_progress     (blocks, retriable)
//   - trusted run, completed+success    → success         (passes)
//   - trusted run, completed+
//     action_required                   → action_required (blocks: tenant apply pending)
//   - trusted run, completed+other      → failure         (blocks)
func (h *Handler) participantCheckOutcomes(
	ctx context.Context, client checkRunFinder,
	repo string, pr int, env, headSHA string,
	expected []api.ExpectedTenant, retryPending bool,
) ([]*storage.Check, bool) {
	checks := make([]*storage.Check, 0, len(expected))
	retriable := false
	for _, tenant := range expected {
		coverageEnvironment := env
		if env == aggregateSentinel {
			coverageEnvironment = ""
		}
		if !tenant.PublishesEnvironment(coverageEnvironment) {
			h.logger.Debug("participant does not publish checks for aggregate environment",
				"repo", repo, "pr", pr, "environment", env, "tenant", tenant.Tenant)
			continue
		}
		outcome := h.resolveParticipantOutcome(ctx, client, repo, pr, env, headSHA, tenant)
		retriable = retriable || outcome.retriable
		checks = append(checks, synthesizeParticipantCheck(outcome, repo, pr, headSHA, env, retryPending))
	}
	return checks, retriable
}

// participantsExpectedOnHead returns the participants an unscoped leader folds
// on headSHA: every participant the PR's proposed files select, plus every
// participant only its listed files select that already published a Check Run
// on headSHA which is not completed and successful.
//
// Narrowing the listed files to what the PR proposes against the default
// branch can drop a participant on the same head commit, when the default
// branch acquires a change the PR carried. A participant that never reported
// has nothing to protect and is dropped. One that did report keeps its own
// verdict on the head: a run still in progress (an apply may be under way) or
// one that failed must keep blocking the aggregate (MG-1, MG-6), so it stays
// expected and the fold renders it like any other participant. A failed read
// is returned as an error: the caller cannot tell which case it is in.
func (h *Handler) participantsExpectedOnHead(
	ctx context.Context, client checkRunFinder,
	repo string, pr int, headSHA string,
	listed, proposed []api.ExpectedTenant,
) ([]api.ExpectedTenant, error) {
	expected := slices.Clone(proposed)
	for _, tenant := range listed {
		if slices.ContainsFunc(proposed, func(p api.ExpectedTenant) bool { return p.Tenant == tenant.Tenant }) {
			continue
		}
		reported, err := h.participantHasNonPassingRunOnHead(ctx, client, repo, pr, headSHA, tenant)
		if err != nil {
			return nil, err
		}
		if reported {
			expected = append(expected, tenant)
		}
	}
	return expected, nil
}

// participantHasNonPassingRunOnHead reports whether a participant the proposed
// files no longer select published a trusted Check Run on headSHA that is not
// completed and successful, or has a same-named run there only from untrusted
// Apps. It uses the same trusted read as the fold and agrees with the fold on
// both: each of them blocks the aggregate.
func (h *Handler) participantHasNonPassingRunOnHead(
	ctx context.Context, client checkRunFinder,
	repo string, pr int, headSHA string,
	tenant api.ExpectedTenant,
) (bool, error) {
	if tenant.CheckName == "" {
		// Config validation requires a check name; without one the leader
		// cannot read the participant's run, so it stays expected and the fold
		// blocks on it.
		h.logger.Warn("participant dropped by narrowing has no check name, keeping it expected",
			"repo", repo, "pr", pr, "head_sha", headSHA, "tenant", tenant.Tenant)
		return true, nil
	}
	checkName := participantCheckName(tenant.CheckName, aggregateSentinel)
	result, untrusted, err := client.FindCheckRunByName(ctx, repo, headSHA, checkName)
	if err != nil {
		return false, fmt.Errorf("read Check Run %q of participant %s the PR no longer proposes work for on %s@%s: %w", checkName, tenant.Tenant, repo, headSHA, err)
	}
	if result == nil && len(untrusted) > 0 {
		// The fold blocks on a same-named run from an untrusted App, so keep the
		// participant expected and let the fold render that block.
		h.logger.Warn("participant the PR no longer proposes work for has a Check Run on the head only from untrusted GitHub Apps, keeping it expected",
			"repo", repo, "pr", pr, "head_sha", headSHA, "tenant", tenant.Tenant,
			"check_name", checkName, "untrusted_app_slugs", untrusted)
		return true, nil
	}
	if result == nil {
		h.logger.Info("participant the PR no longer proposes work for has no Check Run on the head, not expecting it",
			"repo", repo, "pr", pr, "head_sha", headSHA, "tenant", tenant.Tenant, "check_name", checkName)
		return false, nil
	}
	if result.Status == checkStatusCompleted && result.Conclusion == checkConclusionSuccess {
		h.logger.Info("participant the PR no longer proposes work for passed on the head, not expecting it",
			"repo", repo, "pr", pr, "head_sha", headSHA, "tenant", tenant.Tenant, "check_name", checkName)
		return false, nil
	}
	h.logger.Info("participant the PR no longer proposes work for has a Check Run on the head that is not passing, keeping it expected",
		"repo", repo, "pr", pr, "head_sha", headSHA, "tenant", tenant.Tenant, "check_name", checkName,
		"check_status", result.Status, "check_conclusion", result.Conclusion)
	return true, nil
}

func (h *Handler) resolveParticipantOutcome(
	ctx context.Context, client checkRunFinder,
	repo string, pr int, env, headSHA string,
	tenant api.ExpectedTenant,
) participantCheckOutcome {
	if tenant.CheckName == "" {
		// Config validation requires a check name on every leader-expected tenant.
		// Fail closed anyway: without a name the leader cannot resolve the
		// participant's Check Run, so the aggregate must block.
		h.logger.Warn("aggregate leader cannot resolve participant check name, blocking aggregate",
			"repo", repo, "pr", pr, "environment", env, "head_sha", headSHA, "tenant", tenant.Tenant)
		metrics.RecordLeaderParticipantGate(ctx, repo, env, tenant.Tenant, metrics.LeaderParticipantGateError)
		return blockedParticipantOutcome(tenant.Tenant, "", false)
	}

	checkName := participantCheckName(tenant.CheckName, env)

	result, untrusted, err := client.FindCheckRunByName(ctx, repo, headSHA, checkName)
	if err != nil {
		h.logger.Error("failed to query participant check for aggregate leader, blocking aggregate",
			"repo", repo, "pr", pr, "environment", env, "head_sha", headSHA,
			"tenant", tenant.Tenant, "check_name", checkName, "error", err)
		metrics.RecordLeaderParticipantGate(ctx, repo, env, tenant.Tenant, metrics.LeaderParticipantGateError)
		return blockedParticipantOutcome(tenant.Tenant, checkName, true)
	}

	if result == nil && len(untrusted) > 0 {
		// A same-named Check Run exists on this commit but only from untrusted
		// Apps — re-running the participant cannot fix this; only trusting the
		// participant's App (or removing a spoofed check) can.
		h.logger.Warn("participant check exists only from untrusted GitHub Apps, blocking aggregate",
			"repo", repo, "pr", pr, "environment", env, "head_sha", headSHA,
			"tenant", tenant.Tenant, "check_name", checkName, "untrusted_app_slugs", untrusted)
		for _, appSlug := range untrusted {
			metrics.RecordUntrustedAggregateNamedCheck(ctx, repo, env, appSlug, metrics.CheckTrustGateLeaderFold)
		}
		metrics.RecordLeaderParticipantGate(ctx, repo, env, tenant.Tenant, metrics.LeaderParticipantGateUntrusted)
		return blockedParticipantOutcome(tenant.Tenant, checkName, false)
	}

	if result == nil {
		h.logger.Warn("expected participant has not reported its check, blocking aggregate",
			"repo", repo, "pr", pr, "environment", env, "head_sha", headSHA,
			"tenant", tenant.Tenant, "check_name", checkName)
		metrics.RecordLeaderParticipantGate(ctx, repo, env, tenant.Tenant, metrics.LeaderParticipantGateMissing)
		return blockedParticipantOutcome(tenant.Tenant, checkName, true)
	}

	if result.Status != checkStatusCompleted {
		// A run that flips terminal moments later may never comment (a
		// participant with no schema work publishes success silently), so the
		// leader must re-read on its own schedule to observe the finish.
		h.logger.Debug("participant check is not yet terminal, aggregate stays in progress",
			"repo", repo, "pr", pr, "environment", env, "head_sha", headSHA,
			"tenant", tenant.Tenant, "check_name", checkName, "check_status", result.Status)
		metrics.RecordLeaderParticipantGate(ctx, repo, env, tenant.Tenant, metrics.LeaderParticipantGateInProgress)
		return participantCheckOutcome{
			tenant:    tenant.Tenant,
			checkName: checkName,
			status:    checkStatusInProgress,
			retriable: true,
		}
	}

	if result.Conclusion == checkConclusionSuccess {
		h.logger.Debug("participant check succeeded, does not block aggregate",
			"repo", repo, "pr", pr, "environment", env, "head_sha", headSHA,
			"tenant", tenant.Tenant, "check_name", checkName)
		metrics.RecordLeaderParticipantGate(ctx, repo, env, tenant.Tenant, metrics.LeaderParticipantGateSuccess)
		return participantCheckOutcome{
			tenant:     tenant.Tenant,
			checkName:  checkName,
			status:     checkStatusCompleted,
			conclusion: checkConclusionSuccess,
		}
	}

	if result.Conclusion == checkConclusionActionRequired {
		// The participant is blocking on its own pending work (typically an
		// apply awaiting an operator), not a failure. Fold it as action_required
		// so the leader's aggregate reads as pending rather than failed.
		h.logger.Warn("participant check reports pending work, blocking aggregate",
			"repo", repo, "pr", pr, "environment", env, "head_sha", headSHA,
			"tenant", tenant.Tenant, "check_name", checkName)
		metrics.RecordLeaderParticipantGate(ctx, repo, env, tenant.Tenant, metrics.LeaderParticipantGatePending)
		return participantCheckOutcome{
			tenant:     tenant.Tenant,
			checkName:  checkName,
			status:     checkStatusCompleted,
			conclusion: checkConclusionActionRequired,
		}
	}

	h.logger.Warn("participant check completed without success, blocking aggregate",
		"repo", repo, "pr", pr, "environment", env, "head_sha", headSHA,
		"tenant", tenant.Tenant, "check_name", checkName, "check_conclusion", result.Conclusion)
	metrics.RecordLeaderParticipantGate(ctx, repo, env, tenant.Tenant, metrics.LeaderParticipantGateFailure)
	return participantCheckOutcome{
		tenant:     tenant.Tenant,
		checkName:  checkName,
		status:     checkStatusCompleted,
		conclusion: checkConclusionFailure,
	}
}
