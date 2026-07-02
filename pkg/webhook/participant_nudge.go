package webhook

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/metrics"
)

// defaultParticipantNudgeRefoldDelay is the pause before the second re-fold
// after a participant-comment nudge. A participant posts its terminal summary
// comment moments before its Check Run updates, so a single immediate fold can
// read pre-terminal state; the delayed pass converges without a standing
// poller.
const defaultParticipantNudgeRefoldDelay = 45 * time.Second

// participantCommentNudge treats a trusted sibling SchemaBot deployment's PR
// comment as a re-fold trigger for this deployment's aggregate check. The
// comment is a doorbell, not a message: its body is never parsed or trusted.
// GitHub delivers check_run events only to the App that created the check, so
// the leader never hears a participant's check complete directly — but
// issue_comment events are repo-scoped, and participants comment at exactly
// the moments their Check Runs change (plan posted, apply summary, rollback).
// The fold re-reads every participant's authoritative Check Run via the
// trusted API path, so a spurious nudge can cause at most wasted work, never a
// wrongly-passing check.
//
// Returns false when the comment is not a nudge — not a led repo, or not a
// trusted sibling's bot — so the caller keeps its normal bot handling.
func (h *Handler) participantCommentNudge(ctx context.Context, repo string, pr int, installationID int64, authorLogin string) bool {
	if h.service == nil || !h.service.Config().IsAggregateLeaderForRepo(repo) {
		return false
	}
	if !h.isTrustedParticipantBotLogin(repo, authorLogin) {
		return false
	}

	h.logger.Info("trusted participant comment triggered aggregate re-fold",
		"repo", repo, "pr", pr, "author", authorLogin)
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:  "participant_comment_nudge",
		Repository: repo,
		Status:     "success",
	})

	h.goSafe(repo, pr, installationID, func() {
		h.refoldAggregateForPR(repo, pr, installationID, "participant comment")
	})
	time.AfterFunc(h.nudgeRefoldDelay(), func() {
		h.goSafe(repo, pr, installationID, func() {
			h.refoldAggregateForPR(repo, pr, installationID, "participant comment delayed pass")
		})
	})
	return true
}

// isTrustedParticipantBotLogin reports whether a comment author is a trusted
// sibling SchemaBot deployment's GitHub App bot: login "<slug>[bot]" for a
// slug in the repo's trusted-check-app-slugs — the same trust set that gates
// the aggregate fold's Check Run reads.
func (h *Handler) isTrustedParticipantBotLogin(repo, login string) bool {
	slug, ok := strings.CutSuffix(login, "[bot]")
	if !ok {
		return false
	}
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	return slices.Contains(config.TrustedCheckAppSlugsForRepo(repo), slug)
}

func (h *Handler) nudgeRefoldDelay() time.Duration {
	if h.participantNudgeRefoldDelay > 0 {
		return h.participantNudgeRefoldDelay
	}
	return defaultParticipantNudgeRefoldDelay
}

// maxParticipantRefoldAttempts bounds the self-scheduled re-folds a single
// PR's aggregate arms while expected participants remain unresolved. The
// budget resets whenever a fold resolves every participant, so it only caps
// consecutive misses — a participant that never reports leaves the aggregate
// blocked (fail-closed) until a nudge, new commit, or manual plan re-folds.
const maxParticipantRefoldAttempts = 3

// scheduleParticipantRefold arms a delayed aggregate re-fold because at least
// one expected participant resolved as retriable (not reported yet, or a
// failed Checks API read). Participants publish their Check Runs moments after
// the leader's first fold, and a participant with no schema work never
// comments, so without this the leader's aggregate would stay blocked on a
// participant that already reported green. Attempts are bounded per PR so a
// participant that is genuinely down cannot keep a timer chain alive forever.
func (h *Handler) scheduleParticipantRefold(ctx context.Context, repo string, pr int, installationID int64) {
	key := participantRefoldKey(repo, pr)

	h.participantRefoldMu.Lock()
	attempts := h.participantRefoldAttempts[key]
	if attempts >= maxParticipantRefoldAttempts {
		h.participantRefoldMu.Unlock()
		h.logger.Warn("expected participants still unresolved after scheduled re-folds; aggregate stays blocked until a participant comment, new commit, or manual plan re-folds it",
			"repo", repo, "pr", pr, "refold_attempts", attempts)
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "participant_refold",
			Repository: repo,
			Status:     "exhausted",
		})
		return
	}
	if h.participantRefoldAttempts == nil {
		h.participantRefoldAttempts = make(map[string]int)
	}
	h.participantRefoldAttempts[key] = attempts + 1
	h.participantRefoldMu.Unlock()

	h.logger.Info("expected participants have not resolved; scheduling aggregate re-fold",
		"repo", repo, "pr", pr, "delay", h.nudgeRefoldDelay(), "attempt", attempts+1)
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:  "participant_refold",
		Repository: repo,
		Status:     "scheduled",
	})

	time.AfterFunc(h.nudgeRefoldDelay(), func() {
		h.goSafe(repo, pr, installationID, func() {
			h.refoldAggregateForPR(repo, pr, installationID, "unresolved participant delayed re-fold")
		})
	})
}

// clearParticipantRefoldBudget resets the re-fold budget once a fold resolves
// every expected participant, so a later commit on the same PR gets a fresh
// budget.
func (h *Handler) clearParticipantRefoldBudget(repo string, pr int) {
	key := participantRefoldKey(repo, pr)
	h.participantRefoldMu.Lock()
	delete(h.participantRefoldAttempts, key)
	h.participantRefoldMu.Unlock()
}

func participantRefoldKey(repo string, pr int) string {
	return fmt.Sprintf("%s#%d", repo, pr)
}

// refoldAggregateForPR re-reads the PR's current head and re-runs the
// aggregate fold against it. The head is fetched fresh on every pass so a
// nudge raced by a new commit folds the commit that is actually current;
// updateAggregateCheck independently verifies head freshness before writing.
func (h *Handler) refoldAggregateForPR(repo string, pr int, installationID int64, trigger string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for aggregate re-fold",
			"repo", repo, "pr", pr, "trigger", trigger, "error", err)
		return
	}
	prInfo, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to fetch PR head for aggregate re-fold",
			"repo", repo, "pr", pr, "trigger", trigger, "error", err)
		return
	}
	h.updateAggregateCheck(ctx, client, repo, pr, prInfo.HeadSHA)
}
