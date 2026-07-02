package webhook

import (
	"context"
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
