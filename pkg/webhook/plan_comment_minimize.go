package webhook

import (
	"context"
	"sort"
	"strings"
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// planCommentSlot identifies the stream of plan comments a posted comment
// belongs to — one database on one PR — plus the head and environments the
// comment renders. Comments in the same slot supersede each other per
// planCommentSupersedes; manual and auto-plan comments share the slot.
type planCommentSlot struct {
	Database     string
	DatabaseType string
	Environments []string
	HeadSHA      string
}

// environmentScope canonicalizes the slot's environments (sorted,
// comma-joined) so the same set always compares equal regardless of the
// order the caller assembled it in.
func (s planCommentSlot) environmentScope() string {
	envs := append([]string(nil), s.Environments...)
	sort.Strings(envs)
	return strings.Join(envs, ",")
}

// postTrackedPlanComment posts a plan comment, records it in plan_comments,
// and minimizes the prior comments in the same slot that it supersedes.
// Tracking and minimize failures never affect the posted comment: every
// failure mode here leaves extra comments expanded on the PR, never a hidden
// or lost record.
func (h *Handler) postTrackedPlanComment(repo string, pr int, installationID int64, slot planCommentSlot, body string) {
	if slot.Database == "" || slot.HeadSHA == "" {
		// A plan whose every environment failed renders an error-only comment
		// with no resolved database or head, so there is no slot identity to
		// track. Post it untracked: an untracked comment only stays expanded,
		// while tracking it under an empty identity would let error-only
		// comments for different databases supersede each other.
		h.logger.Info("posting plan comment untracked because no database or head resolved to key the slot",
			"repo", repo, "pr", pr, "database", slot.Database, "database_type", slot.DatabaseType,
			"head_sha", slot.HeadSHA)
		h.postComment(repo, pr, installationID, body)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for plan comment",
			"repo", repo, "pr", pr, "installation_id", installationID, "error", err)
		return
	}

	commentID, nodeID, err := client.CreateIssueComment(ctx, repo, pr, h.renderPRComment(body))
	if err != nil {
		h.logger.Error("failed to post plan comment",
			"repo", repo, "pr", pr, "installation_id", installationID, "error", err)
		return
	}

	posted := &storage.PlanComment{
		Repository:       repo,
		PullRequest:      pr,
		DatabaseName:     slot.Database,
		DatabaseType:     slot.DatabaseType,
		EnvironmentScope: slot.environmentScope(),
		HeadSHA:          slot.HeadSHA,
		GitHubCommentID:  commentID,
		GitHubNodeID:     nodeID,
	}
	if err := h.service.Storage().PlanComments().Insert(ctx, posted); err != nil {
		// The comment is live on GitHub either way. Without a row it will
		// never be auto-minimized, so it stays expanded until an operator
		// hides it — still retire its predecessors below.
		h.logger.Error("failed to record posted plan comment; it will stay expanded when superseded",
			"repo", repo, "pr", pr, "database", slot.Database, "database_type", slot.DatabaseType,
			"head_sha", slot.HeadSHA, "comment_id", commentID, "error", err)
	}

	h.minimizeSupersededPlanComments(ctx, client, posted)
}

// minimizeSupersededPlanComments collapses the unminimized plan comments that
// the newly posted comment supersedes, per planCommentSupersedes.
func (h *Handler) minimizeSupersededPlanComments(ctx context.Context, client *ghclient.InstallationClient, posted *storage.PlanComment) {
	h.minimizePlanCommentsForSlot(ctx, client,
		posted.Repository, posted.PullRequest, posted.DatabaseName, posted.DatabaseType, posted.HeadSHA, posted)
}

// minimizeStalePlanComments collapses the slot's unminimized plan comments
// rendered at a head other than the current one, for plan outcomes that
// supersede prior comments without posting a new comment — an auto-plan
// resolving to no changes still moves the head past every older comment,
// whose pending DDL and apply prompt no longer match the branch. Same-head
// comments stay expanded: one may be the only visible plan for its
// environment scope.
func (h *Handler) minimizeStalePlanComments(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, database, databaseType, headSHA string) {
	if database == "" || headSHA == "" {
		// Without a database and head there is no slot identity to sweep;
		// treating the empty identity as a slot could collapse untracked
		// error-only comments across databases.
		h.logger.Info("skipping stale plan comment minimize because no database or head resolved to key the slot",
			"repo", repo, "pr", pr, "database", database, "database_type", databaseType, "head_sha", headSHA)
		return
	}

	if !h.planSweepHeadIsCurrent(ctx, client, repo, pr, headSHA,
		"database", database, "database_type", databaseType) {
		return
	}

	h.minimizePlanCommentsForSlot(ctx, client, repo, pr, database, databaseType, headSHA, nil)
}

// minimizeStalePlanCommentsForPR collapses every plan comment still expanded on
// the PR that renders a head other than the current one, without needing a
// database to key a slot. A delivery that discovers no schema config — or whose
// discovery failed — has no slot to sweep, yet an earlier head's plan comment is
// exactly what it leaves behind: still showing that head's DDL and its apply
// prompt, with only the check run moving on. Same-head comments stay expanded,
// so this never hides the plan for the commit the PR is currently at.
func (h *Handler) minimizeStalePlanCommentsForPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string) {
	if headSHA == "" {
		h.logger.Info("skipping PR-wide stale plan comment sweep because the delivery carries no head to compare against",
			"repo", repo, "pr", pr)
		return
	}

	if !h.planSweepHeadIsCurrent(ctx, client, repo, pr, headSHA) {
		return
	}

	priors, err := h.service.Storage().PlanComments().ListUnminimizedForRepoPR(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to list the PR's plan comments; superseded plan comments stay expanded until the next supersede sweep",
			"repo", repo, "pr", pr, "head_sha", headSHA, "error", err)
		return
	}
	h.minimizeSupersededPlanCommentRows(ctx, client, priors, headSHA, nil)
}

// planSweepHeadIsCurrent reports whether headSHA is still the PR's head, for a
// sweep with no newly posted comment anchoring it. The sweep's head comes from
// the delivery's cached PR fetch, so a concurrent push can make it stale:
// sweeping on the old head would collapse the newer head's live comment with
// nothing replacing it, and no un-minimize path exists. On a fetch failure or a
// moved head nothing is hidden — the newer head's own plan outcome sweeps
// instead. attrs name what is being swept.
func (h *Handler) planSweepHeadIsCurrent(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, attrs ...any) bool {
	sweepAttrs := append([]any{"repo", repo, "pr", pr, "head_sha", headSHA}, attrs...)

	freshPR, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to verify PR head for stale plan comment sweep; prior plan comments stay expanded until the next supersede sweep",
			append(sweepAttrs, "error", err)...)
		return false
	}
	if freshPR.HeadSHA != headSHA {
		h.logger.Info("skipping stale plan comment sweep because the PR head moved past this plan outcome; the current head's own plan outcome sweeps instead",
			append(sweepAttrs, "current_head_sha", freshPR.HeadSHA)...)
		return false
	}
	return true
}

// minimizePlanCommentsForSlot sweeps the slot's unminimized plan comments and
// collapses the superseded ones. posted is the newly posted comment when the
// sweep follows a post: its own row is skipped and supersession follows
// planCommentSupersedes. When posted is nil the sweep follows an outcome with
// no new comment, and only comments from heads other than headSHA are
// superseded. A comment whose plan became an apply is kept expanded: the
// apply makes it the operational record of what ran, and hiding it costs more
// than the noise it saves. Every failure keeps the comment expanded and its
// row unminimized, so the next sweep retries it.
func (h *Handler) minimizePlanCommentsForSlot(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, database, databaseType, headSHA string, posted *storage.PlanComment) {
	slotAttrs := []any{
		"repo", repo, "pr", pr,
		"database", database, "database_type", databaseType,
		"head_sha", headSHA,
	}

	priors, err := h.service.Storage().PlanComments().ListUnminimizedForSlot(ctx,
		repo, pr, database, databaseType)
	if err != nil {
		h.logger.Error("failed to list prior plan comments; superseded plan comments stay expanded until the next supersede sweep",
			append(slotAttrs, "error", err)...)
		return
	}

	h.minimizeSupersededPlanCommentRows(ctx, client, priors, headSHA, posted)
}

// minimizeSupersededPlanCommentRows collapses the superseded comments among
// priors. posted is the newly posted comment when the sweep follows a post: its
// own row is skipped and supersession follows planCommentSupersedes. When
// posted is nil the sweep follows an outcome with no new comment, and only
// comments from heads other than headSHA are superseded.
func (h *Handler) minimizeSupersededPlanCommentRows(ctx context.Context, client *ghclient.InstallationClient, priors []*storage.PlanComment, headSHA string, posted *storage.PlanComment) {
	for _, prior := range priors {
		// Skip the comment that was just posted (its row is in the list too).
		if posted != nil && (prior.ID == posted.ID || prior.GitHubCommentID == posted.GitHubCommentID) {
			continue
		}
		priorAttrs := []any{
			"repo", prior.Repository, "pr", prior.PullRequest,
			"database", prior.DatabaseName, "database_type", prior.DatabaseType,
			"environment_scope", prior.EnvironmentScope, "head_sha", prior.HeadSHA,
			"comment_id", prior.GitHubCommentID,
		}
		if posted != nil {
			if !planCommentSupersedes(posted, prior) {
				h.logger.Debug("keeping plan comment expanded: not superseded (same head, different environment scope)", priorAttrs...)
				continue
			}
		} else if prior.HeadSHA == headSHA {
			h.logger.Debug("keeping plan comment expanded: it renders the current head", priorAttrs...)
			continue
		}

		applyOwned, err := h.service.Storage().Applies().ExistsForDatabaseHead(ctx,
			prior.Repository, prior.PullRequest, prior.DatabaseName, prior.DatabaseType, prior.HeadSHA)
		if err != nil {
			h.logger.Error("failed to check apply ownership for superseded plan comment; keeping it expanded",
				append(priorAttrs, "error", err)...)
			metrics.RecordPlanCommentMinimize(ctx, prior.Repository, "guard_error")
			continue
		}
		if applyOwned {
			h.logger.Info("keeping superseded plan comment expanded: an apply owns its head", priorAttrs...)
			metrics.RecordPlanCommentMinimize(ctx, prior.Repository, "apply_owned")
			continue
		}

		if err := client.MinimizeComment(ctx, prior.Repository, prior.GitHubNodeID); err != nil {
			h.logger.Error("failed to minimize superseded plan comment; it stays expanded and is retried on the next supersede sweep",
				append(priorAttrs, "error", err)...)
			metrics.RecordPlanCommentMinimize(ctx, prior.Repository, "minimize_error")
			continue
		}
		if err := h.service.Storage().PlanComments().MarkMinimized(ctx, prior.ID); err != nil {
			// GitHub already minimized the comment; leaving the row
			// unminimized only means the next supersede re-minimizes it,
			// which is idempotent.
			h.logger.Error("minimized plan comment on GitHub but failed to record it; the next supersede re-minimizes it",
				append(priorAttrs, "error", err)...)
			metrics.RecordPlanCommentMinimize(ctx, prior.Repository, "mark_error")
			continue
		}
		metrics.RecordPlanCommentMinimize(ctx, prior.Repository, "minimized")
		h.logger.Info("minimized superseded plan comment", priorAttrs...)
	}
}

// planCommentSupersedes reports whether the newly posted plan comment makes a
// prior comment in the same slot outdated. Any comment for a different head
// is stale — the plan it shows no longer matches the PR branch. On the same
// head, a comment is only replaced by one covering the same environment
// scope; a differently-scoped comment may be the sole visible plan for its
// environments, so it stays expanded.
func planCommentSupersedes(posted, prior *storage.PlanComment) bool {
	if prior.HeadSHA != posted.HeadSHA {
		return true
	}
	return prior.EnvironmentScope == posted.EnvironmentScope
}
