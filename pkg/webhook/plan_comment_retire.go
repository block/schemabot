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
// and retires the prior comments in the same slot that it supersedes.
// Tracking and retirement failures never affect the posted comment: every
// failure mode here leaves extra comments visible on the PR, never a hidden
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
		// never be auto-retired, so it stays expanded until an operator
		// hides it — still retire its predecessors below.
		h.logger.Error("failed to record posted plan comment; it will stay expanded when superseded",
			"repo", repo, "pr", pr, "database", slot.Database, "database_type", slot.DatabaseType,
			"head_sha", slot.HeadSHA, "comment_id", commentID, "error", err)
	}

	h.retireSupersededPlanComments(ctx, client, posted)
}

// retireSupersededPlanComments retires the still-visible plan comments that
// the newly posted comment supersedes, per planCommentSupersedes.
func (h *Handler) retireSupersededPlanComments(ctx context.Context, client *ghclient.InstallationClient, posted *storage.PlanComment) {
	h.retirePlanCommentsForSlot(ctx, client,
		posted.Repository, posted.PullRequest, posted.DatabaseName, posted.DatabaseType, posted.HeadSHA, posted)
}

// retireStalePlanComments retires the slot's still-visible plan comments
// rendered at a head other than the current one, for plan outcomes that
// supersede prior comments without posting a new comment — an auto-plan
// resolving to no changes still moves the head past every older comment,
// whose pending DDL and apply prompt no longer match the branch. Same-head
// comments stay expanded: one may be the only visible plan for its
// environment scope.
func (h *Handler) retireStalePlanComments(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, database, databaseType, headSHA string) {
	if database == "" || headSHA == "" {
		// Without a database and head there is no slot identity to sweep;
		// treating the empty identity as a slot could retire untracked
		// error-only comments across databases.
		h.logger.Info("skipping stale plan comment retirement because no database or head resolved to key the slot",
			"repo", repo, "pr", pr, "database", database, "database_type", databaseType, "head_sha", headSHA)
		return
	}

	if !h.planSweepHeadIsCurrent(ctx, client, repo, pr, headSHA,
		"database", database, "database_type", databaseType) {
		return
	}

	h.retirePlanCommentsForSlot(ctx, client, repo, pr, database, databaseType, headSHA, nil)
}

// retireStalePlanCommentsForPR retires every plan comment still visible on
// the PR that renders a head other than the current one, without needing a
// database to key a slot. A delivery that discovers no schema config — or whose
// discovery failed — has no slot to sweep, yet an earlier head's plan comment is
// exactly what it leaves behind: still showing that head's DDL and its apply
// prompt, with only the check run moving on. Same-head comments stay expanded,
// so this never hides the plan for the commit the PR is currently at.
func (h *Handler) retireStalePlanCommentsForPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string) {
	if headSHA == "" {
		h.logger.Info("skipping PR-wide stale plan comment sweep because the delivery carries no head to compare against",
			"repo", repo, "pr", pr)
		return
	}

	if !h.planSweepHeadIsCurrent(ctx, client, repo, pr, headSHA) {
		return
	}

	priors, err := h.service.Storage().PlanComments().ListUnretiredForRepoPR(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to list the PR's plan comments; superseded plan comments stay visible until the next supersede sweep",
			"repo", repo, "pr", pr, "head_sha", headSHA, "error", err)
		return
	}
	h.retireSupersededPlanCommentRows(ctx, client, priors, headSHA, nil)
}

// planSweepHeadIsCurrent reports whether headSHA is still the PR's head, for a
// sweep with no newly posted comment anchoring it. The sweep's head comes from
// the delivery's cached PR fetch, so a concurrent push can make it stale:
// sweeping on the old head would retire the newer head's live comment with
// nothing replacing it — minimized comments are never automatically restored,
// and deleted comments cannot be restored at all. On a fetch failure or a
// moved head nothing is retired — the newer head's own plan outcome sweeps
// instead. attrs name what is being swept.
func (h *Handler) planSweepHeadIsCurrent(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, attrs ...any) bool {
	sweepAttrs := append([]any{"repo", repo, "pr", pr, "head_sha", headSHA}, attrs...)

	freshPR, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to verify PR head for stale plan comment sweep; prior plan comments stay visible until the next supersede sweep",
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

// retirePlanCommentsForSlot sweeps the slot's still-visible plan comments and
// retires the superseded ones. posted is the newly posted comment when the
// sweep follows a post: its own row is skipped and supersession follows
// planCommentSupersedes. When posted is nil the sweep follows an outcome with
// no new comment, and only comments from heads other than headSHA are
// superseded. Every failure keeps the comment on the PR and its row
// unretired, so the next sweep retries it.
func (h *Handler) retirePlanCommentsForSlot(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, database, databaseType, headSHA string, posted *storage.PlanComment) {
	slotAttrs := []any{
		"repo", repo, "pr", pr,
		"database", database, "database_type", databaseType,
		"head_sha", headSHA,
	}

	priors, err := h.service.Storage().PlanComments().ListUnretiredForSlot(ctx,
		repo, pr, database, databaseType)
	if err != nil {
		h.logger.Error("failed to list prior plan comments; superseded plan comments stay visible until the next supersede sweep",
			append(slotAttrs, "error", err)...)
		return
	}

	h.retireSupersededPlanCommentRows(ctx, client, priors, headSHA, posted)
}

// retireSupersededPlanCommentRows retires the superseded comments among
// priors. posted is the newly posted comment when the sweep follows a post:
// its own row is skipped and supersession follows planCommentSupersedes. When
// posted is nil the sweep follows an outcome with no new comment, and only
// comments from heads other than headSHA are superseded.
//
// How a superseded comment is retired depends on the repository's configured
// policy. Under the delete-based policy, a comment whose head an apply owns
// is minimized — the apply makes its plan the operational record of what ran,
// so the comment stays on the timeline, collapsed but expandable — while a
// comment no apply ever acted on carries no record worth keeping (its DDL
// never ran and is reproducible from the head it was rendered at) and is
// deleted from the timeline; its storage row keeps the identifiers for
// triage. Under the default minimize-based policy, an apply-owned comment
// stays fully expanded and every other superseded comment is minimized.
// Every failure leaves the comment as it is and its row unretired, so the
// next sweep retries it.
func (h *Handler) retireSupersededPlanCommentRows(ctx context.Context, client *ghclient.InstallationClient, priors []*storage.PlanComment, headSHA string, posted *storage.PlanComment) {
	for _, prior := range priors {
		// Skip the comment that was just posted (its row is in the list too).
		if posted != nil && (prior.ID == posted.ID || prior.GitHubCommentID == posted.GitHubCommentID) {
			continue
		}
		priorAttrs := planCommentAttrs(prior)
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
			h.logger.Error("failed to check apply ownership for superseded plan comment; leaving it untouched",
				append(priorAttrs, "error", err)...)
			metrics.RecordPlanCommentRetirement(ctx, prior.Repository, "guard_error")
			continue
		}

		if h.service.Config().DeletesUnactionedPlanComments() {
			if applyOwned {
				h.minimizeSupersededPlanComment(ctx, client, prior)
			} else {
				h.deleteUnactionedPlanComment(ctx, client, prior)
			}
			continue
		}

		// Minimize-based policy: an apply-owned comment stays fully expanded
		// — the apply makes it the operational record of what ran, and hiding
		// it costs more than the noise it saves.
		if applyOwned {
			h.logger.Info("keeping superseded plan comment expanded: an apply owns its head", priorAttrs...)
			metrics.RecordPlanCommentRetirement(ctx, prior.Repository, "apply_owned")
			continue
		}
		h.minimizeSupersededPlanComment(ctx, client, prior)
	}
}

// minimizeSupersededPlanComment collapses a superseded plan comment in the PR
// timeline. The comment stays expandable, so nothing is lost — minimizing
// hides the noise while keeping the record.
func (h *Handler) minimizeSupersededPlanComment(ctx context.Context, client *ghclient.InstallationClient, prior *storage.PlanComment) {
	priorAttrs := planCommentAttrs(prior)
	if err := client.MinimizeComment(ctx, prior.Repository, prior.GitHubNodeID); err != nil {
		h.logger.Error("failed to minimize superseded plan comment; it stays expanded and is retried on the next supersede sweep",
			append(priorAttrs, "error", err)...)
		metrics.RecordPlanCommentRetirement(ctx, prior.Repository, "minimize_error")
		return
	}
	if err := h.service.Storage().PlanComments().MarkMinimized(ctx, prior.ID); err != nil {
		// GitHub already minimized the comment; leaving the row unretired
		// only means the next supersede re-minimizes it, which is idempotent.
		h.logger.Error("minimized plan comment on GitHub but failed to record it; the next supersede re-minimizes it",
			append(priorAttrs, "error", err)...)
		metrics.RecordPlanCommentRetirement(ctx, prior.Repository, "minimize_mark_error")
		return
	}
	metrics.RecordPlanCommentRetirement(ctx, prior.Repository, "minimized")
	h.logger.Info("minimized superseded plan comment", priorAttrs...)
}

// deleteUnactionedPlanComment removes a superseded plan comment no apply ever
// acted on. The storage row survives with deleted_at stamped, so the
// comment's identifiers stay available for triage even though the timeline
// entry is gone.
func (h *Handler) deleteUnactionedPlanComment(ctx context.Context, client *ghclient.InstallationClient, prior *storage.PlanComment) {
	priorAttrs := planCommentAttrs(prior)
	if err := client.DeleteIssueComment(ctx, prior.Repository, prior.GitHubCommentID); err != nil {
		h.logger.Error("failed to delete unactioned superseded plan comment; it stays visible and is retried on the next supersede sweep",
			append(priorAttrs, "error", err)...)
		metrics.RecordPlanCommentRetirement(ctx, prior.Repository, "delete_error")
		return
	}
	if err := h.service.Storage().PlanComments().MarkDeleted(ctx, prior.ID); err != nil {
		// GitHub already deleted the comment; leaving the row unretired only
		// means the next supersede re-deletes it, and the delete treats an
		// already-gone comment as deleted, so the retry converges.
		h.logger.Error("deleted plan comment on GitHub but failed to record it; the next supersede retries the mark",
			append(priorAttrs, "error", err)...)
		metrics.RecordPlanCommentRetirement(ctx, prior.Repository, "delete_mark_error")
		return
	}
	metrics.RecordPlanCommentRetirement(ctx, prior.Repository, "deleted")
	h.logger.Info("deleted unactioned superseded plan comment", priorAttrs...)
}

// planCommentAttrs builds the triage log attributes identifying one tracked
// plan comment.
func planCommentAttrs(prior *storage.PlanComment) []any {
	return []any{
		"repo", prior.Repository, "pr", prior.PullRequest,
		"database", prior.DatabaseName, "database_type", prior.DatabaseType,
		"environment_scope", prior.EnvironmentScope, "head_sha", prior.HeadSHA,
		"comment_id", prior.GitHubCommentID,
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
