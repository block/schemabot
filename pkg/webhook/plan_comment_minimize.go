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
// the newly posted comment supersedes. A comment whose plan became an apply
// is kept expanded: the apply makes it the operational record of what ran,
// and hiding it costs more than the noise it saves. Every failure keeps the
// comment expanded and its row unminimized, so the next supersede retries it.
func (h *Handler) minimizeSupersededPlanComments(ctx context.Context, client *ghclient.InstallationClient, posted *storage.PlanComment) {
	slotAttrs := []any{
		"repo", posted.Repository, "pr", posted.PullRequest,
		"database", posted.DatabaseName, "database_type", posted.DatabaseType,
		"head_sha", posted.HeadSHA,
	}

	priors, err := h.service.Storage().PlanComments().ListUnminimizedForSlot(ctx,
		posted.Repository, posted.PullRequest, posted.DatabaseName, posted.DatabaseType)
	if err != nil {
		h.logger.Error("failed to list prior plan comments; superseded plan comments stay expanded until the next plan comment posts",
			append(slotAttrs, "error", err)...)
		return
	}

	for _, prior := range priors {
		// Skip the comment that was just posted (its row is in the list too).
		if prior.ID == posted.ID || prior.GitHubCommentID == posted.GitHubCommentID {
			continue
		}
		priorAttrs := []any{
			"repo", prior.Repository, "pr", prior.PullRequest,
			"database", prior.DatabaseName, "database_type", prior.DatabaseType,
			"environment_scope", prior.EnvironmentScope, "head_sha", prior.HeadSHA,
			"comment_id", prior.GitHubCommentID,
		}
		if !planCommentSupersedes(posted, prior) {
			h.logger.Debug("keeping plan comment expanded: not superseded (same head, different environment scope)", priorAttrs...)
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
			h.logger.Error("failed to minimize superseded plan comment; it stays expanded and is retried on the next plan comment",
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
