package webhook

import (
	"context"
	"time"

	"github.com/block/schemabot/pkg/webhook/templates"
)

// defaultUnclaimedCommandGrace is how long the aggregate leader waits before deciding
// that an apply-scoped control command went unclaimed. A deployment that owns
// the named apply acknowledges the comment at its act point, which is one
// storage read after the command arrives, so the grace only has to cover
// ordinary webhook and storage latency across sibling deployments.
const defaultUnclaimedCommandGrace = 30 * time.Second

func (h *Handler) unclaimedCommandGrace() time.Duration {
	if h.unclaimedCommandGraceOverride > 0 {
		return h.unclaimedCommandGraceOverride
	}
	return defaultUnclaimedCommandGrace
}

// answerUnclaimedControlCommand replies to an apply-scoped control command that
// named an apply this deployment does not have, once it can tell that no
// sibling deployment had it either.
//
// On a repository several SchemaBot deployments serve, an unscoped command
// reaches all of them and each one that does not own the named apply stays
// quiet so only the owner answers (AZ-5). That is right whenever an owner
// exists. When none does — a mistyped identifier, one from another repository,
// or one an engine reported rather than SchemaBot — every deployment defers to
// an owner that is never going to speak, and the operator is left with a
// command that produced nothing at all.
//
// The acknowledgment reaction is what resolves it. It is the one signal about
// this command that every deployment can read, and per UX-2 a deployment adds
// it only once it has decided the command is its work. So the leader waits for
// the grace period, re-reads the comment, and answers only if nothing claimed
// it. It marks the comment as it answers, both because answering is itself
// acting on the command and so that a redelivered webhook sees the claim and
// stays quiet rather than posting a second copy.
//
// Only the leader does this: participants hold a partial view of the fleet and
// several of them replying would be the duplicate noise fan-out exists to
// avoid. The reply is operator visibility, never a gate — every failure along
// the way leaves the command exactly as silent as it is today, and says so in
// the logs.
func (h *Handler) answerUnclaimedControlCommand(repo string, pr int, installationID int64, requestedBy, command string, result CommandResult) {
	config, ok := h.serverConfig()
	if !ok {
		h.logger.Warn("cannot tell whether a control command went unclaimed because server config is unavailable",
			"command", command, "repo", repo, "pr", pr,
			"apply_id", result.ApplyID, "environment", result.Environment)
		return
	}
	if !config.IsAggregateLeaderForRepo(repo) {
		h.logger.Debug("leaving an unclaimed control command for the aggregate leader to answer",
			"command", command, "repo", repo, "pr", pr,
			"apply_id", result.ApplyID, "environment", result.Environment)
		return
	}
	if result.CommentID <= 0 {
		h.logger.Warn("cannot tell whether a control command went unclaimed because it carries no comment to read",
			"command", command, "repo", repo, "pr", pr,
			"apply_id", result.ApplyID, "environment", result.Environment)
		return
	}

	h.goSafe(repo, pr, installationID, result.DeliveryID, func() {
		grace := h.unclaimedCommandGrace()
		ctx, cancel := context.WithTimeout(context.Background(), grace+commandTimeout)
		defer cancel()

		select {
		case <-time.After(grace):
		case <-ctx.Done():
			return
		}

		client, err := h.clientForRepo(repo, installationID)
		if err != nil {
			h.logger.Error("failed to create GitHub client to check whether a control command went unclaimed",
				"command", command, "repo", repo, "pr", pr,
				"apply_id", result.ApplyID, "environment", result.Environment, "error", err)
			return
		}
		claimed, err := client.CommentHasReaction(ctx, repo, result.CommentID, commandAcknowledgmentReaction)
		if err != nil {
			h.logger.Error("failed to read command acknowledgment; leaving the command unanswered rather than answering one a sibling deployment may own",
				"command", command, "repo", repo, "pr", pr,
				"apply_id", result.ApplyID, "environment", result.Environment, "error", err)
			return
		}
		if claimed {
			h.logger.Info("control command naming an apply this deployment does not have was claimed elsewhere",
				"command", command, "repo", repo, "pr", pr,
				"apply_id", result.ApplyID, "environment", result.Environment)
			return
		}

		// Mark before posting. A crash between the two leaves the command as
		// silent as it would have been without this path, where posting first
		// would let a redelivery add a second copy.
		if err := client.AddReactionToComment(ctx, repo, result.CommentID, commandAcknowledgmentReaction); err != nil {
			h.logger.Error("failed to mark an unclaimed control command as answered; leaving it unanswered rather than risking a duplicate reply",
				"command", command, "repo", repo, "pr", pr,
				"apply_id", result.ApplyID, "environment", result.Environment, "error", err)
			return
		}

		h.logger.Warn("no deployment claimed a control command; answering it as the aggregate leader",
			"command", command, "repo", repo, "pr", pr,
			"apply_id", result.ApplyID, "environment", result.Environment, "requested_by", requestedBy)
		h.postComment(repo, pr, installationID, templates.RenderUnclaimedControlCommand(templates.UnclaimedControlCommandData{
			Command:     command,
			ApplyID:     result.ApplyID,
			Environment: result.Environment,
			RequestedBy: requestedBy,
		}))
	})
}
