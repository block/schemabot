package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// webhookPayload represents the relevant fields from a GitHub webhook payload.
type webhookPayload struct {
	Action string `json:"action"`
	Issue  *struct {
		Number      int `json:"number"`
		PullRequest *struct {
			URL string `json:"url"`
		} `json:"pull_request"`
	} `json:"issue"`
	Comment *struct {
		ID   int64  `json:"id"`
		Body string `json:"body"`
		User *struct {
			Login string `json:"login"`
			Type  string `json:"type"`
		} `json:"user"`
	} `json:"comment"`
	Repository *struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Installation *struct {
		ID int64 `json:"id"`
	} `json:"installation"`
}

// handleIssueComment processes GitHub issue comment webhooks.
func (h *Handler) handleIssueComment(ctx context.Context, metricApp string, w http.ResponseWriter, body []byte) {
	var payload webhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid webhook payload")
		return
	}

	// Only process "created" comment events on PRs
	if payload.Action != "created" ||
		payload.Issue == nil ||
		payload.Issue.PullRequest == nil ||
		payload.Comment == nil ||
		payload.Repository == nil {
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "event ignored (not a PR comment creation)",
		})
		return
	}

	var payloadInstallationID int64
	if payload.Installation != nil {
		payloadInstallationID = payload.Installation.ID
	}
	// Repo-level webhook deliveries carry no installation id in the payload; the
	// dispatcher resolves it and stashes it on the context.
	installationID := h.effectiveInstallationID(ctx, payloadInstallationID)

	repo := payload.Repository.FullName
	pr := payload.Issue.Number
	requestedBy := ""
	if payload.Comment.User != nil {
		requestedBy = payload.Comment.User.Login
	}

	// Ignore comments from bots to prevent infinite loops. The one exception is
	// a trusted sibling SchemaBot deployment's comment on a repo this
	// deployment leads: it is consumed as an aggregate re-fold nudge — never
	// parsed as a command — because participants comment at exactly the moments
	// their Check Runs change, and GitHub delivers check_run events only to the
	// App that created the check.
	if payload.Comment.User != nil && payload.Comment.User.Type == "Bot" {
		if h.participantCommentNudge(ctx, repo, pr, installationID, requestedBy) {
			h.writeJSON(w, http.StatusOK, map[string]string{
				"message": "participant comment triggered aggregate re-fold",
			})
			return
		}
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "event ignored (comment from bot)",
		})
		return
	}

	// Parse command
	parser := NewCommandParser()
	result := parser.ParseCommand(payload.Comment.Body)

	if !result.IsMention {
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "no SchemaBot command found",
		})
		return
	}

	// Reject commands from repositories not in the configured allowlist
	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("webhook from unregistered repository",
			"event", "issue_comment",
			"action", payload.Action,
			"repo", repo,
			"pr", pr,
			"installation_id", installationID,
			"requested_by", requestedBy)
		metrics.RecordUnregisteredRepositoryWebhook(ctx, metricApp, "issue_comment", payload.Action, repo)
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "repository not registered",
		})
		return
	}

	if result.TenantError {
		h.logger.Info("ignoring command with invalid tenant flag",
			"repo", repo, "pr", pr, "action", result.Action)
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "invalid tenant flag",
		})
		return
	}

	// When a command names a tenant, only the matching isolated deployment should
	// react or post comments. This mirrors allowed_environments routing for -e.
	if result.Tenant != "" && h.service != nil && !h.service.Config().ShouldRespondToTenant(result.Tenant) {
		h.logger.Info("ignoring command for non-owned tenant",
			"repo", repo, "pr", pr, "tenant", result.Tenant, "action", result.Action)
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "tenant handled by another instance",
		})
		return
	}
	if result.Tenant == "" && commandRequiresTenantTarget(result) && h.service != nil && h.service.Config().Tenant != "" {
		if h.fansOutUnscopedCommand(repo) && unscopedCommandFansOut(result) {
			h.logger.Info("aggregate participant fanning out unscoped work command; applying its own databases",
				"repo", repo, "pr", pr, "tenant", h.service.Config().Tenant, "action", result.Action)
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:  "aggregate_participant_fanout",
				Repository: repo,
				Status:     "success",
			})
		} else {
			h.logger.Info("ignoring work command without tenant target",
				"repo", repo, "pr", pr, "tenant", h.service.Config().Tenant, "action", result.Action)
			h.writeJSON(w, http.StatusOK, map[string]string{
				"message": "tenant target required",
			})
			return
		}
	}

	// Handle help command
	if result.IsHelp {
		if result.Tenant == "" && h.service != nil && !h.service.Config().ShouldRespondToUnscoped() {
			h.logger.Debug("skipping help command (respond_to_unscoped is false)", "repo", repo, "pr", pr)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "unscoped command skipped"})
			return
		}
		h.logger.Info("processing help command", "repo", repo, "pr", pr)
		h.postComment(repo, pr, installationID, templates.RenderHelpComment())
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "help posted"})
		return
	}

	// Handle missing -e flag
	if result.MissingEnv {
		if result.Action == action.Plan {
			// Plan without -e: run for all configured environments
			h.logger.Info("plan without -e flag", "repo", repo, "pr", pr)
			h.goSafe(repo, pr, installationID, func() {
				h.handleMultiEnvPlan(repo, pr, result.Database, result.Tenant, installationID, requestedBy, false, true)
			})
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "multi-env plan started"})
			return
		}
		if result.Action == action.Rollback {
			if result.ApplyID == "" {
				h.postComment(repo, pr, installationID, templates.RenderRollbackMissingArguments())
				h.writeJSON(w, http.StatusOK, map[string]string{"message": "missing rollback arguments"})
				return
			}
			h.postComment(repo, pr, installationID, templates.RenderRollbackMissingEnv())
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "missing environment flag"})
			return
		}
		if result.Action == action.RollbackConfirm {
			h.postComment(repo, pr, installationID, templates.RenderRollbackMissingEnv())
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "missing environment flag"})
			return
		}
		h.postComment(repo, pr, installationID, templates.RenderMissingEnv(result.Action))
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "missing environment flag"})
		return
	}

	// When allowed_environments is configured, silently ignore commands targeting
	// environments handled by another instance. The other SchemaBot instance will
	// process the command from its own webhook delivery.
	if result.Found && result.Environment != "" && h.service != nil && !h.service.Config().IsEnvironmentAllowed(result.Environment) {
		h.logger.Info("ignoring command for non-allowed environment",
			"repo", repo, "pr", pr, "environment", result.Environment, "action", result.Action)
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "environment handled by another instance",
		})
		return
	}

	if result.Found && result.Action == action.Rollback && result.ApplyID == "" {
		h.postComment(repo, pr, installationID, templates.RenderRollbackMissingApplyID(h.deploymentTenant()))
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "missing apply ID"})
		return
	}

	// Handle invalid command (schemabot mentioned but command not recognized)
	if !result.Found {
		if result.Tenant == "" && h.service != nil && !h.service.Config().ShouldRespondToUnscoped() {
			h.logger.Debug("skipping invalid command response (respond_to_unscoped is false)", "repo", repo, "pr", pr)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "unscoped command skipped"})
			return
		}
		h.postComment(repo, pr, installationID, templates.RenderInvalidCommand())
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "invalid command"})
		return
	}

	if installationID == 0 {
		h.writeError(w, http.StatusBadRequest, "missing installation ID in webhook payload")
		return
	}

	// Add acknowledgment reaction now that we know this instance will handle
	// the command. Placed after all skip/filter checks so only the owning
	// instance reacts — avoids duplicate reactions in multi-instance setups.
	if payload.Comment.ID > 0 && h.ghClients.Len() > 0 {
		h.goSafe(repo, pr, installationID, func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			client, err := h.clientForRepo(repo, installationID)
			if err != nil {
				h.logger.Error("failed to create GitHub client for reaction", "error", err)
				return
			}
			if err := client.AddReactionToComment(ctx, repo, payload.Comment.ID, "eyes"); err != nil {
				h.logger.Error("failed to add acknowledgment reaction", "error", err)
			}
		})
	}

	// Reject -y/--yes on commands that don't support it
	if result.Action != action.Apply && parser.HasAutoConfirmFlag(payload.Comment.Body) {
		h.postComment(repo, pr, installationID, templates.RenderUnsupportedAutoConfirm(result.Action))
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "unsupported flag"})
		return
	}
	if result.Action == action.Rollback && parser.HasDeferCutoverFlag(payload.Comment.Body) {
		h.postCommandError(repo, pr, installationID, action.Rollback, result.Environment, requestedBy,
			"`--defer-cutover` belongs on `schemabot rollback-confirm`, after reviewing the rollback plan.")
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "unsupported flag"})
		return
	}

	if !commandSupportsDatabaseFlag(result.Action) && parser.HasDatabaseFlag(payload.Comment.Body) {
		h.postComment(repo, pr, installationID, templates.RenderUnsupportedDatabaseFlag(result.Action))
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "unsupported flag"})
		return
	}

	h.logger.Info("processing command",
		"action", result.Action,
		"environment", result.Environment,
		"repo", repo,
		"pr", pr,
	)

	switch result.Action {
	case action.Plan:
		h.handlePlanCommand(w, repo, pr, result.Environment, result.Database, result.Tenant, installationID, requestedBy)
	case action.Help:
		h.postComment(repo, pr, installationID, templates.RenderHelpComment())
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "help posted"})
	case action.Apply:
		h.goSafe(repo, pr, installationID, func() {
			h.handleApplyCommand(repo, pr, result.Environment, result.Database, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "apply started"})
	case action.ApplyConfirm:
		h.goSafe(repo, pr, installationID, func() {
			h.handleApplyConfirmCommand(repo, pr, result.Environment, result.Database, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "apply-confirm started"})
	case action.Unlock:
		h.goSafe(repo, pr, installationID, func() {
			h.handleUnlockCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "unlock started"})
	case action.Rollback:
		h.goSafe(repo, pr, installationID, func() {
			h.handleRollbackCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "rollback started"})
	case action.RollbackConfirm:
		h.goSafe(repo, pr, installationID, func() {
			h.handleRollbackConfirmCommand(repo, pr, result.Environment, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "rollback-confirm started"})
	case action.Stop:
		h.goSafe(repo, pr, installationID, func() {
			h.handleStopCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "stop started"})
	case action.Cancel:
		h.goSafe(repo, pr, installationID, func() {
			h.handleCancelCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "cancel started"})
	case action.Start:
		h.goSafe(repo, pr, installationID, func() {
			h.handleStartCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "start started"})
	case action.Release:
		h.goSafe(repo, pr, installationID, func() {
			h.handleReleaseCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "release started"})
	case action.Cutover:
		h.goSafe(repo, pr, installationID, func() {
			h.handleCutoverCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "cutover started"})
	case action.SkipRevert:
		h.goSafe(repo, pr, installationID, func() {
			h.handleSkipRevertCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "skip-revert started"})
	case action.Revert:
		h.goSafe(repo, pr, installationID, func() {
			h.handleRevertCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "revert started"})
	default:
		h.postComment(repo, pr, installationID, templates.RenderInvalidCommand())
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "invalid command"})
	}
}

func commandRequiresTenantTarget(result CommandResult) bool {
	return !result.IsHelp && (result.Found || result.MissingEnv)
}

// fansOutUnscopedCommand reports whether this deployment should self-serve an
// unscoped work command (no -t tenant) for repo by applying its own databases,
// rather than ignoring it. An aggregate participant fans out: an unscoped
// `apply -e <env>` on a shared repo reaches every participant, and each applies
// only its own databases (its own registry filtered by repo/env/allowed_dirs).
// A tenanted deployment that is not a participant for repo keeps ignoring
// unscoped work commands, since per-tenant routing requires an explicit -t.
func (h *Handler) fansOutUnscopedCommand(repo string) bool {
	if h.service == nil {
		return false
	}
	return h.service.Config().AggregateRoleForRepo(repo) == api.AggregateRoleParticipant
}

// actionFansOutUnscoped reports whether an action is one a participant can serve
// without an explicit -t, by acting on its own databases. plan, apply, and
// apply-confirm route by environment/database, so each participant handles its
// own share of a shared PR; unlock releases only the participant's own database
// locks (locks are keyed by database, not by apply). Commands that target a
// single apply owned by one tenant — rollback and the lifecycle controls (stop,
// cancel, start, release, cutover, skip-revert, revert) — are not in this set:
// an unscoped one would reach every participant and all but the owner would
// report "apply not found", so they require an explicit -t instead.
func actionFansOutUnscoped(a string) bool {
	switch a {
	case action.Plan, action.Apply, action.ApplyConfirm, action.Unlock:
		return true
	default:
		return false
	}
}

// unscopedCommandFansOut reports whether an unscoped (no -t) command is one a
// participant should actually act on when fanning out, as opposed to an error
// case it should stay silent on. Only fan-out actions qualify (see
// actionFansOutUnscoped). A complete command (Found) fans out, and a plan
// without -e fans out as a multi-env plan. A missing-env apply does NOT fan out:
// otherwise every participant on a shared repo would post its own duplicate
// "missing environment" comment. The leader (which never hits the tenant gate)
// posts that error once.
func unscopedCommandFansOut(result CommandResult) bool {
	if !actionFansOutUnscoped(result.Action) {
		return false
	}
	if result.Found {
		return true
	}
	return result.MissingEnv && result.Action == action.Plan
}

// postComment posts a comment on a PR.
func (h *Handler) postComment(repo string, pr int, installationID int64, body string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for comment",
			"repo", repo, "pr", pr, "installation_id", installationID, "error", err)
		return
	}

	if _, err := client.CreateIssueComment(ctx, repo, pr, h.renderPRComment(body)); err != nil {
		h.logger.Error("failed to post comment",
			"repo", repo, "pr", pr, "installation_id", installationID, "error", err)
	}
}

// postAndTrackComment creates a PR comment and stores its ID in apply_comments.
func (h *Handler) postAndTrackComment(
	ctx context.Context, repo string, pr int, installationID int64,
	applyID int64, commentState string, body string,
) {
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for tracked comment", "error", err)
		return
	}

	commentID, err := client.CreateIssueComment(ctx, repo, pr, h.renderPRComment(body))
	if err != nil {
		h.logger.Error("failed to post tracked comment",
			"repo", repo, "pr", pr, "commentState", commentState, "error", err)
		return
	}

	comment := &storage.ApplyComment{
		ApplyID:         applyID,
		CommentState:    commentState,
		GitHubCommentID: commentID,
	}
	if err := h.service.Storage().ApplyComments().Upsert(ctx, comment); err != nil {
		h.logger.Error("failed to store comment ID",
			"applyID", applyID, "commentState", commentState, "commentID", commentID, "error", err)
	}
}

// postInitialProgressComment posts the initial progress comment for a freshly
// accepted apply and, when the apply reached a terminal state before the
// comment landed, finalizes the comment in place. The driver's observer can
// only edit a tracked comment that exists: an apply that finishes faster than
// this post (for example a metadata-only DDL) has already had its terminal
// callback find nothing to edit, so the freshly posted comment would otherwise
// stay frozen at its starting state after the summary comment. Re-checking the
// apply after the post closes that window from this side — whichever of the
// observer's terminal edit and this finalize runs last converges the comment
// on the terminal rendering.
func (h *Handler) postInitialProgressComment(ctx context.Context, repo string, pr int, installationID int64, applyID int64, body string) {
	h.postAndTrackComment(ctx, repo, pr, installationID, applyID, state.Comment.Progress, body)

	apply, err := h.service.Storage().Applies().Get(ctx, applyID)
	if err != nil {
		h.logger.Error("failed to re-check apply state after initial progress comment; if the apply already finished, its progress comment stays at the starting state",
			"repo", repo, "pr", pr, "error", err)
		return
	}
	if apply == nil {
		h.logger.Error("apply missing when re-checking state after initial progress comment",
			"repo", repo, "pr", pr)
		return
	}
	if !state.IsTerminalApplyState(apply.State) {
		h.logger.Debug("apply is still active after initial progress comment; the observer owns all further edits",
			apply.LogAttrs()...)
		return
	}

	h.logger.Info("apply reached a terminal state before its initial progress comment; finalizing the comment in place",
		apply.LogAttrs()...)

	comment, err := h.service.Storage().ApplyComments().Get(ctx, applyID, state.Comment.Progress)
	if err != nil {
		h.logger.Error("failed to load tracked progress comment for finalization",
			append(apply.LogAttrs(), "error", err)...)
		return
	}
	if comment == nil {
		// Nothing to finalize: either the GitHub post itself failed, or the post
		// succeeded but the tracking upsert did not (postAndTrackComment logged
		// which). In the latter case a comment exists on the PR with no stored
		// ID to edit, so it stays at its starting state until reconciliation.
		h.logger.Debug("no tracked progress comment to finalize for terminal apply",
			apply.LogAttrs()...)
		return
	}
	if comment.EditCount > 0 {
		// The observer has already found and edited the tracked comment, so its
		// terminal edit lands (or has landed) with the full per-operation
		// rendering. Skipping keeps this no-operations fallback from
		// overwriting that richer body.
		h.logger.Debug("observer already edits the tracked progress comment; skipping handler finalize",
			apply.LogAttrs()...)
		return
	}

	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client to finalize progress comment",
			append(apply.LogAttrs(), "error", err)...)
		return
	}
	finalBody := formatProgressComment(apply, nil, nil)
	if err := client.EditIssueComment(ctx, repo, comment.GitHubCommentID, h.renderPRComment(finalBody)); err != nil {
		h.logger.Error("failed to finalize progress comment for already-terminal apply",
			append(apply.LogAttrs(), "github_comment_id", comment.GitHubCommentID, "error", err)...)
		return
	}
	if err := h.service.Storage().ApplyComments().IncrementEditCount(ctx, applyID, state.Comment.Progress); err != nil {
		h.logger.Error("failed to increment edit count after finalizing progress comment",
			append(apply.LogAttrs(), "error", err)...)
	}
}

func (h *Handler) renderPRComment(body string) string {
	return appendSupportChannelFooter(body, h.supportChannel())
}

func (h *Handler) supportChannel() api.SupportChannelConfig {
	cfg := h.config()
	if cfg == nil {
		return api.SupportChannelConfig{}
	}
	return cfg.SupportChannel
}

func appendSupportChannelFooter(body string, support api.SupportChannelConfig) string {
	if !support.Enabled() || !shouldShowSupportChannel(body) {
		return body
	}
	return templates.RenderSupportChannelFooter(body, templates.SupportChannelData{
		Name: support.Name,
		URL:  support.URL,
	})
}

func shouldShowSupportChannel(body string) bool {
	firstLine, _, _ := strings.Cut(body, "\n")
	firstLine = strings.ToLower(firstLine)
	if strings.Contains(body, "\n**Status**: Failed\n") {
		return true
	}

	if strings.Contains(firstLine, "help") {
		return true
	}
	for _, marker := range []string{
		"failed",
		"blocked",
		"not authorized",
		"authorization check failed",
		"invalid",
		"missing",
		"not found",
		"no valid",
		"multiple",
		"reconciliation required",
	} {
		if strings.Contains(firstLine, marker) {
			return true
		}
	}
	return strings.Contains(body, "⛔ Unsafe Changes Detected")
}
