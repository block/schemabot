package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

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
func (h *Handler) handleIssueComment(w http.ResponseWriter, body []byte) {
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

	// Ignore comments from bots to prevent infinite loops
	if payload.Comment.User != nil && payload.Comment.User.Type == "Bot" {
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "event ignored (comment from bot)",
		})
		return
	}

	var installationID int64
	if payload.Installation != nil {
		installationID = payload.Installation.ID
	}

	repo := payload.Repository.FullName
	pr := payload.Issue.Number
	requestedBy := ""
	if payload.Comment.User != nil {
		requestedBy = payload.Comment.User.Login
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
		h.logger.Warn("webhook from unregistered repository", "repo", repo, "pr", pr)
		h.postComment(repo, pr, installationID,
			"**Repository not registered.** This repository is not in SchemaBot's configuration. To onboard, add it to the `repos` section of SchemaBot's config and redeploy.")
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "repository not registered",
		})
		return
	}

	// Add instant acknowledgment reaction
	if payload.Comment.ID > 0 && installationID > 0 {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			client, err := h.ghClient.ForInstallation(installationID)
			if err != nil {
				h.logger.Error("failed to create GitHub client for reaction", "error", err)
				return
			}
			if err := client.AddReactionToComment(ctx, repo, payload.Comment.ID, "eyes"); err != nil {
				h.logger.Error("failed to add acknowledgment reaction", "error", err)
			}
		}()
	}

	// Handle help command
	if result.IsHelp {
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
			go h.handleMultiEnvPlan(repo, pr, result.Database, installationID, requestedBy, false)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "multi-env plan started"})
			return
		}
		if result.Action == action.Rollback {
			// Rollback without apply ID — handler will post usage message
			go h.handleRollbackCommand(repo, pr, installationID, requestedBy, result)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "rollback started"})
			return
		}
		h.postComment(repo, pr, installationID, templates.RenderMissingEnv(result.Action))
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "missing environment flag"})
		return
	}

	// Handle invalid command (schemabot mentioned but command not recognized)
	if !result.Found {
		h.postComment(repo, pr, installationID, templates.RenderInvalidCommand())
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "invalid command"})
		return
	}

	if installationID == 0 {
		h.writeError(w, http.StatusBadRequest, "missing installation ID in webhook payload")
		return
	}

	// Reject -y/--yes on commands that don't support it
	if result.Action != action.Apply && parser.autoConfirmRegex.MatchString(payload.Comment.Body) {
		h.postComment(repo, pr, installationID,
			fmt.Sprintf("The `-y` flag is not supported for `%s`.", result.Action))
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
		h.handlePlanCommand(w, repo, pr, result.Environment, result.Database, installationID, requestedBy)
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
			h.handleUnlockCommand(repo, pr, installationID, requestedBy)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "unlock started"})
	case action.Rollback:
		h.goSafe(repo, pr, installationID, func() {
			h.handleRollbackCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "rollback started"})
	case action.RollbackConfirm:
		h.goSafe(repo, pr, installationID, func() {
			h.handleRollbackConfirmCommand(repo, pr, result.Environment, result.Database, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "rollback-confirm started"})
	// Phase 2 commands — acknowledge but not yet implemented
	case action.Stop, action.Revert, action.SkipRevert, action.Cutover:
		h.postComment(repo, pr, installationID,
			fmt.Sprintf("The `%s` command is not yet available via PR comments. Use the CLI instead:\n```\nschemabot %s -e %s\n```",
				result.Action, result.Action, result.Environment))
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": fmt.Sprintf("%s command not yet implemented", result.Action),
		})
	default:
		h.postComment(repo, pr, installationID, templates.RenderInvalidCommand())
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "invalid command"})
	}
}

// postComment posts a comment on a PR.
func (h *Handler) postComment(repo string, pr int, installationID int64, body string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := h.ghClient.ForInstallation(installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client", "error", err)
		return
	}

	if _, err := client.CreateIssueComment(ctx, repo, pr, body); err != nil {
		h.logger.Error("failed to post comment", "repo", repo, "pr", pr, "error", err)
	}
}

// postAndTrackComment creates a PR comment and stores its ID in apply_comments.
// Returns the GitHub comment ID, or 0 if the comment failed to post.
func (h *Handler) postAndTrackComment(
	ctx context.Context, repo string, pr int, installationID int64,
	applyID int64, commentState string, body string,
) int64 {
	client, err := h.ghClient.ForInstallation(installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for tracked comment", "error", err)
		return 0
	}

	commentID, err := client.CreateIssueComment(ctx, repo, pr, body)
	if err != nil {
		h.logger.Error("failed to post tracked comment",
			"repo", repo, "pr", pr, "commentState", commentState, "error", err)
		return 0
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

	return commentID
}

// editTrackedComment looks up a stored comment ID by (applyID, commentState) and edits it.
func (h *Handler) editTrackedComment(
	ctx context.Context, repo string, installationID int64,
	applyID int64, commentState string, body string,
) {
	comment, err := h.service.Storage().ApplyComments().Get(ctx, applyID, commentState)
	if err != nil {
		h.logger.Error("failed to look up tracked comment",
			"applyID", applyID, "commentState", commentState, "error", err)
		return
	}
	if comment == nil {
		h.logger.Warn("no tracked comment found to edit",
			"applyID", applyID, "commentState", commentState)
		return
	}

	client, err := h.ghClient.ForInstallation(installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for edit", "error", err)
		return
	}

	if err := client.EditIssueComment(ctx, repo, comment.GitHubCommentID, body); err != nil {
		h.logger.Error("failed to edit tracked comment",
			"repo", repo, "commentID", comment.GitHubCommentID, "error", err)
	}
}
