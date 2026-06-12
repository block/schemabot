package webhook

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// logControlCommandError logs a failed PR control command at the severity
// matching the error class: internal failures (storage, control request store,
// Tern, or other unexpected errors) log at Error so operators investigate
// them, while operator-actionable rejections such as state conflicts log at
// Warn.
func (h *Handler) logControlCommandError(command, repo string, pr int, applyID, environment, requestedBy string, err error) {
	attrs := []any{
		"repo", repo,
		"pr", pr,
		"apply_id", applyID,
		"environment", environment,
		"requested_by", requestedBy,
		"error", err,
	}
	if api.IsInternalControlError(err) {
		h.logger.Error(command+" PR command failed", attrs...)
	} else {
		h.logger.Warn(command+" PR command rejected", attrs...)
	}
}

func (h *Handler) loadApplyForPRControl(ctx context.Context, repo string, pr int, installationID int64, requestedBy string, result CommandResult, command string) (*storage.Apply, bool) {
	if result.ApplyID == "" {
		h.postComment(repo, pr, installationID, templates.RenderControlMissingApplyID(command))
		return nil, false
	}
	if h.service == nil {
		h.logger.Error("service not configured for PR control command",
			"command", command,
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, command, result.Environment, requestedBy, "SchemaBot service is not configured for "+command+" commands")
		return nil, false
	}
	store := h.service.Storage()
	if store == nil {
		h.logger.Error("storage not configured for PR control command",
			"command", command,
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, command, result.Environment, requestedBy, "SchemaBot storage is not configured for "+command+" commands")
		return nil, false
	}
	applyStore := store.Applies()
	if applyStore == nil {
		h.logger.Error("apply store not configured for PR control command",
			"command", command,
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, command, result.Environment, requestedBy, "SchemaBot apply storage is not configured for "+command+" commands")
		return nil, false
	}
	apply, err := applyStore.GetByApplyIdentifier(ctx, result.ApplyID)
	if err != nil {
		h.logger.Error("failed to load apply for PR control command",
			"command", command,
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy,
			"error", err)
		h.postCommandError(repo, pr, installationID, command, result.Environment, requestedBy, "Failed to look up apply: "+err.Error())
		return nil, false
	}
	if apply == nil {
		h.logger.Warn("PR control command rejected because apply was not found",
			"command", command,
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, command, result.Environment, requestedBy, "Apply not found: "+result.ApplyID)
		return nil, false
	}
	if apply.Repository != repo || apply.PullRequest != pr {
		h.logger.Warn("PR control command rejected because apply belongs to another PR",
			"command", command,
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"apply_repo", apply.Repository,
			"apply_pr", apply.PullRequest,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, command, result.Environment, requestedBy, "Apply "+result.ApplyID+" is not associated with this PR")
		return nil, false
	}
	if apply.Environment != result.Environment {
		h.logger.Warn("PR control command rejected because environment does not match apply",
			"command", command,
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"apply_environment", apply.Environment,
			"requested_environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, command, result.Environment, requestedBy,
			fmt.Sprintf("Apply %s belongs to environment %q, not %q", result.ApplyID, apply.Environment, result.Environment))
		return nil, false
	}
	return apply, true
}

// handleStopCommand handles the "schemabot stop <apply-id> -e <env>" PR
// comment command by recording durable stop intent for the operator owner.
func (h *Handler) handleStopCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(commandTimeout)
	defer cancel()

	apply, ok := h.loadApplyForPRControl(ctx, repo, pr, installationID, requestedBy, result, action.Stop)
	if !ok {
		return
	}
	client, blocked := h.actorAuthorizationClient(repo, pr, installationID, requestedBy, apply.Database, result.Environment, action.Stop)
	if blocked {
		return
	}
	if blocked := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, apply.Database, apply.DatabaseType, result.Environment, action.Stop); blocked {
		return
	}

	caller := fmt.Sprintf("github:%s@%s#%d", requestedBy, repo, pr)
	resp, err := h.service.ExecuteStop(ctx, apitypes.ControlRequest{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		Caller:      caller,
	})
	if err != nil {
		h.logControlCommandError(action.Stop, repo, pr, result.ApplyID, result.Environment, requestedBy, err)
		h.postCommandError(repo, pr, installationID, action.Stop, result.Environment, requestedBy, err.Error())
		return
	}
	if resp == nil {
		h.logger.Error("stop PR command returned no response",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Stop, result.Environment, requestedBy, "SchemaBot accepted the stop command but returned no response")
		return
	}
	if !resp.Accepted {
		detail := resp.ErrorMessage
		if detail == "" {
			detail = "Stop was not accepted"
		}
		h.logger.Warn("stop PR command was not accepted",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy,
			"error_message", resp.ErrorMessage)
		h.postCommandError(repo, pr, installationID, action.Stop, result.Environment, requestedBy, detail)
		return
	}

	h.logger.Info("stop PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy,
		"status", resp.Status,
		"stopped_count", resp.StoppedCount,
		"skipped_count", resp.SkippedCount)
	h.postComment(repo, pr, installationID, templates.RenderStopCommandAccepted(templates.StopCommandAcceptedData{
		ApplyID:      result.ApplyID,
		Environment:  result.Environment,
		RequestedBy:  requestedBy,
		Status:       resp.Status,
		StoppedCount: resp.StoppedCount,
		SkippedCount: resp.SkippedCount,
	}))
}

// handleStartCommand handles the "schemabot start <apply-id> -e <env>" PR
// comment command by recording durable start intent for the operator owner.
func (h *Handler) handleStartCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(commandTimeout)
	defer cancel()

	apply, ok := h.loadApplyForPRControl(ctx, repo, pr, installationID, requestedBy, result, action.Start)
	if !ok {
		return
	}
	client, blocked := h.actorAuthorizationClient(repo, pr, installationID, requestedBy, apply.Database, result.Environment, action.Start)
	if blocked {
		return
	}
	if blocked := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, apply.Database, apply.DatabaseType, result.Environment, action.Start); blocked {
		return
	}

	caller := fmt.Sprintf("github:%s@%s#%d", requestedBy, repo, pr)
	resp, err := h.service.ExecuteStart(ctx, apitypes.ControlRequest{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		Caller:      caller,
	})
	if err != nil {
		h.logControlCommandError(action.Start, repo, pr, result.ApplyID, result.Environment, requestedBy, err)
		h.postCommandError(repo, pr, installationID, action.Start, result.Environment, requestedBy, err.Error())
		return
	}
	if resp == nil {
		h.logger.Error("start PR command returned no response",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Start, result.Environment, requestedBy, "SchemaBot accepted the start command but returned no response")
		return
	}
	if !resp.Accepted {
		detail := resp.ErrorMessage
		if detail == "" {
			detail = "Start was not accepted"
		}
		h.logger.Warn("start PR command was not accepted",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy,
			"error_message", resp.ErrorMessage)
		h.postCommandError(repo, pr, installationID, action.Start, result.Environment, requestedBy, detail)
		return
	}

	h.logger.Info("start PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy,
		"status", resp.Status,
		"started_count", resp.StartedCount,
		"skipped_count", resp.SkippedCount)
	h.postComment(repo, pr, installationID, templates.RenderStartCommandAccepted(templates.StartCommandAcceptedData{
		ApplyID:      result.ApplyID,
		Environment:  result.Environment,
		RequestedBy:  requestedBy,
		Status:       resp.Status,
		StartedCount: resp.StartedCount,
		SkippedCount: resp.SkippedCount,
	}))
}

// handleCutoverCommand handles the "schemabot cutover <apply-id> -e <env>" PR
// comment command by recording durable cutover intent for the operator owner.
func (h *Handler) handleCutoverCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(commandTimeout)
	defer cancel()

	apply, ok := h.loadApplyForPRControl(ctx, repo, pr, installationID, requestedBy, result, action.Cutover)
	if !ok {
		return
	}
	client, blocked := h.actorAuthorizationClient(repo, pr, installationID, requestedBy, apply.Database, result.Environment, action.Cutover)
	if blocked {
		return
	}
	if blocked := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, apply.Database, apply.DatabaseType, result.Environment, action.Cutover); blocked {
		return
	}

	caller := fmt.Sprintf("github:%s@%s#%d", requestedBy, repo, pr)
	resp, err := h.service.ExecuteCutover(ctx, apitypes.ControlRequest{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		Caller:      caller,
	})
	if err != nil {
		h.logControlCommandError(action.Cutover, repo, pr, result.ApplyID, result.Environment, requestedBy, err)
		h.postCommandError(repo, pr, installationID, action.Cutover, result.Environment, requestedBy, err.Error())
		return
	}
	if resp == nil {
		h.logger.Error("cutover PR command returned no response",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Cutover, result.Environment, requestedBy, "SchemaBot accepted the cutover command but returned no response")
		return
	}
	if !resp.Accepted {
		detail := resp.ErrorMessage
		if detail == "" {
			detail = "Cutover was not accepted"
		}
		h.logger.Warn("cutover PR command was not accepted",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy,
			"error_message", resp.ErrorMessage)
		h.postCommandError(repo, pr, installationID, action.Cutover, result.Environment, requestedBy, detail)
		return
	}

	h.logger.Info("cutover PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy,
		"status", resp.Status)
	h.postComment(repo, pr, installationID, templates.RenderCutoverCommandAccepted(templates.CutoverCommandAcceptedData{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		RequestedBy: requestedBy,
		Status:      resp.Status,
	}))
}
