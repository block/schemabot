package webhook

import (
	"context"
	"fmt"
	"strings"

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
	h.acknowledgeCommandActPoint(repo, pr, installationID, result)
	return apply, true
}

// runControlCommand runs the shared lifecycle of a PR control command and
// returns the typed response only when the operation was accepted. It loads and
// authorizes the apply, executes the operation, and converts any failure
// (error, missing response, or rejection) into a PR comment. On a non-nil
// return the caller renders the operation-specific acceptance comment.
func runControlCommand[R any](
	h *Handler,
	ctx context.Context,
	repo string, pr int, installationID int64, requestedBy string,
	result CommandResult,
	actionName string,
	execute func(ctx context.Context, req apitypes.ControlRequest) (*R, error),
	accepted func(*R) bool,
	errorMessage func(*R) string,
) *R {
	apply, ok := h.loadApplyForPRControl(ctx, repo, pr, installationID, requestedBy, result, actionName)
	if !ok {
		return nil
	}
	client, blocked := h.actorAuthorizationClient(repo, pr, installationID, requestedBy, apply.Database, result.Environment, actionName)
	if blocked {
		return nil
	}
	if blocked := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, apply.Database, apply.DatabaseType, result.Environment, actionName); blocked {
		return nil
	}

	caller := formatGitHubCaller(requestedBy, repo, pr)
	resp, err := execute(ctx, apitypes.ControlRequest{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		Caller:      caller,
	})
	if err != nil {
		h.logControlCommandError(actionName, repo, pr, result.ApplyID, result.Environment, requestedBy, err)
		h.postCommandError(repo, pr, installationID, actionName, result.Environment, requestedBy, err.Error())
		return nil
	}
	if resp == nil {
		h.logger.Error(actionName+" PR command returned no response",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, actionName, result.Environment, requestedBy,
			fmt.Sprintf("SchemaBot accepted the %s command but returned no response", actionName))
		return nil
	}
	if !accepted(resp) {
		detail := errorMessage(resp)
		if detail == "" {
			detail = strings.ToUpper(actionName[:1]) + actionName[1:] + " was not accepted"
		}
		h.logger.Warn(actionName+" PR command was not accepted",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy,
			"error_message", errorMessage(resp))
		h.postCommandError(repo, pr, installationID, actionName, result.Environment, requestedBy, detail)
		return nil
	}
	return resp
}

// handleStopCommand handles the "schemabot stop <apply-id> -e <env>" PR
// comment command by recording durable stop intent for the operator owner.
func (h *Handler) handleStopCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(commandTimeout)
	defer cancel()

	resp := runControlCommand(h, ctx, repo, pr, installationID, requestedBy, result, action.Stop,
		h.service.ExecuteStop,
		func(r *apitypes.StopResponse) bool { return r.Accepted },
		func(r *apitypes.StopResponse) string { return r.ErrorMessage })
	if resp == nil {
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

// handleCancelCommand handles the "schemabot cancel <apply-id> -e <env>" PR
// comment command by recording durable cancel intent for the operator owner.
func (h *Handler) handleCancelCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(commandTimeout)
	defer cancel()

	resp := runControlCommand(h, ctx, repo, pr, installationID, requestedBy, result, action.Cancel,
		h.service.ExecuteCancel,
		func(r *apitypes.CancelResponse) bool { return r.Accepted },
		func(r *apitypes.CancelResponse) string { return r.ErrorMessage })
	if resp == nil {
		return
	}

	h.logger.Info("cancel PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy,
		"status", resp.Status,
		"cancelled_count", resp.CancelledCount,
		"skipped_count", resp.SkippedCount)
	h.postComment(repo, pr, installationID, templates.RenderCancelCommandAccepted(templates.CancelCommandAcceptedData{
		ApplyID:        result.ApplyID,
		Environment:    result.Environment,
		RequestedBy:    requestedBy,
		Status:         resp.Status,
		CancelledCount: resp.CancelledCount,
		SkippedCount:   resp.SkippedCount,
	}))
}

// handleStartCommand handles the "schemabot start <apply-id> -e <env>" PR
// comment command by recording durable start intent for the operator owner.
func (h *Handler) handleStartCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(commandTimeout)
	defer cancel()

	resp := runControlCommand(h, ctx, repo, pr, installationID, requestedBy, result, action.Start,
		h.service.ExecuteStart,
		func(r *apitypes.StartResponse) bool { return r.Accepted },
		func(r *apitypes.StartResponse) string { return r.ErrorMessage })
	if resp == nil {
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

// handleReleaseCommand handles the "schemabot release <apply-id> -e <env>" PR
// comment command by recording a durable release latch so the operator lets a
// rollout paused after an on_failure=pause failure proceed.
func (h *Handler) handleReleaseCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(commandTimeout)
	defer cancel()

	resp := runControlCommand(h, ctx, repo, pr, installationID, requestedBy, result, action.Release,
		h.service.ExecuteRelease,
		func(r *apitypes.ReleaseResponse) bool { return r.Accepted },
		func(r *apitypes.ReleaseResponse) string { return r.ErrorMessage })
	if resp == nil {
		return
	}

	h.logger.Info("release PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy,
		"status", resp.Status)
	h.postComment(repo, pr, installationID, templates.RenderReleaseCommandAccepted(templates.ReleaseCommandAcceptedData{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		RequestedBy: requestedBy,
		Status:      resp.Status,
	}))
}

// handleCutoverCommand handles the "schemabot cutover <apply-id> -e <env>" PR
// comment command by recording durable cutover intent for the operator owner.
func (h *Handler) handleCutoverCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(commandTimeout)
	defer cancel()

	resp := runControlCommand(h, ctx, repo, pr, installationID, requestedBy, result, action.Cutover,
		h.service.ExecuteCutover,
		func(r *apitypes.ControlResponse) bool { return r.Accepted },
		func(r *apitypes.ControlResponse) string { return r.ErrorMessage })
	if resp == nil {
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

func (h *Handler) handleSkipRevertCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(commandTimeout)
	defer cancel()

	resp := runControlCommand(h, ctx, repo, pr, installationID, requestedBy, result, action.SkipRevert,
		h.service.ExecuteSkipRevert,
		func(r *apitypes.ControlResponse) bool { return r.Accepted },
		func(r *apitypes.ControlResponse) string { return r.ErrorMessage })
	if resp == nil {
		return
	}

	h.logger.Info("skip-revert PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy)
	h.postComment(repo, pr, installationID, templates.RenderSkipRevertCommandAccepted(templates.SkipRevertCommandAcceptedData{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		RequestedBy: requestedBy,
	}))
}

func (h *Handler) handleRevertCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(commandTimeout)
	defer cancel()

	resp := runControlCommand(h, ctx, repo, pr, installationID, requestedBy, result, action.Revert,
		h.service.ExecuteRevert,
		func(r *apitypes.ControlResponse) bool { return r.Accepted },
		func(r *apitypes.ControlResponse) string { return r.ErrorMessage })
	if resp == nil {
		return
	}

	h.logger.Info("revert PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy)
	h.postComment(repo, pr, installationID, templates.RenderRevertCommandAccepted(templates.RevertCommandAcceptedData{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		RequestedBy: requestedBy,
	}))
}
