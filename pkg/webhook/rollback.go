package webhook

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// handleRollbackCommand handles the "schemabot rollback <apply-id>" PR comment command.
// It looks up the specified apply, generates a rollback plan from its original schema,
// acquires a lock, and posts the plan for confirmation.
func (h *Handler) handleRollbackCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	applyID := result.ApplyID
	if applyID == "" {
		h.postComment(repo, pr, installationID,
			"## Missing Apply ID\n\n"+
				"Usage: `schemabot rollback <apply-id>`\n\n"+
				"You can find the apply ID in the summary comment of a completed apply, "+
				"or by running `schemabot status`.")
		return
	}

	if h.service == nil {
		h.logger.Error("service not configured for rollback")
		return
	}

	// Look up the apply to get database/environment/type
	apply, err := h.service.Storage().Applies().GetByApplyIdentifier(ctx, applyID)
	if err != nil {
		h.logger.Error("failed to look up apply", "applyID", applyID, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			CommandName: "rollback",
			ErrorDetail: "Failed to look up apply: " + err.Error(),
		}))
		return
	}
	if apply == nil {
		h.postComment(repo, pr, installationID, fmt.Sprintf(
			"## Apply Not Found\n\n"+
				"No apply found with ID `%s`. Check the ID and try again.", applyID))
		return
	}

	database := apply.Database
	environment := apply.Environment
	dbType := apply.DatabaseType

	// Check for existing lock
	existingLock, err := h.service.Storage().Locks().Get(ctx, database, dbType)
	if err != nil {
		h.logger.Error("failed to check lock", "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Environment: environment,
			CommandName: "rollback",
			ErrorDetail: "Failed to check lock status: " + err.Error(),
		}))
		return
	}

	lockOwner := fmt.Sprintf("%s#%d", repo, pr)

	if existingLock != nil && existingLock.Owner != lockOwner {
		if existingLock.PullRequest > 0 && existingLock.Repository != "" {
			h.postComment(repo, pr, installationID, fmt.Sprintf(
				"## Rollback Blocked\n\n"+
					"**Database**: `%s` | **Environment**: `%s`\n\n"+
					"A lock is currently held by [%s#%d](https://github.com/%s/pull/%d).\n\n"+
					"Wait for that operation to complete, or ask the lock owner to run `schemabot unlock`.",
				database, environment,
				existingLock.Repository, existingLock.PullRequest,
				existingLock.Repository, existingLock.PullRequest))
		} else {
			h.postComment(repo, pr, installationID, fmt.Sprintf(
				"## Rollback Blocked\n\n"+
					"**Database**: `%s` | **Environment**: `%s`\n\n"+
					"A lock is currently held by `%s`.\n\n"+
					"Wait for that operation to complete, or ask the lock owner to release it.",
				database, environment, existingLock.Owner))
		}
		return
	}

	// Generate rollback plan (uses the most recent completed apply for this database/environment)
	planResp, err := h.service.ExecuteRollbackPlan(ctx, database, environment, "")
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "no completed") {
			h.postComment(repo, pr, installationID, templates.RenderRollbackNoCompletedApply(database, environment))
			return
		}
		h.logger.Error("rollback plan failed", "repo", repo, "pr", pr, "applyID", applyID, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Environment: environment,
			CommandName: "rollback",
			ErrorDetail: errMsg,
		}))
		return
	}

	if len(planResp.FlatTables()) == 0 {
		h.postComment(repo, pr, installationID, fmt.Sprintf(
			"## Nothing to Rollback\n\n"+
				"**Database**: `%s` | **Environment**: `%s`\n\n"+
				"The database schema already matches the state before apply `%s`. No rollback needed.",
			database, environment, applyID))
		return
	}

	// Acquire lock
	lock := &storage.Lock{
		DatabaseName: database,
		DatabaseType: dbType,
		Owner:        lockOwner,
		Repository:   repo,
		PullRequest:  pr,
	}
	if err := h.service.Storage().Locks().Acquire(ctx, lock); err != nil {
		h.logger.Error("failed to acquire lock", "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Environment: environment,
			CommandName: "rollback",
			ErrorDetail: "Failed to acquire lock: " + err.Error(),
		}))
		return
	}

	// Build comment data
	commentData := templates.PlanCommentData{
		Database:    database,
		Environment: environment,
		RequestedBy: requestedBy,
		IsMySQL:     dbType == "mysql",
	}

	for _, sc := range planResp.Changes {
		nsData := templates.KeyspaceChangeData{
			Keyspace: sc.Namespace,
		}
		for _, t := range sc.TableChanges {
			nsData.Statements = append(nsData.Statements, t.DDL)
		}
		if diff, ok := sc.Metadata["vschema"]; ok {
			nsData.VSchemaChanged = true
			nsData.VSchemaDiff = diff
		}
		commentData.Changes = append(commentData.Changes, nsData)
	}

	for _, w := range planResp.LintNonErrors() {
		commentData.LintViolations = append(commentData.LintViolations, templates.LintViolationData{
			Message: w.Message,
			Table:   w.Table,
		})
	}
	commentData.Errors = planResp.Errors

	h.postComment(repo, pr, installationID, templates.RenderRollbackPlanComment(commentData))
}

// handleRollbackConfirmCommand handles the "schemabot rollback-confirm -e <env>" PR comment command.
// It verifies the lock, re-generates the rollback plan for drift detection, and executes the apply.
func (h *Handler) handleRollbackConfirmCommand(repo string, pr int, environment, databaseName string, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := h.ghClient.ForInstallation(installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client", "error", err)
		return
	}

	// Discover database config from PR's schemabot.yaml
	schemaResult, err := client.CreateSchemaRequestFromPR(ctx, repo, pr, environment, databaseName)
	if err != nil {
		h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, "rollback-confirm", err)
		return
	}

	database := schemaResult.Database
	dbType := schemaResult.Type
	lockOwner := fmt.Sprintf("%s#%d", repo, pr)

	// Check lock ownership
	existingLock, err := h.service.Storage().Locks().Get(ctx, database, dbType)
	if err != nil {
		h.logger.Error("failed to check lock", "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Environment: environment,
			CommandName: "rollback-confirm",
			ErrorDetail: "Failed to check lock status: " + err.Error(),
		}))
		return
	}
	if existingLock == nil {
		h.postComment(repo, pr, installationID, templates.RenderRollbackConfirmNoLock(database, environment))
		return
	}
	if existingLock.Owner != lockOwner {
		h.postComment(repo, pr, installationID, fmt.Sprintf(
			"## Lock Not Owned\n\n"+
				"**Database**: `%s` | **Environment**: `%s`\n\n"+
				"The lock is held by `%s`, not this PR. Cannot confirm rollback.",
			database, environment, existingLock.Owner))
		return
	}

	// Re-generate rollback plan for drift detection
	planResp, err := h.service.ExecuteRollbackPlan(ctx, database, environment, "")
	if err != nil {
		h.logger.Error("rollback plan failed on confirm", "repo", repo, "pr", pr, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Timestamp:   time.Now().UTC().Format("2006-01-02 15:04:05"),
			Environment: environment,
			CommandName: "rollback-confirm",
			ErrorDetail: err.Error(),
		}))
		return
	}

	// If no changes remain, release lock and notify
	if len(planResp.FlatTables()) == 0 {
		_ = h.service.Storage().Locks().Release(ctx, database, dbType, lockOwner)
		h.postComment(repo, pr, installationID, fmt.Sprintf(
			"## Already Rolled Back\n\n"+
				"**Database**: `%s` | **Environment**: `%s`\n\n"+
				"The database schema already matches the original state. Lock released.",
			database, environment))
		return
	}

	// Build apply options — rollback always allows unsafe changes
	options := map[string]string{
		"allow_unsafe": "true",
	}
	if result.DeferCutover {
		options["defer_cutover"] = "true"
	}

	// Execute apply with the rollback plan
	applyReq := api.ApplyRequest{
		PlanID:         planResp.PlanID,
		Environment:    environment,
		Options:        options,
		Caller:         lockOwner,
		InstallationID: installationID,
	}

	applyResp, applyID, err := h.service.ExecuteApply(ctx, applyReq)
	if err != nil {
		h.logger.Error("rollback apply failed", "repo", repo, "pr", pr, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Timestamp:   time.Now().UTC().Format("2006-01-02 15:04:05"),
			Environment: environment,
			CommandName: "rollback-confirm",
			ErrorDetail: "Failed to execute rollback: " + err.Error(),
		}))
		return
	}

	if !applyResp.Accepted {
		h.postComment(repo, pr, installationID, fmt.Sprintf(
			"## Rollback Not Accepted\n\n"+
				"**Database**: `%s` | **Environment**: `%s`\n\n"+
				"The rollback was not accepted: %s",
			database, environment, applyResp.ErrorMessage))
		return
	}

	// Spawn background progress watcher
	if applyID > 0 {
		apply, err := h.service.Storage().Applies().Get(ctx, applyID)
		if err != nil || apply == nil {
			h.logger.Error("failed to load apply for progress watch", "applyID", applyID, "error", err)
			return
		}
		go h.watchApplyProgress(context.Background(), apply)
	}
}
