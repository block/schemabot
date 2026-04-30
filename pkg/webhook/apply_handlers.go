package webhook

import (
	"context"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// handleApplyCommand handles the "schemabot apply -e <env>" PR comment command.
// It generates a plan, acquires a lock, and posts a plan comment with a confirmation footer.
func (h *Handler) handleApplyCommand(repo string, pr int, environment, databaseName string, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := h.ghClient.ForInstallation(installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client", "error", err)
		return
	}

	// Discover config and fetch schema files from PR
	schemaResult, err := client.CreateSchemaRequestFromPR(ctx, repo, pr, environment, databaseName)
	if err != nil {
		h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.Apply, err)
		return
	}

	// Tier 1: CODEOWNERS review gate (path-scoped to this database's schema directory)
	if blocked := h.enforceReviewGate(ctx, client, repo, pr, installationID, schemaResult, environment, requestedBy, action.Apply); blocked {
		h.logger.Info("apply blocked by review gate", "repo", repo, "pr", pr, "environment", environment, "requested_by", requestedBy)
		return
	}

	database := schemaResult.Database
	dbType := schemaResult.Type
	lockOwner := fmt.Sprintf("%s#%d", repo, pr)

	// Environment ordering enforcement: prior environments must be clean before applying.
	// e.g., for [sandbox, staging, production], applying to production requires both
	// sandbox and staging checks to be success.
	if blocked := h.checkPriorEnvironments(ctx, repo, pr, database, dbType, environment, schemaResult.Environments, installationID, requestedBy); blocked {
		h.logger.Info("apply blocked by environment ordering", "repo", repo, "pr", pr, "database", database, "environment", environment)
		return
	}

	// Check for existing lock
	existingLock, err := h.service.Storage().Locks().Get(ctx, database, dbType)
	if err != nil {
		h.logger.Error("failed to check lock", "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Environment: environment,
			CommandName: action.Apply,
			ErrorDetail: "Failed to check lock status: " + err.Error(),
		}))
		return
	}

	if existingLock != nil {
		if existingLock.Owner != lockOwner {
			// Lock held by a different entity
			h.logger.Info("apply blocked by lock conflict", "repo", repo, "pr", pr, "database", database, "lock_owner", existingLock.Owner)
			h.postComment(repo, pr, installationID, templates.RenderApplyBlockedByOtherPR(templates.ApplyLockConflictData{
				Database:    database,
				Environment: environment,
				RequestedBy: requestedBy,
				LockOwner:   existingLock.Owner,
				LockRepo:    existingLock.Repository,
				LockPR:      existingLock.PullRequest,
				LockCreated: existingLock.CreatedAt,
			}))
			return
		}

		// Lock held by this PR — check for active applies
		applies, err := h.service.Storage().Applies().GetByPR(ctx, repo, pr)
		if err != nil {
			h.logger.Error("failed to check active applies", "error", err)
			return
		}
		for _, a := range applies {
			if a.Database == database && !state.IsTerminalApplyState(a.State) {
				h.logger.Info("apply blocked by in-progress apply", "repo", repo, "pr", pr, "database", database, "apply_id", a.ApplyIdentifier, "state", a.State)
				h.postComment(repo, pr, installationID, templates.RenderApplyInProgress(templates.ApplyLockConflictData{
					Database:    database,
					Environment: environment,
					RequestedBy: requestedBy,
					ApplyID:     a.ApplyIdentifier,
					ApplyState:  a.State,
				}))
				return
			}
		}

		// Stale lock from this PR (no active applies) — release it so we can re-plan
		if err := h.service.Storage().Locks().ForceRelease(ctx, database, dbType); err != nil {
			h.logger.Error("failed to release stale lock", "error", err)
		}
	}

	// Generate plan
	prNumber := int32(pr)
	planReq := api.PlanRequest{
		Database:    schemaResult.Database,
		Environment: environment,
		Type:        schemaResult.Type,
		SchemaFiles: schemaResult.SchemaFiles,
		Repository:  repo,
		PullRequest: &prNumber,
		Target:      schemaResult.Target,
	}

	planResp, err := h.service.ExecutePlan(ctx, planReq)
	if err != nil {
		h.logger.Error("plan execution failed", "repo", repo, "pr", pr, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Environment: environment,
			CommandName: action.Apply,
			ErrorDetail: err.Error(),
		}))
		return
	}

	// No changes — post a regular plan comment (no lock, no confirm footer)
	if len(planResp.FlatTables()) == 0 {
		commentData := buildPlanCommentData(schemaResult, planResp, environment, requestedBy)
		h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))
		return
	}

	// Block unsafe changes unless --allow-unsafe was specified
	if len(planResp.UnsafeChanges()) > 0 && !result.AllowUnsafe {
		commentData := buildPlanCommentData(schemaResult, planResp, environment, requestedBy)
		h.logger.Info("apply blocked by unsafe changes", "repo", repo, "pr", pr, "database", database, "environment", environment)
		h.postComment(repo, pr, installationID, templates.RenderUnsafeChangesBlocked(commentData))
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
			CommandName: action.Apply,
			ErrorDetail: "Failed to acquire lock: " + err.Error(),
		}))
		return
	}

	// Build plan comment data with lock info
	commentData := buildPlanCommentData(schemaResult, planResp, environment, requestedBy)
	commentData.IsLocked = true
	commentData.LockOwner = lockOwner
	commentData.LockAcquired = time.Now().UTC().Format("2006-01-02 15:04:05 UTC")
	commentData.DeferCutover = result.DeferCutover
	commentData.SkipRevert = result.SkipRevert
	commentData.AllowUnsafe = result.AllowUnsafe

	// Auto-confirm (-y): check safety conditions before proceeding
	if result.AutoConfirm {
		// Check 1: HEAD SHA hasn't changed since auto-plan.
		// Fail closed: if we can't verify the SHA (missing check record, API error),
		// downgrade to manual confirmation rather than proceeding with stale data.
		check, checkErr := h.service.Storage().Checks().Get(ctx, repo, pr, environment, dbType, database)
		prInfo, prErr := client.FetchPullRequest(ctx, repo, pr)

		shaVerified := checkErr == nil && prErr == nil && check != nil && prInfo != nil && check.HeadSHA == prInfo.HeadSHA
		if !shaVerified {
			reason := "Could not verify HEAD SHA — confirm manually"
			if checkErr == nil && prErr == nil && check != nil && prInfo != nil {
				reason = "New commits pushed since auto-plan"
			}
			h.logger.Info("auto-confirm downgraded",
				"repo", repo, "pr", pr, "database", database, "reason", reason)
			commentData.AutoConfirmDowngradeReason = reason
			h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))
			headSHA, checkRunErr := h.createApplyPlanCheckRun(ctx, client, repo, pr, schemaResult, planResp, environment, installationID)
			if checkRunErr != nil {
				h.logger.Error("failed to create apply plan check run", "repo", repo, "pr", pr, "error", checkRunErr)
			}
			if headSHA != "" {
				h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
			}
			return
		}

		// Look up the plan we just created for DDL comparison in executeApply.
		// Fail closed: if we can't load the plan, downgrade to manual confirmation
		// rather than skipping the DDL drift check entirely.
		storedPlan, planErr := h.service.Storage().Plans().Get(ctx, planResp.PlanID)
		if planErr != nil || storedPlan == nil {
			h.logger.Info("auto-confirm downgraded: could not load plan for DDL comparison",
				"repo", repo, "pr", pr, "planID", planResp.PlanID, "error", planErr)
			commentData.AutoConfirmDowngradeReason = "Could not verify plan — confirm manually"
			h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))
			headSHA, checkRunErr := h.createApplyPlanCheckRun(ctx, client, repo, pr, schemaResult, planResp, environment, installationID)
			if checkRunErr != nil {
				h.logger.Error("failed to create apply plan check run", "repo", repo, "pr", pr, "error", checkRunErr)
			}
			if headSHA != "" {
				h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
			}
			return
		}

		commentData.AutoConfirm = true
		h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))
		headSHA, checkErr := h.createApplyPlanCheckRun(ctx, client, repo, pr, schemaResult, planResp, environment, installationID)
		if checkErr != nil {
			h.logger.Error("failed to create apply plan check run", "repo", repo, "pr", pr, "error", checkErr)
		}
		if headSHA != "" {
			h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
		}

		// Check 2 (DDL drift) happens inside executeApply after re-plan
		h.executeApply(ctx, client, repo, pr, schemaResult, environment, installationID, requestedBy, result, storedPlan)
		return
	}

	h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))

	// Create check run (action_required — waiting for apply-confirm) and update aggregate
	headSHA, checkErr := h.createApplyPlanCheckRun(ctx, client, repo, pr, schemaResult, planResp, environment, installationID)
	if checkErr != nil {
		h.logger.Error("failed to create apply plan check run", "repo", repo, "pr", pr, "error", checkErr)
	}
	if headSHA != "" {
		h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
	}
}

// handleApplyConfirmCommand handles the "schemabot apply-confirm -e <env>" PR comment command.
// It verifies lock ownership, re-plans for drift detection, executes the apply, and watches progress.
func (h *Handler) handleApplyConfirmCommand(repo string, pr int, environment, databaseName string, installationID int64, requestedBy string, result CommandResult) {
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
		h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.ApplyConfirm, err)
		return
	}

	// Tier 1: CODEOWNERS review gate (re-check on confirm to prevent bypass)
	if blocked := h.enforceReviewGate(ctx, client, repo, pr, installationID, schemaResult, environment, requestedBy, action.ApplyConfirm); blocked {
		h.logger.Info("apply-confirm blocked by review gate", "repo", repo, "pr", pr, "environment", environment, "requested_by", requestedBy)
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
			CommandName: action.ApplyConfirm,
			ErrorDetail: "Failed to check lock status: " + err.Error(),
		}))
		return
	}
	if existingLock == nil {
		h.logger.Info("apply-confirm rejected: no lock held", "repo", repo, "pr", pr, "database", database, "environment", environment)
		h.postComment(repo, pr, installationID, templates.RenderApplyConfirmNoLock(database, environment))
		return
	}
	if existingLock.Owner != lockOwner {
		h.logger.Info("apply-confirm blocked by lock conflict", "repo", repo, "pr", pr, "database", database, "lock_owner", existingLock.Owner)
		h.postComment(repo, pr, installationID, templates.RenderApplyBlockedByOtherPR(templates.ApplyLockConflictData{
			Database:    database,
			Environment: environment,
			RequestedBy: requestedBy,
			LockOwner:   existingLock.Owner,
			LockRepo:    existingLock.Repository,
			LockPR:      existingLock.PullRequest,
			LockCreated: existingLock.CreatedAt,
		}))
		return
	}

	h.executeApply(ctx, client, repo, pr, schemaResult, environment, installationID, requestedBy, result, nil)
}

// executeApply re-plans for drift detection and executes the apply. This is the shared
// execution core used by both handleApplyConfirmCommand and handleApplyCommand (with -y).
//
// When storedPlan is non-nil (auto-confirm path), the re-plan DDL is compared against it.
// If the DDL differs, execution is downgraded to manual confirmation — a plan comment is
// posted with a warning and the user must run apply-confirm separately.
func (h *Handler) executeApply(
	ctx context.Context, client *ghclient.InstallationClient,
	repo string, pr int, schemaResult *ghclient.SchemaRequestResult,
	environment string, installationID int64, requestedBy string,
	result CommandResult, storedPlan *storage.Plan,
) {
	database := schemaResult.Database
	dbType := schemaResult.Type

	// Re-plan for drift detection
	prNumber := int32(pr)
	planReq := api.PlanRequest{
		Database:    schemaResult.Database,
		Environment: environment,
		Type:        schemaResult.Type,
		SchemaFiles: schemaResult.SchemaFiles,
		Repository:  repo,
		PullRequest: &prNumber,
		Target:      schemaResult.Target,
	}

	planResp, err := h.service.ExecutePlan(ctx, planReq)
	if err != nil {
		h.logger.Error("plan execution failed on confirm", "repo", repo, "pr", pr, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Environment: environment,
			CommandName: action.Apply,
			ErrorDetail: err.Error(),
		}))
		return
	}

	// No changes — release lock and notify
	if len(planResp.FlatTables()) == 0 {
		_ = h.service.Storage().Locks().ForceRelease(ctx, database, dbType)
		h.postComment(repo, pr, installationID, templates.RenderApplyConfirmNoChanges(database, environment))
		return
	}

	// Auto-confirm DDL drift check: if the re-plan DDL differs from the stored auto-plan,
	// downgrade to manual confirmation so the user reviews the new plan.
	if storedPlan != nil && !ddlMatchesStoredPlan(planResp, storedPlan) {
		h.logger.Info("auto-confirm downgraded: DDL drift detected",
			"repo", repo, "pr", pr, "database", database, "environment", environment)
		commentData := buildPlanCommentData(schemaResult, planResp, environment, requestedBy)
		commentData.IsLocked = true
		commentData.AutoConfirmDowngradeReason = "Schema changes differ from auto-plan — review and confirm manually"
		h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))
		return
	}

	// Block unsafe changes on confirm (re-plan may have detected new unsafe changes)
	if len(planResp.UnsafeChanges()) > 0 && !result.AllowUnsafe {
		commentData := buildPlanCommentData(schemaResult, planResp, environment, requestedBy)
		h.logger.Info("apply blocked by unsafe changes", "repo", repo, "pr", pr, "database", database, "environment", environment)
		h.postComment(repo, pr, installationID, templates.RenderUnsafeChangesBlocked(commentData))
		return
	}

	// Build apply options
	options := make(map[string]string)
	if result.DeferCutover {
		options["defer_cutover"] = "true"
	}
	if result.SkipRevert {
		options["skip_revert"] = "true"
	}
	if result.AllowUnsafe {
		options["allow_unsafe"] = "true"
	}

	caller := fmt.Sprintf("github:%s@%s#%d", requestedBy, repo, pr)

	applyReq := api.ApplyRequest{
		PlanID:         planResp.PlanID,
		Environment:    environment,
		Options:        options,
		Caller:         caller,
		InstallationID: installationID,
	}

	applyResp, applyID, err := h.service.ExecuteApply(ctx, applyReq)
	if err != nil {
		h.logger.Error("apply execution failed", "repo", repo, "pr", pr, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Environment: environment,
			CommandName: action.Apply,
			ErrorDetail: "Failed to execute apply: " + err.Error(),
		}))
		return
	}

	if !applyResp.Accepted {
		h.logger.Info("apply rejected by engine", "repo", repo, "pr", pr, "database", database, "environment", environment, "error", applyResp.ErrorMessage)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Environment: environment,
			CommandName: action.Apply,
			ErrorDetail: "Apply was not accepted: " + applyResp.ErrorMessage,
		}))
		return
	}

	// Update check run to in-progress (transitions action_required → in_progress)
	h.updateCheckRunForApplyStart(ctx, client, repo, pr, schemaResult, environment, installationID)

	if applyID <= 0 {
		return
	}

	apply, err := h.service.Storage().Applies().Get(ctx, applyID)
	if err != nil || apply == nil {
		h.logger.Error("failed to load apply for progress watch", "applyID", applyID, "error", err)
		return
	}

	// Wait briefly for fast results (instant DDL, immediate failures)
	// before deciding whether to go async.
	time.Sleep(2 * time.Second)

	apply, err = h.service.Storage().Applies().Get(ctx, applyID)
	if err != nil || apply == nil {
		h.logger.Error("failed to reload apply after wait", "applyID", applyID, "error", err)
		return
	}

	if state.IsTerminalApplyState(apply.State) {
		// Fast result — handle synchronously (no watcher needed)
		tasks, _ := h.service.Storage().Tasks().GetByApplyID(ctx, applyID)
		summaryBody := formatSummaryComment(apply, tasks)
		h.postComment(repo, pr, installationID, summaryBody)
		h.updateCheckRunForApplyResult(ctx, repo, pr, installationID, apply)
		return
	}

	// Still running — post progress comment and spawn async watcher
	progressBody := templates.RenderApplyStarted(templates.ApplyStatusCommentData{
		ApplyID:     applyResp.ApplyID,
		Database:    database,
		Environment: environment,
		RequestedBy: requestedBy,
		State:       apply.State,
		Engine:      schemaResult.Type,
	})
	h.postAndTrackComment(ctx, repo, pr, installationID, applyID, state.Comment.Progress, progressBody)

	go h.watchApplyProgress(context.Background(), repo, pr, installationID, apply, true)
}

// ddlMatchesStoredPlan compares the re-plan DDL against a previously stored plan.
// Uses order-independent comparison since FlatTables() and FlatDDLChanges() may
// return statements in different order.
func ddlMatchesStoredPlan(planResp *apitypes.PlanResponse, storedPlan *storage.Plan) bool {
	newDDL := planResp.FlatTables()
	storedDDL := storedPlan.FlatDDLChanges()

	if len(newDDL) != len(storedDDL) {
		return false
	}

	// Build a set of DDL strings from the stored plan
	storedSet := make(map[string]int, len(storedDDL))
	for _, s := range storedDDL {
		storedSet[s.DDL]++
	}

	for _, n := range newDDL {
		if storedSet[n.DDL] <= 0 {
			return false
		}
		storedSet[n.DDL]--
	}
	return true
}

// handleUnlockCommand handles the "schemabot unlock" PR comment command.
// It finds all locks held by this PR and releases them.
func (h *Handler) handleUnlockCommand(repo string, pr int, installationID int64, requestedBy string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Find locks held by this PR
	locks, err := h.service.Storage().Locks().GetByPR(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to look up locks", "repo", repo, "pr", pr, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			CommandName: action.Unlock,
			ErrorDetail: "Failed to look up locks: " + err.Error(),
		}))
		return
	}

	if len(locks) == 0 {
		h.logger.Info("unlock: no locks found", "repo", repo, "pr", pr)
		h.postComment(repo, pr, installationID, templates.RenderNoLocksFound())
		return
	}

	// Check for active applies on any locked database
	for _, lock := range locks {
		applies, err := h.service.Storage().Applies().GetByPR(ctx, repo, pr)
		if err != nil {
			h.logger.Error("failed to check active applies", "error", err)
			continue
		}
		for _, a := range applies {
			if a.Database == lock.DatabaseName && !state.IsTerminalApplyState(a.State) {
				h.postComment(repo, pr, installationID, templates.RenderCannotUnlock(
					lock.DatabaseName, a.Environment, a.ApplyIdentifier, a.State))
				return
			}
		}
	}

	// Release all locks
	for _, lock := range locks {
		if err := h.service.Storage().Locks().ForceRelease(ctx, lock.DatabaseName, lock.DatabaseType); err != nil {
			h.logger.Error("failed to release lock", "database", lock.DatabaseName, "error", err)
			continue
		}

		h.postComment(repo, pr, installationID, templates.RenderUnlockSuccess(
			lock.DatabaseName, "", requestedBy))

		// Update check run to neutral
		h.updateCheckRunAfterUnlock(ctx, repo, pr, lock, installationID)
	}
}

// createApplyPlanCheckRun creates or updates the check run when an apply plan is posted.
// Uses the same check name as the plan check run so it updates the existing one.
func (h *Handler) createApplyPlanCheckRun(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schema *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment string, installationID int64) (string, error) {
	return h.createPlanCheckRun(ctx, client, repo, pr, schema, planResp, environment, installationID)
}

// updateCheckRunForApplyStart updates the existing plan check run to "in_progress"
// when an apply begins execution. This transitions the check from action_required → in_progress.
func (h *Handler) updateCheckRunForApplyStart(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schema *ghclient.SchemaRequestResult, environment string, installationID int64) {
	check, err := h.service.Storage().Checks().Get(ctx, repo, pr, environment, schema.Type, schema.Database)
	if err != nil {
		h.logger.Error("failed to look up check record", "error", err)
		return
	}
	if check == nil || check.CheckRunID == 0 {
		// No existing check run — create a new one
		prInfo, err := client.FetchPullRequest(ctx, repo, pr)
		if err != nil {
			h.logger.Error("failed to fetch PR for check run", "error", err)
			return
		}
		checkName := checkRunName(environment, schema.Type, schema.Database)
		opts := ghclient.CheckRunOptions{
			Name:   checkName,
			Status: checkStatusInProgress,
			Output: &ghclient.CheckRunOutput{
				Title:   "Schema change in progress",
				Summary: "Applying schema changes...",
			},
		}
		checkRunID, err := client.CreateCheckRun(ctx, repo, prInfo.HeadSHA, opts)
		if err != nil {
			h.logger.Error("failed to create apply in-progress check run", "error", err)
			return
		}
		// Store the check record so updateCheckRunForApplyResult can find it
		newCheck := &storage.Check{
			Repository:   repo,
			PullRequest:  pr,
			HeadSHA:      prInfo.HeadSHA,
			Environment:  environment,
			DatabaseType: schema.Type,
			DatabaseName: schema.Database,
			CheckRunID:   checkRunID,
			HasChanges:   true,
			Status:       checkStatusInProgress,
			Conclusion:   "",
		}
		if err := h.service.Storage().Checks().Upsert(ctx, newCheck); err != nil {
			h.logger.Error("failed to upsert new check record", "error", err)
		}

		// Update aggregate check to reflect in_progress state
		h.updateAggregateCheck(ctx, client, repo, pr, prInfo.HeadSHA)
		return
	}

	// Update existing check run to in_progress
	checkName := checkRunName(check.Environment, check.DatabaseType, check.DatabaseName)
	opts := ghclient.CheckRunOptions{
		Name:   checkName,
		Status: checkStatusInProgress,
		Output: &ghclient.CheckRunOutput{
			Title:   "Schema change in progress",
			Summary: "Applying schema changes...",
		},
	}
	if err := client.UpdateCheckRun(ctx, repo, check.CheckRunID, opts); err != nil {
		h.logger.Error("failed to update check run to in_progress", "error", err)
	}

	// Update stored record
	check.Status = checkStatusInProgress
	check.Conclusion = ""
	if err := h.service.Storage().Checks().Upsert(ctx, check); err != nil {
		h.logger.Error("failed to upsert check record", "error", err)
	}

	// Update aggregate check to reflect in_progress state
	h.updateAggregateCheck(ctx, client, repo, pr, check.HeadSHA)
}

// updateCheckRunAfterUnlock updates a check run to neutral after lock release.
func (h *Handler) updateCheckRunAfterUnlock(ctx context.Context, repo string, pr int, lock *storage.Lock, installationID int64) {
	client, err := h.ghClient.ForInstallation(installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for check run update", "error", err)
		return
	}

	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to fetch PR for check run update", "error", err)
		return
	}

	checkName := fmt.Sprintf("SchemaBot Apply: /%s/%s", lock.DatabaseType, lock.DatabaseName)

	opts := ghclient.CheckRunOptions{
		Name:       checkName,
		Status:     checkStatusCompleted,
		Conclusion: checkConclusionNeutral,
		Output: &ghclient.CheckRunOutput{
			Title:   "Lock released",
			Summary: "Schema change cancelled. Lock has been released.",
		},
	}

	if _, err := client.CreateCheckRun(ctx, repo, prInfo.HeadSHA, opts); err != nil {
		h.logger.Error("failed to update check run after unlock", "error", err)
	}
}
