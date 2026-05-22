package webhook

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
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
	// Dedupe FetchPullRequest calls within this command invocation.
	ctx = ghclient.WithPRInfoCache(ctx)

	client, err := h.ghClient.ForInstallation(installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client", "error", err)
		return
	}

	// Fix checks stuck at "in_progress" from crashed applies
	if err := h.reconcileStaleChecks(ctx, client, repo, pr); err != nil {
		h.logger.Error("failed to reconcile stale status checks", "repo", repo, "pr", pr, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Timestamp:   time.Now().UTC().Format("2006-01-02 15:04:05"),
			Environment: environment,
			CommandName: action.Apply,
			ErrorDetail: "Failed to reconcile stale status checks: " + err.Error(),
		}))
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

	// Tier 2: PR checks gate — block if non-SchemaBot checks are failing
	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to fetch PR for checks gate", "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Environment: environment,
			CommandName: action.Apply,
			ErrorDetail: "Failed to fetch PR info: " + err.Error(),
		}))
		return
	}
	if blocked := h.enforcePassingChecks(ctx, client, repo, pr, installationID, prInfo.HeadSHA, environment); blocked {
		return
	}

	database := schemaResult.Database
	dbType := schemaResult.Type
	lockOwner := fmt.Sprintf("%s#%d", repo, pr)

	// Environment ordering enforcement: prior environments must be clean before applying.
	// The repo config only opts into environments; server config owns their order.
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
		// Reject if the PR HEAD advanced after discovery loaded schema files.
		// Running against the loaded files would execute DDL derived from an
		// older commit than the branch is on right now. Release the lock
		// acquired above so the user can re-run `schemabot apply -e <env>`
		// cleanly without a manual unlock.
		//
		// Use FetchPullRequestNoCache: the cached FetchPullRequest used by
		// discovery would return the discovery-time HeadSHA, masking the race.
		prInfo, prErr := client.FetchPullRequestNoCache(ctx, repo, pr)
		if prErr != nil {
			h.logger.Error("failed to fetch PR for stale-schema check, releasing lock",
				"repo", repo, "pr", pr, "database", database, "error", prErr)
			h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
				RequestedBy: requestedBy,
				Environment: environment,
				CommandName: action.Apply,
				ErrorDetail: "Failed to verify PR HEAD before auto-confirm: " + prErr.Error(),
			}))
			if relErr := h.service.Storage().Locks().ForceRelease(ctx, database, dbType); relErr != nil {
				h.logger.Error("failed to release lock after PR fetch failure",
					"repo", repo, "pr", pr, "database", database, "database_type", dbType, "error", relErr)
			}
			return
		}
		if rejected := h.assertSchemaStillCurrent(ctx, repo, pr, installationID, schemaResult, prInfo.HeadSHA, environment, requestedBy, action.Apply); rejected {
			if relErr := h.service.Storage().Locks().ForceRelease(ctx, database, dbType); relErr != nil {
				h.logger.Error("failed to release lock after stale-schema rejection",
					"repo", repo, "pr", pr, "database", database, "database_type", dbType, "error", relErr)
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
			headSHA, checkRunErr := h.storeApplyPlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment)
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
		headSHA, checkErr := h.storeApplyPlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment)
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

	// Manual apply: reject if the PR HEAD advanced after discovery loaded
	// schema files. Posting the confirmation plan against the loaded files
	// would render DDL for a commit the branch is no longer on — and the
	// user's subsequent `apply-confirm` does its own fresh discovery, so the
	// confirm-time freshness check passes against the new HEAD even though
	// the plan the user reviewed was rendered for the old commit. Catching
	// it here is the symmetric guard to the auto-confirm branch above.
	// Use FetchPullRequestNoCache; the cached fetch returns the discovery
	// SHA. The lock was acquired by this handler invocation, so ForceRelease
	// is safe.
	prInfo, prErr := client.FetchPullRequestNoCache(ctx, repo, pr)
	if prErr != nil {
		h.logger.Error("failed to fetch PR for stale-schema check, releasing lock",
			"repo", repo, "pr", pr, "database", database, "error", prErr)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Environment: environment,
			CommandName: action.Apply,
			ErrorDetail: "Failed to verify PR HEAD before posting plan: " + prErr.Error(),
		}))
		if relErr := h.service.Storage().Locks().ForceRelease(ctx, database, dbType); relErr != nil {
			h.logger.Error("failed to release lock after PR fetch failure",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "error", relErr)
		}
		return
	}
	if rejected := h.assertSchemaStillCurrent(ctx, repo, pr, installationID, schemaResult, prInfo.HeadSHA, environment, requestedBy, action.Apply); rejected {
		if relErr := h.service.Storage().Locks().ForceRelease(ctx, database, dbType); relErr != nil {
			h.logger.Error("failed to release lock after stale-schema rejection",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "error", relErr)
		}
		return
	}

	h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))

	// Create check run (action_required — waiting for apply-confirm) and update aggregate
	headSHA, checkErr := h.storeApplyPlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment)
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
	// Dedupe FetchPullRequest calls within this command invocation.
	ctx = ghclient.WithPRInfoCache(ctx)

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

	// Tier 2: PR checks gate — re-check on confirm to prevent bypass.
	//
	// Use FetchPullRequestNoCache here — the whole point of re-checking on
	// confirm is to use the *current* GitHub HEAD. The dedupe-friendly
	// FetchPullRequest would return the cached HeadSHA populated by
	// CreateSchemaRequestFromPR above, making enforcePassingChecks run
	// against a stale HeadSHA if a new commit landed during this delivery.
	confirmPRInfo, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to fetch PR for checks gate", "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Environment: environment,
			CommandName: action.ApplyConfirm,
			ErrorDetail: "Failed to fetch PR info: " + err.Error(),
		}))
		return
	}

	// Reject if the PR HEAD advanced after discovery loaded schema files.
	// Running against the loaded files would render a plan against an older
	// commit than the branch is on right now. Release the lock so the user
	// can re-run `schemabot apply -e <env>` cleanly.
	//
	// Use owner-scoped Release rather than ForceRelease: this handler runs on
	// every PR comment, so a stale-schema rejection on PR #2 must never clear
	// a lock held by PR #1 for the same target. Release deletes only when
	// owner matches; ErrLockNotFound / ErrLockNotOwned are expected here
	// (lock may be absent or held by another PR) and are not logged as errors.
	if rejected := h.assertSchemaStillCurrent(ctx, repo, pr, installationID, schemaResult, confirmPRInfo.HeadSHA, environment, requestedBy, action.ApplyConfirm); rejected {
		lockOwner := fmt.Sprintf("%s#%d", repo, pr)
		relErr := h.service.Storage().Locks().Release(ctx, schemaResult.Database, schemaResult.Type, lockOwner)
		if relErr != nil && !errors.Is(relErr, storage.ErrLockNotFound) && !errors.Is(relErr, storage.ErrLockNotOwned) {
			h.logger.Error("failed to release lock after stale-schema rejection",
				"repo", repo, "pr", pr, "database", schemaResult.Database, "database_type", schemaResult.Type, "error", relErr)
		}
		return
	}

	if blocked := h.enforcePassingChecks(ctx, client, repo, pr, installationID, confirmPRInfo.HeadSHA, environment); blocked {
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

	// Set observer before queuing the apply so ExecuteApply can register it on
	// the durable apply row before scheduler dispatch starts.
	observer := NewCommentObserver(CommentObserverConfig{
		GHClient:       h.ghClient,
		Storage:        h.service.Storage(),
		Repo:           repo,
		PR:             pr,
		InstallationID: installationID,
		DeferCutover:   options["defer_cutover"] == "true",
		Logger:         h.logger,
		OnTerminalHook: func(apply *storage.Apply) {
			updated, err := h.updateCheckRecordForApplyResult(context.Background(), repo, pr, apply)
			if err != nil {
				h.logger.Error("observer: failed to update check record",
					"repo", repo, "pr", pr, "database", apply.Database,
					"environment", apply.Environment, "apply_id", apply.ID, "error", err)
				return
			}
			if !updated {
				h.logger.Debug("observer: skipping aggregate check update, apply no longer owns check state",
					"repo", repo, "pr", pr, "apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier)
				return
			}
			ghInstClient, err := h.ghClient.ForInstallation(installationID)
			if err != nil {
				h.logger.Error("observer: failed to create GitHub client",
					"repo", repo, "pr", pr, "apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier,
					"installation_id", installationID, "error", err)
				return
			}
			checkRecord, err := h.service.Storage().Checks().Get(context.Background(), repo, pr, apply.Environment, apply.DatabaseType, apply.Database)
			if err != nil {
				h.logger.Error("observer: failed to load check record for aggregate update",
					"repo", repo, "pr", pr, "database", apply.Database,
					"database_type", apply.DatabaseType, "environment", apply.Environment,
					"apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier,
					"error", err)
				return
			}
			if checkRecord == nil {
				h.logger.Warn("observer: check record missing for aggregate update",
					"repo", repo, "pr", pr, "database", apply.Database,
					"database_type", apply.DatabaseType, "environment", apply.Environment,
					"apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier)
				return
			}
			h.updateAggregateCheck(context.Background(), ghInstClient, repo, pr, checkRecord.HeadSHA)
		},
	})
	h.service.SetPendingObserver(database, "", environment, observer)

	applyReq := api.ApplyRequest{
		PlanID:         planResp.PlanID,
		Environment:    environment,
		Options:        options,
		Caller:         caller,
		InstallationID: installationID,
	}

	applyResp, applyID, err := h.service.ExecuteApply(ctx, applyReq)
	if err != nil {
		h.service.SetPendingObserver(database, "", environment, nil)
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
		h.service.SetPendingObserver(database, "", environment, nil)
		h.logger.Info("apply rejected by engine", "repo", repo, "pr", pr, "database", database, "environment", environment, "error", applyResp.ErrorMessage)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Environment: environment,
			CommandName: action.Apply,
			ErrorDetail: "Apply was not accepted: " + applyResp.ErrorMessage,
		}))
		return
	}

	// ExecuteApply rejects accepted applies unless SchemaBot stored its own
	// apply row. Keep this guard fail-closed in case that invariant changes.
	if applyID <= 0 {
		h.service.SetPendingObserver(database, "", environment, nil)
		h.logger.Error("accepted apply did not return an apply id",
			"repo", repo, "pr", pr, "database", database,
			"database_type", schemaResult.Type, "environment", environment)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Timestamp:   time.Now().UTC().Format("2006-01-02 15:04:05"),
			Environment: environment,
			CommandName: action.Apply,
			ErrorDetail: "Apply was accepted, but SchemaBot did not receive a stored apply ID. SchemaBot cannot safely track progress or update required status checks. An operator must reconcile the apply state before retrying.",
		}))
		return
	}

	apply, err := h.service.Storage().Applies().Get(ctx, applyID)
	if err != nil {
		h.logger.Error("failed to load apply after accepted apply",
			"repo", repo, "pr", pr, "database", database,
			"database_type", schemaResult.Type, "environment", environment,
			"apply_id", applyID, "error", err)
		return
	}
	if apply == nil {
		h.logger.Error("apply missing after accepted apply",
			"repo", repo, "pr", pr, "database", database,
			"database_type", schemaResult.Type, "environment", environment,
			"apply_id", applyID)
		return
	}

	// Post the progress comment immediately so the observer always has a
	// comment to edit. This must happen before any terminal check — otherwise
	// the apply could complete between the check and the post, leaving a
	// stale "In Progress" comment that the observer never edits.
	progressBody := templates.RenderApplyStarted(templates.ApplyStatusCommentData{
		ApplyID:     applyResp.ApplyID,
		Database:    database,
		Environment: environment,
		RequestedBy: requestedBy,
		State:       apply.State,
		Engine:      schemaResult.Type,
	})
	h.postAndTrackComment(ctx, repo, pr, installationID, applyID, state.Comment.Progress, progressBody)

	// Update stored check state to in_progress (transitions action_required to in_progress).
	if err := h.updateCheckRecordForApplyStart(ctx, client, repo, pr, schemaResult, environment, applyID); err != nil {
		h.logger.Error("failed to mark check in_progress for apply",
			"repo", repo, "pr", pr, "database", database,
			"database_type", schemaResult.Type, "environment", environment,
			"apply_id", applyID, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Timestamp:   time.Now().UTC().Format("2006-01-02 15:04:05"),
			Environment: environment,
			CommandName: action.Apply,
			ErrorDetail: "Apply was accepted, but SchemaBot could not update the required status check: " + err.Error(),
		}))
		return
	}
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

// filterFailingNonSchemaBotChecks returns checks that are failing, excluding
// SchemaBot's own checks and checks with conclusion "neutral", "skipped", or "success".
// Only checks with completed status and conclusion "failure", "error", or "timed_out"
// are considered failing.
func filterFailingNonSchemaBotChecks(statuses []ghclient.PRCheckStatus) []templates.BlockingCheck {
	var failing []templates.BlockingCheck
	for _, s := range statuses {
		if s.IsSchemaBot {
			continue
		}
		if s.Status != "completed" {
			continue
		}
		switch s.Conclusion {
		case "failure", "error", "timed_out":
			failing = append(failing, templates.BlockingCheck{
				Name:  s.Name,
				State: s.Conclusion,
			})
		}
	}
	return failing
}

// enforcePassingChecks verifies that all non-SchemaBot PR checks are passing.
// Returns true if apply was blocked (caller should return), false if it may proceed.
// Blocks on both failing checks and in-progress checks with distinct messages.
func (h *Handler) enforcePassingChecks(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, headSHA, environment string) bool {
	if !h.service.Config().ShouldRequirePassingChecks() {
		h.logger.Debug("passing checks gate disabled", "repo", repo, "pr", pr)
		return false
	}

	statuses, err := client.GetPRCheckStatuses(ctx, repo, headSHA)
	if err != nil {
		h.logger.Error("failed to fetch PR check statuses, blocking apply",
			"repo", repo, "pr", pr, "environment", environment, "error", err)
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByCheckStatusError(environment, err))
		return true
	}

	failing := filterFailingNonSchemaBotChecks(statuses)
	inProgress := filterInProgressNonSchemaBotChecks(statuses)

	if len(failing) > 0 {
		h.logger.Info("apply blocked by failing PR checks",
			"repo", repo, "pr", pr, "environment", environment,
			"failing_count", len(failing))
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByFailingChecks(environment, failing))
		return true
	}

	if len(inProgress) > 0 {
		h.logger.Info("apply blocked by in-progress PR checks",
			"repo", repo, "pr", pr, "environment", environment,
			"in_progress_count", len(inProgress))
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByInProgressChecks(environment, inProgress))
		return true
	}

	return false
}

// filterInProgressNonSchemaBotChecks returns checks that are still running,
// excluding SchemaBot's own checks.
func filterInProgressNonSchemaBotChecks(statuses []ghclient.PRCheckStatus) []templates.BlockingCheck {
	var inProgress []templates.BlockingCheck
	for _, s := range statuses {
		if s.IsSchemaBot {
			continue
		}
		switch s.Status {
		case "in_progress", "queued", "pending":
			inProgress = append(inProgress, templates.BlockingCheck{
				Name:  s.Name,
				State: s.Status,
			})
		}
	}
	return inProgress
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

// storeApplyPlanCheckRecord stores a check record when an apply plan is posted.
func (h *Handler) storeApplyPlanCheckRecord(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schema *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment string) (string, error) {
	return h.storePlanCheckRecord(ctx, client, repo, pr, schema, planResp, environment)
}

// updateCheckRecordForApplyStart updates the stored check state to "in_progress"
// when an apply begins execution. The aggregate check is updated to reflect the state.
func (h *Handler) updateCheckRecordForApplyStart(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schema *ghclient.SchemaRequestResult, environment string, applyID int64) error {
	check, err := h.service.Storage().Checks().Get(ctx, repo, pr, environment, schema.Type, schema.Database)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "apply_started",
			Repository:   repo,
			Database:     schema.Database,
			DatabaseType: schema.Type,
			Environment:  environment,
			Status:       "error",
		})
		return fmt.Errorf("look up stored check state for apply start repo %s pr %d environment %s database_type %s database %s apply_id %d: %w",
			repo, pr, environment, schema.Type, schema.Database, applyID, err)
	}

	if check == nil {
		// No existing record: create one using the current PR head.
		prInfo, err := client.FetchPullRequest(ctx, repo, pr)
		if err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:    "apply_started",
				Repository:   repo,
				Database:     schema.Database,
				DatabaseType: schema.Type,
				Environment:  environment,
				Status:       "error",
			})
			return fmt.Errorf("fetch PR for apply start check repo %s pr %d environment %s database_type %s database %s apply_id %d: %w",
				repo, pr, environment, schema.Type, schema.Database, applyID, err)
		}
		check = &storage.Check{
			Repository:   repo,
			PullRequest:  pr,
			HeadSHA:      prInfo.HeadSHA,
			Environment:  environment,
			DatabaseType: schema.Type,
			DatabaseName: schema.Database,
			ApplyID:      applyID,
			HasChanges:   true,
			Status:       checkStatusInProgress,
			Conclusion:   "",
		}
	} else {
		check.ApplyID = applyID
		check.HasChanges = true
		check.Status = checkStatusInProgress
		check.Conclusion = ""
		check.BlockingReason = ""
		check.ErrorMessage = ""
	}

	if err := h.service.Storage().Checks().Upsert(ctx, check); err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "apply_started",
			Repository:   repo,
			Database:     schema.Database,
			DatabaseType: schema.Type,
			Environment:  environment,
			Status:       "error",
		})
		return fmt.Errorf("upsert stored check state for apply start repo %s pr %d environment %s database_type %s database %s apply_id %d head_sha %s: %w",
			repo, pr, environment, schema.Type, schema.Database, applyID, check.HeadSHA, err)
	}
	h.logger.Info("check record marked in_progress for apply",
		"repo", repo, "pr", pr, "database", schema.Database,
		"database_type", schema.Type, "environment", environment,
		"apply_id", applyID, "head_sha", check.HeadSHA)

	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:    "apply_started",
		Repository:   repo,
		Database:     schema.Database,
		DatabaseType: schema.Type,
		Environment:  environment,
		Status:       "success",
	})
	h.updateAggregateCheck(ctx, client, repo, pr, check.HeadSHA)
	return nil
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
