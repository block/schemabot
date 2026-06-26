package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// handleApplyCommand handles the "schemabot apply -e <env>" PR comment command.
// It generates a plan, acquires a lock, and applies automatically unless safety
// rechecks require a manual confirmation.
func (h *Handler) handleApplyCommand(repo string, pr int, environment, databaseName string, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel, client, err := h.commandBootstrap(repo, installationID)
	if err != nil {
		h.logger.Error("apply: failed to bootstrap command", "error", err)
		return
	}
	defer cancel()

	if handled, err := h.handleNoManagedSchemaChangesForCommand(ctx, client, repo, pr, installationID, action.Apply, environment, databaseName, requestedBy); err != nil {
		h.logger.Error("failed to check whether apply command needs schema change reconciliation", "repo", repo, "pr", pr, "environment", environment, "database", databaseName, "error", err)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, err.Error())
		return
	} else if handled {
		return
	}

	// Discover config and fetch schema files from PR
	schemaResult, err := h.createManagedSchemaRequestFromPR(ctx, client, repo, pr, environment, databaseName, action.Apply)
	if err != nil {
		h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.Apply, err)
		return
	}
	if err := h.attachServerEnvironments(schemaResult, environment); err != nil {
		h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.Apply, err)
		return
	}

	if blocked := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, schemaResult.Database, schemaResult.Type, environment, action.Apply); blocked {
		return
	}

	// Fix checks stuck at "in_progress" from crashed applies after the actor
	// is authorized to run apply for this database.
	if err := h.reconcileStaleChecks(ctx, client, repo, pr); err != nil {
		h.logger.Error("failed to reconcile stale status checks", "repo", repo, "pr", pr, "error", err)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Failed to reconcile stale status checks: "+err.Error())
		return
	}

	// Tier 1: review gate (server-owned review policy for this database)
	if blocked := h.enforceReviewGate(ctx, client, repo, pr, installationID, schemaResult, environment, requestedBy, action.Apply); blocked {
		h.logger.Info("apply blocked by review gate", "repo", repo, "pr", pr, "environment", environment, "requested_by", requestedBy)
		return
	}

	// Tier 2: PR checks gate — block if non-SchemaBot checks are not passing
	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to fetch PR for checks gate", "error", err)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Failed to fetch PR info: "+err.Error())
		return
	}
	if blocked := h.enforcePassingChecks(ctx, client, repo, pr, installationID, prInfo.HeadSHA, environment); blocked {
		return
	}

	database := schemaResult.Database
	dbType := schemaResult.Type
	lockOwner := fmt.Sprintf("%s#%d", repo, pr)

	// Environment ordering enforcement: prior server-configured environments must be clean before applying.
	if blocked := h.checkPriorEnvironments(ctx, repo, pr, database, dbType, environment, schemaResult.Environments, installationID); blocked {
		h.logger.Info("apply blocked by environment ordering", "repo", repo, "pr", pr, "database", database, "environment", environment)
		return
	}

	// Check for existing lock
	existingLock, err := h.service.Storage().Locks().Get(ctx, database, dbType)
	if err != nil {
		h.logger.Error("failed to check lock", "error", err)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Failed to check lock status: "+err.Error())
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

		// Stale lock from this PR (no active applies) — release it so we can re-plan.
		// Use owner-scoped Release: ownership can change between the Get above
		// and this Release (e.g. an unrelated `schemabot unlock` clears the lock
		// and another PR acquires it). ErrLockNotFound / ErrLockNotOwned are
		// expected and silently no-op'd — the loop below will reacquire if free.
		relErr := h.service.Storage().Locks().Release(ctx, database, dbType, lockOwner)
		if relErr != nil && !errors.Is(relErr, storage.ErrLockNotFound) && !errors.Is(relErr, storage.ErrLockNotOwned) {
			h.logger.Error("failed to release stale lock",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "error", relErr)
		}
	}

	// Generate plan
	prNumber := int32(pr)
	planReq := api.PlanRequest{
		Database:      schemaResult.Database,
		Environment:   environment,
		Type:          schemaResult.Type,
		SchemaFiles:   schemaResult.SchemaFiles,
		Repository:    repo,
		PullRequest:   &prNumber,
		HeadSHA:       &schemaResult.HeadSHA,
		SchemaPath:    schemaResult.SchemaPath,
		SourceTrusted: true,
	}

	planResp, err := h.executePlanWithTransientRetry(ctx, planReq, repo, pr)
	if err != nil {
		h.logger.Error("plan execution failed", "repo", repo, "pr", pr, "error", err)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, err.Error())
		return
	}

	// No changes — post a regular plan comment (no lock, no confirm footer)
	if len(planResp.FlatTables()) == 0 {
		commentData := buildPlanCommentData(schemaResult, planResp, environment, result.Tenant, requestedBy)
		h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))
		return
	}

	// Block unsafe changes unless --allow-unsafe was specified
	if len(planResp.UnsafeChanges()) > 0 && !result.AllowUnsafe {
		commentData := buildPlanCommentData(schemaResult, planResp, environment, result.Tenant, requestedBy)
		h.logger.Info("apply blocked by unsafe changes", "repo", repo, "pr", pr, "database", database, "environment", environment)
		h.postComment(repo, pr, installationID, templates.RenderUnsafeChangesBlocked(commentData))
		return
	}

	// Acquire lock. PendingPlanID pins the confirmation plan this lock was
	// posted with so apply-confirm can load the exact plan the human reviewed
	// (not whatever happens to be newest in the plans table at confirm time).
	lock := &storage.Lock{
		DatabaseName:  database,
		DatabaseType:  dbType,
		Owner:         lockOwner,
		Repository:    repo,
		PullRequest:   pr,
		PendingPlanID: planResp.PlanID,
	}
	if err := h.service.Storage().Locks().Acquire(ctx, lock); err != nil {
		h.logger.Error("failed to acquire lock", "error", err)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Failed to acquire lock: "+err.Error())
		return
	}

	// Build plan comment data with lock info
	commentData := buildPlanCommentData(schemaResult, planResp, environment, result.Tenant, requestedBy)
	commentData.IsLocked = true
	commentData.LockOwner = lockOwner
	commentData.LockAcquired = time.Now().UTC().Format("2006-01-02 15:04:05 UTC")
	commentData.DeferCutover = result.DeferCutover
	commentData.SkipRevert = result.SkipRevert
	commentData.AllowUnsafe = result.AllowUnsafe

	// Check safety conditions before proceeding.
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
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Failed to verify PR HEAD before apply: "+prErr.Error())
		relErr := h.service.Storage().Locks().Release(ctx, database, dbType, lockOwner)
		if relErr != nil && !errors.Is(relErr, storage.ErrLockNotFound) && !errors.Is(relErr, storage.ErrLockNotOwned) {
			h.logger.Error("failed to release lock after PR fetch failure",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "error", relErr)
		}
		return
	}
	if rejected := h.assertSchemaStillCurrent(ctx, repo, pr, installationID, schemaResult, prInfo.HeadSHA, environment, requestedBy, action.Apply); rejected {
		relErr := h.service.Storage().Locks().Release(ctx, database, dbType, lockOwner)
		if relErr != nil && !errors.Is(relErr, storage.ErrLockNotFound) && !errors.Is(relErr, storage.ErrLockNotOwned) {
			h.logger.Error("failed to release lock after stale-schema rejection",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "error", relErr)
		}
		return
	}

	// Re-evaluate the checks gate against the fresh HEAD before executing.
	// The early gate at the top of handleApplyCommand ran against the
	// discovery-time HeadSHA. On the automatic apply path there is no second
	// user action between plan and apply, so a required check that
	// transitioned to failing on the same SHA (e.g. CI re-ran red, or a
	// new required check was added) would otherwise sneak past. Release
	// the lock on block so the user can re-run `schemabot apply -e <env>`
	// once the checks recover, without a manual unlock.
	//
	// Use owner-scoped Release rather than ForceRelease: although this
	// handler invocation acquired the lock earlier, ownership can change
	// between acquisition and this point (e.g. `schemabot unlock` clears
	// the lock and another PR acquires it). Release deletes only when
	// owner matches; ErrLockNotFound / ErrLockNotOwned are expected here
	// (lock may be absent or now held by another PR) and are not logged
	// as errors.
	if blocked := h.enforcePassingChecks(ctx, client, repo, pr, installationID, prInfo.HeadSHA, environment); blocked {
		relErr := h.service.Storage().Locks().Release(ctx, database, dbType, lockOwner)
		if relErr != nil && !errors.Is(relErr, storage.ErrLockNotFound) && !errors.Is(relErr, storage.ErrLockNotOwned) {
			h.logger.Error("failed to release lock after fresh-HEAD checks gate block",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "error", relErr)
		}
		return
	}

	// Look up the plan we just created for DDL comparison in executeApply.
	// Fail closed: if we can't load the plan, downgrade to manual confirmation
	// rather than skipping the DDL drift check entirely.
	storedPlan, planErr := h.service.Storage().Plans().Get(ctx, planResp.PlanID)
	if planErr != nil || storedPlan == nil {
		h.logger.Info("automatic apply downgraded: could not load plan for DDL comparison",
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
}

// handleApplyConfirmCommand handles the "schemabot apply-confirm -e <env>" PR comment command.
// It verifies lock ownership, re-plans for drift detection, executes the apply, and watches progress.
func (h *Handler) handleApplyConfirmCommand(repo string, pr int, environment, databaseName string, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel, client, err := h.commandBootstrap(repo, installationID)
	if err != nil {
		h.logger.Error("apply-confirm: failed to bootstrap command", "error", err)
		return
	}
	defer cancel()

	if handled, err := h.handleNoManagedSchemaChangesForCommand(ctx, client, repo, pr, installationID, action.ApplyConfirm, environment, databaseName, requestedBy); err != nil {
		h.logger.Error("failed to check whether apply-confirm command needs schema change reconciliation", "repo", repo, "pr", pr, "environment", environment, "database", databaseName, "error", err)
		h.postCommandError(repo, pr, installationID, action.ApplyConfirm, environment, requestedBy, err.Error())
		return
	} else if handled {
		return
	}

	// Discover database config from PR's schemabot.yaml
	schemaResult, err := h.createManagedSchemaRequestFromPR(ctx, client, repo, pr, environment, databaseName, action.ApplyConfirm)
	if err != nil {
		h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.ApplyConfirm, err)
		return
	}
	if err := h.attachServerEnvironments(schemaResult, environment); err != nil {
		h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.ApplyConfirm, err)
		return
	}

	if blocked := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, schemaResult.Database, schemaResult.Type, environment, action.ApplyConfirm); blocked {
		return
	}

	// Tier 1: review gate (re-check on confirm to prevent bypass)
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
		h.postCommandError(repo, pr, installationID, action.ApplyConfirm, environment, requestedBy, "Failed to fetch PR info: "+err.Error())
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
		h.postCommandError(repo, pr, installationID, action.ApplyConfirm, environment, requestedBy, "Failed to check lock status: "+err.Error())
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
	if strings.HasPrefix(existingLock.PendingPlanID, rollbackPendingPlanPrefix) {
		h.logger.Info("apply-confirm rejected: lock belongs to rollback plan", "repo", repo, "pr", pr,
			"database", database, "environment", environment, "pending_plan_id", existingLock.PendingPlanID)
		h.postCommandError(repo, pr, installationID, action.ApplyConfirm, environment, requestedBy,
			"This lock belongs to a rollback plan. Use `schemabot rollback-confirm` to execute it, or `schemabot unlock` to cancel it.")
		return
	}

	// Cross-delivery freshness check: reject if the confirmation plan (the one
	// the user reviewed) was rendered against a commit that is no longer the
	// PR HEAD. This closes the window that assertSchemaStillCurrent cannot
	// see — HEAD advancing between the plan being posted and the user clicking
	// apply-confirm. We compare against the *stored plan's* SHA, not the
	// confirm-time discovery SHA, because at this point both ends of a
	// confirm-time-discovery-vs-fresh-HEAD comparison would see the new SHA.
	//
	// We load the plan by lock.PendingPlanID — the plan_identifier this lock
	// was acquired with — instead of "newest plan for repo+pr+env+database".
	// The newest-plan lookup is unsafe because plain `schemabot plan` results
	// land in the same plans table and can supersede the confirmation plan a
	// reviewer is about to confirm.
	//
	// Use owner-scoped Release rather than ForceRelease even though the
	// ownership check above just succeeded: ownership can change between that
	// Get and this delete (intervening unlock/reacquire by another PR), and
	// ForceRelease would clear the new owner's lock. Release deletes only when
	// owner still matches; ErrLockNotFound / ErrLockNotOwned are expected if
	// ownership has already changed and are not logged as errors.
	storedPlan, planLoadErr := h.confirmationPlanForLock(ctx, existingLock)
	if planLoadErr != nil {
		h.logger.Error("failed to load confirmation plan for cross-delivery freshness check",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
			"pending_plan_id", existingLock.PendingPlanID, "error", planLoadErr)
		h.postCommandError(repo, pr, installationID, action.ApplyConfirm, environment, requestedBy, "Failed to load confirmation plan: "+planLoadErr.Error())
		return
	}
	if rejected := h.assertPlanStillCurrent(ctx, repo, pr, installationID, storedPlan, confirmPRInfo.HeadSHA, environment, requestedBy); rejected {
		relErr := h.service.Storage().Locks().Release(ctx, database, dbType, lockOwner)
		if relErr != nil && !errors.Is(relErr, storage.ErrLockNotFound) && !errors.Is(relErr, storage.ErrLockNotOwned) {
			h.logger.Error("failed to release lock after stale-plan rejection",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "error", relErr)
		}
		return
	}

	h.executeApply(ctx, client, repo, pr, schemaResult, environment, installationID, requestedBy, result, nil)
}

// handleUnlockCommand handles the "schemabot unlock" PR comment command.
// By default it finds all locks held by this PR and releases them. With
// `--force`, it can also release a CLI-owned lock for the database inferred
// from this PR's SchemaBot config; `-d <database>` disambiguates multi-database
// PRs. This lets a PR author clear a stale local-session lock from the PR
// workflow that it is blocking.
func (h *Handler) handleUnlockCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(30 * time.Second)
	defer cancel()
	if result.Force && result.Database == "" {
		database, err := h.inferUnlockDatabase(ctx, repo, pr, installationID)
		if err != nil {
			h.logger.Error("failed to infer database for force unlock", "repo", repo, "pr", pr, "error", err)
			h.postCommandError(repo, pr, installationID, action.Unlock, "", requestedBy, "Failed to infer database for force unlock: "+err.Error())
			return
		}
		result.Database = database
	}

	locks, err := h.locksForUnlock(ctx, repo, pr, result)
	if err != nil {
		h.logger.Error("failed to look up unlock targets", "repo", repo, "pr", pr, "database", result.Database, "force", result.Force, "error", err)
		h.postCommandError(repo, pr, installationID, action.Unlock, "", requestedBy, "Failed to look up locks: "+err.Error())
		return
	}

	if len(locks) == 0 {
		h.logger.Info("unlock: no locks found", "repo", repo, "pr", pr)
		h.postComment(repo, pr, installationID, templates.RenderNoLocksFound())
		return
	}

	// Unlock mutates lock state for every matched database, including
	// force-releasing CLI-owned locks, so the actor must be an authorized
	// admin/operator for each affected database before any lock is released.
	// Locks are not environment-scoped, so authorization is enforced per
	// database without an environment.
	client, blocked := h.actorAuthorizationClient(repo, pr, installationID, requestedBy, locks[0].DatabaseName, "", action.Unlock)
	if blocked {
		return
	}
	for _, lock := range locks {
		if blocked := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, lock.DatabaseName, lock.DatabaseType, "", action.Unlock); blocked {
			return
		}
	}

	// Check for active applies on any locked database. Even force-unlock should
	// not break a lock while SchemaBot still has a non-terminal apply recorded
	// for the same database/type. When apply state cannot be read, the unlock
	// fails closed: storage uncertainty must never release a lock that could be
	// protecting an in-flight apply.
	for _, lock := range locks {
		applies, err := h.service.Storage().Applies().GetByDatabase(ctx, lock.DatabaseName, lock.DatabaseType, "")
		if err != nil {
			h.logger.Error("unlock refused: cannot verify active applies, no locks will be released",
				"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType, "error", err)
			h.postCommandError(repo, pr, installationID, action.Unlock, "", requestedBy,
				"Failed to verify active applies for database `"+lock.DatabaseName+"`: "+err.Error()+". No locks were released.")
			return
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
		var err error
		if result.Force {
			err = h.service.Storage().Locks().ForceRelease(ctx, lock.DatabaseName, lock.DatabaseType)
		} else {
			err = h.service.Storage().Locks().Release(ctx, lock.DatabaseName, lock.DatabaseType, lock.Owner)
		}
		if err != nil {
			h.logger.Error("failed to release lock", "database", lock.DatabaseName, "error", err)
			continue
		}

		h.postComment(repo, pr, installationID, templates.RenderUnlockSuccess(
			lock.DatabaseName, "", requestedBy))

		// Update check run to neutral for PR-owned locks. CLI-owned locks have no
		// associated PR check record to update.
		if lock.Repository != "" && lock.PullRequest != 0 {
			h.updateCheckRunAfterUnlock(ctx, repo, pr, lock, installationID)
		}
	}
}

func (h *Handler) locksForUnlock(ctx context.Context, repo string, pr int, result CommandResult) ([]*storage.Lock, error) {
	if result.Force {
		if result.Database == "" {
			return nil, fmt.Errorf("--force requires a database target")
		}

		locks, err := h.service.Storage().Locks().List(ctx)
		if err != nil {
			return nil, err
		}

		var matches []*storage.Lock
		for _, lock := range locks {
			if lock.DatabaseName != result.Database {
				continue
			}
			if lock.PullRequest != 0 && (lock.Repository != repo || lock.PullRequest != pr) {
				return nil, fmt.Errorf("lock for %s is held by %s#%d", result.Database, lock.Repository, lock.PullRequest)
			}
			matches = append(matches, lock)
		}
		return matches, nil
	}

	locks, err := h.service.Storage().Locks().GetByPR(ctx, repo, pr)
	if err != nil {
		return nil, err
	}
	if result.Database == "" {
		return locks, nil
	}

	filtered := locks[:0]
	for _, lock := range locks {
		if lock.DatabaseName == result.Database {
			filtered = append(filtered, lock)
		}
	}
	return filtered, nil
}

func (h *Handler) inferUnlockDatabase(ctx context.Context, repo string, pr int, installationID int64) (string, error) {
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		return "", err
	}

	config, configDir, err := client.FindConfigForPR(ctx, repo, pr)
	if errors.Is(err, ghclient.ErrNoConfig) {
		config, configDir, _, err = client.FindConfigInRepo(ctx, repo, pr)
	}
	if err != nil {
		if errors.Is(err, ghclient.ErrMultipleConfigs) {
			return "", fmt.Errorf("multiple SchemaBot configs match this PR; retry with `schemabot unlock -d <database> --force`: %w", err)
		}
		return "", err
	}
	if !h.configPathManagedByRepo(ctx, repo, pr, "", config, configDir, action.Unlock) {
		return "", ghclient.ErrNoConfig
	}
	if config == nil || config.Database == "" {
		return "", fmt.Errorf("no database found in SchemaBot config")
	}
	return config.Database, nil
}
