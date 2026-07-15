package webhook

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

const (
	rollbackPendingPlanPrefix = "rollback:"
	vSchemaArtifactName       = "vschema.json"
)

// handleRollbackCommand handles the "schemabot rollback <apply-id> -e <env>" PR comment command.
// It looks up the specified apply, generates a rollback plan from its original schema files,
// acquires a lock, and posts the plan for confirmation.
func (h *Handler) handleRollbackCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(commandTimeout)
	defer cancel()

	applyID := result.ApplyID
	if applyID == "" {
		h.postComment(repo, pr, installationID, templates.RenderRollbackMissingApplyID(h.deploymentTenant()))
		return
	}

	if h.service == nil {
		h.logger.Error("service not configured for rollback")
		return
	}

	stor := h.service.Storage()
	if stor == nil {
		h.logger.Error("storage not configured for rollback", "repo", repo, "pr", pr, "apply_id", applyID)
		h.postCommandError(repo, pr, installationID, action.Rollback, result.Environment, requestedBy, "Storage is not available")
		return
	}
	applyStore := stor.Applies()
	if applyStore == nil {
		h.logger.Error("apply store not configured for rollback", "repo", repo, "pr", pr, "apply_id", applyID)
		h.postCommandError(repo, pr, installationID, action.Rollback, result.Environment, requestedBy, "Apply store is not available")
		return
	}
	apply, err := applyStore.GetByApplyIdentifier(ctx, applyID)
	if err != nil {
		h.logger.Error("failed to look up rollback apply", "repo", repo, "pr", pr, "apply_id", applyID, "error", err)
		h.postCommandError(repo, pr, installationID, action.Rollback, result.Environment, requestedBy, "Failed to look up apply: "+err.Error())
		return
	}
	if apply == nil {
		// On an aggregate repo an unscoped rollback fans out to every
		// deployment, but the apply lives in exactly one tenant's storage. A
		// deployment that doesn't have it is not the owner and stays silent so
		// only the owning deployment answers.
		if h.silentOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("unscoped fan-out rollback targets an apply not stored on this deployment; staying silent so the owning deployment responds",
				"repo", repo, "pr", pr, "apply_id", applyID, "environment", result.Environment)
			return
		}
		h.postComment(repo, pr, installationID, templates.RenderRollbackApplyNotFound(applyID))
		return
	}

	database := apply.Database
	environment := apply.Environment
	dbType := apply.DatabaseType

	// In multi-instance setups, only the instance that owns this environment
	// should process the rollback. Without this check, every instance receives
	// the comment delivery and can react to the same rollback request.
	if h.service != nil && !h.service.Config().IsEnvironmentAllowed(environment) {
		h.logger.Info("ignoring rollback for non-allowed environment",
			"repo", repo, "pr", pr, "apply_id", applyID, "environment", environment)
		return
	}
	h.acknowledgeCommandActPoint(repo, pr, installationID, result)

	// Rollback executes DDL against the target database, so the actor must be an
	// authorized admin/operator before SchemaBot reveals any lock or plan detail
	// for the database. The gate runs as soon as the database is known and only
	// after the environment-routing check above, so a non-owning instance stays
	// silent instead of posting a denial for an environment it does not manage.
	// An unauthorized actor must not learn lock ownership or rollback plan
	// contents by probing apply IDs.
	client, blocked := h.actorAuthorizationClient(repo, pr, installationID, requestedBy, database, environment, action.Rollback)
	if blocked {
		return
	}
	if blocked := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, database, dbType, environment, action.Rollback); blocked {
		return
	}

	if _, _, err := h.service.ValidateRollbackSourceApply(ctx, api.RollbackSourceRequest{
		ApplyIdentifier:         applyID,
		Environment:             result.Environment,
		Repository:              repo,
		PullRequest:             pr,
		RequirePullRequestScope: true,
	}); err != nil {
		h.handleRollbackSourceError(repo, pr, installationID, requestedBy, apply, applyID, result.Environment, err)
		return
	}

	// Check for existing lock
	lockStore := stor.Locks()
	if lockStore == nil {
		h.logger.Error("lock store not configured for rollback", "repo", repo, "pr", pr, "apply_id", applyID, "database", database, "database_type", dbType)
		h.postCommandError(repo, pr, installationID, action.Rollback, environment, requestedBy, "Lock store is not available")
		return
	}
	existingLock, err := lockStore.Get(ctx, database, dbType)
	if err != nil {
		h.logger.Error("failed to check lock", "error", err)
		h.postCommandError(repo, pr, installationID, action.Rollback, environment, requestedBy, "Failed to check lock status: "+err.Error())
		return
	}

	lockOwner := fmt.Sprintf("%s#%d", repo, pr)

	if existingLock != nil && existingLock.Owner != lockOwner {
		h.postComment(repo, pr, installationID, templates.RenderRollbackBlockedByLock(
			database, environment,
			existingLock.Owner, existingLock.Repository, existingLock.PullRequest,
			h.deploymentTenant()))
		return
	}

	lockAcquiredByCommand := existingLock == nil
	if lockAcquiredByCommand {
		lock := &storage.Lock{
			DatabaseName: database,
			DatabaseType: dbType,
			Owner:        lockOwner,
			Repository:   repo,
			PullRequest:  pr,
		}
		if err := lockStore.Acquire(ctx, lock); err != nil {
			h.logger.Error("failed to acquire lock", "error", err)
			h.postCommandError(repo, pr, installationID, action.Rollback, environment, requestedBy, "Failed to acquire lock: "+err.Error())
			return
		}
	}

	if _, _, err := h.service.ValidateRollbackSourceApply(ctx, api.RollbackSourceRequest{
		ApplyIdentifier:         applyID,
		Environment:             result.Environment,
		Repository:              repo,
		PullRequest:             pr,
		RequirePullRequestScope: true,
	}); err != nil {
		h.releaseRollbackLockAfterRejectedPlan(ctx, database, dbType, lockOwner, lockAcquiredByCommand)
		h.logger.Warn("rollback rejected by source apply guardrails after lock acquisition",
			"repo", repo, "pr", pr, "apply_id", applyID,
			"environment", result.Environment, "database", database, "error", err)
		h.postRollbackRejected(repo, pr, installationID, apply, applyID, environment, database, err.Error())
		return
	}

	// Generate the rollback plan from the requested apply's captured original
	// schema. The lock is already held and the source apply was revalidated
	// after lock acquisition, so the plan can be pinned for confirmation.
	planResp, err := h.service.ExecuteRollbackPlanForApply(ctx, apply)
	if err != nil {
		h.releaseRollbackLockAfterRejectedPlan(ctx, database, dbType, lockOwner, lockAcquiredByCommand)
		errMsg := err.Error()
		if strings.Contains(errMsg, "no completed") {
			h.postComment(repo, pr, installationID, templates.RenderRollbackNoCompletedApply(database, environment))
			return
		}
		h.logger.Error("rollback plan failed", "repo", repo, "pr", pr, "apply_id", applyID, "error", err)
		h.postCommandError(repo, pr, installationID, action.Rollback, environment, requestedBy, errMsg)
		return
	}

	if !rollbackPlanResponseHasChanges(planResp) {
		h.releaseRollbackLockAfterRejectedPlan(ctx, database, dbType, lockOwner, lockAcquiredByCommand)
		h.postComment(repo, pr, installationID,
			templates.RenderRollbackNothingToDo(database, environment, applyID))
		return
	}

	lock := &storage.Lock{
		DatabaseName:  database,
		DatabaseType:  dbType,
		Owner:         lockOwner,
		Repository:    repo,
		PullRequest:   pr,
		PendingPlanID: rollbackPendingPlanID(planResp.PlanID),
	}
	if err := lockStore.Acquire(ctx, lock); err != nil {
		h.releaseRollbackLockAfterRejectedPlan(ctx, database, dbType, lockOwner, lockAcquiredByCommand)
		h.logger.Error("failed to pin rollback plan on lock", "repo", repo, "pr", pr,
			"database", database, "database_type", dbType, "environment", environment,
			"plan_id", planResp.PlanID, "error", err)
		h.postCommandError(repo, pr, installationID, action.Rollback, environment, requestedBy, "Failed to pin rollback plan on lock: "+err.Error())
		return
	}

	// Build comment data. The source apply ID stays in the comment metadata for
	// auditability, but rollback-confirm loads the lock-pinned rollback plan so
	// the user does not need to repeat the apply ID.
	commentData := templates.PlanCommentData{
		Database:     database,
		Environment:  environment,
		RequestedBy:  requestedBy,
		DatabaseType: dbType,
		IsMySQL:      dbType == "mysql",
		ApplyID:      apply.ApplyIdentifier,
		Tenant:       h.deploymentTenant(),
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

func (h *Handler) handleRollbackSourceError(repo string, pr int, installationID int64, requestedBy string, apply *storage.Apply, applyID, environment string, err error) {
	status := api.ControlOperationHTTPStatus(err)
	if status == http.StatusNotFound {
		h.postComment(repo, pr, installationID, templates.RenderRollbackApplyNotFound(applyID))
		return
	}
	if status >= http.StatusInternalServerError {
		h.logger.Error("rollback source validation failed",
			"repo", repo, "pr", pr, "apply_id", applyID,
			"environment", environment, "error", err)
		h.postCommandError(repo, pr, installationID, action.Rollback, environment, requestedBy, err.Error())
		return
	}
	h.logger.Warn("rollback rejected by source apply guardrails",
		"repo", repo, "pr", pr, "apply_id", applyID,
		"environment", environment, "error", err)
	h.postRollbackRejected(repo, pr, installationID, apply, applyID, environment, "", err.Error())
}

func (h *Handler) postRollbackRejected(repo string, pr int, installationID int64, apply *storage.Apply, applyID, environment, database, reason string) {
	data := templates.RollbackRejectedData{
		ApplyID:     applyID,
		Database:    database,
		Environment: environment,
		Reason:      reason,
	}
	if apply != nil {
		data.Database = apply.Database
		data.Environment = apply.Environment
	}
	h.postComment(repo, pr, installationID, templates.RenderRollbackRejected(data))
}

func (h *Handler) releaseRollbackLockAfterRejectedPlan(ctx context.Context, database, dbType, lockOwner string, release bool) {
	if !release {
		return
	}
	if err := h.service.Storage().Locks().Release(ctx, database, dbType, lockOwner); err != nil {
		h.logger.Error("failed to release rollback lock after rejected plan",
			"database", database, "database_type", dbType, "owner", lockOwner, "error", err)
	}
}

// handleRollbackConfirmCommand handles the "schemabot rollback-confirm -e <env>" PR comment command.
// It verifies the lock, loads the rollback plan pinned by the preceding rollback
// command, and executes the apply.
func (h *Handler) handleRollbackConfirmCommand(repo string, pr int, environment string, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel, client, err := h.commandBootstrap(repo, installationID)
	if err != nil {
		h.logger.Error("rollback-confirm: failed to bootstrap command", "error", err)
		return
	}
	defer cancel()

	lockOwner := fmt.Sprintf("%s#%d", repo, pr)
	existingLock, rollbackPlan, err := h.rollbackConfirmPlanForPR(ctx, repo, pr, environment, lockOwner)
	if err != nil {
		h.logger.Error("failed to resolve rollback-confirm plan", "repo", repo, "pr", pr,
			"environment", environment, "error", err)
		h.postCommandError(repo, pr, installationID, action.RollbackConfirm, environment, requestedBy, err.Error())
		return
	}
	if existingLock == nil || rollbackPlan == nil {
		// On an aggregate repo an unscoped rollback-confirm fans out to every
		// deployment, but only the deployment holding the pinned rollback lock
		// has anything to confirm. One with no pending rollback stays silent so
		// only the owning deployment answers.
		if h.silentOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("unscoped fan-out rollback-confirm found no pending rollback on this deployment; staying silent so the owning deployment responds",
				"repo", repo, "pr", pr, "environment", environment)
			return
		}
		h.postComment(repo, pr, installationID, templates.RenderRollbackConfirmNoLock("", environment, h.deploymentTenant()))
		return
	}
	h.acknowledgeCommandActPoint(repo, pr, installationID, result)

	database := rollbackPlan.Database
	dbType := rollbackPlan.DatabaseType
	schemaResult := &ghclient.SchemaRequestResult{
		Database:    database,
		Type:        dbType,
		Repository:  repo,
		PullRequest: pr,
	}

	// Rollback-confirm executes DDL with unsafe changes allowed, so the actor
	// must be an authorized admin/operator before any lock is released or acted
	// on. The database comes from the lock-pinned rollback plan instead of
	// current PR files so confirmation follows the reviewed rollback artifact.
	if blocked := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, database, dbType, environment, action.RollbackConfirm); blocked {
		return
	}

	// If no changes remain, release lock and notify
	if !planHasChanges(rollbackPlan) {
		if err := h.service.Storage().Locks().Release(ctx, database, dbType, lockOwner); err != nil {
			h.logger.Error("rollback-confirm found nothing to roll back but failed to release the database lock; applies on this database will be blocked until the lock is released manually",
				"repo", repo, "pr", pr, "database", database,
				"database_type", dbType, "environment", environment,
				"lock_owner", lockOwner, "error", err)
			h.postComment(repo, pr, installationID,
				templates.RenderRollbackAlreadyRolledBackLockHeld(database, environment, lockOwner, h.deploymentTenant()))
			return
		}
		h.postComment(repo, pr, installationID,
			templates.RenderRollbackAlreadyRolledBack(database, environment))
		return
	}

	// Build apply options — rollback always allows unsafe changes, and is marked
	// as a rollback so the terminal check update lands action_required (the PR's
	// change is reverted) even when an operator driver, not this command's
	// observer, publishes the terminal result.
	options := map[string]string{
		"allow_unsafe": "true",
		"rollback":     "true",
	}
	if result.DeferCutover {
		options["defer_cutover"] = "true"
	}

	factory, factoryErr := h.factoryForRepo(repo)
	if factoryErr != nil {
		h.logger.Error("rollback blocked: cannot resolve GitHub App client for repo",
			"repo", repo, "pr", pr, "database", database, "environment", environment, "error", factoryErr)
		return
	}

	observer := NewCommentObserver(CommentObserverConfig{
		GHClient:       factory,
		Storage:        h.service.Storage(),
		Repo:           repo,
		PR:             pr,
		InstallationID: installationID,
		DeferCutover:   options["defer_cutover"] == "true",
		SupportChannel: h.supportChannel(),
		Tenant:         h.deploymentTenant(),
		Logger:         h.logger,
		OnTerminalHook: func(a *storage.Apply) {
			// refreshChecksForTerminalApply routes a completed rollback straight
			// to action_required so the stored check state never passes through
			// success while the PR's schema change is reverted on the target.
			h.refreshChecksForTerminalApply(context.Background(), a, "rollback confirm")
		},
	})
	h.service.SetPendingObserver(database, rollbackPlan.Deployment, environment, observer)

	// Execute apply with the rollback plan. The caller attributes the apply to
	// the user who confirmed the rollback, not the lock owner (repo#pr), so
	// history and progress views show who acted.
	applyReq := api.ApplyRequest{
		PlanID:         rollbackPlan.PlanIdentifier,
		Environment:    environment,
		Options:        options,
		Caller:         formatGitHubCaller(requestedBy, repo, pr),
		InstallationID: installationID,
	}

	applyResp, applyID, err := h.service.ExecuteApply(ctx, applyReq)
	if err != nil {
		h.service.SetPendingObserver(database, rollbackPlan.Deployment, environment, nil)
		h.logger.Error("rollback apply failed", "repo", repo, "pr", pr, "error", err)
		h.postCommandError(repo, pr, installationID, action.RollbackConfirm, environment, requestedBy, "Failed to execute rollback: "+err.Error())
		return
	}

	if !applyResp.Accepted {
		h.service.SetPendingObserver(database, rollbackPlan.Deployment, environment, nil)
		h.postComment(repo, pr, installationID,
			templates.RenderRollbackNotAccepted(database, environment, applyResp.ErrorMessage))
		return
	}

	// Track rollback progress. After the rollback apply completes, set the check
	// to action_required because the PR's schema changes need to be re-applied.
	// ExecuteApply rejects accepted rollbacks unless SchemaBot stored its own
	// apply row. Keep this guard fail-closed in case that invariant changes.
	if applyID <= 0 {
		h.service.SetPendingObserver(database, rollbackPlan.Deployment, environment, nil)
		h.logger.Error("accepted rollback did not return an apply id",
			"repo", repo, "pr", pr, "database", database,
			"database_type", dbType, "environment", environment)
		h.postCommandError(repo, pr, installationID, action.RollbackConfirm, environment, requestedBy, "Rollback was accepted, but SchemaBot did not receive a stored apply ID. SchemaBot cannot safely track progress or update required status checks. An operator must reconcile the apply state before retrying.")
		return
	}

	apply, err := h.service.Storage().Applies().Get(ctx, applyID)
	if err != nil {
		h.logger.Error("failed to load rollback apply after accepted rollback",
			"repo", repo, "pr", pr, "database", database,
			"database_type", dbType, "environment", environment,
			"apply_id", applyID, "error", err)
		return
	}
	if apply == nil {
		h.logger.Error("rollback apply missing after accepted apply",
			"repo", repo, "pr", pr, "database", database,
			"database_type", dbType, "environment", environment,
			"apply_id", applyID)
		return
	}
	if err := h.updateCheckRecordForApplyStart(ctx, client, repo, pr, schemaResult, environment, applyID); err != nil {
		h.logger.Error("failed to mark check in_progress for rollback",
			"repo", repo, "pr", pr, "database", database,
			"database_type", dbType, "environment", environment,
			"apply_id", applyID, "error", err)
		h.postCommandError(repo, pr, installationID, action.RollbackConfirm, environment, requestedBy, "Rollback was accepted, but SchemaBot could not update the required status check: "+err.Error())
		return
	}

	// Post initial progress comment for the observer to edit. VSchema status is
	// omitted on this first comment — the observer refreshes it from engine
	// display metadata on the next progress tick.
	progressBody := formatProgressComment(apply, nil, nil, h.deploymentTenant())
	h.postInitialProgressComment(ctx, repo, pr, installationID, apply, progressBody)
}

func (h *Handler) rollbackConfirmPlanForPR(ctx context.Context, repo string, pr int, environment, lockOwner string) (*storage.Lock, *storage.Plan, error) {
	locks, err := h.service.Storage().Locks().GetByPR(ctx, repo, pr)
	if err != nil {
		return nil, nil, fmt.Errorf("list locks for %s#%d: %w", repo, pr, err)
	}

	var matchedLock *storage.Lock
	var matchedPlan *storage.Plan
	for _, lock := range locks {
		if lock == nil {
			h.logger.Warn("rollback-confirm skipping nil lock from storage", "repo", repo, "pr", pr, "environment", environment)
			continue
		}
		if !strings.HasPrefix(lock.PendingPlanID, rollbackPendingPlanPrefix) {
			h.logger.Debug("rollback-confirm skipping non-rollback lock",
				"repo", repo, "pr", pr, "database", lock.DatabaseName,
				"database_type", lock.DatabaseType, "pending_plan_id", lock.PendingPlanID)
			continue
		}
		if lock.Owner != lockOwner {
			return nil, nil, fmt.Errorf("rollback lock for %s/%s belongs to %s, not %s",
				lock.DatabaseName, lock.DatabaseType, lock.Owner, lockOwner)
		}

		plan, err := h.rollbackPlanForLock(ctx, lock)
		if err != nil {
			return nil, nil, fmt.Errorf("load rollback plan for %s/%s: %w", lock.DatabaseName, lock.DatabaseType, err)
		}
		if plan == nil {
			h.logger.Warn("rollback-confirm skipping rollback lock with no pinned plan",
				"repo", repo, "pr", pr, "database", lock.DatabaseName,
				"database_type", lock.DatabaseType, "pending_plan_id", lock.PendingPlanID)
			continue
		}
		if plan.Environment != environment {
			h.logger.Debug("rollback-confirm skipping rollback plan for another environment",
				"repo", repo, "pr", pr, "database", lock.DatabaseName,
				"database_type", lock.DatabaseType, "plan_id", plan.PlanIdentifier,
				"plan_environment", plan.Environment, "requested_environment", environment)
			continue
		}
		if mismatch := rollbackPlanCommandMismatch(plan, repo, pr, lock.DatabaseName, lock.DatabaseType, environment); mismatch != "" {
			return nil, nil, fmt.Errorf("rollback lock %s/%s has mismatched pinned plan: %s", lock.DatabaseName, lock.DatabaseType, mismatch)
		}
		if matchedPlan != nil {
			return nil, nil, fmt.Errorf("multiple rollback plans are pending for environment %s; cancel one with `schemabot unlock` before retrying `schemabot rollback-confirm -e %s`", environment, environment)
		}
		matchedLock = lock
		matchedPlan = plan
	}

	return matchedLock, matchedPlan, nil
}

func rollbackPendingPlanID(planID string) string {
	if planID == "" {
		return ""
	}
	return rollbackPendingPlanPrefix + planID
}

func rollbackPlanIDFromLock(lock *storage.Lock) (string, bool) {
	if lock == nil || !strings.HasPrefix(lock.PendingPlanID, rollbackPendingPlanPrefix) {
		return "", false
	}
	planID := strings.TrimPrefix(lock.PendingPlanID, rollbackPendingPlanPrefix)
	return planID, planID != ""
}

func rollbackPlanResponseHasChanges(resp *apitypes.PlanResponse) bool {
	if resp == nil {
		return false
	}
	for _, sc := range resp.Changes {
		if len(sc.TableChanges) > 0 {
			return true
		}
		if sc.Metadata["vschema"] != "" || sc.Metadata["vschema_changed"] == "true" {
			return true
		}
	}
	return false
}

func (h *Handler) rollbackPlanForLock(ctx context.Context, lock *storage.Lock) (*storage.Plan, error) {
	planID, ok := rollbackPlanIDFromLock(lock)
	if !ok {
		return nil, nil
	}
	plan, err := h.service.Storage().Plans().Get(ctx, planID)
	if err != nil {
		return nil, fmt.Errorf("load rollback plan %s: %w", planID, err)
	}
	if plan == nil {
		return nil, fmt.Errorf("rollback plan not found: %s", planID)
	}
	return plan, nil
}

func rollbackPlanCommandMismatch(plan *storage.Plan, repo string, pr int, database, dbType, environment string) string {
	if plan == nil {
		return "rollback plan is missing"
	}
	if plan.Repository != repo || plan.PullRequest != pr {
		return fmt.Sprintf("rollback plan %s belongs to %s#%d, not %s#%d",
			plan.PlanIdentifier, plan.Repository, plan.PullRequest, repo, pr)
	}
	if plan.Database != database {
		return fmt.Sprintf("rollback plan %s belongs to database %s, not %s",
			plan.PlanIdentifier, plan.Database, database)
	}
	if plan.DatabaseType != dbType {
		return fmt.Sprintf("rollback plan %s belongs to database type %s, not %s",
			plan.PlanIdentifier, plan.DatabaseType, dbType)
	}
	if plan.Environment != environment {
		return fmt.Sprintf("rollback plan %s belongs to environment %s, not %s",
			plan.PlanIdentifier, plan.Environment, environment)
	}
	return ""
}

func planHasChanges(plan *storage.Plan) bool {
	if plan == nil {
		return false
	}
	if len(plan.FlatDDLChanges()) > 0 {
		return true
	}
	for _, nsData := range plan.Namespaces {
		if nsData != nil && nsData.Artifacts[vSchemaArtifactName] != "" {
			return true
		}
	}
	return false
}
