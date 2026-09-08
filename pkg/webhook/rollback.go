package webhook

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

const (
	rollbackPendingPlanPrefix = "rollback:"

	// rollbackLockReleaseTimeout bounds the lock release that runs after a
	// rollback will not proceed. The release runs detached from the command's
	// own context, which may already be cancelled at that point, so it needs
	// its own deadline.
	rollbackLockReleaseTimeout = 5 * time.Second
)

// handleRollbackCommand handles the "schemabot rollback <apply-id> -e <env>"
// PR comment command. It is the synchronous goSafe entry point: it runs
// rollbackCommandCore and discards the durability disposition, which only a
// durable issue_comment driver consumes.
func (h *Handler) handleRollbackCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	_, _ = h.rollbackCommandCore(context.Background(), repo, pr, installationID, requestedBy, result)
}

// rollbackCommandCore looks up the specified apply, generates a rollback plan
// from its captured original schema, acquires a lock, and posts the plan for
// confirmation. It returns a durability disposition for a durable
// issue_comment driver:
//
//   - retry=true, err!=nil — a transient infrastructure failure (command
//     bootstrap, a storage read, lock acquisition, remote-deployment
//     unavailability during plan generation, or plan pinning) that a durable
//     driver should re-drive; the same window may succeed on a later attempt.
//     When the exit also released a command-acquired lock, a failed release
//     joins the returned error so the disposition never asserts a release
//     that did not happen.
//   - retry=false, err=nil — a terminal outcome that is the command's answer
//     (a missing apply ID, an apply not found, a non-owned environment, a gate
//     block on the merits, a source-apply guardrail rejection, a lock held by
//     another PR, a typed deterministic plan-generation rejection, nothing to
//     do, or a posted rollback plan). Untyped plan-generation failures, such
//     as storage and remote-service failures, remain retryable. A
//     source-apply validation failure is terminal only when it is a typed
//     deterministic rejection or invariant violation; transient storage
//     failures there stay retryable.
//   - retry=false, err!=nil — a deterministic failure a re-drive would only
//     reproduce (a GitHub App resolution failure resolving the authorization
//     client): the delivery must not be re-driven, but no PR comment could
//     be posted without a client, so the error is recorded on the delivery
//     as its only triage trail rather than marking it completed.
//
// A gate block is terminal only when the gate evaluated its inputs and
// blocked on the merits. A gate that could not evaluate (for example a
// GitHub or storage read inside the gate failed) returns an error, which
// the core classifies retryable: the gate still fails closed for this
// delivery, but the outcome is not the command's answer.
//
// A posted PR comment does not imply a terminal disposition: retryable sites
// post best-effort error comments too, so a durable driver re-driving one may
// post the same comment again.
//
// The parent context scopes the command beyond its own timeout: the
// synchronous wrapper passes context.Background(), while the durable driver
// passes its run context so lease loss or shutdown cancels in-flight work.
//
// The core logs each failure at its site, so the synchronous wrapper can
// discard the result without losing observability.
func (h *Handler) rollbackCommandCore(parent context.Context, repo string, pr int, installationID int64, requestedBy string, result CommandResult) (bool, error) {
	ctx, cancel := h.commandContext(parent, commandTimeout)
	defer cancel()

	applyID := result.ApplyID
	if applyID == "" {
		if h.silentUsageErrorOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("skipping missing-apply-id reply for unscoped fan-out rollback; the leader posts it once",
				"repo", repo,
				"pr", pr,
				"environment", result.Environment,
				"requested_by", requestedBy)
			return false, nil
		}
		h.postComment(repo, pr, installationID, templates.RenderRollbackMissingApplyID(h.deploymentTenant()))
		return false, nil
	}

	if h.service == nil {
		h.logger.Error("service not configured for rollback")
		return true, fmt.Errorf("rollback command %s#%d: service not configured", repo, pr)
	}

	stor := h.service.Storage()
	if stor == nil {
		h.logger.Error("storage not configured for rollback", "repo", repo, "pr", pr, "apply_id", applyID)
		h.postCommandError(repo, pr, installationID, action.Rollback, result.Environment, requestedBy, "Storage is not available")
		return true, fmt.Errorf("rollback command %s#%d: storage not configured", repo, pr)
	}
	applyStore := stor.Applies()
	if applyStore == nil {
		h.logger.Error("apply store not configured for rollback", "repo", repo, "pr", pr, "apply_id", applyID)
		h.postCommandError(repo, pr, installationID, action.Rollback, result.Environment, requestedBy, "Apply store is not available")
		return true, fmt.Errorf("rollback command %s#%d: apply store not configured", repo, pr)
	}
	apply, err := applyStore.GetByApplyIdentifier(ctx, applyID)
	if err != nil {
		h.logger.Error("failed to look up rollback apply", "repo", repo, "pr", pr, "apply_id", applyID, "error", err)
		h.postCommandError(repo, pr, installationID, action.Rollback, result.Environment, requestedBy, "Failed to look up apply: "+err.Error())
		return true, fmt.Errorf("rollback command apply lookup %s#%d apply %s: %w", repo, pr, applyID, err)
	}
	if apply == nil {
		// On an aggregate repo an unscoped rollback fans out to every
		// deployment, but the apply lives in exactly one tenant's storage. A
		// deployment that doesn't have it is not the owner and stays silent so
		// only the owning deployment answers.
		if h.silentOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("unscoped fan-out rollback targets an apply not stored on this deployment; staying silent so the owning deployment responds",
				"repo", repo, "pr", pr, "apply_id", applyID, "environment", result.Environment)
			return false, nil
		}
		h.postComment(repo, pr, installationID, templates.RenderRollbackApplyNotFound(applyID))
		return false, nil
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
		return false, nil
	}
	h.acknowledgeCommandActPoint(repo, pr, installationID, result)

	// Rollback executes DDL against the target database, so the actor must be an
	// authorized admin/operator before SchemaBot reveals any lock or plan detail
	// for the database. The gate runs as soon as the database is known and only
	// after the environment-routing check above, so a non-owning instance stays
	// silent instead of posting a denial for an environment it does not manage.
	// An unauthorized actor must not learn lock ownership or rollback plan
	// contents by probing apply IDs.
	client, err := h.actorAuthorizationClient(repo, pr, installationID, requestedBy, database, environment, action.Rollback)
	if err != nil {
		// The gate has already logged the client-creation failure and posted
		// its best-effort authorization-unavailable comment; it fails closed
		// for this delivery. A GitHub App resolution failure is deterministic
		// per deployment config, so a re-drive reproduces it and the error is
		// recorded on the delivery instead; any other cause (an installation
		// token fetch, for example) may clear on a later attempt.
		if errors.Is(err, errGitHubAppResolution) {
			return false, fmt.Errorf("rollback command actor authorization client %s#%d: %w", repo, pr, err)
		}
		return true, fmt.Errorf("rollback command actor authorization client %s#%d: %w", repo, pr, err)
	}
	blocked, authErr := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, database, dbType, environment, action.Rollback, result.SuppressRetryComments)
	if authErr != nil {
		return true, fmt.Errorf("rollback command actor authorization gate %s#%d database %s: %w", repo, pr, database, authErr)
	}
	if blocked {
		return false, nil
	}

	if _, _, err := h.service.ValidateRollbackSourceApply(ctx, api.RollbackSourceRequest{
		ApplyIdentifier:         applyID,
		Environment:             result.Environment,
		Repository:              repo,
		PullRequest:             pr,
		RequirePullRequestScope: true,
	}); err != nil {
		return h.handleRollbackSourceError(repo, pr, installationID, requestedBy, apply, applyID, result.Environment, err)
	}

	// Check for existing lock
	lockStore := stor.Locks()
	if lockStore == nil {
		h.logger.Error("lock store not configured for rollback", "repo", repo, "pr", pr, "apply_id", applyID, "database", database, "database_type", dbType)
		h.postCommandError(repo, pr, installationID, action.Rollback, environment, requestedBy, "Lock store is not available")
		return true, fmt.Errorf("rollback command %s#%d: lock store not configured", repo, pr)
	}
	existingLock, err := lockStore.Get(ctx, database, dbType)
	if err != nil {
		h.logger.Error("failed to check lock", "error", err)
		h.postCommandError(repo, pr, installationID, action.Rollback, environment, requestedBy, "Failed to check lock status: "+err.Error())
		return true, fmt.Errorf("rollback command lock lookup %s#%d database %s: %w", repo, pr, database, err)
	}

	lockOwner := fmt.Sprintf("%s#%d", repo, pr)

	if existingLock != nil && existingLock.Owner != lockOwner {
		h.postComment(repo, pr, installationID, templates.RenderRollbackBlockedByLock(
			database, environment,
			existingLock.Owner, existingLock.Repository, existingLock.PullRequest,
			h.deploymentTenant()))
		return false, nil
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
			return true, fmt.Errorf("rollback command acquire lock %s#%d database %s: %w", repo, pr, database, err)
		}
	}

	if _, _, err := h.service.ValidateRollbackSourceApply(ctx, api.RollbackSourceRequest{
		ApplyIdentifier:         applyID,
		Environment:             result.Environment,
		Repository:              repo,
		PullRequest:             pr,
		RequirePullRequestScope: true,
	}); err != nil {
		releaseErr := h.releaseRollbackLockAfterRejectedPlan(ctx, database, dbType, lockOwner, lockAcquiredByCommand)
		// The released lock lets a re-drive start over, so an internal
		// validation failure stays retryable; a deterministic guardrail
		// rejection is the command's answer.
		if !api.IsTerminalControlError(err) {
			h.logger.Error("rollback source revalidation failed after lock acquisition",
				"repo", repo, "pr", pr, "apply_id", applyID,
				"environment", result.Environment, "database", database, "error", err)
			h.postCommandError(repo, pr, installationID, action.Rollback, environment, requestedBy, err.Error())
			return true, errors.Join(
				fmt.Errorf("rollback command revalidate source apply %s#%d apply %s: %w", repo, pr, applyID, err),
				releaseErr)
		}
		h.logRollbackLockReleaseFailure(repo, pr, database, dbType, lockOwner, releaseErr)
		if api.IsInternalControlError(err) {
			h.postRollbackTerminalInvariant("rollback source revalidation found a terminal data invariant violation",
				repo, pr, installationID, requestedBy, environment, applyID, database, err)
			return false, nil
		}
		h.logger.Warn("rollback rejected by source apply guardrails after lock acquisition",
			"repo", repo, "pr", pr, "apply_id", applyID,
			"environment", result.Environment, "database", database, "error", err)
		h.postRollbackRejected(repo, pr, installationID, apply, applyID, environment, database, err.Error())
		return false, nil
	}

	// Generate the rollback plan from the requested apply's captured original
	// schema. The lock is already held and the source apply was revalidated
	// after lock acquisition, so the plan can be pinned for confirmation.
	planResp, err := h.service.ExecuteRollbackPlanForApply(ctx, apply)
	if err != nil {
		releaseErr := h.releaseRollbackLockAfterRejectedPlan(ctx, database, dbType, lockOwner, lockAcquiredByCommand)
		// ExecuteRollbackPlanForApply classifies its own failures: transient
		// infrastructure (storage reads/writes, remote unavailability) stays
		// untyped and retryable, while data invariant violations and
		// deterministic engine rejections come back typed terminal.
		retryable := !api.IsTerminalControlError(err)
		h.logger.Error("rollback plan failed", "repo", repo, "pr", pr, "apply_id", applyID, "retryable", retryable, "error", err)
		h.postCommandError(repo, pr, installationID, action.Rollback, environment, requestedBy, err.Error())
		if retryable {
			return true, errors.Join(
				fmt.Errorf("rollback command plan %s#%d apply %s: %w", repo, pr, applyID, err),
				releaseErr)
		}
		h.logRollbackLockReleaseFailure(repo, pr, database, dbType, lockOwner, releaseErr)
		return false, nil
	}

	if planResp == nil || !planResp.HasChanges() {
		releaseErr := h.releaseRollbackLockAfterRejectedPlan(ctx, database, dbType, lockOwner, lockAcquiredByCommand)
		h.logRollbackLockReleaseFailure(repo, pr, database, dbType, lockOwner, releaseErr)
		h.postComment(repo, pr, installationID,
			templates.RenderRollbackNothingToDo(database, environment, applyID))
		return false, nil
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
		releaseErr := h.releaseRollbackLockAfterRejectedPlan(ctx, database, dbType, lockOwner, lockAcquiredByCommand)
		h.logger.Error("failed to pin rollback plan on lock", "repo", repo, "pr", pr,
			"database", database, "database_type", dbType, "environment", environment,
			"plan_id", planResp.PlanID, "error", err)
		h.postCommandError(repo, pr, installationID, action.Rollback, environment, requestedBy, "Failed to pin rollback plan on lock: "+err.Error())
		return true, errors.Join(
			fmt.Errorf("rollback command pin plan on lock %s#%d database %s plan %s: %w", repo, pr, database, planResp.PlanID, err),
			releaseErr)
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
		AgentHint:    h.agentHint(),
	}

	for _, sc := range planResp.Changes {
		nsData := templates.KeyspaceChangeData{
			Keyspace: sc.Namespace,
		}
		for _, t := range sc.TableChanges {
			nsData.Statements = append(nsData.Statements, t.DDL)
		}
		if sc.HasVSchemaChange() {
			nsData.VSchemaChanged = true
			nsData.VSchemaDiff = sc.Metadata[apitypes.VSchemaDiffMetadataKey]
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
	return false, nil
}

// handleRollbackSourceError posts the user-facing answer for a source-apply
// validation failure and returns its durability disposition: a missing apply
// and a guardrail rejection are deterministic answers, while an internal
// validation failure stays retryable for a durable driver.
func (h *Handler) handleRollbackSourceError(repo string, pr int, installationID int64, requestedBy string, apply *storage.Apply, applyID, environment string, err error) (bool, error) {
	if api.ControlOperationHTTPStatus(err) == http.StatusNotFound {
		h.postComment(repo, pr, installationID, templates.RenderRollbackApplyNotFound(applyID))
		return false, nil
	}
	if !api.IsTerminalControlError(err) {
		h.logger.Error("rollback source validation failed",
			"repo", repo, "pr", pr, "apply_id", applyID,
			"environment", environment, "error", err)
		h.postCommandError(repo, pr, installationID, action.Rollback, environment, requestedBy, err.Error())
		return true, fmt.Errorf("rollback command validate source apply %s#%d apply %s: %w", repo, pr, applyID, err)
	}
	if api.IsInternalControlError(err) {
		database := ""
		if apply != nil {
			database = apply.Database
		}
		h.postRollbackTerminalInvariant("rollback source validation found a terminal data invariant violation",
			repo, pr, installationID, requestedBy, environment, applyID, database, err)
		return false, nil
	}
	h.logger.Warn("rollback rejected by source apply guardrails",
		"repo", repo, "pr", pr, "apply_id", applyID,
		"environment", environment, "error", err)
	h.postRollbackRejected(repo, pr, installationID, apply, applyID, environment, "", err.Error())
	return false, nil
}

// postRollbackTerminalInvariant logs an internal terminal invariant violation
// and posts the command error as the delivery's answer. It runs only after
// IsTerminalControlError has already classified the failure as terminal;
// IsInternalControlError is re-checked at the call sites because terminal
// covers two classes with different surfacing: an internal invariant
// violation (error-level log and a generic command error, handled here) and a
// sub-500 guardrail rejection (warn-level log and a rejection comment, handled
// by the caller).
func (h *Handler) postRollbackTerminalInvariant(logMsg, repo string, pr int, installationID int64, requestedBy, environment, applyID, database string, err error) {
	attrs := []any{"repo", repo, "pr", pr, "apply_id", applyID, "environment", environment}
	if database != "" {
		attrs = append(attrs, "database", database)
	}
	h.logger.Error(logMsg, append(attrs, "error", err)...)
	h.postCommandError(repo, pr, installationID, action.Rollback, environment, requestedBy, err.Error())
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

// releaseRollbackLockAfterRejectedPlan releases the lock this command acquired
// once the rollback will not proceed. The release runs detached from the
// command's cancellation with its own timeout: the exits that need it include
// an expired command deadline, and the release must still happen so the
// database is not left locked with no pinned plan. The caller decides what a
// failed release means for its disposition: retryable exits join it into the
// returned error, terminal exits log it.
func (h *Handler) releaseRollbackLockAfterRejectedPlan(ctx context.Context, database, dbType, lockOwner string, release bool) error {
	if !release {
		return nil
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackLockReleaseTimeout)
	defer cancel()
	if err := h.service.Storage().Locks().Release(releaseCtx, database, dbType, lockOwner); err != nil {
		return fmt.Errorf("release rollback lock database %s type %s owner %s: %w", database, dbType, lockOwner, err)
	}
	return nil
}

// logRollbackLockReleaseFailure records a lock release that did not happen on
// a terminal exit. The terminal disposition is still the command's answer —
// a re-drive would reproduce it, and a re-drive under the same lock owner
// skips the release anyway — but the database stays locked until an operator
// runs unlock, so the failure must be visible for triage.
func (h *Handler) logRollbackLockReleaseFailure(repo string, pr int, database, dbType, lockOwner string, err error) {
	if err == nil {
		return
	}
	h.logger.Error("rollback lock release failed; database stays locked until an operator runs unlock",
		"repo", repo, "pr", pr, "database", database, "database_type", dbType, "owner", lockOwner, "error", err)
}

// handleRollbackConfirmCommand handles the "schemabot rollback-confirm -e <env>"
// PR comment command. It is the synchronous goSafe entry point: it runs
// rollbackConfirmCommandCore and discards the durability disposition, which
// only a durable issue_comment driver consumes.
func (h *Handler) handleRollbackConfirmCommand(repo string, pr int, environment string, installationID int64, requestedBy string, result CommandResult) {
	_, _ = h.rollbackConfirmCommandCore(context.Background(), repo, pr, environment, installationID, requestedBy, result)
}

// rollbackConfirmRejectionError marks a deterministic rollback-confirm
// rejection inside the pinned-plan resolution: the message is the command's
// answer, so a durable driver must not re-drive the delivery, while plain
// errors from the same lookups stay retryable.
type rollbackConfirmRejectionError struct{ err error }

func (e *rollbackConfirmRejectionError) Error() string { return e.err.Error() }
func (e *rollbackConfirmRejectionError) Unwrap() error { return e.err }

func rollbackConfirmRejection(err error) error { return &rollbackConfirmRejectionError{err: err} }

func isRollbackConfirmRejection(err error) bool {
	var rejection *rollbackConfirmRejectionError
	return errors.As(err, &rejection)
}

// rollbackConfirmCommandCore verifies the lock, loads the rollback plan pinned
// by the preceding rollback command, and executes the apply. It returns a
// durability disposition for a durable issue_comment driver:
//
//   - retry=true, err!=nil — a transient infrastructure failure before the
//     rollback apply is dispatched (command bootstrap, a storage read inside
//     the pinned-plan resolution, an unevaluable authorization gate, or a
//     failed lock release when there is nothing left to roll back) that a
//     durable driver should re-drive; the same window may succeed on a later
//     attempt.
//   - retry=false, err=nil — a terminal outcome that is the command's answer
//     (no pending rollback, a deterministic pinned-plan rejection, an
//     authorization block on the merits, nothing left to roll back, or any
//     exit at or after the ExecuteApply dispatch).
//   - retry=false, err!=nil — a deterministic failure a re-drive would only
//     reproduce (a GitHub App resolution failure): the delivery must not be
//     re-driven, but the command never ran and no PR comment could be posted,
//     so the error is recorded on the delivery as its only triage trail
//     rather than marking it completed.
//
// Every exit at or after the ExecuteApply call is terminal regardless of
// outcome: once the dispatch is attempted, the rollback DDL may already be
// executing on the target, so a durable re-drive could double-execute it.
// The pinned lock is not released on those failures, so recovery for a
// pre-acceptance failure is the user re-issuing rollback-confirm.
//
// A gate block is terminal only when the gate evaluated its inputs and
// blocked on the merits. A gate that could not evaluate (for example a
// GitHub team-membership read failed) returns an error, which the core
// classifies retryable: the gate still fails closed for this delivery, but
// the outcome is not the command's answer.
//
// A posted PR comment does not imply a terminal disposition: retryable sites
// post best-effort error comments too, so a durable driver re-driving one may
// post the same comment again.
//
// The parent context scopes the command beyond its own timeout: the
// synchronous wrapper passes context.Background(), while the durable driver
// passes its run context so lease loss or shutdown cancels in-flight work.
//
// The core logs each failure at its site, so the synchronous wrapper can
// discard the result without losing observability.
func (h *Handler) rollbackConfirmCommandCore(parent context.Context, repo string, pr int, environment string, installationID int64, requestedBy string, result CommandResult) (bool, error) {
	ctx, cancel, client, err := h.commandBootstrap(parent, repo, installationID)
	if err != nil {
		// A GitHub App resolution failure inside the bootstrap is deterministic
		// per deployment config, so a re-drive reproduces it; recovery is an
		// operator fixing the App mapping and the user re-issuing the command.
		// The command never ran and no PR comment could be posted, so the
		// error is returned (with retry=false) to record the failure on the
		// delivery instead of marking it completed. Other bootstrap failures
		// (an installation token fetch, for example) are transient and stay
		// retryable.
		if errors.Is(err, errGitHubAppResolution) {
			h.logger.Error("rollback-confirm blocked: cannot resolve GitHub App client for repo",
				"repo", repo, "pr", pr, "environment", environment, "error", err)
			return false, fmt.Errorf("rollback-confirm command bootstrap %s#%d: %w", repo, pr, err)
		}
		h.logger.Error("rollback-confirm: failed to bootstrap command", "repo", repo, "pr", pr,
			"environment", environment, "error", err)
		return true, fmt.Errorf("rollback-confirm command bootstrap %s#%d: %w", repo, pr, err)
	}
	defer cancel()

	lockOwner := fmt.Sprintf("%s#%d", repo, pr)
	existingLock, rollbackPlan, err := h.rollbackConfirmPlanForPR(ctx, repo, pr, environment, lockOwner)
	if err != nil {
		h.logger.Error("failed to resolve rollback-confirm plan", "repo", repo, "pr", pr,
			"environment", environment, "error", err)
		h.postCommandError(repo, pr, installationID, action.RollbackConfirm, environment, requestedBy, err.Error())
		if isRollbackConfirmRejection(err) {
			return false, nil
		}
		return true, fmt.Errorf("rollback-confirm command resolve pinned plan %s#%d environment %s: %w", repo, pr, environment, err)
	}
	if existingLock == nil || rollbackPlan == nil {
		// On an aggregate repo an unscoped rollback-confirm fans out to every
		// deployment, but only the deployment holding the pinned rollback lock
		// has anything to confirm. One with no pending rollback stays silent so
		// only the owning deployment answers.
		if h.silentOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("unscoped fan-out rollback-confirm found no pending rollback on this deployment; staying silent so the owning deployment responds",
				"repo", repo, "pr", pr, "environment", environment)
			return false, nil
		}
		h.postComment(repo, pr, installationID, templates.RenderRollbackConfirmNoLock("", environment, h.deploymentTenant()))
		return false, nil
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
	blocked, authErr := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, database, dbType, environment, action.RollbackConfirm, result.SuppressRetryComments)
	if authErr != nil {
		return true, fmt.Errorf("rollback-confirm command actor authorization gate %s#%d database %s: %w", repo, pr, database, authErr)
	}
	if blocked {
		return false, nil
	}

	// If no changes remain, release the lock and notify. The release is
	// conditioned on the observed pending plan so a newer same-owner intent
	// acquired after the resolution above stays intact; a mismatch or missing
	// lock is a no-op because the release's job is already done. A failed
	// release stays retryable: nothing has executed yet and a re-drive
	// re-resolves the pinned plan and retries the release, so the lock is not
	// stranded on a transient storage write failure.
	if !planHasChanges(rollbackPlan) {
		released, relErr := h.service.Storage().Locks().ReleaseIfPendingPlanID(ctx, database, dbType, lockOwner, existingLock.PendingPlanID)
		if relErr != nil {
			h.logger.Error("rollback-confirm found nothing to roll back but failed to release the database lock; applies on this database stay blocked until a re-issued rollback-confirm releases it",
				"repo", repo, "pr", pr, "database", database,
				"database_type", dbType, "environment", environment,
				"lock_owner", lockOwner, "error", relErr)
			h.postComment(repo, pr, installationID,
				templates.RenderRollbackAlreadyRolledBackLockHeld(database, environment, lockOwner, h.deploymentTenant()))
			return true, fmt.Errorf("rollback-confirm command release lock with nothing to roll back %s#%d database %s type %s owner %s: %w",
				repo, pr, database, dbType, lockOwner, relErr)
		}
		if !released {
			h.logger.Info("rollback-confirm found nothing to roll back and the pinned rollback lock was already released or superseded; leaving the current lock state untouched",
				"repo", repo, "pr", pr, "database", database,
				"database_type", dbType, "environment", environment,
				"lock_owner", lockOwner, "pending_plan_id", existingLock.PendingPlanID)
		}
		h.postComment(repo, pr, installationID,
			templates.RenderRollbackAlreadyRolledBack(database, environment))
		return false, nil
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

	// The command bootstrap already resolved this factory, so this lookup only
	// fails when the deployment config changed mid-command. The failure is
	// still deterministic for a re-drive against the changed config, so it
	// stays terminal; recovery is fixing the config and re-issuing the command.
	factory, factoryErr := h.factoryForRepo(repo)
	if factoryErr != nil {
		h.logger.Error("rollback blocked: cannot resolve GitHub App client for repo",
			"repo", repo, "pr", pr, "database", database, "environment", environment, "error", factoryErr)
		return false, nil
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

	// Every exit from here on is terminal for a durable driver: the dispatch
	// has been attempted, so the rollback DDL may already be executing and a
	// re-drive could double-execute it. A dispatch error leaves the pinned
	// lock in place, so the user can re-issue rollback-confirm.
	applyResp, applyID, err := h.service.ExecuteApply(ctx, applyReq)
	if err != nil {
		h.service.SetPendingObserver(database, rollbackPlan.Deployment, environment, nil)
		h.logger.Error("rollback apply failed", "repo", repo, "pr", pr, "error", err)
		h.postCommandError(repo, pr, installationID, action.RollbackConfirm, environment, requestedBy, "Failed to execute rollback: "+err.Error())
		return false, nil
	}

	if !applyResp.Accepted {
		h.service.SetPendingObserver(database, rollbackPlan.Deployment, environment, nil)
		h.postComment(repo, pr, installationID,
			templates.RenderRollbackNotAccepted(database, environment, applyResp.ErrorMessage))
		return false, nil
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
		return false, nil
	}

	apply, err := h.service.Storage().Applies().Get(ctx, applyID)
	if err != nil {
		h.logger.Error("failed to load rollback apply after accepted rollback",
			"repo", repo, "pr", pr, "database", database,
			"database_type", dbType, "environment", environment,
			"apply_id", applyResp.ApplyID, "error", err)
		return false, nil
	}
	if apply == nil {
		h.logger.Error("rollback apply missing after accepted apply",
			"repo", repo, "pr", pr, "database", database,
			"database_type", dbType, "environment", environment,
			"apply_id", applyResp.ApplyID)
		return false, nil
	}
	if err := h.updateCheckRecordForApplyStart(ctx, client, repo, pr, schemaResult, environment, apply); err != nil {
		h.logger.Error("failed to mark check in_progress for rollback",
			append(apply.LogAttrs(), "error", err)...)
		h.postCommandError(repo, pr, installationID, action.RollbackConfirm, environment, requestedBy, "Rollback was accepted, but SchemaBot could not update the required status check: "+err.Error())
		return false, nil
	}

	// Post initial progress comment for the observer to edit. VSchema status is
	// omitted on this first comment — the observer refreshes it from engine
	// display metadata on the next progress tick.
	progressBody := formatProgressComment(apply, nil, nil, h.deploymentTenant())
	h.postInitialProgressComment(ctx, repo, pr, installationID, apply, progressBody)
	return false, nil
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
			return nil, nil, rollbackConfirmRejection(fmt.Errorf("rollback lock for %s/%s belongs to %s, not %s",
				lock.DatabaseName, lock.DatabaseType, lock.Owner, lockOwner))
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
			return nil, nil, rollbackConfirmRejection(fmt.Errorf("rollback lock %s/%s has mismatched pinned plan: %s", lock.DatabaseName, lock.DatabaseType, mismatch))
		}
		if matchedPlan != nil {
			return nil, nil, rollbackConfirmRejection(fmt.Errorf("multiple rollback plans are pending for environment %s; cancel one with `schemabot unlock` before retrying `schemabot rollback-confirm -e %s`", environment, environment))
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

// rollbackPlanForLock loads the rollback plan the lock pins. A failed storage
// read stays a plain (retryable) error; a pinned plan whose row is missing is
// a deterministic rejection — a re-drive re-reads the same dangling pin.
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
		return nil, rollbackConfirmRejection(fmt.Errorf("rollback plan not found: %s", planID))
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
	return len(plan.VSchemaNamespaces()) > 0
}
