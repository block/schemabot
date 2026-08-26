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
// It is the synchronous goSafe entry point: it runs applyCommandCore and
// discards the durability disposition, which only a durable issue_comment
// driver consumes.
func (h *Handler) handleApplyCommand(repo string, pr int, environment, databaseName string, installationID int64, requestedBy string, result CommandResult) {
	_, _ = h.applyCommandCore(context.Background(), repo, pr, environment, databaseName, installationID, requestedBy, result)
}

// applyCommandCore generates a plan, acquires a lock, and applies automatically
// unless safety rechecks require a manual confirmation. It returns a durability
// disposition for the durable issue_comment driver:
//
//   - retry=true, err!=nil — a transient infrastructure failure (command
//     bootstrap, a GitHub read, or a storage operation) that a durable driver
//     should re-drive; the same window may succeed on a later attempt.
//   - retry=false, err=nil — a terminal outcome that is the command's answer
//     (lock conflict, apply-in-progress, gate blocks, stale-schema rejection,
//     no changes, unsafe changes, deterministic plan rejections,
//     downgrade-to-manual, or a dispatched apply). A schema-request failure is
//     terminal only when handleSchemaRequestError recognizes it as a
//     user-facing rejection; an unexpected failure there (for example a
//     transient GitHub config read) stays retryable.
//   - retry=false, err!=nil — a deterministic failure a re-drive would only
//     reproduce (a GitHub App resolution failure inside the bootstrap): the
//     delivery must not be re-driven, but the command never ran and no PR
//     comment could be posted, so the error is recorded on the delivery as
//     its only triage trail rather than marking it completed.
//
// A gate block is terminal only when the gate evaluated its inputs and
// blocked on the merits. A gate that could not evaluate (for example a
// GitHub or storage read inside the gate failed) returns an error, which
// the core classifies retryable: the gate still fails closed for this
// delivery, but the outcome is not the command's answer.
//
// Retryable failures post their best-effort error comment synchronously.
// Durable attempts stay silent because the driver will retry and post the
// single terminal answer if the retry budget is exhausted.
//
// The parent context scopes the command beyond its own timeout: the
// synchronous wrapper passes context.Background(), while the durable driver
// passes its run context so lease loss or shutdown cancels in-flight work.
//
// The core logs each failure at its site, so the synchronous wrapper can discard
// the result without losing observability.
func (h *Handler) applyCommandCore(parent context.Context, repo string, pr int, environment, databaseName string, installationID int64, requestedBy string, result CommandResult) (bool, error) {
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
			h.logger.Error("apply blocked: cannot resolve GitHub App client for repo",
				"repo", repo, "pr", pr, "database", databaseName, "environment", environment, "error", err)
			return false, fmt.Errorf("apply command bootstrap %s#%d: %w", repo, pr, err)
		}
		h.logger.Error("apply: failed to bootstrap command", "repo", repo, "pr", pr, "database", databaseName, "environment", environment, "error", err)
		return true, fmt.Errorf("apply command bootstrap %s#%d: %w", repo, pr, err)
	}
	defer cancel()

	if handled, err := h.handleNoManagedSchemaChangesForCommand(ctx, client, repo, pr, installationID, action.Apply, environment, databaseName, requestedBy); err != nil {
		h.logger.Error("failed to check whether apply command needs schema change reconciliation", "repo", repo, "pr", pr, "environment", environment, "database", databaseName, "error", err)
		if !result.SuppressRetryComments {
			h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, err.Error())
		}
		return true, fmt.Errorf("apply command managed-schema check %s#%d: %w", repo, pr, err)
	} else if handled {
		return false, nil
	}

	ackedEarly := h.acknowledgeCommandEarlyIfOwned(ctx, client, repo, pr, databaseName, result.Tenant, installationID, result.DeliveryID, result.CommentID)

	// Discover config and fetch schema files from PR
	schemaResult, err := h.createManagedSchemaRequestFromPR(ctx, client, repo, pr, environment, databaseName, action.Apply)
	if err != nil {
		if h.silentDiscoveryFailureOnUnscopedFanOut(repo, result.Tenant, err) {
			h.logger.Debug("unscoped fan-out apply resolves to no schema this deployment answers for; staying silent",
				"repo", repo, "pr", pr, "environment", environment, "database", databaseName, "error", err)
			return false, nil
		}
		if h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.Apply, err, result.SuppressRetryComments) {
			return false, nil
		}
		return true, fmt.Errorf("apply command schema request %s#%d: %w", repo, pr, err)
	}
	if err := h.attachServerEnvironments(schemaResult, environment); err != nil {
		if h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.Apply, err, result.SuppressRetryComments) {
			return false, nil
		}
		return true, fmt.Errorf("apply command attach server environments %s#%d: %w", repo, pr, err)
	}
	if !ackedEarly {
		h.acknowledgeCommandActPoint(repo, pr, installationID, result)
	}

	if blocked, gateErr := h.enforceOpenPR(ctx, client, repo, pr, installationID, action.Apply, environment, requestedBy, result.SuppressRetryComments); gateErr != nil {
		return true, fmt.Errorf("apply command open-PR gate %s#%d: %w", repo, pr, gateErr)
	} else if blocked {
		return false, nil
	}

	if blocked, gateErr := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, schemaResult.Database, schemaResult.Type, environment, action.Apply, result.SuppressRetryComments); gateErr != nil {
		return true, fmt.Errorf("apply command actor authorization gate %s#%d: %w", repo, pr, gateErr)
	} else if blocked {
		return false, nil
	}

	// Fix checks stuck at "in_progress" from crashed applies after the actor
	// is authorized to run apply for this database.
	if err := h.reconcileStaleChecks(ctx, client, repo, pr); err != nil {
		h.logger.Error("failed to reconcile stale status checks", "repo", repo, "pr", pr, "error", err)
		if !result.SuppressRetryComments {
			h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Failed to reconcile stale status checks. Retry, and see server logs if it persists.")
		}
		return true, fmt.Errorf("apply command reconcile stale checks %s#%d: %w", repo, pr, err)
	}

	// Tier 1: review gate (server-owned review policy for this database)
	if blocked, gateErr := h.enforceReviewGate(ctx, client, repo, pr, installationID, schemaResult, environment, requestedBy, action.Apply, result.SuppressRetryComments); gateErr != nil {
		return true, fmt.Errorf("apply command review gate %s#%d: %w", repo, pr, gateErr)
	} else if blocked {
		h.logger.Info("apply blocked by review gate", "repo", repo, "pr", pr, "environment", environment, "requested_by", requestedBy)
		return false, nil
	}

	// Tier 2: PR checks gate — block if non-SchemaBot checks are not passing
	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to fetch PR for checks gate", "repo", repo, "pr", pr, "database", schemaResult.Database, "database_type", schemaResult.Type, "environment", environment, "error", err)
		if !result.SuppressRetryComments {
			h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Failed to fetch PR info. Retry, and see server logs if it persists.")
		}
		return true, fmt.Errorf("apply command fetch PR for checks gate %s#%d: %w", repo, pr, err)
	}
	if blocked, gateErr := h.enforcePassingChecks(ctx, client, repo, pr, installationID, prInfo.HeadSHA, environment, result.SuppressRetryComments); gateErr != nil {
		return true, fmt.Errorf("apply command checks gate %s#%d: %w", repo, pr, gateErr)
	} else if blocked {
		return false, nil
	}

	database := schemaResult.Database
	dbType := schemaResult.Type
	lockOwner := fmt.Sprintf("%s#%d", repo, pr)

	// Environment ordering enforcement: prior server-configured environments must be clean before applying.
	if blocked, gateErr := h.checkPriorEnvironments(ctx, repo, pr, database, dbType, environment, schemaResult.Environments, installationID, result.SuppressRetryComments); gateErr != nil {
		return true, fmt.Errorf("apply command prior environment gate %s#%d: %w", repo, pr, gateErr)
	} else if blocked {
		h.logger.Info("apply blocked by environment ordering", "repo", repo, "pr", pr, "database", database, "environment", environment)
		return false, nil
	}

	// Check for existing lock
	existingLock, err := h.service.Storage().Locks().Get(ctx, database, dbType)
	if err != nil {
		h.logger.Error("failed to check lock", "repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment, "error", err)
		if !result.SuppressRetryComments {
			h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Failed to check lock status: "+err.Error())
		}
		return true, fmt.Errorf("apply command check lock %s#%d: %w", repo, pr, err)
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
			return false, nil
		}

		// Lock held by this PR — check for active applies
		applies, err := h.service.Storage().Applies().GetByPR(ctx, repo, pr)
		if err != nil {
			h.logger.Error("failed to check active applies", "error", err)
			return true, fmt.Errorf("apply command check active applies %s#%d: %w", repo, pr, err)
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
				return false, nil
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
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment, "error", relErr)
		}
	}

	// Reject a stale snapshot before planning. A stale branch needs an update
	// no matter what a plan would say, so freshness rejections outrank every
	// plan-derived response — in particular the unsafe-change prompt, which
	// would otherwise coach the user toward `--allow-unsafe` for DDL that only
	// looks destructive because the branch is missing newer base-branch schema
	// changes. No lock is held here, so rejections need no release.
	//
	// Use FetchPullRequestNoCache: the cached FetchPullRequest used by
	// discovery would return the discovery-time HeadSHA, masking the race.
	prInfo, prErr := client.FetchPullRequestNoCache(ctx, repo, pr)
	if prErr != nil {
		h.logger.Error("failed to fetch PR for stale-schema check",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment, "error", prErr)
		if !result.SuppressRetryComments {
			h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy,
				"SchemaBot could not verify the current PR state. The apply was rejected; retry the command.")
		}
		return true, fmt.Errorf("apply command fetch PR for stale-schema check %s#%d: %w", repo, pr, prErr)
	}
	if rejected := h.assertSchemaStillCurrent(ctx, repo, pr, installationID, schemaResult, prInfo.HeadSHA, environment, requestedBy, action.Apply); rejected {
		return false, nil
	}
	if rejected, gateErr := h.assertBaseSchemaStillCurrent(ctx, client, repo, pr, installationID, schemaResult, prInfo, environment, requestedBy, action.Apply); gateErr != nil {
		return true, fmt.Errorf("apply command base schema freshness gate %s#%d: %w", repo, pr, gateErr)
	} else if rejected {
		return false, nil
	}

	// Generate plan
	prNumber := int32(pr)
	planReq := api.PlanRequest{
		Database:          schemaResult.Database,
		Environment:       environment,
		Type:              schemaResult.Type,
		SchemaFiles:       schemaResult.SchemaFiles,
		Repository:        repo,
		PullRequest:       &prNumber,
		HeadSHA:           &schemaResult.HeadSHA,
		SchemaPath:        schemaResult.SchemaPath,
		IgnoredNamespaces: schemaResult.IgnoredNamespaces,
		SourceTrusted:     true,
	}

	planResp, err := h.executePlanWithTransientRetry(ctx, planReq, repo, pr)
	if err != nil {
		h.logger.Error("plan execution failed", "repo", repo, "pr", pr, "error", err)
		if isTransientRemotePlanError(err) {
			if !result.SuppressRetryComments {
				h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, err.Error())
			}
			return true, fmt.Errorf("apply command plan %s#%d: %w", repo, pr, err)
		}
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, err.Error())
		// Failures other than remote-deployment unavailability are treated
		// as deterministic: the posted error is the command's answer. Some
		// of them are not truly deterministic (planning runs against a live
		// target database, which can be briefly unreachable), so recovery
		// for those is the user re-issuing the command.
		return false, nil
	}

	// No changes (neither table DDL nor a VSchema update) — record the passing
	// check and refresh the aggregate the same as the no-change plan path, so a
	// stale non-success record (e.g. a target reconciled out-of-band) cannot
	// keep the prior-environment gate blocking later environments. Then post a
	// regular plan comment (no lock, no confirm footer).
	if !planResp.HasChanges() {
		commentData := buildPlanCommentData(schemaResult, planResp, environment, result.Tenant, requestedBy, h.agentHint())
		if headSHA, checkErr := h.storeApplyPlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment); checkErr != nil {
			h.logger.Error("failed to record no-changes check for apply command",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment, "error", checkErr)
		} else if headSHA != "" {
			h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
		}
		h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))
		return false, nil
	}

	// Engine-blocked changes reject the apply before the unsafe gate: no flag
	// lets a refused statement through, so the user must never be coached
	// toward --allow-unsafe for a guaranteed failure. No lock is held yet, so
	// the rejection needs no release.
	if planResp.HasBlockedChanges() {
		commentData := buildPlanCommentData(schemaResult, planResp, environment, result.Tenant, requestedBy, h.agentHint())
		h.logger.Info("apply rejected: plan contains engine-blocked changes",
			"repo", repo, "pr", pr, "database", database, "environment", environment)
		h.postComment(repo, pr, installationID, templates.RenderBlockedChangesApplyRejected(commentData))
		return false, nil
	}

	// --defer-cutover only affects engine-driven statements; an all-direct
	// plan has no cutover to defer, so reject the flag instead of silently
	// ignoring it.
	if result.DeferCutover && planResp.AllChangesDirect() {
		h.logger.Info("apply rejected: --defer-cutover on an all-direct plan",
			"repo", repo, "pr", pr, "database", database, "environment", environment)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy,
			msgDeferCutoverAllDirect)
		return false, nil
	}

	// Block unsafe changes unless --allow-unsafe was specified
	if len(planResp.UnsafeChanges()) > 0 && !result.AllowUnsafe {
		commentData := buildPlanCommentData(schemaResult, planResp, environment, result.Tenant, requestedBy, h.agentHint())
		h.annotateAttributedChanges(ctx, client, &commentData, planResp, repo, pr, environment)
		h.logger.Info("apply blocked by unsafe changes", "repo", repo, "pr", pr, "database", database, "environment", environment)
		h.postComment(repo, pr, installationID, templates.RenderUnsafeChangesBlocked(commentData))
		return false, nil
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
		if errors.Is(err, storage.ErrLockHeld) {
			// Another owner won the lock between the pre-check above and this
			// acquire: the same answer the pre-check gives, so it is terminal.
			h.logger.Info("apply blocked by lock conflict at acquire",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment)
			h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Failed to acquire lock: "+err.Error())
			return false, nil
		}
		h.logger.Error("failed to acquire lock", "error", err)
		if !result.SuppressRetryComments {
			h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Failed to acquire lock: "+err.Error())
		}
		return true, fmt.Errorf("apply command acquire lock %s#%d: %w", repo, pr, err)
	}

	// Build plan comment data with lock info. Attributed changes are annotated
	// here so that a downgrade to manual confirmation below discloses them on
	// the comment apply-confirm acts on; the template omits them when the
	// apply proceeds automatically and the unsafe opt-in already solicited
	// consent for every attributed table, where the re-plan choice the
	// disclosure coaches is no longer open.
	commentData := buildPlanCommentData(schemaResult, planResp, environment, result.Tenant, requestedBy, h.agentHint())
	h.annotateAttributedChanges(ctx, client, &commentData, planResp, repo, pr, environment)
	commentData.IsLocked = true
	commentData.LockOwner = lockOwner
	commentData.LockAcquired = time.Now().UTC().Format("2006-01-02 15:04:05 UTC")
	commentData.DeferCutover = result.DeferCutover
	commentData.SkipRevert = result.SkipRevert
	commentData.AllowUnsafe = result.AllowUnsafe

	// Re-evaluate the checks gate against the freshness-checked HEAD before
	// executing. The early gate at the top of applyCommandCore ran against
	// the discovery-time HeadSHA. On the automatic apply path there is no
	// second user action between plan and apply, so a required check that
	// transitioned to failing on the same SHA (e.g. CI re-ran red, or a
	// new required check was added) would otherwise sneak past. Release
	// the lock on block so the user can re-run `schemabot apply -e <env>`
	// once the checks recover, without a manual unlock. The release is keyed
	// on the (owner, pending plan) intent acquired above, so a lock that has
	// since changed hands or been re-pinned is preserved. A check-status read
	// failure releases the same way — the retryable re-drive (or a user retry)
	// re-plans from the top and reacquires the lock, so leaving it pinned
	// would only force the stale-lock release path on the next attempt.
	if blocked, gateErr := h.enforcePassingChecks(ctx, client, repo, pr, installationID, prInfo.HeadSHA, environment, result.SuppressRetryComments); gateErr != nil {
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, planResp.PlanID, "fresh-HEAD checks gate read failure")
		return true, fmt.Errorf("apply command fresh-HEAD checks gate %s#%d: %w", repo, pr, gateErr)
	} else if blocked {
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, planResp.PlanID, "fresh-HEAD checks gate block")
		return false, nil
	}

	// Direct-execution changes never run without explicit confirmation: the
	// operator must consent to their blocking, non-revertible native DDL
	// against the locked comment that discloses it, so the apply never
	// proceeds in one step — downgrade to the two-step confirm.
	if len(planResp.DirectChanges()) > 0 {
		h.logger.Info("automatic apply downgraded: plan contains direct-execution changes",
			"repo", repo, "pr", pr, "database", database, "environment", environment)
		commentData.AutoConfirmDowngradeReason = "Plan contains direct-execution changes — review the disclosure and confirm manually"
		h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))
		headSHA, checkRunErr := h.storeApplyPlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment)
		if checkRunErr != nil {
			h.logger.Error("failed to create apply plan check run", "repo", repo, "pr", pr, "error", checkRunErr)
		}
		if headSHA != "" {
			h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
		}
		return false, nil
	}

	// Discarding an unfinished copy destroys work already done on the target —
	// often hours of it — so it never happens in one step. Downgrade to the
	// two-step confirm against the locked comment that discloses what is being
	// thrown away, the same way a direct-execution change does: the operator
	// spends the hours, so the operator decides, and there is no flag that
	// converts an automatic apply into that consent.
	if discarded := planResp.DiscardedCopies(); len(discarded) > 0 {
		h.logger.Info("automatic apply downgraded: applying discards an existing copy",
			"repo", repo, "pr", pr, "database", database, "environment", environment,
			"discarded_copies", len(discarded))
		// Store the check record before posting the paused comment: the pause
		// is acknowledged only once the stored check state blocks the merge
		// gate on the pending changes. A storage failure releases the lock
		// (keyed on this plan's intent) and stays retryable — the re-drive
		// re-plans from the top, reacquires the lock, and reaches this gate
		// again — so the pause is never acknowledged over unknown check state.
		headSHA, checkRunErr := h.storeApplyPlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment)
		if checkRunErr != nil {
			h.logger.Error("failed to store check state for copy-discard downgrade; the merge gate does not reflect the pending changes, so the command stays retryable",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType,
				"environment", environment, "error", checkRunErr)
			h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, planResp.PlanID, "copy-discard downgrade check state store failure")
			if !result.SuppressRetryComments {
				h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy,
					"SchemaBot could not record the check state for this apply. Retry the command, and see server logs if it persists.")
			}
			return true, fmt.Errorf("apply command copy-discard downgrade check record %s#%d: %w", repo, pr, checkRunErr)
		}
		if headSHA != "" {
			h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
		}
		commentData.AutoConfirmDowngradeReason = msgCopyDiscardDowngrade
		h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))
		return false, nil
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
		return false, nil
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
	h.executeApply(ctx, client, repo, pr, schemaResult, environment, installationID, requestedBy, result, storedPlan, planResp.PlanID)
	return false, nil
}

// handleApplyConfirmCommand handles the "schemabot apply-confirm -e <env>" PR comment command.
// It is the synchronous goSafe entry point: it runs applyConfirmCommandCore and
// discards the durability disposition, which only a durable issue_comment
// driver consumes.
func (h *Handler) handleApplyConfirmCommand(repo string, pr int, environment, databaseName string, installationID int64, requestedBy string, result CommandResult) {
	_, _ = h.applyConfirmCommandCore(context.Background(), repo, pr, environment, databaseName, installationID, requestedBy, result)
}

// applyConfirmCommandCore verifies lock ownership, re-plans for drift detection,
// executes the apply, and watches progress. It returns a durability disposition
// for the durable issue_comment driver:
//
//   - retry=true, err!=nil — a transient infrastructure failure (command
//     bootstrap, a GitHub read, or a storage operation) that a durable driver
//     should re-drive; the same window may succeed on a later attempt.
//   - retry=false, err=nil — a terminal outcome that is the command's answer
//     (silent fan-out skip, no pending confirmation, gate blocks, lock conflict,
//     stale-schema/base/plan rejection, or a hand-off to executeApply, which
//     may itself fail before dispatching). A schema-request failure is terminal
//     only when handleSchemaRequestError recognizes it as a user-facing
//     rejection; an unexpected failure there (for example a transient GitHub
//     config read) stays retryable.
//   - retry=false, err!=nil — a deterministic failure a re-drive would only
//     reproduce (a GitHub App resolution failure inside the bootstrap): the
//     delivery must not be re-driven, but the command never ran and no PR
//     comment could be posted, so the error is recorded on the delivery as
//     its only triage trail rather than marking it completed.
//
// A gate block is terminal only when the gate evaluated its inputs and
// blocked on the merits. A gate that could not evaluate (for example a
// GitHub or storage read inside the gate failed) returns an error, which
// the core classifies retryable: the gate still fails closed for this
// delivery, but the outcome is not the command's answer. In particular a
// base-schema freshness verification failure keeps the pending confirmation
// lock pinned, so a re-drive (or user retry) can still confirm the reviewed
// plan.
//
// Retryable failures post their best-effort error comment synchronously.
// Durable attempts stay silent because the driver will retry and post the
// single terminal answer if the retry budget is exhausted.
//
// The parent context scopes the command beyond its own timeout: the
// synchronous wrapper passes context.Background(), while the durable driver
// passes its run context so lease loss or shutdown cancels in-flight work.
//
// The core logs each failure at its site, so the synchronous wrapper can discard
// the result without losing observability.
func (h *Handler) applyConfirmCommandCore(parent context.Context, repo string, pr int, environment, databaseName string, installationID int64, requestedBy string, result CommandResult) (bool, error) {
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
			h.logger.Error("apply-confirm blocked: cannot resolve GitHub App client for repo",
				"repo", repo, "pr", pr, "database", databaseName, "environment", environment, "error", err)
			return false, fmt.Errorf("apply-confirm command bootstrap %s#%d: %w", repo, pr, err)
		}
		h.logger.Error("apply-confirm: failed to bootstrap command", "repo", repo, "pr", pr, "database", databaseName, "environment", environment, "error", err)
		return true, fmt.Errorf("apply-confirm command bootstrap %s#%d: %w", repo, pr, err)
	}
	defer cancel()

	if handled, err := h.handleNoManagedSchemaChangesForCommand(ctx, client, repo, pr, installationID, action.ApplyConfirm, environment, databaseName, requestedBy); err != nil {
		h.logger.Error("failed to check whether apply-confirm command needs schema change reconciliation", "repo", repo, "pr", pr, "environment", environment, "database", databaseName, "error", err)
		if !result.SuppressRetryComments {
			h.postCommandError(repo, pr, installationID, action.ApplyConfirm, environment, requestedBy, err.Error())
		}
		return true, fmt.Errorf("apply-confirm command managed-schema check %s#%d: %w", repo, pr, err)
	} else if handled {
		return false, nil
	}

	// Discover database config from PR's schemabot.yaml
	schemaResult, err := h.createManagedSchemaRequestFromPR(ctx, client, repo, pr, environment, databaseName, action.ApplyConfirm)
	if err != nil {
		if h.silentDiscoveryFailureOnUnscopedFanOut(repo, result.Tenant, err) {
			h.logger.Debug("unscoped fan-out apply-confirm resolves to no schema this deployment answers for; staying silent",
				"repo", repo, "pr", pr, "environment", environment, "database", databaseName, "error", err)
			return false, nil
		}
		if h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.ApplyConfirm, err, result.SuppressRetryComments) {
			return false, nil
		}
		return true, fmt.Errorf("apply-confirm command schema request %s#%d: %w", repo, pr, err)
	}
	if err := h.attachServerEnvironments(schemaResult, environment); err != nil {
		if h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.ApplyConfirm, err, result.SuppressRetryComments) {
			return false, nil
		}
		return true, fmt.Errorf("apply-confirm command attach server environments %s#%d: %w", repo, pr, err)
	}

	if blocked, gateErr := h.enforceOpenPR(ctx, client, repo, pr, installationID, action.ApplyConfirm, environment, requestedBy, result.SuppressRetryComments); gateErr != nil {
		return true, fmt.Errorf("apply-confirm command open-PR gate %s#%d: %w", repo, pr, gateErr)
	} else if blocked {
		return false, nil
	}

	if blocked, gateErr := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, schemaResult.Database, schemaResult.Type, environment, action.ApplyConfirm, result.SuppressRetryComments); gateErr != nil {
		return true, fmt.Errorf("apply-confirm command actor authorization gate %s#%d: %w", repo, pr, gateErr)
	} else if blocked {
		return false, nil
	}

	// Tier 1: review gate (re-check on confirm to prevent bypass)
	if blocked, gateErr := h.enforceReviewGate(ctx, client, repo, pr, installationID, schemaResult, environment, requestedBy, action.ApplyConfirm, result.SuppressRetryComments); gateErr != nil {
		return true, fmt.Errorf("apply-confirm command review gate %s#%d: %w", repo, pr, gateErr)
	} else if blocked {
		h.logger.Info("apply-confirm blocked by review gate", "repo", repo, "pr", pr, "environment", environment, "requested_by", requestedBy)
		return false, nil
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
		h.logger.Error("failed to fetch PR for checks gate", "repo", repo, "pr", pr, "database", schemaResult.Database, "database_type", schemaResult.Type, "environment", environment, "error", err)
		if !result.SuppressRetryComments {
			h.postCommandError(repo, pr, installationID, action.ApplyConfirm, environment, requestedBy, "Failed to fetch PR info. Retry, and see server logs if it persists.")
		}
		return true, fmt.Errorf("apply-confirm command fetch PR for checks gate %s#%d: %w", repo, pr, err)
	}

	if blocked, gateErr := h.enforcePassingChecks(ctx, client, repo, pr, installationID, confirmPRInfo.HeadSHA, environment, result.SuppressRetryComments); gateErr != nil {
		return true, fmt.Errorf("apply-confirm command checks gate %s#%d: %w", repo, pr, gateErr)
	} else if blocked {
		return false, nil
	}

	database := schemaResult.Database
	dbType := schemaResult.Type
	lockOwner := fmt.Sprintf("%s#%d", repo, pr)

	// Check lock ownership
	existingLock, err := h.service.Storage().Locks().Get(ctx, database, dbType)
	if err != nil {
		h.logger.Error("failed to check lock", "repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment, "error", err)
		if !result.SuppressRetryComments {
			h.postCommandError(repo, pr, installationID, action.ApplyConfirm, environment, requestedBy, "Failed to check lock status: "+err.Error())
		}
		return true, fmt.Errorf("apply-confirm command check lock %s#%d: %w", repo, pr, err)
	}
	if existingLock == nil {
		if h.silentOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("unscoped fan-out apply-confirm found no pending confirmation on this deployment; staying silent",
				"repo", repo, "pr", pr, "database", database, "environment", environment)
			return false, nil
		}
		h.logger.Info("apply-confirm rejected: no lock held", "repo", repo, "pr", pr, "database", database, "environment", environment)
		h.postComment(repo, pr, installationID, templates.RenderApplyConfirmNoLock(database, environment))
		return false, nil
	}
	// A same-PR rollback lock uses the same owner string but is confirmed via
	// rollback-confirm, never here. Reject it before the freshness checks so no
	// rejection path below can release it.
	if existingLock.Owner == lockOwner && strings.HasPrefix(existingLock.PendingPlanID, rollbackPendingPlanPrefix) {
		h.logger.Info("apply-confirm rejected: lock belongs to rollback plan", "repo", repo, "pr", pr,
			"database", database, "environment", environment, "pending_plan_id", existingLock.PendingPlanID)
		h.postCommandError(repo, pr, installationID, action.ApplyConfirm, environment, requestedBy,
			"This lock belongs to a rollback plan. Use `schemabot rollback-confirm` to execute it, or `schemabot unlock` to cancel it.")
		return false, nil
	}

	// Freshness rejections outrank the lock-conflict comment: a stale branch
	// needs a rebase no matter who holds the lock. Release only when this PR
	// owns the lock, keyed on the pending intent observed above, so another
	// PR's lock — or this PR's lock re-pinned by a newer plan mid-check — is
	// never removed.
	releaseObservedApplyIntent := func(reason string) {
		if existingLock.Owner != lockOwner {
			h.logger.Info("left apply lock in place after pre-execution rejection because another PR holds it",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType,
				"environment", environment, "reason", reason, "lock_owner", existingLock.Owner)
			return
		}
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, existingLock.PendingPlanID, reason)
	}
	if rejected := h.assertSchemaStillCurrent(ctx, repo, pr, installationID, schemaResult, confirmPRInfo.HeadSHA, environment, requestedBy, action.ApplyConfirm); rejected {
		releaseObservedApplyIntent("stale-schema rejection")
		return false, nil
	}
	// A verification failure keeps the pending confirmation pinned: the plan
	// is not known stale, and confirming again (or a durable re-drive)
	// requires the lock, so releasing it would turn a transient GitHub read
	// failure into a lost confirmation. Only a verified-stale rejection
	// releases the observed intent.
	if rejected, gateErr := h.assertBaseSchemaStillCurrent(ctx, client, repo, pr, installationID, schemaResult, confirmPRInfo, environment, requestedBy, action.ApplyConfirm); gateErr != nil {
		return true, fmt.Errorf("apply-confirm command base schema freshness gate %s#%d: %w", repo, pr, gateErr)
	} else if rejected {
		releaseObservedApplyIntent("base-schema freshness rejection")
		return false, nil
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
		return false, nil
	}
	h.acknowledgeCommandActPoint(repo, pr, installationID, result)

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
	// The rejection release is keyed on the (owner, pending plan) intent
	// observed above, so a lock that has since changed hands or been re-pinned
	// by a newer plan is preserved.
	storedPlan, planLoadErr := h.confirmationPlanForLock(ctx, existingLock)
	if planLoadErr != nil {
		h.logger.Error("failed to load confirmation plan for cross-delivery freshness check",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
			"pending_plan_id", existingLock.PendingPlanID, "error", planLoadErr)
		if !result.SuppressRetryComments {
			h.postCommandError(repo, pr, installationID, action.ApplyConfirm, environment, requestedBy, "Failed to load confirmation plan: "+planLoadErr.Error())
		}
		return true, fmt.Errorf("apply-confirm command load confirmation plan %s#%d: %w", repo, pr, planLoadErr)
	}
	if rejected := h.assertPlanStillCurrent(ctx, repo, pr, installationID, storedPlan, confirmPRInfo.HeadSHA, environment, requestedBy); rejected {
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, existingLock.PendingPlanID, "stale-plan rejection")
		return false, nil
	}

	h.executeApply(ctx, client, repo, pr, schemaResult, environment, installationID, requestedBy, result, nil, existingLock.PendingPlanID)
	return false, nil
}

// handleUnlockCommand handles the "schemabot unlock" PR comment command. It is
// the synchronous goSafe entry point: it runs unlockCommandCore and discards
// the durability disposition, which only a durable issue_comment driver
// consumes.
func (h *Handler) handleUnlockCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	_, _ = h.unlockCommandCore(context.Background(), time.Now(), repo, pr, installationID, requestedBy, result)
}

// unlockRejectionError marks a deterministic unlock rejection: the message is
// the command's answer, which the same input will always reproduce, so a
// durable driver must not re-drive it. It renders the wrapped error verbatim
// so the marker never leaks into a PR comment.
type unlockRejectionError struct{ err error }

func (e *unlockRejectionError) Error() string { return e.err.Error() }
func (e *unlockRejectionError) Unwrap() error { return e.err }

func unlockRejection(err error) error { return &unlockRejectionError{err: err} }

func isUnlockRejection(err error) bool {
	var rejection *unlockRejectionError
	return errors.As(err, &rejection)
}

// unlockCommandCore finds all locks held by this PR and releases them. With
// `--force`, it can also release a CLI-owned lock for the database inferred
// from this PR's SchemaBot config; `-d <database>` disambiguates multi-database
// PRs. This lets a PR author clear a stale local-session lock from the PR
// workflow that it is blocking. It returns a durability disposition for the
// durable issue_comment driver:
//
//   - retry=true, err!=nil — a transient infrastructure failure (a GitHub
//     read during database inference, a storage lock lookup, the active-apply
//     verification, or a lock release) that a durable driver should re-drive.
//     Released locks drop out of the lookup, so a re-drive retries only the
//     locks that remain.
//   - retry=false, err=nil — a terminal outcome that is the command's answer
//     (a completed release, a deterministic inference or lookup rejection, no
//     locks found, an authorization block on the merits, an active apply
//     still protecting the lock, or a command that predates every lock it
//     matched).
//   - retry=false, err!=nil — a deterministic failure a re-drive would only
//     reproduce (a GitHub App resolution failure during database inference or
//     the authorization client): the delivery must not be re-driven, but no
//     PR comment could be posted without a client, so the error is recorded
//     on the delivery as its only triage trail rather than marking it
//     completed.
//
// issuedAt bounds the release targets: a lock acquired after issuedAt is
// never released. On the durable path issuedAt is the delivery's received-at,
// so a re-drive minutes later still releases only the locks the command
// covered when it arrived — not a lock acquired since, which may be
// protecting a fresh apply awaiting confirmation or another session's work.
// The bound is per delivery receipt, not per original comment: a GitHub
// Redeliver reopens the stored delivery with a fresh received-at, so a
// redelivered command may release locks acquired after the original comment,
// as long as they predate the redelivery. That is deliberate — Redeliver is
// an operator action to re-run the command now, and the authorization and
// active-apply gates re-evaluate against current state. The synchronous
// wrapper passes time.Now().
//
// A terminal rejection is a per-delivery decision: it is deterministic for
// the PR head this delivery read, and a later push can change the answer.
// The recovery path is a fresh comment after the push — never a re-drive of
// the superseded delivery.
//
// A gate block is terminal only when the gate evaluated its inputs and
// blocked on the merits. A gate that could not evaluate (for example a
// GitHub or storage read inside the gate failed) returns an error, which
// the core classifies retryable: the gate still fails closed for this
// delivery, but the outcome is not the command's answer.
//
// Retryable failures post their best-effort error comment synchronously.
// Durable attempts stay silent because the driver will retry and post the
// single terminal answer if the retry budget is exhausted.
//
// The parent context scopes the command beyond its own timeout: the
// synchronous wrapper passes context.Background(), while the durable driver
// passes its run context so lease loss or shutdown cancels in-flight work.
//
// Every failure is logged where it is classified — by the core at its own
// exits, and by the authorization gates for gate outcomes — so the synchronous
// wrapper can discard the result without losing observability.
func (h *Handler) unlockCommandCore(parent context.Context, issuedAt time.Time, repo string, pr int, installationID int64, requestedBy string, result CommandResult) (bool, error) {
	ctx, cancel := h.commandContext(parent, 30*time.Second)
	defer cancel()
	if result.Force && result.Database == "" {
		database, err := h.inferUnlockDatabase(ctx, repo, pr, installationID)
		if err != nil {
			h.logger.Error("failed to infer database for force unlock", "repo", repo, "pr", pr, "error", err)
			if isUnlockRejection(err) {
				h.postCommandError(repo, pr, installationID, action.Unlock, "", requestedBy, "Failed to infer database for force unlock: "+err.Error())
				return false, nil
			}
			// A GitHub App resolution failure is deterministic per deployment
			// config, so a re-drive reproduces it; recovery is an operator
			// fixing the App mapping and the user re-issuing the command. No
			// PR comment could be posted without a client, so the error is
			// returned (with retry=false) to record the failure on the
			// delivery instead of marking it completed.
			if errors.Is(err, errGitHubAppResolution) {
				return false, fmt.Errorf("unlock command infer database %s#%d: %w", repo, pr, err)
			}
			if !result.SuppressRetryComments {
				h.postCommandError(repo, pr, installationID, action.Unlock, "", requestedBy, "Failed to infer database for force unlock: "+err.Error())
			}
			return true, fmt.Errorf("unlock command infer database %s#%d: %w", repo, pr, err)
		}
		result.Database = database
	}

	locks, err := h.locksForUnlock(ctx, repo, pr, result)
	if err != nil {
		h.logger.Error("failed to look up unlock targets", "repo", repo, "pr", pr, "database", result.Database, "force", result.Force, "error", err)
		if isUnlockRejection(err) {
			h.postCommandError(repo, pr, installationID, action.Unlock, "", requestedBy, "Failed to look up locks: "+err.Error())
			return false, nil
		}
		if !result.SuppressRetryComments {
			h.postCommandError(repo, pr, installationID, action.Unlock, "", requestedBy, "Failed to look up locks: "+err.Error())
		}
		return true, fmt.Errorf("unlock command lock lookup %s#%d: %w", repo, pr, err)
	}

	// Release only locks that existed when the command was received. A lock
	// acquired afterwards was never covered by the command's intent and may be
	// protecting newer work — a fresh apply awaiting confirmation, or another
	// session's CLI lock on a force unlock. The comparison spans two clocks
	// (lock.CreatedAt is the storage DB's clock, issuedAt the webhook pod's);
	// skew fails safe — a wrongly skipped lock gets the stale-command answer
	// prompting a fresh comment, never a wrongful release.
	fresh := make([]*storage.Lock, 0, len(locks))
	var skippedNewer int
	for _, lock := range locks {
		if lock.CreatedAt.After(issuedAt) {
			skippedNewer++
			h.logger.Warn("unlock will not release a lock acquired after the command was received",
				"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType,
				"owner", lock.Owner, "lock_created_at", lock.CreatedAt, "command_received_at", issuedAt)
			continue
		}
		fresh = append(fresh, lock)
	}
	locks = fresh
	if len(locks) == 0 && skippedNewer > 0 {
		h.logger.Info("unlock released nothing because every matched lock postdates the command", "repo", repo, "pr", pr)
		h.postCommandError(repo, pr, installationID, action.Unlock, "", requestedBy,
			"Every lock matched by this unlock command was acquired after the command was received, so nothing was released. Comment `schemabot unlock` again to release the current locks.")
		return false, nil
	}

	if len(locks) == 0 {
		// On an unscoped fan-out every deployment runs the same lookup, but only
		// the one holding a lock has anything to release. One with no locks stays
		// silent so only the owning deployment answers.
		if h.silentOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("unscoped fan-out unlock found no locks on this deployment; staying silent so the owning deployment responds",
				"repo", repo, "pr", pr)
			return false, nil
		}
		h.logger.Info("unlock: no locks found", "repo", repo, "pr", pr)
		h.postComment(repo, pr, installationID, templates.RenderNoLocksFound())
		return false, nil
	}
	h.acknowledgeCommandActPoint(repo, pr, installationID, result)

	// Unlock mutates lock state for every matched database, including
	// force-releasing CLI-owned locks, so the actor must be an authorized
	// admin/operator for each affected database before any lock is released.
	// Locks are not environment-scoped, so authorization is enforced per
	// database without an environment.
	client, err := h.actorAuthorizationClient(repo, pr, installationID, requestedBy, locks[0].DatabaseName, "", action.Unlock)
	if err != nil {
		// The gate has already logged the client-creation failure and posted
		// its best-effort authorization-unavailable comment; it fails closed
		// for this delivery. A GitHub App resolution failure is deterministic
		// per deployment config, so a re-drive reproduces it and the error is
		// recorded on the delivery instead; any other cause (an installation
		// token fetch, for example) may clear on a later attempt.
		if errors.Is(err, errGitHubAppResolution) {
			return false, fmt.Errorf("unlock command actor authorization client %s#%d: %w", repo, pr, err)
		}
		return true, fmt.Errorf("unlock command actor authorization client %s#%d: %w", repo, pr, err)
	}
	for _, lock := range locks {
		blocked, authErr := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, lock.DatabaseName, lock.DatabaseType, "", action.Unlock, result.SuppressRetryComments)
		if authErr != nil {
			return true, fmt.Errorf("unlock command actor authorization gate %s#%d database %s: %w", repo, pr, lock.DatabaseName, authErr)
		}
		if blocked {
			return false, nil
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
			if !result.SuppressRetryComments {
				h.postCommandError(repo, pr, installationID, action.Unlock, "", requestedBy,
					"Failed to verify active applies for database `"+lock.DatabaseName+"`: "+err.Error()+". No locks were released.")
			}
			return true, fmt.Errorf("unlock command verify active applies %s#%d database %s: %w", repo, pr, lock.DatabaseName, err)
		}
		for _, a := range applies {
			if a.Database == lock.DatabaseName && !state.IsTerminalApplyState(a.State) {
				h.postComment(repo, pr, installationID, templates.RenderCannotUnlock(
					lock.DatabaseName, a.Environment, a.ApplyIdentifier, a.State))
				return false, nil
			}
		}
	}

	// Release all locks. A failed release is logged and the loop continues so
	// one failure does not strand the remaining locks; the collected errors
	// make the delivery retryable, and a re-drive only sees the locks that are
	// still held.
	var releaseErrs []error
	var released, alreadyGone int
	for _, lock := range locks {
		var err error
		if result.Force {
			err = h.service.Storage().Locks().ForceRelease(ctx, lock.DatabaseName, lock.DatabaseType)
		} else {
			err = h.service.Storage().Locks().Release(ctx, lock.DatabaseName, lock.DatabaseType, lock.Owner)
		}
		if errors.Is(err, storage.ErrLockNotFound) || errors.Is(err, storage.ErrLockNotOwned) {
			// The lock vanished (or changed owner) between lookup and release —
			// a concurrent unlock or apply's own stale-lock cleanup got there
			// first. The lock this command targeted is gone, which is the
			// command's goal, so this is not a failure to retry; skip the
			// success comment too, since this command did not do the releasing.
			alreadyGone++
			h.logger.Info("unlock target already released",
				"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType,
				"owner", lock.Owner, "force", result.Force, "error", err)
			continue
		}
		if err != nil {
			h.logger.Error("failed to release lock",
				"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType,
				"owner", lock.Owner, "force", result.Force, "error", err)
			releaseErrs = append(releaseErrs, fmt.Errorf("release lock for %s: %w", lock.DatabaseName, err))
			continue
		}

		released++
		h.postComment(repo, pr, installationID, templates.RenderUnlockSuccess(
			lock.DatabaseName, "", requestedBy))
	}
	if len(releaseErrs) > 0 {
		return true, fmt.Errorf("unlock command release locks %s#%d: %w", repo, pr, errors.Join(releaseErrs...))
	}
	if released == 0 && alreadyGone > 0 {
		// Every matched lock was released by a concurrent operation before this
		// command could act. The per-lock skips stay silent, so without an
		// aggregate answer the acked command would end without any comment;
		// an acknowledged command must always answer.
		h.logger.Info("unlock: every matched lock was already released concurrently",
			"repo", repo, "pr", pr, "already_gone", alreadyGone)
		h.postComment(repo, pr, installationID, templates.RenderLocksAlreadyReleased())
	}
	return false, nil
}

func (h *Handler) locksForUnlock(ctx context.Context, repo string, pr int, result CommandResult) ([]*storage.Lock, error) {
	if result.Force {
		if result.Database == "" {
			return nil, unlockRejection(errors.New("--force requires a database target"))
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
				return nil, unlockRejection(fmt.Errorf("lock for %s is held by %s#%d", result.Database, lock.Repository, lock.PullRequest))
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

	config, _, err := h.resolveUnscopedManagedConfig(ctx, client, repo, pr, action.Unlock)
	if err != nil {
		// Schema another deployment owns — whether outside allowed_dirs or for
		// a database not in this deployment's registry — means there is nothing
		// for this deployment to unlock: same outcome as no config at all.
		if isSchemaUnownedByDeploymentError(err) {
			return "", unlockRejection(ghclient.ErrNoConfig)
		}
		// A repo with no config, or only malformed ones, is a deterministic
		// discovery outcome the same delivery always reproduces — the
		// command's answer, not a transient read failure.
		if errors.Is(err, ghclient.ErrNoConfig) || errors.Is(err, ghclient.ErrInvalidConfig) {
			return "", unlockRejection(err)
		}
		if errors.Is(err, ghclient.ErrMultipleConfigs) {
			return "", unlockRejection(fmt.Errorf("multiple SchemaBot configs match this PR; retry with `schemabot unlock -d <database> --force`: %w", err))
		}
		return "", err
	}
	if config == nil || config.Database == "" {
		return "", unlockRejection(errors.New("no database found in SchemaBot config"))
	}
	return config.Database, nil
}
