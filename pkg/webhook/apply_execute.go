package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// executeApply re-plans for drift detection and executes the apply. This is the shared
// execution core used by both handleApplyConfirmCommand and handleApplyCommand.
//
// When storedPlan is non-nil (auto-confirm path), the re-plan DDL is compared against it.
// If the DDL differs, execution is downgraded to manual confirmation — a plan comment is
// posted with a warning and the user must run apply-confirm separately.
func (h *Handler) executeApply(
	ctx context.Context, client *ghclient.InstallationClient,
	repo string, pr int, schemaResult *ghclient.SchemaRequestResult,
	environment string, installationID int64, requestedBy string,
	result CommandResult, storedPlan *storage.Plan, expectedPendingPlanID string,
) {
	database := schemaResult.Database
	dbType := schemaResult.Type

	// Re-plan for drift detection
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
		h.logger.Error("plan execution failed on confirm", "repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment, "error", err)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, err.Error())
		return
	}

	// No changes — release the lock (keyed on the pending intent this handler
	// observed, so a lock re-pinned by a newer plan is preserved) and notify.
	if len(planResp.FlatTables()) == 0 {
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "no changes to apply")
		// The target already matches the PR schema — apply found nothing to do.
		// Record the passing (no-change) check result and refresh the aggregate so
		// the schema check reflects that the target is up to date, the same as the
		// no-change plan path.
		if headSHA, checkErr := h.storeApplyPlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment); checkErr != nil {
			h.logger.Error("failed to record no-changes check after apply",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment, "error", checkErr)
		} else if headSHA != "" {
			h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
		}
		h.postComment(repo, pr, installationID, templates.RenderApplyConfirmNoChanges(database, environment))
		return
	}

	// Automatic apply DDL drift check: if the re-plan DDL differs from the stored auto-plan,
	// downgrade to manual confirmation so the user reviews the new plan.
	if storedPlan != nil && !ddlMatchesStoredPlan(planResp, storedPlan) {
		h.logger.Info("automatic apply downgraded: DDL drift detected",
			"repo", repo, "pr", pr, "database", database, "environment", environment)
		commentData := buildPlanCommentData(schemaResult, planResp, environment, result.Tenant, requestedBy)
		commentData.IsLocked = true
		commentData.AutoConfirmDowngradeReason = "Schema changes differ from auto-plan — review and confirm manually"
		h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))
		return
	}

	// Block unsafe changes on confirm (re-plan may have detected new unsafe changes)
	if len(planResp.UnsafeChanges()) > 0 && !result.AllowUnsafe {
		commentData := buildPlanCommentData(schemaResult, planResp, environment, result.Tenant, requestedBy)
		h.logger.Info("apply blocked by unsafe changes", "repo", repo, "pr", pr, "database", database, "environment", environment)
		h.postComment(repo, pr, installationID, templates.RenderUnsafeChangesBlocked(commentData))
		return
	}

	// Revalidate the PR immediately before the apply is queued. The earlier
	// handler checks provide fast rejection, but the apply-time re-plan can be
	// retried and the base branch or PR HEAD can advance while it runs.
	actionName := action.ApplyConfirm
	if storedPlan != nil {
		actionName = action.Apply
	}
	freshPRInfo, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		h.logger.Error("apply rejected: failed final PR freshness fetch",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType,
			"environment", environment, "action", actionName, "error", err)
		h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy,
			"SchemaBot could not verify the current PR state. The apply was rejected; retry the command.")
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "final PR freshness fetch failure")
		return
	}
	if rejected := h.assertSchemaStillCurrent(ctx, repo, pr, installationID, schemaResult, freshPRInfo.HeadSHA, environment, requestedBy, actionName); rejected {
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "final stale-schema rejection")
		return
	}
	if rejected := h.assertBaseSchemaStillCurrent(ctx, client, repo, pr, installationID, schemaResult, freshPRInfo, environment, requestedBy, actionName); rejected {
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "final base-schema freshness rejection")
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

	caller := formatGitHubCaller(requestedBy, repo, pr)

	// Resolve the App factory for this repo once so the observer captures
	// the correct App for all subsequent GitHub calls (comments, check runs).
	// Failure here is unrecoverable for outbound calls — the same error would
	// also block postComment — so log and return without attempting a comment.
	factory, factoryErr := h.factoryForRepo(repo)
	if factoryErr != nil {
		h.logger.Error("apply blocked: cannot resolve GitHub App client for repo",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment, "error", factoryErr)
		return
	}

	// Set observer before queuing the apply so ExecuteApply can register it on
	// the durable apply row before operator dispatch starts.
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
		OnTerminalHook: func(apply *storage.Apply) {
			// refreshChecksForTerminalApply routes a completed rollback straight
			// to action_required. The observer registered here can be consumed by
			// a rollback apply (pending observers share a per-target key), so the
			// terminal ordering must honor the rollback intent from the durable
			// apply, not from the command that registered the observer.
			h.refreshChecksForTerminalApply(context.Background(), apply, "apply command")
		},
	})
	h.service.SetPendingObserver(database, "", environment, observer)

	applyReq := api.ApplyRequest{
		PlanID:                planResp.PlanID,
		Environment:           environment,
		Options:               options,
		Caller:                caller,
		InstallationID:        installationID,
		ExpectedLockOwner:     fmt.Sprintf("%s#%d", repo, pr),
		ExpectedPendingPlanID: expectedPendingPlanID,
	}

	applyResp, applyID, err := h.service.ExecuteApply(ctx, applyReq)
	if err != nil {
		h.service.SetPendingObserver(database, "", environment, nil)
		h.logger.Error("apply execution failed", "repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment, "error", err)
		message := "Failed to execute apply. See SchemaBot server logs for details."
		if errors.Is(err, storage.ErrLockIntentChanged) {
			message = "The pending schema change changed while this command was running. The apply was rejected; review the latest plan and run the command again."
		}
		h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy, message)
		return
	}

	if !applyResp.Accepted {
		h.service.SetPendingObserver(database, "", environment, nil)
		h.logger.Info("apply rejected by engine", "repo", repo, "pr", pr, "database", database, "environment", environment, "error", applyResp.ErrorMessage)
		h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy, "The apply was not accepted. See SchemaBot server logs for details.")
		return
	}

	// ExecuteApply rejects accepted applies unless SchemaBot stored its own
	// apply row. Keep this guard fail-closed in case that invariant changes.
	if applyID <= 0 {
		h.service.SetPendingObserver(database, "", environment, nil)
		h.logger.Error("accepted apply did not return an apply id",
			"repo", repo, "pr", pr, "database", database,
			"database_type", schemaResult.Type, "environment", environment,
			"apply_id", applyResp.ApplyID)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Apply was accepted, but SchemaBot did not receive a stored apply ID. SchemaBot cannot safely track progress or update required status checks. An operator must reconcile the apply state before retrying.")
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
	h.postInitialProgressComment(ctx, repo, pr, installationID, apply, progressBody)

	// Update stored check state to in_progress (transitions action_required to in_progress).
	if err := h.updateCheckRecordForApplyStart(ctx, client, repo, pr, schemaResult, environment, applyID); err != nil {
		h.logger.Error("failed to mark check in_progress for apply",
			"repo", repo, "pr", pr, "database", database,
			"database_type", schemaResult.Type, "environment", environment,
			"apply_id", applyID, "error", err)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Apply was accepted, but SchemaBot could not update the required status check: "+err.Error())
		return
	}
}

// releaseApplyLockIfIntentUnchanged releases this PR's apply lock after a
// pre-execution gate rejected (or obviated) the apply, but only while the lock
// still carries the exact pending intent the rejecting handler observed. A lock
// whose pending plan has since changed — e.g. a rollback plan re-pinned it while
// the gate ran — belongs to that newer intent and is preserved. The reason names
// the gate that triggered the release so logs distinguish the call sites.
func (h *Handler) releaseApplyLockIfIntentUnchanged(ctx context.Context, repo string, pr int, database, dbType, environment, expectedPendingPlanID, reason string) {
	lockOwner := fmt.Sprintf("%s#%d", repo, pr)
	released, relErr := h.service.Storage().Locks().ReleaseIfPendingPlanID(ctx, database, dbType, lockOwner, expectedPendingPlanID)
	if relErr != nil {
		h.logger.Error("failed to release apply lock after pre-execution rejection",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType,
			"environment", environment, "reason", reason, "error", relErr)
		return
	}
	if !released {
		h.logger.Info("preserved apply lock after pre-execution rejection because its pending intent changed",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType,
			"environment", environment, "reason", reason,
			"expected_pending_plan_id", expectedPendingPlanID)
	}
}

// planChangeIdentity is the drift-comparison key for a single table change. A
// bare DDL string is not enough: the same DDL text can move between namespaces,
// tables, or operations (e.g. one keyspace dropping a table and another creating
// it), which a DDL-only multiset would treat as unchanged and auto-apply. The
// full identity is what the operator reviewed, so drift must be judged on it.
type planChangeIdentity struct {
	namespace string
	table     string
	operation string
	ddl       string
}

// ddlMatchesStoredPlan reports whether the re-plan describes the same set of
// table changes the operator reviewed in storedPlan. Comparison is
// order-independent (the flattening helpers may emit changes in different order)
// and keyed on the full change identity, not DDL text alone. Any mismatch means
// drift, and the caller downgrades an automatic apply to manual confirmation —
// so this errs toward requiring re-review, never toward silently applying a
// changed plan.
func ddlMatchesStoredPlan(planResp *apitypes.PlanResponse, storedPlan *storage.Plan) bool {
	newChanges := responsePlanIdentities(planResp)
	storedChanges := storedPlanIdentities(storedPlan)

	if len(newChanges) != len(storedChanges) {
		return false
	}

	for identity, count := range newChanges {
		if storedChanges[identity] != count {
			return false
		}
	}
	return true
}

// responsePlanIdentities builds the change-identity multiset from a re-plan
// response. Namespace comes from the SchemaChangeResponse grouping (the
// authoritative source; FlatTables() does not carry it onto each change) and is
// normalized the same way the stored plan is — an empty namespace becomes
// "default" — so the two multisets are keyed identically.
func responsePlanIdentities(planResp *apitypes.PlanResponse) map[planChangeIdentity]int {
	identities := make(map[planChangeIdentity]int)
	for _, sc := range planResp.Changes {
		namespace := normalizePlanNamespace(sc.Namespace)
		for _, tc := range sc.TableChanges {
			identities[planChangeIdentity{
				namespace: namespace,
				table:     tc.TableName,
				operation: strings.ToLower(tc.ChangeType),
				ddl:       tc.DDL,
			}]++
		}
	}
	return identities
}

// storedPlanIdentities builds the change-identity multiset from a stored plan.
// FlatDDLChanges backfills each change's namespace from its map key, which the
// store already normalized (empty → "default"), so it matches the response side.
func storedPlanIdentities(storedPlan *storage.Plan) map[planChangeIdentity]int {
	identities := make(map[planChangeIdentity]int)
	for _, tc := range storedPlan.FlatDDLChanges() {
		identities[planChangeIdentity{
			namespace: normalizePlanNamespace(tc.Namespace),
			table:     tc.Table,
			operation: strings.ToLower(tc.Operation),
			ddl:       tc.DDL,
		}]++
	}
	return identities
}

// normalizePlanNamespace mirrors the store's namespace handling so a plan whose
// proto namespace is empty (persisted as "default") compares equal to the
// re-plan response that still carries the empty grouping namespace.
func normalizePlanNamespace(namespace string) string {
	if namespace == "" {
		return "default"
	}
	return namespace
}
