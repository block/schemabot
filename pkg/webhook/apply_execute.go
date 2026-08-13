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

	// Revalidate the PR before interpreting the re-plan. The earlier handler
	// checks provide fast rejection, but the re-plan can be retried and the
	// base branch or PR HEAD can advance while it runs. A stale snapshot must
	// be rejected before any plan-derived response — the "no changes" release,
	// the drift downgrade, and especially the unsafe-change prompt would all
	// misread DDL that is only an artifact of the stale branch.
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
	// executeApply runs past the durable hand-off boundary, so a verification
	// failure and a verified-stale rejection both stop the apply here and
	// release the observed lock intent; the gate has already logged and
	// posted the distinction, and the user's recovery is re-issuing the
	// command.
	rejected, freshnessErr := h.assertBaseSchemaStillCurrent(ctx, client, repo, pr, installationID, schemaResult, freshPRInfo, environment, requestedBy, actionName)
	if freshnessErr != nil {
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "final base-schema freshness verification failure")
		return
	}
	if rejected {
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "final base-schema freshness rejection")
		return
	}

	// No changes (neither table DDL nor a VSchema update) — release the lock
	// (keyed on the pending intent this handler observed, so a lock re-pinned by
	// a newer plan is preserved) and notify.
	if !planResp.HasChanges() {
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

	// Engine-blocked changes reject the apply outright — the re-plan may have
	// resolved a change to blocked even if the reviewed plan had none (e.g.
	// the direct execution policy changed, or the table grew past its bound).
	// Release the lock: no retry of this command can succeed, so holding it
	// would only force a manual unlock after the schema is rewritten.
	if planResp.HasBlockedChanges() {
		commentData := buildPlanCommentData(schemaResult, planResp, environment, result.Tenant, requestedBy)
		h.logger.Info("apply rejected: re-plan contains engine-blocked changes",
			"repo", repo, "pr", pr, "database", database, "environment", environment, "action", actionName)
		h.postComment(repo, pr, installationID, templates.RenderBlockedChangesApplyRejected(commentData))
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "engine-blocked changes rejection")
		return
	}

	// Automatic apply DDL drift check: if the re-plan DDL differs from the stored auto-plan,
	// downgrade to manual confirmation so the user reviews the new plan.
	if storedPlan != nil && !ddlMatchesStoredPlan(planResp, storedPlan) {
		h.logger.Info("automatic apply downgraded: DDL drift detected",
			"repo", repo, "pr", pr, "database", database, "environment", environment)
		h.postAutoConfirmDowngrade(ctx, client, repo, pr, installationID, schemaResult, planResp, environment, result, requestedBy,
			"Schema changes differ from auto-plan — review and confirm manually")
		return
	}

	// Direct-execution changes never run from the automatic apply path: the
	// operator must confirm the blocking, non-revertible native DDL against
	// the locked plan comment that discloses it, so downgrade to manual
	// confirmation.
	if storedPlan != nil && len(planResp.DirectChanges()) > 0 {
		h.logger.Info("automatic apply downgraded: plan contains direct-execution changes",
			"repo", repo, "pr", pr, "database", database, "environment", environment)
		h.postAutoConfirmDowngrade(ctx, client, repo, pr, installationID, schemaResult, planResp, environment, result, requestedBy,
			"Plan contains direct-execution changes — review the disclosure and confirm manually")
		return
	}

	// --defer-cutover only affects engine-driven statements; an all-direct
	// plan has no cutover to defer, so reject the flag instead of silently
	// ignoring it. Only apply-confirm reaches this gate (the apply command
	// rejects the flag before locking, and an automatic apply whose re-plan
	// carries direct changes downgrades above), so keep the lock: it still
	// pins the plan the operator confirmed against, and re-running
	// apply-confirm without the flag executes it.
	if result.DeferCutover && planResp.AllChangesDirect() {
		h.logger.Info("apply rejected: --defer-cutover on an all-direct plan; the pending confirmation is preserved",
			"repo", repo, "pr", pr, "database", database, "environment", environment, "action", actionName)
		h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy,
			fmt.Sprintf(msgDeferCutoverAllDirectConfirm, environment))
		return
	}

	// Block unsafe changes on confirm (re-plan may have detected new unsafe changes)
	if len(planResp.UnsafeChanges()) > 0 && !result.AllowUnsafe {
		commentData := buildPlanCommentData(schemaResult, planResp, environment, result.Tenant, requestedBy)
		h.annotateAttributedChanges(ctx, client, &commentData, planResp, repo, pr, environment)
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
		h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy, applyExecutionErrorMessage(err))
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
			"apply_id", applyResp.ApplyID, "error", err)
		return
	}
	if apply == nil {
		h.logger.Error("apply missing after accepted apply",
			"repo", repo, "pr", pr, "database", database,
			"database_type", schemaResult.Type, "environment", environment,
			"apply_id", applyResp.ApplyID)
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
	if err := h.updateCheckRecordForApplyStart(ctx, client, repo, pr, schemaResult, environment, apply); err != nil {
		h.logger.Error("failed to mark check in_progress for apply",
			append(apply.LogAttrs(), "error", err)...)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Apply was accepted, but SchemaBot could not update the required status check: "+err.Error())
		return
	}
}

func applyExecutionErrorMessage(err error) string {
	if errors.Is(err, storage.ErrLockIntentChanged) {
		return "The pending schema change changed while this command was running. The apply was rejected; review the latest plan and run the command again."
	}
	var featureErr *api.UnsupportedFeatureError
	if errors.As(err, &featureErr) {
		return featureErr.Error()
	}
	return "Failed to execute apply. See SchemaBot server logs for details."
}

// postAutoConfirmDowngrade posts the locked plan comment that pauses an
// automatic apply for manual confirmation. It carries the original command's
// flags and the lock owner so the coached apply-confirm command re-issues the
// operator's full intent and the comment shows who holds the lock.
//
// The re-plan this downgrade acts on is the first time an automatic apply sees
// the live database, so it is also the first time it can see a destructive
// change to a table another pull request owns. The attributed-change disclosure
// belongs on this comment for the same reason as the direct-execution one: it
// must sit on the comment the confirmation acts on.
func (h *Handler) postAutoConfirmDowngrade(
	ctx context.Context, client *ghclient.InstallationClient,
	repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult,
	planResp *apitypes.PlanResponse, environment string, result CommandResult, requestedBy, reason string,
) {
	commentData := buildPlanCommentData(schemaResult, planResp, environment, result.Tenant, requestedBy)
	h.annotateAttributedChanges(ctx, client, &commentData, planResp, repo, pr, environment)
	commentData.IsLocked = true
	commentData.LockOwner = fmt.Sprintf("%s#%d", repo, pr)
	commentData.AllowUnsafe = result.AllowUnsafe
	commentData.DeferCutover = result.DeferCutover
	commentData.SkipRevert = result.SkipRevert
	commentData.AutoConfirmDowngradeReason = reason
	h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))
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
