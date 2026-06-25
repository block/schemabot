package webhook

import (
	"context"
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

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
		h.logger.Error("plan execution failed on confirm", "repo", repo, "pr", pr, "error", err)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, err.Error())
		return
	}

	// No changes — release lock and notify. Use owner-scoped Release so we
	// can't clobber a lock that has changed ownership since this handler
	// acquired it; ErrLockNotFound / ErrLockNotOwned are expected.
	if len(planResp.FlatTables()) == 0 {
		lockOwner := fmt.Sprintf("%s#%d", repo, pr)
		relErr := h.service.Storage().Locks().Release(ctx, database, dbType, lockOwner)
		if relErr != nil && !errors.Is(relErr, storage.ErrLockNotFound) && !errors.Is(relErr, storage.ErrLockNotOwned) {
			h.logger.Error("failed to release lock after no-changes confirm",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "error", relErr)
		}
		h.postComment(repo, pr, installationID, templates.RenderApplyConfirmNoChanges(database, environment))
		return
	}

	// Auto-confirm DDL drift check: if the re-plan DDL differs from the stored auto-plan,
	// downgrade to manual confirmation so the user reviews the new plan.
	if storedPlan != nil && !ddlMatchesStoredPlan(planResp, storedPlan) {
		h.logger.Info("auto-confirm downgraded: DDL drift detected",
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

	// Resolve the App factory for this repo once so the observer captures
	// the correct App for all subsequent GitHub calls (comments, check runs).
	// Failure here is unrecoverable for outbound calls — the same error would
	// also block postComment — so log and return without attempting a comment.
	factory, factoryErr := h.factoryForRepo(repo)
	if factoryErr != nil {
		h.logger.Error("apply blocked: cannot resolve GitHub App client for repo",
			"repo", repo, "pr", pr, "database", database, "environment", environment, "error", factoryErr)
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
			ghInstClient, err := factory.ForInstallation(installationID)
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
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Failed to execute apply: "+err.Error())
		return
	}

	if !applyResp.Accepted {
		h.service.SetPendingObserver(database, "", environment, nil)
		h.logger.Info("apply rejected by engine", "repo", repo, "pr", pr, "database", database, "environment", environment, "error", applyResp.ErrorMessage)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Apply was not accepted: "+applyResp.ErrorMessage)
		return
	}

	// ExecuteApply rejects accepted applies unless SchemaBot stored its own
	// apply row. Keep this guard fail-closed in case that invariant changes.
	if applyID <= 0 {
		h.service.SetPendingObserver(database, "", environment, nil)
		h.logger.Error("accepted apply did not return an apply id",
			"repo", repo, "pr", pr, "database", database,
			"database_type", schemaResult.Type, "environment", environment)
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
	h.postAndTrackComment(ctx, repo, pr, installationID, applyID, state.Comment.Progress, progressBody)

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
