package webhook

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// reviewDriftState is how a plan-check write treats review-time deployment
// drift. It distinguishes "the rollup ran and was clean" from "drift was not
// evaluated on this write" — both carry no block, but only the former may clear
// a stored drift block. See storage.PlanDriftState, which it mirrors.
type reviewDriftState int

const (
	// driftNotEvaluated: the write did not run the rollup (e.g. an apply-time
	// plan). Zero value so an unset outcome fails safe — it preserves, never
	// clears, an existing drift block.
	driftNotEvaluated reviewDriftState = iota
	// driftClean: the rollup ran and every deployment matched the reviewed plan.
	driftClean
	// driftBlocked: the rollup ran and a deployment diverged or could not be
	// confirmed, so the plan check fails closed.
	driftBlocked
)

// reviewDriftOutcome carries the review-time per-deployment drift outcome into
// the plan check record. When the state is driftBlocked the plan check fails
// closed regardless of whether the reviewed primary plan itself had changes,
// because a deployment's live schema no longer matches what was reviewed (or
// could not be confirmed to match). summary explains why for the check's Change
// column and logs.
type reviewDriftOutcome struct {
	state   reviewDriftState
	summary string
}

// blocks reports whether this outcome must fail the plan check closed.
func (o reviewDriftOutcome) blocks() bool { return o.state == driftBlocked }

// planDriftState maps the outcome to the storage-layer write intent that tells
// UpsertPlanResult whether it may clear, must set, or must preserve a stored
// drift block.
func (o reviewDriftOutcome) planDriftState() storage.PlanDriftState {
	switch o.state {
	case driftClean:
		return storage.PlanDriftClean
	case driftBlocked:
		return storage.PlanDriftBlocked
	default:
		return storage.PlanDriftNotEvaluated
	}
}

// storePlanCheckRecord stores per-database check state after a plan is generated.
// The state is used internally by the aggregate check to compute its overall status.
// No per-database GitHub Check Run is created — only the aggregate is visible on the PR.
// Returns the commit SHA used for the plan. Failures are non-fatal.
func (h *Handler) storePlanCheckRecord(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schema *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment string, drift reviewDriftOutcome) (string, error) {
	headSHA, _, err := h.upsertPlanCheckRecord(ctx, client, repo, pr, schema, planResp, environment, drift)
	return headSHA, err
}

// storeManualPlanCheckRecord stores per-database check state after a manual
// plan and then reconciles same-head apply-owned stored check state when the manual
// plan proves the target already matches the PR schema.
func (h *Handler) storeManualPlanCheckRecord(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schema *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment string, drift reviewDriftOutcome) (string, bool, error) {
	headSHA, check, err := h.upsertPlanCheckRecord(ctx, client, repo, pr, schema, planResp, environment, drift)
	if err != nil {
		return headSHA, false, err
	}

	// A drift-blocked check is not a clean no-op plan even when the primary's own
	// diff is empty: a non-primary deployment drifted or could not be confirmed,
	// so recovering (clearing) apply-owned check state here would wrongly unblock
	// the PR. Leave the blocking check state in place for an operator to reconcile.
	if drift.blocks() {
		h.logger.Info("plan check is deployment-drift blocked; leaving stored check state in place and skipping apply-owned no-op recovery so the block is not cleared",
			"repo", repo,
			"pr", pr,
			"database", schema.Database,
			"database_type", schema.Type,
			"environment", environment)
		return headSHA, false, nil
	}

	recoveredApplyOwnedCheckState, err := h.service.Storage().Checks().RecoverApplyOwnedCheckWithNoOpPlan(ctx, check)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "plan_check_recorded",
			Repository:   repo,
			Database:     schema.Database,
			DatabaseType: schema.Type,
			Environment:  environment,
			Status:       "error",
		})
		return headSHA, false, fmt.Errorf("recover apply-owned check state with no-op plan repo %s pr %d environment %s database_type %s database %s head_sha %s: %w",
			repo, pr, environment, schema.Type, schema.Database, headSHA, err)
	}
	if recoveredApplyOwnedCheckState {
		h.logger.Info("no-op plan recovered apply-owned check state",
			"repo", repo,
			"pr", pr,
			"head_sha", headSHA,
			"environment", environment,
			"database_type", schema.Type,
			"database", schema.Database)
	}
	return headSHA, recoveredApplyOwnedCheckState, nil
}

// planCheckConclusion decides a plan check's stored conclusion. Review-time
// drift fails the check closed ahead of the plan's own outcome: a deployment
// whose live schema no longer matches the reviewed plan (or that could not be
// confirmed to match) must block the PR even when the primary's diff is clean or
// empty. A primary plan that reported errors likewise fails; otherwise changes
// require an apply and an empty diff passes.
func planCheckConclusion(hasChanges, hasPlanErrors, driftBlocked bool) string {
	switch {
	case driftBlocked:
		return checkConclusionFailure
	case hasPlanErrors:
		return checkConclusionFailure
	case hasChanges:
		return checkConclusionActionRequired
	default:
		return checkConclusionSuccess
	}
}

func (h *Handler) upsertPlanCheckRecord(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schema *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment string, drift reviewDriftOutcome) (string, *storage.Check, error) {
	headSHA := schema.HeadSHA
	if headSHA == "" {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "plan_check_recorded",
			Repository:   repo,
			Database:     schema.Database,
			DatabaseType: schema.Type,
			Environment:  environment,
			Status:       "error",
		})
		return "", nil, fmt.Errorf("schema request missing head SHA for stored check state repo %s pr %d environment %s database_type %s database %s",
			repo, pr, environment, schema.Type, schema.Database)
	}

	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "plan_check_recorded",
			Repository:   repo,
			Database:     schema.Database,
			DatabaseType: schema.Type,
			Environment:  environment,
			Status:       "error",
		})
		return "", nil, fmt.Errorf("fetch PR for stored check state: %w", err)
	}
	if prInfo.HeadSHA != headSHA {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "plan_check_recorded",
			Repository:   repo,
			Database:     schema.Database,
			DatabaseType: schema.Type,
			Environment:  environment,
			Status:       "stale",
		})
		return headSHA, nil, fmt.Errorf("skip stale plan check record for repo %s pr %d environment %s database_type %s database %s: plan head SHA %s no longer matches current head SHA for PR %s",
			repo, pr, environment, schema.Type, schema.Database, headSHA, prInfo.HeadSHA)
	}

	tables := planResp.FlatTables()
	hasChanges := len(tables) > 0
	driftBlocked := drift.blocks()

	conclusion := planCheckConclusion(hasChanges, len(planResp.Errors) > 0, driftBlocked)

	// Review-time drift is a first-class blocking reason, not an overload of the
	// plan facts: HasChanges stays "the reviewed primary plan has changes", and
	// the block rides on BlockingReason + Conclusion so a stored drift block is
	// legible and durable across write paths.
	changeSummary := summarizePlanChanges(schema, planResp, environment)
	blockingReason := ""
	if driftBlocked {
		changeSummary = drift.summary
		blockingReason = reviewTimeDeploymentDriftBlock.blockingReason
	}

	check := &storage.Check{
		Repository:     repo,
		PullRequest:    pr,
		HeadSHA:        headSHA,
		Environment:    environment,
		DatabaseType:   schema.Type,
		DatabaseName:   schema.Database,
		HasChanges:     hasChanges,
		Status:         checkStatusCompleted,
		Conclusion:     conclusion,
		BlockingReason: blockingReason,
		ChangeSummary:  changeSummary,
	}
	if err := h.service.Storage().Checks().UpsertPlanResult(ctx, check, drift.planDriftState()); err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "plan_check_recorded",
			Repository:   repo,
			Database:     schema.Database,
			DatabaseType: schema.Type,
			Environment:  environment,
			Status:       "error",
		})
		return headSHA, nil, fmt.Errorf("store check state: %w", err)
	}

	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:    "plan_check_recorded",
		Repository:   repo,
		Database:     schema.Database,
		DatabaseType: schema.Type,
		Environment:  environment,
		Status:       "success",
	})
	return headSHA, check, nil
}

// summarizePlanChanges renders the per-database change summary stored on the
// check and shown in the aggregate check's Change column. It derives the counts
// from the same PlanCommentData the plan comment renders from, so the summary
// (e.g. "5 created, 3 altered · 2 vschema updates") always agrees with the plan
// comment's summary line. Returns "" when the plan has no changes.
func summarizePlanChanges(schema *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment string) string {
	commentData := buildPlanCommentData(schema, planResp, environment, "", "")
	return templates.SummarizeChanges(commentData)
}

type applyCheckKey struct {
	environment  string
	databaseType string
	databaseName string
}

func latestApplyByCheckKey(applies []*storage.Apply) map[applyCheckKey]*storage.Apply {
	latest := make(map[applyCheckKey]*storage.Apply, len(applies))
	for _, apply := range applies {
		key := applyCheckKey{
			environment:  apply.Environment,
			databaseType: apply.DatabaseType,
			databaseName: apply.Database,
		}
		if existing, ok := latest[key]; !ok || isApplyNewer(apply, existing) {
			latest[key] = apply
		}
	}
	return latest
}

func isApplyNewer(candidate, existing *storage.Apply) bool {
	// Apply IDs reflect storage insertion order; reconciliation wants the
	// newest stored apply row, not wall-clock ordering.
	return candidate.ID > existing.ID
}

// checkNeedsTerminalReconcile reports whether stored check state is stale
// relative to the newest apply for its target and must be repaired from that
// apply's terminal outcome. Two stale shapes exist:
//
//   - an in_progress row whose newest apply is already terminal: the driver
//     died between finishing the apply and updating stored check state;
//   - an apply-owned successful row whose newest apply is a completed
//     rollback: the rollback never claimed the row (its claim failed or the
//     driver crashed before the claim landed), so the row's success predates
//     the revert and would let the PR merge with the change missing.
//
// Successful rows without apply ownership are left alone: releasing ownership
// is how a deliberate stale-cleanup unblock records its decision, and
// re-blocking such a row here would fight the operator.
func checkNeedsTerminalReconcile(check *storage.Check, apply *storage.Apply) bool {
	if !state.IsTerminalApplyState(apply.State) {
		return false
	}
	if check.Status == checkStatusInProgress {
		return true
	}
	return check.ApplyID != 0 &&
		check.Conclusion == checkConclusionSuccess &&
		isCompletedRollback(apply)
}

// reconcileStaleChecks repairs stored check state from authoritative apply
// state. The visible GitHub Check Run is the PR merge gate, but the apply row is
// the source of truth for whether a schema change is still running. If a driver
// dies after the apply reaches a terminal state but before it updates stored
// check state, the PR can be left with an in_progress aggregate forever.
// Reconciliation runs before plan and apply commands so normal user activity can
// close that gap without operators manually editing stored check state.
func (h *Handler) reconcileStaleChecks(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int) error {
	checks, err := h.service.Storage().Checks().GetByPR(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_reconciliation",
			Repository: repo,
			Status:     "error",
		})
		return fmt.Errorf("fetch checks for stale reconciliation repo %s pr %d: %w", repo, pr, err)
	}

	applies, err := h.service.Storage().Applies().GetByPR(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_reconciliation",
			Repository: repo,
			Status:     "error",
		})
		return fmt.Errorf("look up applies for stale checks repo %s pr %d: %w", repo, pr, err)
	}
	latestApplies := latestApplyByCheckKey(applies)

	reconciled := false
	for _, check := range checks {
		if isAggregateCheck(check) {
			continue
		}

		key := applyCheckKey{
			environment:  check.Environment,
			databaseType: check.DatabaseType,
			databaseName: check.DatabaseName,
		}
		apply := latestApplies[key]
		if apply == nil {
			// A row with no apply for its target is normal for plan-only checks;
			// only an in_progress row with no apply is worth a trace.
			if check.Status == checkStatusInProgress {
				h.logger.Debug("skipping in_progress check without matching apply",
					"repo", repo, "pr", pr,
					"database", check.DatabaseName, "database_type", check.DatabaseType,
					"environment", check.Environment, "check_apply_id", check.ApplyID,
					"check_head_sha", check.HeadSHA)
			}
			continue
		}
		if !checkNeedsTerminalReconcile(check, apply) {
			// The common case: the stored row already reflects the newest apply's
			// outcome (or that apply is still running and will write its own
			// terminal update). An in_progress row that stays behind is the one
			// worth a trace.
			if check.Status == checkStatusInProgress {
				h.logger.Debug("skipping in_progress check because latest apply is not terminal",
					"repo", repo, "pr", pr,
					"database", check.DatabaseName, "database_type", check.DatabaseType,
					"environment", check.Environment,
					"apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier,
					"apply_state", apply.State, "check_apply_id", check.ApplyID,
					"check_head_sha", check.HeadSHA)
			}
			continue
		}

		h.logger.Info("reconciling stale check from the latest apply's terminal outcome",
			"repo", repo, "pr", pr,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment,
			"apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier,
			"apply_state", apply.State, "check_apply_id", check.ApplyID,
			"check_status", check.Status, "check_conclusion", check.Conclusion,
			"check_head_sha", check.HeadSHA)

		updated, err := h.updateCheckRecordForApplyResult(ctx, repo, pr, apply)
		if err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:    "stale_check_reconciliation",
				Repository:   repo,
				Database:     check.DatabaseName,
				DatabaseType: check.DatabaseType,
				Environment:  check.Environment,
				Status:       "error",
			})
			return err
		}
		if updated {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:    "stale_check_reconciliation",
				Repository:   repo,
				Database:     check.DatabaseName,
				DatabaseType: check.DatabaseType,
				Environment:  check.Environment,
				Status:       "success",
			})
			reconciled = true
		}
	}

	if !reconciled {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_reconciliation",
			Repository: repo,
			Status:     "noop",
		})
		return nil
	}

	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_reconciliation",
			Repository: repo,
			Status:     "error",
		})
		return fmt.Errorf("fetch latest PR commit SHA for stale reconciliation aggregate repo %s pr %d: %w", repo, pr, err)
	}
	if prInfo.HeadSHA != "" {
		h.updateAggregateCheck(ctx, client, repo, pr, prInfo.HeadSHA)
	}
	return nil
}

// isCompletedRollback reports whether the apply is a rollback that reached
// Completed. A completed rollback reverted the PR's schema change from the
// target environment, so its stored check must land action_required rather
// than success — the PR must not merge with the change missing.
func isCompletedRollback(a *storage.Apply) bool {
	return a.IsRollback() && state.IsState(a.State, state.Apply.Completed)
}

// updateCheckRecordForApplyResult updates stored check state after an apply
// reaches a terminal state. A completed rollback lands action_required; other
// terminal states map to success or failure. The rollback routing lives here —
// not in callers — because every terminal path (observer, operator-driven
// drive, recovery, stale reconciliation) must honor the rollback intent from
// the durable apply row. The aggregate check is updated separately to reflect
// the new status on the PR.
func (h *Handler) updateCheckRecordForApplyResult(ctx context.Context, repo string, pr int, apply *storage.Apply) (bool, error) {
	// Metrics keep rollback finalization distinct from ordinary apply completion
	// so operators can alert on the two outcomes separately.
	operation := "apply_finished"
	if isCompletedRollback(apply) {
		operation = "rollback_finished"
	}
	recordOutcome := func(status string) {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    operation,
			Repository:   repo,
			Database:     apply.Database,
			DatabaseType: apply.DatabaseType,
			Environment:  apply.Environment,
			Status:       status,
		})
	}

	check, err := h.service.Storage().Checks().Get(ctx, repo, pr, apply.Environment, apply.DatabaseType, apply.Database)
	if err != nil {
		recordOutcome("error")
		return false, fmt.Errorf("look up check for apply result repo %s pr %d environment %s database_type %s database %s: %w",
			repo, pr, apply.Environment, apply.DatabaseType, apply.Database, err)
	}
	if check == nil {
		recordOutcome("error")
		return false, fmt.Errorf("no stored check state found to update after apply repo %s pr %d environment %s database_type %s database %s",
			repo, pr, apply.Environment, apply.DatabaseType, apply.Database)
	}

	// A stopped apply is a resumable pause, not a terminal outcome. Driving the
	// stored check to a terminal conclusion here both misrepresents the pause and
	// locks out the eventual completion: CompleteForApply only advances a check
	// that is still in_progress, so once a stop marks it completed the resumed
	// apply's success can never update it. Leave the check in_progress so the PR
	// stays blocked while paused and a later resume can complete it.
	if state.IsState(apply.State, state.Apply.Stopped) {
		recordOutcome("noop")
		h.logger.Info("apply stopped; leaving check in_progress so a resume can complete it",
			"repo", repo, "pr", pr, "database", apply.Database,
			"database_type", apply.DatabaseType, "environment", apply.Environment,
			"apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier,
			"check_status", check.Status)
		return false, nil
	}

	var updated bool
	if isCompletedRollback(apply) {
		check.Status = checkStatusCompleted
		check.Conclusion = checkConclusionActionRequired
		check.HasChanges = true
		check.BlockingReason = rollbackCompletedBlock.blockingReason
		check.ErrorMessage = rollbackCompletedBlock.message
		// MarkActionRequiredForApply releases check ownership (unlike
		// CompleteForApply, which retains it) so a re-apply of the reverted
		// change can claim the row.
		updated, err = h.service.Storage().Checks().MarkActionRequiredForApply(ctx, check, apply)
		if err != nil {
			recordOutcome("error")
			return false, fmt.Errorf("mark stored check state action_required after rollback repo %s pr %d environment %s database_type %s database %s: %w",
				repo, pr, apply.Environment, apply.DatabaseType, apply.Database, err)
		}
	} else {
		var conclusion string
		switch {
		case state.IsState(apply.State, state.Apply.Completed) && checkBlockedByRemovedSchemaAfterApply(check):
			conclusion = checkConclusionActionRequired
		case state.IsState(apply.State, state.Apply.Completed):
			conclusion = checkConclusionSuccess
		case state.IsState(apply.State, state.Apply.Failed):
			conclusion = checkConclusionFailure
		default:
			conclusion = checkConclusionFailure
		}

		check.Status = checkStatusCompleted
		check.Conclusion = conclusion
		check.HasChanges = conclusion != checkConclusionSuccess
		if conclusion == checkConclusionSuccess {
			check.BlockingReason = ""
			check.ErrorMessage = ""
		}
		updated, err = h.service.Storage().Checks().CompleteForApply(ctx, check, apply)
		if err != nil {
			recordOutcome("error")
			return false, fmt.Errorf("update stored check state after apply repo %s pr %d environment %s database_type %s database %s: %w",
				repo, pr, apply.Environment, apply.DatabaseType, apply.Database, err)
		}
	}

	if !updated {
		metrics.RecordCheckOwnershipMiss(ctx, operation, repo, apply.Database, apply.DatabaseType, apply.Deployment, apply.Environment)
		recordOutcome("skipped")
		// The two writes skip for different reasons: the rollback write yields
		// only to an apply newer than the rollback, while the ordinary completion
		// requires the row to still be owned by this apply.
		msg := "skipping check state update because stored state no longer belongs to apply"
		if isCompletedRollback(apply) {
			msg = "skipping rollback action_required update because a newer apply supersedes the rollback"
		}
		h.logger.Warn(msg,
			"repo", repo, "pr", pr, "database", apply.Database,
			"database_type", apply.DatabaseType, "environment", apply.Environment,
			"apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier,
			"apply_state", apply.State, "check_apply_id", check.ApplyID,
			"check_status", check.Status, "check_head_sha", check.HeadSHA)
		return false, nil
	}

	h.logger.Info("stored check state updated after apply",
		"repo", repo, "pr", pr, "database", apply.Database,
		"database_type", apply.DatabaseType, "environment", apply.Environment,
		"apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier,
		"apply_state", apply.State, "conclusion", check.Conclusion,
		"blocking_reason", check.BlockingReason)
	recordOutcome("success")
	return true, nil
}
