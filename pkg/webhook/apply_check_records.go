package webhook

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// storeApplyPlanCheckRecord stores a check record when an apply plan is posted.
// The apply-time plan does not evaluate review-time deployment drift, so it must
// not clear a stored drift block: the block depends on live deployment state,
// not PR content, and only a fresh rollup may clear it.
func (h *Handler) storeApplyPlanCheckRecord(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schema *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment string) (string, error) {
	return h.storePlanCheckRecord(ctx, client, repo, pr, schema, planResp, environment, reviewDriftOutcome{state: driftNotEvaluated})
}

// updateCheckRecordForApplyStart updates the stored check state to "in_progress"
// when an apply begins execution. The aggregate check is updated to reflect the
// state. If the apply is already terminal by the time the claim lands, the
// stored check state is immediately refreshed to the apply's terminal outcome.
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

	// A stored review-time deployment drift block must not be cleared by starting
	// an apply: the block means a deployment's live schema no longer matches the
	// reviewed plan, so transitioning the row to in_progress (which clears the
	// block) would let the apply proceed against unverified drift. Fail closed and
	// leave the block for an operator to reconcile.
	if check != nil && check.BlockingReason == storage.ReviewTimeDeploymentDriftBlockingReason {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "apply_started",
			Repository:   repo,
			Database:     schema.Database,
			DatabaseType: schema.Type,
			Environment:  environment,
			Status:       "drift_blocked",
		})
		h.logger.Warn("apply start refused: review-time deployment drift block is present; reconcile the deployment drift before applying",
			"repo", repo, "pr", pr, "environment", environment,
			"database_type", schema.Type, "database", schema.Database,
			"apply_id", applyID, "head_sha", check.HeadSHA)
		return fmt.Errorf("apply start refused for repo %s pr %d environment %s database_type %s database %s apply_id %d: review-time deployment drift block present",
			repo, pr, environment, schema.Type, schema.Database, applyID)
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

	// The claim above races the driver: a fast apply can reach a terminal state
	// before the claim lands, and the driver's terminal check update skips
	// fail-closed when the stored row is not yet owned by the apply. Reload the
	// apply now that the claim is durable; if the apply already finished, run
	// the terminal refresh here so the stored check state converges to the
	// apply's outcome instead of staying in_progress with no writer left to
	// complete it.
	apply, err := h.service.Storage().Applies().Get(ctx, applyID)
	if err != nil {
		return fmt.Errorf("reload apply after check claim repo %s pr %d environment %s database_type %s database %s apply_id %d: %w",
			repo, pr, environment, schema.Type, schema.Database, applyID, err)
	}
	if apply == nil {
		return fmt.Errorf("apply missing after check claim repo %s pr %d environment %s database_type %s database %s apply_id %d",
			repo, pr, environment, schema.Type, schema.Database, applyID)
	}
	if state.IsTerminalApplyState(apply.State) {
		h.logger.Info("apply finished before its check claim; refreshing stored check state to the terminal outcome",
			apply.LogAttrs()...)
		h.refreshChecksForTerminalApply(ctx, apply, "apply finished before check claim")
		// The refresh resolves its own GitHub client from the durable apply; if
		// that resolution fails, the stored check state has still converged but
		// the visible aggregate Check Run has not. Refresh it with the request's
		// client too — recomputing the aggregate is idempotent, and this path
		// only runs when an apply outraces its own claim.
		h.updateAggregateCheck(ctx, client, repo, pr, check.HeadSHA)
		return nil
	}

	h.updateAggregateCheck(ctx, client, repo, pr, check.HeadSHA)
	return nil
}

// updateCheckRunAfterUnlock updates a check run to neutral after lock release.
func (h *Handler) updateCheckRunAfterUnlock(ctx context.Context, repo string, pr int, lock *storage.Lock, installationID int64) {
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for check run update",
			"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType, "error", err)
		return
	}

	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to fetch PR for check run update",
			"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType, "error", err)
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
		h.logger.Error("failed to update check run after unlock",
			"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType, "head_sha", prInfo.HeadSHA, "error", err)
	}
}
