package webhook

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

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
