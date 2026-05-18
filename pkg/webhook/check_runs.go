package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// maxCheckRunTextLength is the GitHub API limit for check run output text.
const maxCheckRunTextLength = 65530

// GitHub Check Run status values.
const (
	checkStatusCompleted  = "completed"
	checkStatusInProgress = "in_progress"
)

// GitHub Check Run conclusion values.
const (
	checkConclusionSuccess        = "success"
	checkConclusionFailure        = "failure"
	checkConclusionActionRequired = "action_required"
	checkConclusionNeutral        = "neutral"
)

// aggregateCheckName is the GitHub Check Run name to require in branch protection.
// Per-database stored check state provides granular internal status per
// environment and database; the aggregate rolls it into one visible conclusion so
// branch protection only needs one stable name.
const aggregateCheckName = "SchemaBot"

// aggregateSentinel is used for database type and database name when storing
// aggregate check state in the checks table. For the environment field,
// per-environment aggregates use the real environment name while the global
// aggregate (no allowed_environments) uses aggregateSentinel.
const aggregateSentinel = "_aggregate"

type checkBlockReason struct {
	blockingReason string
	message        string
}

// schemaRemovedAfterApplyBlock is used when the latest PR commit removes a
// schema change after an apply has already started. blockingReason is the stable
// machine-readable value stored in checks.blocking_reason; message is shown to
// users in per-database check state.
var schemaRemovedAfterApplyBlock = checkBlockReason{
	blockingReason: "schema_removed_after_apply_started",
	message:        "Schema changes were removed from the PR after an apply started; operator action is required before this check can pass.",
}

// rollbackCompletedBlock is used after a rollback succeeds. The target
// environment no longer has the schema requested by the PR, so the check must
// stay blocked until the schema change is applied again or the PR is updated.
var rollbackCompletedBlock = checkBlockReason{
	blockingReason: "rollback_completed",
	message:        "Schema changes were rolled back in this environment; apply again before this check can pass.",
}

// githubConfigDiscoveryUnavailableBlock is used when GitHub is unavailable
// while SchemaBot is discovering which managed schema changes exist. The
// aggregate check must fail closed until SchemaBot can read PR metadata and
// repository contents.
var githubConfigDiscoveryUnavailableBlock = checkBlockReason{
	blockingReason: "github_schema_config_discovery_unavailable",
	message:        "SchemaBot failed this check closed because GitHub was unavailable while inspecting the PR schema files. Retry the check.",
}

// configDiscoveryFailedBlock is used when SchemaBot cannot inspect PR schema
// files well enough to know which managed schema changes exist for a reason
// other than GitHub availability. The aggregate check must fail closed until
// SchemaBot can determine the managed schema configuration.
var configDiscoveryFailedBlock = checkBlockReason{
	blockingReason: "schema_config_discovery_failed",
	message:        "SchemaBot failed this check closed because it could not determine the managed schema configuration for this PR. Review the SchemaBot configuration and retry the check.",
}

// aggregateCheckNameForEnv returns the environment-scoped aggregate check name.
// e.g., "SchemaBot (staging)" or "SchemaBot (production)".
func aggregateCheckNameForEnv(env string) string {
	return fmt.Sprintf("SchemaBot (%s)", env)
}

// filterChecksByEnvironment returns only stored check state for the given environment.
// Aggregate state is excluded.
func filterChecksByEnvironment(checks []*storage.Check, env string) []*storage.Check {
	var filtered []*storage.Check
	for _, c := range checks {
		if isAggregateCheck(c) {
			continue
		}
		if c.Environment == env {
			filtered = append(filtered, c)
		}
	}
	return filtered
}

// isAggregateCheck returns true if stored check state is aggregate, not per-database.
// Aggregate state uses the sentinel value for DatabaseType and DatabaseName.
// The Environment field is either aggregateSentinel (global aggregate) or a real
// environment name (per-environment aggregate when allowed_environments is configured).
func isAggregateCheck(c *storage.Check) bool {
	return c.DatabaseType == aggregateSentinel &&
		c.DatabaseName == aggregateSentinel
}

// checkHasStartedApply returns true once work may already have reached, or may
// still reach, the live database. ApplyID remains set on terminal apply-owned
// rows so later PR commits cannot clean them up as plan-only state.
func checkHasStartedApply(c *storage.Check) bool {
	return c.Status == checkStatusInProgress || c.ApplyID != 0
}

func checkBlockedByRemovedSchemaAfterApply(c *storage.Check) bool {
	return c.BlockingReason == schemaRemovedAfterApplyBlock.blockingReason
}

func checkBlocksPassingAggregate(c *storage.Check) bool {
	if isAggregateCheck(c) {
		return false
	}
	if checkHasStartedApply(c) {
		return true
	}
	return c.Conclusion == checkConclusionFailure || c.Conclusion == checkConclusionActionRequired
}

func hasBlockingCheckForEnvironment(checks []*storage.Check, environment string) bool {
	for _, c := range checks {
		if environment != aggregateSentinel && c.Environment != environment {
			continue
		}
		if checkBlocksPassingAggregate(c) {
			return true
		}
	}
	return false
}

// storePlanCheckRecord stores per-database check state after a plan is generated.
// The state is used internally by the aggregate check to compute its overall status.
// No per-database GitHub Check Run is created — only the aggregate is visible on the PR.
// Returns the commit SHA used for the plan. Failures are non-fatal.
func (h *Handler) storePlanCheckRecord(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schema *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment string) (string, error) {
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
		return "", fmt.Errorf("schema request missing head SHA for stored check state repo %s pr %d environment %s database_type %s database %s",
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
		return "", fmt.Errorf("fetch PR for stored check state: %w", err)
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
		return headSHA, fmt.Errorf("skip stale plan check record for repo %s pr %d environment %s database_type %s database %s: plan head SHA %s no longer matches current head SHA for PR %s",
			repo, pr, environment, schema.Type, schema.Database, headSHA, prInfo.HeadSHA)
	}

	tables := planResp.FlatTables()
	hasChanges := len(tables) > 0

	var conclusion string
	switch {
	case len(planResp.Errors) > 0:
		conclusion = checkConclusionFailure
	case hasChanges:
		conclusion = checkConclusionActionRequired
	default:
		conclusion = checkConclusionSuccess
	}

	check := &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      headSHA,
		Environment:  environment,
		DatabaseType: schema.Type,
		DatabaseName: schema.Database,
		HasChanges:   hasChanges,
		Status:       checkStatusCompleted,
		Conclusion:   conclusion,
	}
	if err := h.service.Storage().Checks().UpsertPlanResult(ctx, check); err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "plan_check_recorded",
			Repository:   repo,
			Database:     schema.Database,
			DatabaseType: schema.Type,
			Environment:  environment,
			Status:       "error",
		})
		return headSHA, fmt.Errorf("store check state: %w", err)
	}

	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:    "plan_check_recorded",
		Repository:   repo,
		Database:     schema.Database,
		DatabaseType: schema.Type,
		Environment:  environment,
		Status:       "success",
	})
	return headSHA, nil
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

// reconcileStaleChecks repairs stored check state from authoritative apply
// state. The visible GitHub Check Run is the PR merge gate, but the apply row is
// the source of truth for whether a schema change is still running. If a worker
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
		if check.Status != checkStatusInProgress {
			continue
		}
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
			h.logger.Debug("skipping in_progress check without matching apply",
				"repo", repo, "pr", pr,
				"database", check.DatabaseName, "database_type", check.DatabaseType,
				"environment", check.Environment, "check_apply_id", check.ApplyID,
				"check_head_sha", check.HeadSHA)
			continue
		}
		if !state.IsTerminalApplyState(apply.State) {
			h.logger.Debug("skipping in_progress check because latest apply is not terminal",
				"repo", repo, "pr", pr,
				"database", check.DatabaseName, "database_type", check.DatabaseType,
				"environment", check.Environment,
				"apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier,
				"apply_state", apply.State, "check_apply_id", check.ApplyID,
				"check_head_sha", check.HeadSHA)
			continue
		}

		h.logger.Info("reconciling stale in_progress check",
			"repo", repo, "pr", pr,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment,
			"apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier,
			"apply_state", apply.State, "check_apply_id", check.ApplyID,
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

// updateCheckRecordForApplyResult updates stored check state after an apply
// reaches a terminal state. The aggregate check is updated separately to reflect
// the new status on the PR.
func (h *Handler) updateCheckRecordForApplyResult(ctx context.Context, repo string, pr int, apply *storage.Apply) (bool, error) {
	check, err := h.service.Storage().Checks().Get(ctx, repo, pr, apply.Environment, apply.DatabaseType, apply.Database)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "apply_finished",
			Repository:   repo,
			Database:     apply.Database,
			DatabaseType: apply.DatabaseType,
			Environment:  apply.Environment,
			Status:       "error",
		})
		return false, fmt.Errorf("look up check for apply result repo %s pr %d environment %s database_type %s database %s: %w",
			repo, pr, apply.Environment, apply.DatabaseType, apply.Database, err)
	}
	if check == nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "apply_finished",
			Repository:   repo,
			Database:     apply.Database,
			DatabaseType: apply.DatabaseType,
			Environment:  apply.Environment,
			Status:       "error",
		})
		return false, fmt.Errorf("no stored check state found to update after apply repo %s pr %d environment %s database_type %s database %s",
			repo, pr, apply.Environment, apply.DatabaseType, apply.Database)
	}

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
	updated, err := h.service.Storage().Checks().CompleteForApply(ctx, check, apply)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "apply_finished",
			Repository:   repo,
			Database:     apply.Database,
			DatabaseType: apply.DatabaseType,
			Environment:  apply.Environment,
			Status:       "error",
		})
		return false, fmt.Errorf("update stored check state after apply repo %s pr %d environment %s database_type %s database %s: %w",
			repo, pr, apply.Environment, apply.DatabaseType, apply.Database, err)
	}
	if !updated {
		metrics.RecordCheckOwnershipMiss(ctx, "apply_finished", repo, apply.Database, apply.DatabaseType, apply.Environment)
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "apply_finished",
			Repository:   repo,
			Database:     apply.Database,
			DatabaseType: apply.DatabaseType,
			Environment:  apply.Environment,
			Status:       "skipped",
		})
		h.logger.Warn("skipping check state update because stored state no longer belongs to apply",
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
		"apply_state", apply.State, "conclusion", conclusion,
		"blocking_reason", check.BlockingReason)
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:    "apply_finished",
		Repository:   repo,
		Database:     apply.Database,
		DatabaseType: apply.DatabaseType,
		Environment:  apply.Environment,
		Status:       "success",
	})
	return true, nil
}

// setCheckActionRequired sets the rollback apply's check back to action_required.
// Used after a rollback completes because the PR's schema changes need to be re-applied.
func (h *Handler) setCheckActionRequired(repo string, pr int, installationID int64, apply *storage.Apply) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	check, err := h.service.Storage().Checks().Get(ctx, repo, pr, apply.Environment, apply.DatabaseType, apply.Database)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "rollback_finished",
			Repository:   repo,
			Database:     apply.Database,
			DatabaseType: apply.DatabaseType,
			Environment:  apply.Environment,
			Status:       "error",
		})
		h.logger.Error("failed to look up stored check state after rollback",
			"repo", repo, "pr", pr, "database", apply.Database,
			"database_type", apply.DatabaseType, "environment", apply.Environment,
			"apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier,
			"error", err)
		return
	}
	if check == nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "rollback_finished",
			Repository:   repo,
			Database:     apply.Database,
			DatabaseType: apply.DatabaseType,
			Environment:  apply.Environment,
			Status:       "error",
		})
		h.logger.Warn("no stored check state to update after rollback",
			"repo", repo, "pr", pr, "database", apply.Database,
			"database_type", apply.DatabaseType, "environment", apply.Environment,
			"apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier)
		return
	}

	check.Status = checkStatusCompleted
	check.Conclusion = checkConclusionActionRequired
	check.HasChanges = true
	check.BlockingReason = rollbackCompletedBlock.blockingReason
	check.ErrorMessage = rollbackCompletedBlock.message
	updated, err := h.service.Storage().Checks().MarkActionRequiredForApply(ctx, check, apply)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "rollback_finished",
			Repository:   repo,
			Database:     apply.Database,
			DatabaseType: apply.DatabaseType,
			Environment:  apply.Environment,
			Status:       "error",
		})
		h.logger.Error("failed to set check to action_required after rollback",
			"repo", repo, "pr", pr, "database", apply.Database,
			"database_type", apply.DatabaseType, "environment", apply.Environment,
			"apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier,
			"check_apply_id", check.ApplyID, "check_status", check.Status,
			"check_head_sha", check.HeadSHA, "error", err)
		return
	}
	if !updated {
		metrics.RecordCheckOwnershipMiss(ctx, "rollback_finished", repo, apply.Database, apply.DatabaseType, apply.Environment)
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "rollback_finished",
			Repository:   repo,
			Database:     apply.Database,
			DatabaseType: apply.DatabaseType,
			Environment:  apply.Environment,
			Status:       "skipped",
		})
		h.logger.Warn("skipping rollback action_required update because check no longer belongs to apply",
			"repo", repo, "pr", pr, "database", apply.Database,
			"database_type", apply.DatabaseType, "environment", apply.Environment,
			"apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier,
			"apply_state", apply.State, "check_apply_id", check.ApplyID,
			"check_status", check.Status, "check_head_sha", check.HeadSHA)
		return
	}

	h.logger.Info("check set to action_required after rollback",
		"repo", repo, "pr", pr, "database", apply.Database,
		"database_type", apply.DatabaseType, "environment", apply.Environment,
		"apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier,
		"apply_state", apply.State)
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:    "rollback_finished",
		Repository:   repo,
		Database:     apply.Database,
		DatabaseType: apply.DatabaseType,
		Environment:  apply.Environment,
		Status:       "success",
	})

	// Update the aggregate check to reflect the rollback
	if aggClient, err := h.ghClient.ForInstallation(installationID); err == nil {
		h.updateAggregateCheck(ctx, aggClient, repo, pr, check.HeadSHA)
	} else {
		h.logger.Error("failed to create GitHub client for rollback aggregate update",
			"repo", repo, "pr", pr, "database", apply.Database,
			"database_type", apply.DatabaseType, "environment", apply.Environment,
			"apply_id", apply.ID, "apply_identifier", apply.ApplyIdentifier,
			"error", err)
	}
}

// checkPriorEnvironments enforces environment ordering: all environments before
// the current one in the configured list must have their check run at "success".
// Returns true if the apply is blocked (caller should return).
//
// For environments: [sandbox, staging, production]
//   - applying to sandbox: no prior envs, always allowed
//   - applying to staging: sandbox must be success
//   - applying to production: both sandbox and staging must be success
//
// When allowed_environments is configured, this instance only owns a subset of
// environments. For prior environments owned by this instance, the local database
// is checked. For prior environments owned by another instance, the GitHub Checks
// API is queried for the per-environment aggregate check run.
func (h *Handler) checkPriorEnvironments(
	ctx context.Context, repo string, pr int,
	database, dbType, environment string,
	environments []string,
	installationID int64, requestedBy string,
) bool {
	// Find the index of the current environment
	currentIdx := -1
	for i, env := range environments {
		if env == environment {
			currentIdx = i
			break
		}
	}

	// First environment or not in list — no prior environments to check
	if currentIdx <= 0 {
		return false
	}

	config := h.service.Config()

	// Check all prior environments
	for i := 0; i < currentIdx; i++ {
		priorEnv := environments[i]

		if config.IsEnvironmentAllowed(priorEnv) {
			// This instance owns the prior environment — check local database
			if blocked := h.checkPriorEnvViaLocal(ctx, repo, pr, database, dbType, environment, priorEnv, installationID); blocked {
				return true
			}
		} else {
			// Another instance owns this environment — check GitHub Checks API
			if blocked := h.checkPriorEnvViaGitHub(ctx, repo, pr, database, environment, priorEnv, installationID); blocked {
				return true
			}
		}
	}

	return false
}

// checkPriorEnvViaLocal checks the prior environment status using the local database.
func (h *Handler) checkPriorEnvViaLocal(
	ctx context.Context, repo string, pr int,
	database, dbType, environment, priorEnv string,
	installationID int64,
) bool {
	check, err := h.service.Storage().Checks().Get(ctx, repo, pr, priorEnv, dbType, database)
	if err != nil {
		h.logger.Error("failed to look up prior environment check",
			"repo", repo, "pr", pr,
			"database", database, "database_type", dbType,
			"environment", environment, "prior_environment", priorEnv,
			"error", err)
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnvCheckError(priorEnv, "read SchemaBot storage", err))
		return true
	}

	if check == nil {
		// No check exists for this prior environment — it may not have changes.
		// This is OK (e.g., staging has no changes but production does).
		h.logger.Debug("no prior environment check found, allowing apply",
			"repo", repo, "pr", pr,
			"database", database, "database_type", dbType,
			"environment", environment, "prior_environment", priorEnv)
		return false
	}

	switch {
	case check.Conclusion == checkConclusionSuccess:
		h.logger.Debug("prior environment check passed, allowing apply",
			"repo", repo, "pr", pr,
			"database", database, "database_type", dbType,
			"environment", environment, "prior_environment", priorEnv,
			"check_status", check.Status, "check_conclusion", check.Conclusion)
		return false
	case check.Status == checkStatusInProgress:
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnvInProgress(database, environment, priorEnv))
		return true
	default:
		status := "has pending changes"
		action := fmt.Sprintf("Apply %s first", priorEnv)
		if check.Conclusion == checkConclusionFailure {
			status = "failed"
			action = fmt.Sprintf("Fix the issue and re-apply %s", priorEnv)
		}
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnv(database, environment, priorEnv, status, action))
		return true
	}
}

// checkPriorEnvViaGitHub checks the prior environment status by querying the
// GitHub Checks API for the per-environment aggregate check run created by the
// other SchemaBot instance that owns that environment.
//
// The remote check uses the aggregate "SchemaBot (staging)" which rolls up ALL
// databases in the prior environment. This is stricter than per-database checking:
// production apply for any database is blocked until ALL databases in staging are
// applied. This is the correct behavior for the remote case — we cannot query
// per-database check state from another instance, and it is safer to require the
// entire environment to be healthy before promoting.
func (h *Handler) checkPriorEnvViaGitHub(
	ctx context.Context, repo string, pr int,
	database, environment, priorEnv string,
	installationID int64,
) bool {
	client, err := h.ghClient.ForInstallation(installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for prior env check, blocking apply",
			"prior_env", priorEnv, "error", err)
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnvCheckError(priorEnv, "create GitHub client", err))
		return true
	}

	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to fetch PR for prior env check, blocking apply",
			"prior_env", priorEnv, "error", err)
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnvCheckError(priorEnv, "fetch PR details", err))
		return true
	}

	checkName := aggregateCheckNameForEnv(priorEnv)
	checkResult, err := client.FindCheckRunByName(ctx, repo, prInfo.HeadSHA, checkName)
	if err != nil {
		h.logger.Error("failed to query GitHub check for prior environment, blocking apply",
			"prior_env", priorEnv, "check_name", checkName, "error", err)
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnvCheckError(priorEnv, "query check runs", err))
		return true
	}

	if checkResult == nil {
		// No check run from the other instance. This could mean:
		// (a) The prior environment has no schema changes for this PR (legit — allow proceed)
		// (b) The other instance hasn't processed the webhook yet (race — should block)
		// We cannot distinguish these cases without additional coordination. For now,
		// allow proceed since blocking on missing checks would break PRs that only
		// touch a subset of environments. A future improvement could add a brief retry
		// with backoff to handle case (b).
		slog.Info("no GitHub check found for prior environment, allowing proceed",
			"prior_env", priorEnv, "check_name", checkName, "repo", repo, "pr", pr)
		return false
	}

	switch {
	case checkResult.Status == checkStatusCompleted && checkResult.Conclusion == checkConclusionSuccess:
		slog.Debug("prior environment verified via GitHub check",
			"prior_env", priorEnv, "conclusion", checkResult.Conclusion)
		return false
	case checkResult.Status == checkStatusInProgress:
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnvInProgress(database, environment, priorEnv))
		return true
	default:
		status := "has pending changes"
		action := fmt.Sprintf("Apply %s first", priorEnv)
		if checkResult.Conclusion == checkConclusionFailure {
			status = "failed"
			action = fmt.Sprintf("Fix the issue and re-apply %s", priorEnv)
		}
		h.postComment(repo, pr, installationID,
			templates.RenderApplyBlockedByPriorEnv(database, environment, priorEnv, status, action))
		return true
	}
}

// verifyHeadSHAStillCurrentForPR returns false when writing status check state
// for headSHA would be unsafe because the PR now points at a different commit
// SHA. It records a metric and logs the reason before every false return so
// callers can stop without adding duplicate log noise.
func (h *Handler) verifyHeadSHAStillCurrentForPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, operation string) bool {
	if headSHA == "" {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  operation,
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("refusing to update status check without head SHA", "repo", repo, "pr", pr, "operation", operation)
		return false
	}

	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  operation,
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to verify status check head SHA before update",
			"repo", repo, "pr", pr, "head_sha", headSHA, "operation", operation, "error", err)
		return false
	}
	if prInfo.HeadSHA != headSHA {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  operation,
			Repository: repo,
			Status:     "stale",
		})
		h.logger.Info("skipping stale status check update because head SHA is no longer current for PR",
			"repo", repo, "pr", pr, "operation", operation,
			"stale_head_sha", headSHA, "current_head_sha", prInfo.HeadSHA)
		return false
	}
	return true
}

// updateAggregateCheck recomputes and creates/updates aggregate check runs that roll
// up per-database checks for a PR.
//
// When allowed_environments is configured, per-environment aggregates are created
// (e.g., "SchemaBot (staging)") that only roll up checks for that environment. This
// allows separate SchemaBot instances to each publish their own aggregate without
// conflicting with each other.
//
// When allowed_environments is NOT configured, a single "SchemaBot" aggregate is
// created that rolls up all per-database checks.
//
// Aggregate logic (first match wins):
//   - ANY check "in_progress"     → aggregate status "in_progress"
//   - ANY check "failure"         → aggregate "failure"
//   - ANY check "action_required" → aggregate "action_required"
//   - ALL checks "success"        → aggregate "success"
//   - NO per-database checks      → no aggregate (PR doesn't touch schema)
func (h *Handler) updateAggregateCheck(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string) {
	if !h.verifyHeadSHAStillCurrentForPR(ctx, client, repo, pr, headSHA, "aggregate_check_sync") {
		return
	}

	checks, err := h.service.Storage().Checks().GetByPR(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "aggregate_check_sync",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to fetch checks for aggregate", "repo", repo, "pr", pr, "error", err)
		return
	}

	// Filter out aggregate checks — only per-database checks contribute
	var dbChecks []*storage.Check
	for _, c := range checks {
		if !isAggregateCheck(c) {
			dbChecks = append(dbChecks, c)
		}
	}

	// No per-database checks means the PR doesn't touch schema files (or all check
	// records were already deleted by PR close cleanup). No aggregate to create.
	if len(dbChecks) == 0 {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "aggregate_check_sync",
			Repository: repo,
			Status:     "noop",
		})
		h.logger.Debug("no per-database checks for aggregate", "repo", repo, "pr", pr)
		return
	}

	config := h.service.Config()

	if len(config.AllowedEnvironments) > 0 {
		// Per-environment aggregates: create one aggregate per allowed environment.
		// Each uses the real environment name in the storage key to avoid collisions
		// between environments (e.g., staging vs production aggregates).
		for _, env := range config.AllowedEnvironments {
			envChecks := filterChecksByEnvironment(dbChecks, env)
			if len(envChecks) == 0 {
				continue
			}
			checkName := aggregateCheckNameForEnv(env)
			h.upsertAggregateCheckRun(ctx, client, repo, pr, headSHA, envChecks, checkName, env)
		}
	} else {
		// Single aggregate. Uses aggregateSentinel for the environment field
		// since there is no per-environment scoping.
		h.upsertAggregateCheckRun(ctx, client, repo, pr, headSHA, dbChecks, aggregateCheckName, aggregateSentinel)
	}
}

// upsertAggregateCheckRun computes the aggregate conclusion from the given checks
// and creates or updates a GitHub check run with the specified name.
//
// The environment parameter controls the storage key: for per-environment aggregates
// it is the real environment name (e.g., "staging"), for the global aggregate it is
// aggregateSentinel. DatabaseType and DatabaseName always use aggregateSentinel.
func (h *Handler) upsertAggregateCheckRun(
	ctx context.Context, client *ghclient.InstallationClient,
	repo string, pr int, headSHA string,
	dbChecks []*storage.Check, checkName string, environment string,
) {
	conclusion, status := computeAggregate(dbChecks)
	title, summary := aggregateSummary(dbChecks, conclusion)

	opts := ghclient.CheckRunOptions{
		Name:   checkName,
		Status: status,
		Output: &ghclient.CheckRunOutput{
			Title:   title,
			Summary: summary,
		},
	}
	// GitHub requires conclusion only when status is "completed"
	if status == checkStatusCompleted {
		opts.Conclusion = conclusion
	}

	// Look up existing aggregate check state using the environment-specific key.
	existing, err := h.service.Storage().Checks().Get(ctx, repo, pr, environment, aggregateSentinel, aggregateSentinel)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   "aggregate_check_sync",
			Repository:  repo,
			Environment: environment,
			Status:      "error",
		})
		h.logger.Error("failed to look up aggregate check", "repo", repo, "pr", pr, "environment", environment, "error", err)
		return
	}

	// Create a new check run if no existing record, or if the HEAD SHA changed
	// (new commit pushed). Updating an old check run tied to a previous SHA is
	// invisible on the PR — GitHub only shows checks for the HEAD commit.
	var checkRunID int64
	if existing != nil && existing.CheckRunID != 0 && existing.HeadSHA == headSHA {
		if err := client.UpdateCheckRun(ctx, repo, existing.CheckRunID, opts); err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: environment,
				Status:      "error",
			})
			h.logger.Error("failed to update aggregate check run",
				"repo", repo, "pr", pr, "check_name", checkName,
				"environment", environment, "check_run_id", existing.CheckRunID,
				"head_sha", headSHA, "status", status,
				"conclusion", conclusion, "error", err)
			return
		}
		checkRunID = existing.CheckRunID
	} else {
		if existing != nil && existing.HeadSHA != headSHA {
			h.logger.Info("re-creating aggregate check on new HEAD SHA",
				"repo", repo, "pr", pr,
				"old_sha", existing.HeadSHA, "new_sha", headSHA)
		}
		id, err := client.CreateCheckRun(ctx, repo, headSHA, opts)
		if err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: environment,
				Status:      "error",
			})
			h.logger.Error("failed to create aggregate check run",
				"repo", repo, "pr", pr, "check_name", checkName,
				"environment", environment, "head_sha", headSHA,
				"status", status, "conclusion", conclusion, "error", err)
			return
		}
		checkRunID = id
	}

	aggCheck := &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      headSHA,
		Environment:  environment,
		DatabaseType: aggregateSentinel,
		DatabaseName: aggregateSentinel,
		CheckRunID:   checkRunID,
		HasChanges:   conclusion != checkConclusionSuccess,
		Status:       status,
		Conclusion:   conclusion,
	}
	if err := h.service.Storage().Checks().Upsert(ctx, aggCheck); err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   "aggregate_check_sync",
			Repository:  repo,
			Environment: environment,
			Status:      "error",
		})
		h.logger.Error("failed to store aggregate check state",
			"repo", repo, "pr", pr, "check_name", checkName,
			"environment", environment, "check_run_id", checkRunID,
			"head_sha", headSHA, "status", status,
			"conclusion", conclusion, "error", err)
		return
	}

	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:   "aggregate_check_sync",
		Repository:  repo,
		Environment: environment,
		Status:      "success",
	})
	h.logger.Info("aggregate check updated",
		"repo", repo, "pr", pr, "check_name", checkName,
		"environment", environment, "check_run_id", checkRunID,
		"status", status, "conclusion", conclusion,
		"per_database_checks", len(dbChecks))
}

// postPassingAggregates posts a passing aggregate check for each allowed environment.
// Called when this instance has no work to do for a PR — either because the PR doesn't
// touch schema files, or because the databases don't have environments this instance
// manages. Without this, branch protection would block indefinitely waiting for a
// check that would never come. It does not publish success over existing
// per-database state that still needs operator attention.
func (h *Handler) postPassingAggregates(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, title, summary string) {
	if !h.verifyHeadSHAStillCurrentForPR(ctx, client, repo, pr, headSHA, "aggregate_check_sync") {
		return
	}

	config := h.service.Config()
	storedChecks, err := h.service.Storage().Checks().GetByPR(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "aggregate_check_sync",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to fetch checks before passing aggregate", "repo", repo, "pr", pr, "error", err)
		return
	}

	type envCheck struct {
		name        string
		environment string
	}

	var checks []envCheck
	if len(config.AllowedEnvironments) > 0 {
		for _, env := range config.AllowedEnvironments {
			checks = append(checks, envCheck{
				name:        aggregateCheckNameForEnv(env),
				environment: env,
			})
		}
	} else {
		checks = append(checks, envCheck{
			name:        aggregateCheckName,
			environment: aggregateSentinel,
		})
	}

	h.logger.Debug("posting passing aggregates", "repo", repo, "pr", pr, "head_sha", headSHA, "count", len(checks))

	for _, ec := range checks {
		checkName := ec.name

		if hasBlockingCheckForEnvironment(storedChecks, ec.environment) {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "blocked",
			})
			h.logger.Info("skipping passing aggregate because stored checks still block",
				"repo", repo, "pr", pr, "check_name", checkName, "environment", ec.environment)
			continue
		}

		opts := ghclient.CheckRunOptions{
			Name:       checkName,
			Status:     checkStatusCompleted,
			Conclusion: checkConclusionSuccess,
			Output: &ghclient.CheckRunOutput{
				Title:   title,
				Summary: summary,
			},
		}

		existing, err := h.service.Storage().Checks().Get(ctx, repo, pr, ec.environment, aggregateSentinel, aggregateSentinel)
		if err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "error",
			})
			h.logger.Error("failed to look up aggregate check", "repo", repo, "pr", pr, "env", ec.environment, "error", err)
			continue
		}

		// Skip if already passing for this SHA
		if existing != nil && existing.HeadSHA == headSHA && existing.Conclusion == checkConclusionSuccess {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "noop",
			})
			h.logger.Debug("passing aggregate already exists", "repo", repo, "pr", pr, "check_name", checkName)
			continue
		}

		var checkRunID int64
		if existing != nil && existing.CheckRunID != 0 && existing.HeadSHA == headSHA {
			if err := client.UpdateCheckRun(ctx, repo, existing.CheckRunID, opts); err != nil {
				metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
					Operation:   "aggregate_check_sync",
					Repository:  repo,
					Environment: ec.environment,
					Status:      "error",
				})
				h.logger.Error("failed to update passing aggregate",
					"repo", repo, "pr", pr, "check_name", checkName,
					"environment", ec.environment, "check_run_id", existing.CheckRunID,
					"head_sha", headSHA, "error", err)
				continue
			}
			checkRunID = existing.CheckRunID
		} else {
			id, err := client.CreateCheckRun(ctx, repo, headSHA, opts)
			if err != nil {
				metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
					Operation:   "aggregate_check_sync",
					Repository:  repo,
					Environment: ec.environment,
					Status:      "error",
				})
				h.logger.Error("failed to create passing aggregate",
					"repo", repo, "pr", pr, "check_name", checkName,
					"environment", ec.environment, "head_sha", headSHA, "error", err)
				continue
			}
			checkRunID = id
		}

		aggCheck := &storage.Check{
			Repository:   repo,
			PullRequest:  pr,
			HeadSHA:      headSHA,
			Environment:  ec.environment,
			DatabaseType: aggregateSentinel,
			DatabaseName: aggregateSentinel,
			CheckRunID:   checkRunID,
			HasChanges:   false,
			Status:       checkStatusCompleted,
			Conclusion:   checkConclusionSuccess,
		}
		if err := h.service.Storage().Checks().Upsert(ctx, aggCheck); err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "error",
			})
			h.logger.Error("failed to store passing aggregate check",
				"repo", repo, "pr", pr, "check_name", checkName,
				"environment", ec.environment, "check_run_id", checkRunID,
				"head_sha", headSHA, "error", err)
			continue
		}

		action := "created"
		if existing != nil && existing.CheckRunID != 0 && existing.HeadSHA == headSHA {
			action = "updated"
		}
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   "aggregate_check_sync",
			Repository:  repo,
			Environment: ec.environment,
			Status:      "success",
		})
		h.logger.Info("posted passing aggregate",
			"repo", repo, "pr", pr, "check_name", checkName, "env", ec.environment, "action", action)
	}
}

// postFailingAggregates posts a failing aggregate check for each allowed environment
// that has errors. Called when all environments fail during planning so branch
// protection shows a clear failure instead of waiting indefinitely.
func (h *Handler) postFailingAggregates(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, errors map[string]string) {
	h.postFailingAggregatesWithBlock(ctx, client, repo, pr, headSHA, errors, checkBlockReason{})
}

// postFailingAggregatesWithBlock stores a blocking reason only for callers with
// a stable failure class. Generic plan errors should use postFailingAggregates.
func (h *Handler) postFailingAggregatesWithBlock(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, errors map[string]string, block checkBlockReason) {
	if !h.verifyHeadSHAStillCurrentForPR(ctx, client, repo, pr, headSHA, "aggregate_check_sync") {
		return
	}

	config := h.service.Config()

	type envCheck struct {
		name        string
		environment string
	}

	var checks []envCheck
	if len(config.AllowedEnvironments) > 0 {
		for _, env := range config.AllowedEnvironments {
			if _, hasError := errors[env]; hasError {
				checks = append(checks, envCheck{
					name:        aggregateCheckNameForEnv(env),
					environment: env,
				})
			}
		}
	} else {
		checks = append(checks, envCheck{
			name:        aggregateCheckName,
			environment: aggregateSentinel,
		})
	}

	if len(checks) == 0 {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "aggregate_check_sync",
			Repository: repo,
			Status:     "noop",
		})
		h.logger.Debug("no failing aggregate checks to post", "repo", repo, "pr", pr)
		return
	}

	for _, ec := range checks {
		// Build summary from the error for this environment
		summary := "Plan failed"
		if errMsg, ok := errors[ec.environment]; ok {
			summary = errMsg
		} else if len(errors) > 0 {
			// Single-instance mode: use first error
			for _, msg := range errors {
				summary = msg
				break
			}
		}

		opts := ghclient.CheckRunOptions{
			Name:       ec.name,
			Status:     checkStatusCompleted,
			Conclusion: checkConclusionFailure,
			Output: &ghclient.CheckRunOutput{
				Title:   "Plan failed",
				Summary: summary,
			},
		}

		id, err := client.CreateCheckRun(ctx, repo, headSHA, opts)
		if err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "error",
			})
			h.logger.Error("failed to create failing aggregate", "repo", repo, "pr", pr, "error", err)
			continue
		}

		aggCheck := &storage.Check{
			Repository:   repo,
			PullRequest:  pr,
			HeadSHA:      headSHA,
			Environment:  ec.environment,
			DatabaseType: aggregateSentinel,
			DatabaseName: aggregateSentinel,
			CheckRunID:   id,
			HasChanges:   false,
			Status:       checkStatusCompleted,
			Conclusion:   checkConclusionFailure,
		}
		if block.blockingReason != "" {
			aggCheck.BlockingReason = block.blockingReason
			aggCheck.ErrorMessage = summary
		}
		if err := h.service.Storage().Checks().Upsert(ctx, aggCheck); err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "error",
			})
			h.logger.Error("failed to store failing aggregate check", "error", err)
			continue
		}

		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   "aggregate_check_sync",
			Repository:  repo,
			Environment: ec.environment,
			Status:      "success",
		})
		h.logger.Info("posted failing aggregate",
			"repo", repo, "pr", pr, "check_name", ec.name, "env", ec.environment)
	}
}

// computeAggregate determines the aggregate conclusion and status from per-database checks.
func computeAggregate(checks []*storage.Check) (conclusion, status string) {
	// in_progress takes precedence — the aggregate should show running
	for _, c := range checks {
		if c.Status == checkStatusInProgress {
			return "", checkStatusInProgress
		}
	}

	// All checks are completed — compute conclusion
	for _, c := range checks {
		if c.Conclusion == checkConclusionFailure {
			return checkConclusionFailure, checkStatusCompleted
		}
	}
	for _, c := range checks {
		if c.Conclusion == checkConclusionActionRequired {
			return checkConclusionActionRequired, checkStatusCompleted
		}
	}

	return checkConclusionSuccess, checkStatusCompleted
}

// aggregateSummary builds a human-readable title and markdown summary for the aggregate check.
func aggregateSummary(checks []*storage.Check, conclusion string) (title, summary string) {
	switch conclusion {
	case checkConclusionSuccess:
		title = "All applies complete"
		summary = buildAggregateTable(checks)
	case checkConclusionFailure:
		title = "Apply failed"
		summary = buildAggregateTable(checks)
	case checkConclusionActionRequired:
		pending := 0
		for _, c := range checks {
			if c.Conclusion == checkConclusionActionRequired {
				pending++
			}
		}
		if pending == 1 {
			title = "1 apply pending"
		} else {
			title = fmt.Sprintf("%d applies pending", pending)
		}
		summary = buildAggregateTable(checks)
	default:
		// in_progress — conclusion is empty
		title = "Apply in progress"
		summary = buildAggregateTable(checks)
	}
	return title, summary
}

// buildAggregateTable builds a markdown table showing the status of each per-database check.
// Truncates to stay within GitHub's check run output limits.
func buildAggregateTable(checks []*storage.Check) string {
	var sb strings.Builder
	sb.WriteString("| Database | Environment | Status |\n")
	sb.WriteString("|----------|-------------|--------|\n")

	for i, c := range checks {
		row := fmt.Sprintf("| `%s` | %s | %s |\n", c.DatabaseName, c.Environment, conclusionEmoji(c.Status, c.Conclusion))
		if sb.Len()+len(row) > maxCheckRunTextLength-1000 {
			fmt.Fprintf(&sb, "\n... and %d more check(s)\n", len(checks)-i)
			break
		}
		sb.WriteString(row)
	}

	return sb.String()
}

// conclusionEmoji returns a short status label for a check.
func conclusionEmoji(status, conclusion string) string {
	if status == checkStatusInProgress {
		return "In progress"
	}
	switch conclusion {
	case checkConclusionSuccess:
		return "Applied"
	case checkConclusionFailure:
		return "Failed"
	case checkConclusionActionRequired:
		return "Pending"
	case checkConclusionNeutral:
		return "Cancelled"
	default:
		return conclusion
	}
}
