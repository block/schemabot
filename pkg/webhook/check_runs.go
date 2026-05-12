package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
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

// aggregateCheckName is the check name to require in branch protection.
// Per-database checks (e.g., "SchemaBot: staging/mysql/orders") provide granular
// visibility per environment and database; the aggregate rolls them into a single
// conclusion so branch protection only needs one stable name.
const aggregateCheckName = "SchemaBot"

// aggregateSentinel is used for database type and database name when storing
// aggregate check records in the checks table. For the environment field,
// per-environment aggregates use the real environment name while the global
// aggregate (no allowed_environments) uses aggregateSentinel.
const aggregateSentinel = "_aggregate"

// aggregateCheckNameForEnv returns the environment-scoped aggregate check name.
// e.g., "SchemaBot (staging)" or "SchemaBot (production)".
func aggregateCheckNameForEnv(env string) string {
	return fmt.Sprintf("SchemaBot (%s)", env)
}

// filterChecksByEnvironment returns only checks that belong to the given environment.
// Aggregate checks are excluded.
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

// isAggregateCheck returns true if the check is an aggregate (not a per-database check).
// Aggregate checks use the sentinel value for DatabaseType and DatabaseName.
// The Environment field is either aggregateSentinel (global aggregate) or a real
// environment name (per-environment aggregate when allowed_environments is configured).
func isAggregateCheck(c *storage.Check) bool {
	return c.DatabaseType == aggregateSentinel &&
		c.DatabaseName == aggregateSentinel
}

// storePlanCheckRecord stores a per-database check record after a plan is generated.
// The record is used internally by the aggregate check to compute its overall status.
// No per-database GitHub Check Run is created — only the aggregate is visible on the PR.
// Returns the PR head SHA. Failures are non-fatal.
func (h *Handler) storePlanCheckRecord(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schema *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment string) (string, error) {
	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return "", fmt.Errorf("fetch PR for check record: %w", err)
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
		HeadSHA:      prInfo.HeadSHA,
		Environment:  environment,
		DatabaseType: schema.Type,
		DatabaseName: schema.Database,
		HasChanges:   hasChanges,
		Status:       checkStatusCompleted,
		Conclusion:   conclusion,
	}
	if err := h.service.Storage().Checks().Upsert(ctx, check); err != nil {
		return prInfo.HeadSHA, fmt.Errorf("store check record: %w", err)
	}

	return prInfo.HeadSHA, nil
}

// updateCheckRecordForApplyResult updates the stored check record after an apply
// reaches a terminal state. The aggregate check is updated separately to reflect
// the new status on the PR.
func (h *Handler) updateCheckRecordForApplyResult(ctx context.Context, repo string, pr int, apply *storage.Apply) {
	check, err := h.service.Storage().Checks().Get(ctx, repo, pr, apply.Environment, apply.DatabaseType, apply.Database)
	if err != nil {
		h.logger.Error("failed to look up check for apply result", "error", err)
		return
	}
	if check == nil {
		h.logger.Warn("no check record found to update after apply",
			"repo", repo, "pr", pr, "database", apply.Database, "environment", apply.Environment)
		return
	}

	var conclusion string
	switch apply.State {
	case state.Apply.Completed:
		conclusion = checkConclusionSuccess
	case state.Apply.Failed:
		conclusion = checkConclusionFailure
	default:
		conclusion = checkConclusionFailure
	}

	check.Status = checkStatusCompleted
	check.Conclusion = conclusion
	check.HasChanges = conclusion != checkConclusionSuccess
	if err := h.service.Storage().Checks().Upsert(ctx, check); err != nil {
		h.logger.Error("failed to update check record after apply", "error", err)
	}

	h.logger.Info("check record updated after apply",
		"repo", repo, "pr", pr, "database", apply.Database,
		"environment", apply.Environment, "conclusion", conclusion)
}

// setCheckActionRequired sets the check for a database/environment back to action_required.
// Used after a rollback completes — the PR's schema changes need to be re-applied.
func (h *Handler) setCheckActionRequired(repo string, pr int, environment, dbType, database string, installationID int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	check, err := h.service.Storage().Checks().Get(ctx, repo, pr, environment, dbType, database)
	if err != nil || check == nil {
		h.logger.Warn("no check record to update after rollback",
			"repo", repo, "pr", pr, "database", database, "environment", environment)
		return
	}

	check.Status = checkStatusCompleted
	check.Conclusion = checkConclusionActionRequired
	check.HasChanges = true
	if err := h.service.Storage().Checks().Upsert(ctx, check); err != nil {
		h.logger.Error("failed to set check to action_required after rollback", "error", err)
		return
	}

	h.logger.Info("check set to action_required after rollback",
		"repo", repo, "pr", pr, "database", database, "environment", environment)

	// Update the aggregate check to reflect the rollback
	if aggClient, err := h.ghClient.ForInstallation(installationID); err == nil {
		h.updateAggregateCheck(ctx, aggClient, repo, pr, check.HeadSHA)
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
			"environment", priorEnv, "error", err)
		// Graceful degradation: allow proceed if check lookup fails
		return false
	}

	if check == nil {
		// No check exists for this prior environment — it may not have changes.
		// This is OK (e.g., staging has no changes but production does).
		return false
	}

	switch {
	case check.Conclusion == checkConclusionSuccess:
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
			fmt.Sprintf("## ❌ Apply Blocked\n\nCould not verify %s status: failed to create GitHub client. Retry the apply command.\n\n_Error: %v_", priorEnv, err))
		return true
	}

	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to fetch PR for prior env check, blocking apply",
			"prior_env", priorEnv, "error", err)
		h.postComment(repo, pr, installationID,
			fmt.Sprintf("## ❌ Apply Blocked\n\nCould not verify %s status: failed to fetch PR details. Retry the apply command.\n\n_Error: %v_", priorEnv, err))
		return true
	}

	checkName := aggregateCheckNameForEnv(priorEnv)
	checkResult, err := client.FindCheckRunByName(ctx, repo, prInfo.HeadSHA, checkName)
	if err != nil {
		h.logger.Error("failed to query GitHub check for prior environment, blocking apply",
			"prior_env", priorEnv, "check_name", checkName, "error", err)
		h.postComment(repo, pr, installationID,
			fmt.Sprintf("## ❌ Apply Blocked\n\nCould not verify %s status: failed to query check runs. Retry the apply command.\n\n_Error: %v_", priorEnv, err))
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

// updateAggregateCheck recomputes and creates/updates aggregate check runs that roll
// up per-database checks for a PR.
//
// When allowed_environments is configured, per-environment aggregates are created
// (e.g., "SchemaBot (staging)") that only roll up checks for that environment. This
// allows separate SchemaBot instances to each publish their own aggregate without
// conflicting with each other.
//
// When allowed_environments is NOT configured, a single "SchemaBot" aggregate is
// created that rolls up all per-database checks (backwards compatible).
//
// Aggregate logic (first match wins):
//   - ANY check "in_progress"     → aggregate status "in_progress"
//   - ANY check "failure"         → aggregate "failure"
//   - ANY check "action_required" → aggregate "action_required"
//   - ALL checks "success"        → aggregate "success"
//   - NO per-database checks      → no aggregate (PR doesn't touch schema)
func (h *Handler) updateAggregateCheck(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string) {
	checks, err := h.service.Storage().Checks().GetByPR(ctx, repo, pr)
	if err != nil {
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
		// Single aggregate: backwards-compatible behavior. Uses aggregateSentinel
		// for the environment field since there is no per-environment scoping.
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

	// Look up existing aggregate check record using the environment-specific key
	existing, err := h.service.Storage().Checks().Get(ctx, repo, pr, environment, aggregateSentinel, aggregateSentinel)
	if err != nil {
		h.logger.Error("failed to look up aggregate check", "repo", repo, "pr", pr, "environment", environment, "error", err)
		return
	}

	// Create a new check run if no existing record, or if the HEAD SHA changed
	// (new commit pushed). Updating an old check run tied to a previous SHA is
	// invisible on the PR — GitHub only shows checks for the HEAD commit.
	var checkRunID int64
	if existing != nil && existing.CheckRunID != 0 && existing.HeadSHA == headSHA {
		if err := client.UpdateCheckRun(ctx, repo, existing.CheckRunID, opts); err != nil {
			h.logger.Error("failed to update aggregate check run", "checkRunID", existing.CheckRunID, "error", err)
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
			h.logger.Error("failed to create aggregate check run", "error", err)
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
		h.logger.Error("failed to store aggregate check record", "error", err)
	}

	h.logger.Info("aggregate check updated",
		"repo", repo, "pr", pr, "check_name", checkName,
		"status", status, "conclusion", conclusion,
		"per_database_checks", len(dbChecks))
}

// postPassingAggregates posts a passing aggregate check for each allowed environment.
// Called when this instance has no work to do for a PR — either because the PR doesn't
// touch schema files, or because the databases don't have environments this instance
// manages. Without this, branch protection would block indefinitely waiting for a
// check that would never come.
func (h *Handler) postPassingAggregates(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, title, summary string) {
	config := h.service.Config()

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
			h.logger.Error("failed to look up aggregate check", "repo", repo, "pr", pr, "env", ec.environment, "error", err)
			continue
		}

		// Skip if already passing for this SHA
		if existing != nil && existing.HeadSHA == headSHA && existing.Conclusion == checkConclusionSuccess {
			h.logger.Debug("passing aggregate already exists", "repo", repo, "pr", pr, "check_name", checkName)
			continue
		}

		var checkRunID int64
		if existing != nil && existing.CheckRunID != 0 && existing.HeadSHA == headSHA {
			if err := client.UpdateCheckRun(ctx, repo, existing.CheckRunID, opts); err != nil {
				h.logger.Error("failed to update passing aggregate", "error", err)
				continue
			}
			checkRunID = existing.CheckRunID
		} else {
			id, err := client.CreateCheckRun(ctx, repo, headSHA, opts)
			if err != nil {
				h.logger.Error("failed to create passing aggregate", "error", err)
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
			h.logger.Error("failed to store passing aggregate check", "error", err)
		}

		action := "created"
		if existing != nil && existing.CheckRunID != 0 && existing.HeadSHA == headSHA {
			action = "updated"
		}
		h.logger.Info("posted passing aggregate",
			"repo", repo, "pr", pr, "check_name", checkName, "env", ec.environment, "action", action)
	}
}

// postFailingAggregates posts a failing aggregate check for each allowed environment
// that has errors. Called when all environments fail during planning so branch
// protection shows a clear failure instead of waiting indefinitely.
func (h *Handler) postFailingAggregates(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, errors map[string]string) {
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
		if err := h.service.Storage().Checks().Upsert(ctx, aggCheck); err != nil {
			h.logger.Error("failed to store failing aggregate check", "error", err)
		}

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
