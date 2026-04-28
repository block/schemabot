package webhook

import (
	"context"
	"fmt"
	"strings"

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

// aggregateSentinel is used for environment, database type, and database name when
// storing the aggregate check record in the checks table. Distinguishes it from
// per-database checks which use real values.
const aggregateSentinel = "_aggregate"

// isAggregateCheck returns true if the check is the aggregate (not a per-database check).
func isAggregateCheck(c *storage.Check) bool {
	return c.Environment == aggregateSentinel &&
		c.DatabaseType == aggregateSentinel &&
		c.DatabaseName == aggregateSentinel
}

// checkRunName returns the canonical check run name for a given environment, database type, and database.
func checkRunName(environment, dbType, database string) string {
	return fmt.Sprintf("SchemaBot: %s/%s/%s", environment, dbType, database)
}

// createPlanCheckRun creates a GitHub Check Run after a plan is generated.
// Returns the PR head SHA used for the check run. Check run failures are non-fatal —
// they should not prevent the plan from being posted.
func (h *Handler) createPlanCheckRun(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schema *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment string, installationID int64) (string, error) {
	// Get PR head SHA for the check run
	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return "", fmt.Errorf("fetch PR for check run: %w", err)
	}

	tables := planResp.FlatTables()
	hasChanges := len(tables) > 0
	checkName := checkRunName(environment, schema.Type, schema.Database)

	var conclusion string
	var title string
	var summary string

	switch {
	case len(planResp.Errors) > 0:
		conclusion = checkConclusionFailure
		title = "Plan failed"
		summary = fmt.Sprintf("Plan failed with %d error(s)", len(planResp.Errors))
	case hasChanges:
		conclusion = checkConclusionActionRequired
		title = fmt.Sprintf("%d schema change(s) detected", len(tables))
		summary = buildCheckRunSummary(planResp)
	default:
		conclusion = checkConclusionSuccess
		title = "No schema changes"
		summary = "Schema is up to date — no changes detected."
	}

	// Build detailed text with DDL statements
	text := buildCheckRunText(planResp)

	opts := ghclient.CheckRunOptions{
		Name:       checkName,
		Status:     checkStatusCompleted,
		Conclusion: conclusion,
		Output: &ghclient.CheckRunOutput{
			Title:   title,
			Summary: summary,
			Text:    text,
		},
	}

	checkRunID, err := client.CreateCheckRun(ctx, repo, prInfo.HeadSHA, opts)
	if err != nil {
		return "", fmt.Errorf("create check run: %w", err)
	}

	// Store check record in CheckStore
	check := &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      prInfo.HeadSHA,
		Environment:  environment,
		DatabaseType: schema.Type,
		DatabaseName: schema.Database,
		CheckRunID:   checkRunID,
		HasChanges:   hasChanges,
		Status:       checkStatusCompleted,
		Conclusion:   conclusion,
	}
	if err := h.service.Storage().Checks().Upsert(ctx, check); err != nil {
		return prInfo.HeadSHA, fmt.Errorf("store check record: %w", err)
	}

	return prInfo.HeadSHA, nil
}

// buildCheckRunSummary builds a brief summary for the check run.
func buildCheckRunSummary(planResp *apitypes.PlanResponse) string {
	tables := planResp.FlatTables()
	var sb strings.Builder
	fmt.Fprintf(&sb, "**%d DDL statement(s)**\n\n", len(tables))

	for _, table := range tables {
		fmt.Fprintf(&sb, "- `%s` (%s)\n", table.TableName, table.ChangeType)
	}

	if lintViolations := planResp.LintNonErrors(); len(lintViolations) > 0 {
		fmt.Fprintf(&sb, "\n**%d lint warning(s)**\n", len(lintViolations))
	}

	if lintErrors := planResp.LintErrors(); len(lintErrors) > 0 {
		fmt.Fprintf(&sb, "\n**%d unsafe change(s) require --allow-unsafe**\n", len(lintErrors))
	}

	return sb.String()
}

// buildCheckRunText builds detailed DDL output for the check run.
func buildCheckRunText(planResp *apitypes.PlanResponse) string {
	tables := planResp.FlatTables()
	if len(tables) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("## DDL Statements\n\n```sql\n")

	for i, table := range tables {
		if table.IsUnsafe {
			fmt.Fprintf(&sb, "-- WARNING: unsafe change (%s)\n", table.UnsafeReason)
		}
		fmt.Fprintf(&sb, "-- %s: %s\n", table.ChangeType, table.TableName)
		sb.WriteString(table.DDL)
		if i < len(tables)-1 {
			sb.WriteString("\n\n")
		} else {
			sb.WriteString("\n")
		}
	}
	sb.WriteString("```\n")

	// Truncate if over GitHub's limit
	text := sb.String()
	if len(text) > maxCheckRunTextLength {
		text = text[:maxCheckRunTextLength-100] + "\n...\n(truncated — view full plan in PR comment)"
	}

	return text
}

// updateCheckRunForApplyResult updates the GitHub check run after an apply reaches
// a terminal state. On success, the check transitions to "success" (unblocking merge).
// On failure/stop/revert, it transitions to "failure".
func (h *Handler) updateCheckRunForApplyResult(ctx context.Context, repo string, pr int, installationID int64, apply *storage.Apply) {
	// Look up the stored check record for this PR+env+database
	check, err := h.service.Storage().Checks().Get(ctx, repo, pr, apply.Environment, apply.DatabaseType, apply.Database)
	if err != nil {
		h.logger.Error("failed to look up check for apply result", "error", err)
		return
	}
	if check == nil || check.CheckRunID == 0 {
		h.logger.Warn("no check run found to update after apply",
			"repo", repo, "pr", pr, "database", apply.Database, "environment", apply.Environment)
		return
	}

	client, err := h.ghClient.ForInstallation(installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for check update", "error", err)
		return
	}

	var conclusion, title, summary string
	switch apply.State {
	case state.Apply.Completed:
		conclusion = checkConclusionSuccess
		title = "Schema changes applied"
		summary = fmt.Sprintf("All schema changes for `%s` (%s) have been applied successfully.", apply.Database, apply.Environment)
	case state.Apply.Failed:
		conclusion = checkConclusionFailure
		title = "Schema change failed"
		summary = "The apply failed."
		if apply.ErrorMessage != "" {
			summary = fmt.Sprintf("The apply failed: %s", apply.ErrorMessage)
		}
	default:
		// stopped, reverted, or other terminal states
		conclusion = checkConclusionFailure
		title = fmt.Sprintf("Schema change %s", apply.State)
		summary = fmt.Sprintf("The apply was %s.", apply.State)
	}

	opts := ghclient.CheckRunOptions{
		Name:       checkRunName(check.Environment, check.DatabaseType, check.DatabaseName),
		Status:     checkStatusCompleted,
		Conclusion: conclusion,
		Output: &ghclient.CheckRunOutput{
			Title:   title,
			Summary: summary,
		},
	}

	if err := client.UpdateCheckRun(ctx, repo, check.CheckRunID, opts); err != nil {
		h.logger.Error("failed to update check run after apply",
			"checkRunID", check.CheckRunID, "conclusion", conclusion, "error", err)
		return
	}

	// Update stored check record
	check.Status = checkStatusCompleted
	check.Conclusion = conclusion
	check.HasChanges = conclusion != checkConclusionSuccess
	if err := h.service.Storage().Checks().Upsert(ctx, check); err != nil {
		h.logger.Error("failed to update check record after apply", "error", err)
	}

	h.logger.Info("check run updated after apply",
		"repo", repo, "pr", pr, "database", apply.Database,
		"environment", apply.Environment, "conclusion", conclusion)

	// Update aggregate check to reflect the terminal state
	h.updateAggregateCheck(ctx, client, repo, pr, check.HeadSHA)
}

// checkPriorEnvironments enforces environment ordering: all environments before
// the current one in the configured list must have their check run at "success".
// Returns true if the apply is blocked (caller should return).
//
// For environments: [sandbox, staging, production]
//   - applying to sandbox: no prior envs, always allowed
//   - applying to staging: sandbox must be success
//   - applying to production: both sandbox and staging must be success
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

	// Check all prior environments
	for i := 0; i < currentIdx; i++ {
		priorEnv := environments[i]
		check, err := h.service.Storage().Checks().Get(ctx, repo, pr, priorEnv, dbType, database)
		if err != nil {
			h.logger.Error("failed to look up prior environment check",
				"environment", priorEnv, "error", err)
			// Graceful degradation: allow proceed if check lookup fails
			continue
		}

		if check == nil {
			// No check exists for this prior environment — it may not have changes.
			// This is OK (e.g., staging has no changes but production does).
			continue
		}

		switch {
		case check.Conclusion == checkConclusionSuccess:
			continue
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

	return false
}

// updateAggregateCheck recomputes and creates/updates the single "SchemaBot" aggregate
// check run that rolls up all per-database checks for a PR. This is the only check
// that needs to be required in branch protection — it works regardless of how many
// databases a PR touches.
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

	// Filter out the aggregate check itself — only per-database checks contribute
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

	conclusion, status := computeAggregate(dbChecks)
	title, summary := aggregateSummary(dbChecks, conclusion)

	opts := ghclient.CheckRunOptions{
		Name:   aggregateCheckName,
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

	// Look up existing aggregate check record
	existing, err := h.service.Storage().Checks().Get(ctx, repo, pr, aggregateSentinel, aggregateSentinel, aggregateSentinel)
	if err != nil {
		h.logger.Error("failed to look up aggregate check", "repo", repo, "pr", pr, "error", err)
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
		Environment:  aggregateSentinel,
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
		"repo", repo, "pr", pr, "status", status, "conclusion", conclusion,
		"per_database_checks", len(dbChecks))
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
		title = "All schema changes applied"
		summary = buildAggregateTable(checks)
	case checkConclusionFailure:
		title = "Schema change failed"
		summary = buildAggregateTable(checks)
	case checkConclusionActionRequired:
		pending := 0
		for _, c := range checks {
			if c.Conclusion == checkConclusionActionRequired {
				pending++
			}
		}
		title = fmt.Sprintf("%d schema change(s) pending", pending)
		summary = buildAggregateTable(checks)
	default:
		// in_progress — conclusion is empty
		title = "Schema changes in progress"
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
