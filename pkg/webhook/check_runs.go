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

// checkRunName returns the canonical check run name for a given environment, database type, and database.
func checkRunName(environment, dbType, database string) string {
	return fmt.Sprintf("SchemaBot: %s/%s/%s", environment, dbType, database)
}

// createPlanCheckRun creates a GitHub Check Run after a plan is generated.
func (h *Handler) createPlanCheckRun(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schema *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment string, installationID int64) {
	// Get PR head SHA for the check run
	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to fetch PR for check run", "error", err)
		return
	}

	tables := planResp.FlatTables()
	hasChanges := len(tables) > 0
	checkName := checkRunName(environment, schema.Type, schema.Database)

	var conclusion string
	var title string
	var summary string

	switch {
	case len(planResp.Errors) > 0:
		conclusion = "failure"
		title = "Plan failed"
		summary = fmt.Sprintf("Plan failed with %d error(s)", len(planResp.Errors))
	case hasChanges:
		conclusion = "action_required"
		title = fmt.Sprintf("%d schema change(s) detected", len(tables))
		summary = buildCheckRunSummary(planResp)
	default:
		conclusion = "success"
		title = "No schema changes"
		summary = "Schema is up to date — no changes detected."
	}

	// Build detailed text with DDL statements
	text := buildCheckRunText(planResp)

	opts := ghclient.CheckRunOptions{
		Name:       checkName,
		Status:     "completed",
		Conclusion: conclusion,
		Output: &ghclient.CheckRunOutput{
			Title:   title,
			Summary: summary,
			Text:    text,
		},
	}

	checkRunID, err := client.CreateCheckRun(ctx, repo, prInfo.HeadSHA, opts)
	if err != nil {
		h.logger.Error("failed to create check run", "error", err)
		return
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
		Status:       "completed",
		Conclusion:   conclusion,
	}
	if err := h.service.Storage().Checks().Upsert(ctx, check); err != nil {
		h.logger.Error("failed to store check record", "error", err)
	}
}

// buildCheckRunSummary builds a brief summary for the check run.
func buildCheckRunSummary(planResp *apitypes.PlanResponse) string {
	tables := planResp.FlatTables()
	var sb strings.Builder
	fmt.Fprintf(&sb, "**%d DDL statement(s)**\n\n", len(tables))

	for _, table := range tables {
		fmt.Fprintf(&sb, "- `%s` (%s)\n", table.TableName, table.ChangeType)
	}

	if lintViolations := planResp.LintViolations(); len(lintViolations) > 0 {
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
		conclusion = "success"
		title = "Schema changes applied"
		summary = fmt.Sprintf("All schema changes for `%s` (%s) have been applied successfully.", apply.Database, apply.Environment)
	case state.Apply.Failed:
		conclusion = "failure"
		title = "Schema change failed"
		summary = "The apply failed."
		if apply.ErrorMessage != "" {
			summary = fmt.Sprintf("The apply failed: %s", apply.ErrorMessage)
		}
	default:
		// stopped, reverted, or other terminal states
		conclusion = "failure"
		title = fmt.Sprintf("Schema change %s", apply.State)
		summary = fmt.Sprintf("The apply was %s.", apply.State)
	}

	opts := ghclient.CheckRunOptions{
		Name:       checkRunName(check.Environment, check.DatabaseType, check.DatabaseName),
		Status:     "completed",
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
	check.Status = "completed"
	check.Conclusion = conclusion
	check.HasChanges = conclusion != "success"
	if err := h.service.Storage().Checks().Upsert(ctx, check); err != nil {
		h.logger.Error("failed to update check record after apply", "error", err)
	}

	h.logger.Info("check run updated after apply",
		"repo", repo, "pr", pr, "database", apply.Database,
		"environment", apply.Environment, "conclusion", conclusion)
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
		case check.Conclusion == "success":
			continue
		case check.Status == "in_progress":
			h.postComment(repo, pr, installationID,
				templates.RenderApplyBlockedByPriorEnvInProgress(database, environment, priorEnv))
			return true
		default:
			status := "has pending changes"
			action := fmt.Sprintf("Apply %s first", priorEnv)
			if check.Conclusion == "failure" {
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
