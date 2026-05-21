package webhook

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// handlePlanCommand handles the "schemabot plan -e <env>" command.
func (h *Handler) handlePlanCommand(w http.ResponseWriter, repo string, pr int, environment, databaseName string, installationID int64, requestedBy string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// Dedupe FetchPullRequest calls within this command invocation.
	ctx = ghclient.WithPRInfoCache(ctx)

	client, err := h.ghClient.ForInstallation(installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client", "error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to initialize GitHub client")
		return
	}

	// Fix checks stuck at "in_progress" from crashed applies
	if err := h.reconcileStaleChecks(ctx, client, repo, pr); err != nil {
		h.logger.Error("failed to reconcile stale status checks", "repo", repo, "pr", pr, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Timestamp:   time.Now().UTC().Format("2006-01-02 15:04:05"),
			Environment: environment,
			CommandName: action.Plan,
			ErrorDetail: "Failed to reconcile stale status checks: " + err.Error(),
		}))
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "status check reconciliation failed"})
		return
	}

	// Discover config and fetch schema files from PR
	schemaResult, err := client.CreateSchemaRequestFromPR(ctx, repo, pr, environment, databaseName)
	if err != nil {
		h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.Plan, err)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "schema request error handled"})
		return
	}

	// Build PlanRequest in the format expected by the API service
	prNumber := int32(pr)
	planReq := api.PlanRequest{
		Database:    schemaResult.Database,
		Environment: environment,
		Type:        schemaResult.Type,
		SchemaFiles: schemaResult.SchemaFiles,
		Repository:  repo,
		PullRequest: &prNumber,
	}

	// Execute plan via the service
	planResp, err := h.service.ExecutePlan(ctx, planReq)
	if err != nil {
		h.logger.Error("plan execution failed", "repo", repo, "pr", pr, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Timestamp:   time.Now().UTC().Format("2006-01-02 15:04:05"),
			Environment: environment,
			CommandName: action.Plan,
			ErrorDetail: err.Error(),
		}))
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "plan failed"})
		return
	}

	// Build plan comment data
	commentData := buildPlanCommentData(schemaResult, planResp, environment, requestedBy)

	// Post plan comment
	h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))

	// Store per-database check record and update aggregate
	headSHA, checkErr := h.storePlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment)
	if checkErr != nil {
		h.logger.Error("failed to store plan check record", "repo", repo, "pr", pr, "error", checkErr)
	}
	if headSHA != "" {
		h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
	}

	h.writeJSON(w, http.StatusOK, map[string]string{
		"message": "plan generated successfully",
		"plan_id": planResp.PlanID,
	})
}

// handleMultiEnvPlan runs plan for all configured environments and posts a single combined comment.
// When isAutoPlan is true and no environments have changes or errors, the comment is skipped to reduce PR noise.
func (h *Handler) handleMultiEnvPlan(repo string, pr int, databaseName string, installationID int64, requestedBy string, isAutoPlan bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// Dedupe FetchPullRequest calls within this plan invocation.
	ctx = ghclient.WithPRInfoCache(ctx)

	client, err := h.ghClient.ForInstallation(installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client", "error", err)
		return
	}

	// Fix checks stuck at "in_progress" from crashed applies
	if err := h.reconcileStaleChecks(ctx, client, repo, pr); err != nil {
		h.logger.Error("failed to reconcile stale status checks", "repo", repo, "pr", pr, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Timestamp:   time.Now().UTC().Format("2006-01-02 15:04:05"),
			CommandName: action.Plan,
			ErrorDetail: "Failed to reconcile stale status checks: " + err.Error(),
		}))
		return
	}

	// Find config to get environments
	var environments []string
	if databaseName != "" {
		config, _, findErr := client.FindConfigByDatabaseName(ctx, repo, pr, databaseName)
		if findErr != nil {
			h.handleSchemaRequestError(repo, pr, installationID, "", databaseName, requestedBy, action.Plan, findErr)
			return
		}
		environments = config.GetEnvironments()
	} else {
		config, _, findErr := client.FindConfigForPR(ctx, repo, pr)
		if findErr != nil {
			h.handleSchemaRequestError(repo, pr, installationID, "", databaseName, requestedBy, action.Plan, findErr)
			return
		}
		environments = config.GetEnvironments()
	}
	configuredEnvironments := append([]string(nil), environments...)
	var allowedEnvironments []string

	// Filter environments to those this service is allowed to handle.
	if h.service != nil {
		config := h.service.Config()
		environments = config.OrderedEnvironments(environments)
		allowedEnvironments = append([]string(nil), config.AllowedEnvironments...)
		if len(config.AllowedEnvironments) > 0 {
			var allowed []string
			for _, env := range environments {
				if config.IsEnvironmentAllowed(env) {
					allowed = append(allowed, env)
				} else {
					h.logger.Debug("skipping environment not allowed for this service",
						"repo", repo, "pr", pr, "env", env)
				}
			}
			environments = allowed
		}
	}

	if len(environments) == 0 {
		prInfo, err := client.FetchPullRequest(ctx, repo, pr)
		if err != nil {
			h.logger.Error("failed to fetch PR for no allowed configured environments failure",
				"repo", repo, "pr", pr, "error", err)
			return
		}

		block := noAllowedConfiguredEnvironmentsBlock
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "schema_config_environment_validation",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Warn("schema changes found but no configured environments are allowed",
			"repo", repo, "pr", pr, "head_sha", prInfo.HeadSHA,
			"blocking_reason", block.blockingReason,
			"configured_environments", configuredEnvironments,
			"allowed_environments", allowedEnvironments)
		h.postFailingAggregatesWithBlock(ctx, client, repo, pr, prInfo.HeadSHA,
			h.aggregateMessagesForAllEnvironments(block.message), block)
		return
	}

	// Collect plans for all environments
	var headSHA string
	multiEnvData := templates.MultiEnvPlanCommentData{
		RequestedBy:  requestedBy,
		Environments: environments,
		Plans:        make(map[string]*templates.PlanCommentData),
		Errors:       make(map[string]string),
	}

	for _, env := range environments {
		schemaResult, err := client.CreateSchemaRequestFromPR(ctx, repo, pr, env, databaseName)
		if err != nil {
			h.logger.Error("schema request failed", "repo", repo, "pr", pr, "env", env, "error", err)
			multiEnvData.Errors[env] = userFacingError(err)
			continue
		}

		// Set database/type from first successful result
		if multiEnvData.Database == "" {
			multiEnvData.Database = schemaResult.Database
			multiEnvData.HeadSHA = schemaResult.HeadSHA
			multiEnvData.Repository = schemaResult.Repository
			multiEnvData.IsMySQL = schemaResult.Type == "mysql"
		}

		prNumber := int32(pr)
		planReq := api.PlanRequest{
			Database:    schemaResult.Database,
			Environment: env,
			Type:        schemaResult.Type,
			SchemaFiles: schemaResult.SchemaFiles,
			Repository:  repo,
			PullRequest: &prNumber,
		}

		planResp, err := h.service.ExecutePlan(ctx, planReq)
		if err != nil {
			h.logger.Error("plan execution failed", "repo", repo, "pr", pr, "env", env, "error", err)
			multiEnvData.Errors[env] = userFacingError(err)
			continue
		}

		commentData := buildPlanCommentData(schemaResult, planResp, env, requestedBy)
		multiEnvData.Plans[env] = &commentData

		// Store per-database check record per environment
		sha, checkErr := h.storePlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, env)
		if checkErr != nil {
			h.logger.Error("failed to store plan check record", "repo", repo, "pr", pr, "env", env, "error", checkErr)
		}
		if sha != "" {
			headSHA = sha
		}
	}

	// Update aggregate check once after all environments are planned.
	// If all environments errored, no check records were stored and headSHA
	// is empty. Post a failing aggregate so branch protection isn't stuck
	// waiting for a check that will never arrive.
	if headSHA != "" {
		h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
	} else if len(multiEnvData.Errors) > 0 {
		prInfo, fetchErr := client.FetchPullRequest(ctx, repo, pr)
		if fetchErr != nil {
			h.logger.Error("failed to fetch PR for error aggregate", "repo", repo, "pr", pr, "error", fetchErr)
		} else {
			h.postFailingAggregates(ctx, client, repo, pr, prInfo.HeadSHA, multiEnvData.Errors)
		}
	}

	// Auto-plan: skip comment if no changes and no errors (reduce PR noise)
	// Check runs are still created above so PR status shows green
	if isAutoPlan {
		hasErrors := len(multiEnvData.Errors) > 0
		anyChanges := false
		for _, plan := range multiEnvData.Plans {
			if plan != nil && len(plan.Changes) > 0 {
				anyChanges = true
				break
			}
		}
		if !anyChanges && !hasErrors {
			h.logger.Info("auto-plan: no changes detected, skipping comment", "repo", repo, "pr", pr)
			return
		}
	}

	// Post a single combined comment
	h.postComment(repo, pr, installationID, templates.RenderMultiEnvPlanComment(multiEnvData))
}

// handleSchemaRequestError maps schema request errors to GitHub comments.
func (h *Handler) handleSchemaRequestError(repo string, pr int, installationID int64, environment, databaseName, requestedBy, commandName string, err error) {
	data := templates.SchemaErrorData{
		RequestedBy:  requestedBy,
		Timestamp:    time.Now().UTC().Format("2006-01-02 15:04:05"),
		Environment:  environment,
		DatabaseName: databaseName,
		CommandName:  commandName,
	}

	var dbNotFoundErr *ghclient.DatabaseNotFoundError
	if errors.As(err, &dbNotFoundErr) {
		h.postComment(repo, pr, installationID, templates.RenderDatabaseNotFound(data))
		return
	}

	if errors.Is(err, ghclient.ErrInvalidConfig) {
		h.postComment(repo, pr, installationID, templates.RenderInvalidConfig(data))
		return
	}

	if errors.Is(err, ghclient.ErrNoConfig) {
		h.postComment(repo, pr, installationID, templates.RenderNoConfig(data))
		return
	}

	if errors.Is(err, ghclient.ErrMultipleConfigs) {
		data.AvailableDatabases = templates.FormatAvailableDatabases(err.Error())
		h.postComment(repo, pr, installationID, templates.RenderMultipleConfigs(data))
		return
	}

	data.ErrorDetail = err.Error()
	h.postComment(repo, pr, installationID, templates.RenderGenericError(data))
}

// buildPlanCommentData converts plan results into template data.
func buildPlanCommentData(schema *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment, requestedBy string) templates.PlanCommentData {
	data := templates.PlanCommentData{
		Database:    schema.Database,
		Environment: environment,
		HeadSHA:     schema.HeadSHA,
		Repository:  schema.Repository,
		RequestedBy: requestedBy,
		IsMySQL:     schema.Type == "mysql",
	}

	// Build keyspace changes from namespace-grouped plan response
	for _, sc := range planResp.Changes {
		ksData := templates.KeyspaceChangeData{
			Keyspace: sc.Namespace,
		}
		for _, t := range sc.TableChanges {
			ksData.Statements = append(ksData.Statements, t.DDL)
		}
		// Extract VSchema changes from metadata
		if diff, ok := sc.Metadata["vschema"]; ok {
			ksData.VSchemaChanged = true
			ksData.VSchemaDiff = diff
		}
		data.Changes = append(data.Changes, ksData)
	}

	unsafeChanges := planResp.UnsafeChanges()
	if len(unsafeChanges) > 0 {
		data.HasUnsafeChanges = true
		for _, uc := range unsafeChanges {
			data.UnsafeChanges = append(data.UnsafeChanges, templates.UnsafeChangeData{
				Table:  uc.Table,
				Reason: uc.Reason,
			})
		}
	}

	// Add lint violations (error-severity results are shown via UnsafeChanges instead)
	for _, w := range planResp.LintNonErrors() {
		data.LintViolations = append(data.LintViolations, templates.LintViolationData{
			Message: w.Message,
			Table:   w.Table,
		})
	}

	// Add errors
	data.Errors = planResp.Errors

	return data
}

// userFacingError returns the error message as-is. Detailed errors are logged
// server-side; the PR comment shows the full chain so users can report issues.
func userFacingError(err error) string {
	return err.Error()
}
