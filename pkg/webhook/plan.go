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
func (h *Handler) handlePlanCommand(w http.ResponseWriter, repo string, pr int, environment, databaseName, tenant string, installationID int64, requestedBy string) {
	ctx, cancel, client, err := h.commandBootstrap(repo, installationID)
	if err != nil {
		h.logger.Error("plan: failed to bootstrap command", "error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to initialize GitHub client")
		return
	}
	defer cancel()

	// Fix checks stuck at "in_progress" from crashed applies
	if err := h.reconcileStaleChecks(ctx, client, repo, pr); err != nil {
		h.logger.Error("failed to reconcile stale status checks", "repo", repo, "pr", pr, "error", err)
		h.postCommandError(repo, pr, installationID, action.Plan, environment, requestedBy, "Failed to reconcile stale status checks: "+err.Error())
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "status check reconciliation failed"})
		return
	}

	if handled, err := h.handleNoManagedSchemaChangesForCommand(ctx, client, repo, pr, installationID, action.Plan, environment, databaseName, requestedBy); err != nil {
		h.logger.Error("failed to check whether plan command needs schema change reconciliation", "repo", repo, "pr", pr, "environment", environment, "database", databaseName, "error", err)
		h.postCommandError(repo, pr, installationID, action.Plan, environment, requestedBy, err.Error())
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "schema reconciliation check failed"})
		return
	} else if handled {
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "no managed schema changes handled"})
		return
	}

	// Discover config and fetch schema files from PR
	schemaResult, err := h.createManagedSchemaRequestFromPR(ctx, client, repo, pr, environment, databaseName, action.Plan)
	if err != nil {
		h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.Plan, err)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "schema request error handled"})
		return
	}
	if err := h.attachServerEnvironments(schemaResult, environment); err != nil {
		h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.Plan, err)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "schema request error handled"})
		return
	}

	// Reject if the PR HEAD advanced after discovery loaded schema files.
	// Rendering a plan comment against stale files would mislead the user
	// (and feed directly into apply-confirm against the wrong artifact).
	// Use FetchPullRequestNoCache: the cached FetchPullRequest used by
	// discovery would return the discovery-time HeadSHA, masking the race.
	freshPRInfo, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to fetch PR for stale-schema check", "repo", repo, "pr", pr, "error", err)
		h.postCommandError(repo, pr, installationID, action.Plan, environment, requestedBy, "Failed to verify PR HEAD: "+err.Error())
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "PR fetch failed"})
		return
	}
	if rejected := h.assertSchemaStillCurrent(ctx, repo, pr, installationID, schemaResult, freshPRInfo.HeadSHA, environment, requestedBy, action.Plan); rejected {
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "plan rejected: schema discovery stale"})
		return
	}

	// Build PlanRequest in the format expected by the API service
	prNumber := int32(pr)
	deployment := ""
	if resolvedTarget, err := h.service.Config().ResolvePrimaryDatabaseTarget(schemaResult.Database, environment); err != nil {
		h.logger.Warn("plan metric deployment is unknown because target resolution failed",
			"repo", repo,
			"pr", pr,
			"database", schemaResult.Database,
			"environment", environment,
			"error", err)
	} else {
		deployment = resolvedTarget.Deployment
	}
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

	// Execute plan via the service
	planResp, err := h.service.ExecutePlan(ctx, planReq)
	if err != nil {
		h.logger.Error("plan execution failed", "repo", repo, "pr", pr, "database", schemaResult.Database, "deployment", deployment, "environment", environment, "error", err)
		metrics.RecordPlan(ctx, repo, schemaResult.Database, deployment, environment, "error")
		userError := userFacingError(err)
		h.postFailingAggregates(ctx, client, repo, pr, schemaResult.HeadSHA, map[string]string{
			environment: userError,
		})
		h.postCommandError(repo, pr, installationID, action.Plan, environment, requestedBy, userError)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "plan failed"})
		return
	}

	// Build plan comment data
	commentData := buildPlanCommentData(schemaResult, planResp, environment, tenant, requestedBy)

	metrics.RecordPlan(ctx, repo, schemaResult.Database, deployment, environment, "success")

	// Store per-database check record and update aggregate
	headSHA, recoveredApplyOwnedCheckState, checkErr := h.storeManualPlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment)
	if checkErr != nil {
		h.logger.Error("failed to store plan check record", "repo", repo, "pr", pr, "database", schemaResult.Database, "deployment", deployment, "environment", environment, "error", checkErr)
	}
	commentData.RecoveredApplyOwnedCheckState = recoveredApplyOwnedCheckState

	// Post plan comment
	h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))

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
func (h *Handler) handleMultiEnvPlan(repo string, pr int, databaseName, tenant string, installationID int64, requestedBy string, isAutoPlan bool) {
	ctx, cancel, client, err := h.commandBootstrap(repo, installationID)
	if err != nil {
		h.logger.Error("multi-env plan: failed to bootstrap command", "error", err)
		return
	}
	defer cancel()

	// Fix checks stuck at "in_progress" from crashed applies
	if err := h.reconcileStaleChecks(ctx, client, repo, pr); err != nil {
		h.logger.Error("failed to reconcile stale status checks", "repo", repo, "pr", pr, "error", err)
		h.postCommandError(repo, pr, installationID, action.Plan, "", requestedBy, "Failed to reconcile stale status checks: "+err.Error())
		return
	}

	// Find config to get the database identity. Environments are server-owned.
	var schemaDatabase string
	if databaseName != "" {
		config, configDir, findErr := client.FindConfigByDatabaseName(ctx, repo, pr, databaseName)
		if findErr != nil {
			h.handleSchemaRequestError(repo, pr, installationID, "", databaseName, requestedBy, action.Plan, findErr)
			return
		}
		if !h.configPathManagedByRepo(ctx, repo, pr, "", config, configDir, action.Plan) {
			h.handleSchemaRequestError(repo, pr, installationID, "", databaseName, requestedBy, action.Plan, newSchemaConfigOutsideAllowedDirsError(config, configDir))
			return
		}
		schemaDatabase = config.Database
	} else {
		config, configDir, findErr := client.FindConfigForPR(ctx, repo, pr)
		if findErr != nil {
			h.handleSchemaRequestError(repo, pr, installationID, "", databaseName, requestedBy, action.Plan, findErr)
			return
		}
		if !h.configPathManagedByRepo(ctx, repo, pr, "", config, configDir, action.Plan) {
			h.handleSchemaRequestError(repo, pr, installationID, "", databaseName, requestedBy, action.Plan, newSchemaConfigOutsideAllowedDirsError(config, configDir))
			return
		}
		schemaDatabase = config.Database
	}
	configuredEnvironments, envErr := h.configuredDatabaseEnvironments(schemaDatabase)
	if envErr != nil {
		if isAutoPlan {
			h.postFailingAggregateForMultiEnvSetupError(ctx, client, repo, pr, schemaDatabase, envErr)
		}
		h.handleSchemaRequestError(repo, pr, installationID, "", databaseName, requestedBy, action.Plan, envErr)
		return
	}
	environments, envErr := h.allowedDatabaseEnvironments(schemaDatabase)
	if envErr != nil {
		if isAutoPlan {
			h.postFailingAggregateForMultiEnvSetupError(ctx, client, repo, pr, schemaDatabase, envErr)
		}
		h.handleSchemaRequestError(repo, pr, installationID, "", databaseName, requestedBy, action.Plan, envErr)
		return
	}
	configuredEnvironments = append([]string(nil), configuredEnvironments...)
	allowedEnvironments := append([]string(nil), h.service.Config().AllowedEnvironments...)

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
		schemaResult, err := h.createManagedSchemaRequestFromPR(ctx, client, repo, pr, env, databaseName, action.Plan)
		if err != nil {
			h.logger.Error("schema request failed", "repo", repo, "pr", pr, "env", env, "error", err)
			multiEnvData.Errors[env] = userFacingError(err)
			continue
		}
		if err := h.attachServerEnvironments(schemaResult, env); err != nil {
			h.logger.Error("schema environment validation failed", "repo", repo, "pr", pr, "env", env, "error", err)
			multiEnvData.Errors[env] = userFacingError(err)
			continue
		}

		// Set database/type from first successful result
		if multiEnvData.Database == "" {
			multiEnvData.Database = schemaResult.Database
			multiEnvData.HeadSHA = schemaResult.HeadSHA
			multiEnvData.Repository = schemaResult.Repository
			multiEnvData.DatabaseType = schemaResult.Type
			multiEnvData.IsMySQL = schemaResult.Type == "mysql"

			// Stale-schema gate for user-triggered `schemabot plan` only.
			// Auto-plan from pull_request webhooks is covered by the next
			// synchronize delivery superseding any stale comment; gating it
			// here is out of scope. All per-env schemaResults share the same
			// HeadSHA (cached FetchPullRequest within this delivery), so one
			// check on the first successful result covers every env.
			if !isAutoPlan {
				freshPRInfo, prErr := client.FetchPullRequestNoCache(ctx, repo, pr)
				if prErr != nil {
					h.logger.Error("failed to fetch PR for stale-schema check",
						"repo", repo, "pr", pr, "error", prErr)
					h.postCommandError(repo, pr, installationID, action.Plan, "", requestedBy, "Failed to verify PR HEAD: "+prErr.Error())
					return
				}
				if rejected := h.assertSchemaStillCurrent(ctx, repo, pr, installationID, schemaResult, freshPRInfo.HeadSHA, env, requestedBy, action.Plan); rejected {
					return
				}
			}
		}

		prNumber := int32(pr)
		planReq := api.PlanRequest{
			Database:      schemaResult.Database,
			Environment:   env,
			Type:          schemaResult.Type,
			SchemaFiles:   schemaResult.SchemaFiles,
			Repository:    repo,
			PullRequest:   &prNumber,
			HeadSHA:       &schemaResult.HeadSHA,
			SchemaPath:    schemaResult.SchemaPath,
			SourceTrusted: true,
		}

		planResp, err := h.service.ExecutePlan(ctx, planReq)
		if err != nil {
			h.logger.Error("plan execution failed", "repo", repo, "pr", pr, "env", env, "error", err)
			multiEnvData.Errors[env] = userFacingError(err)
			continue
		}

		// Store per-database check record per environment
		var recoveredApplyOwnedCheckState bool
		var sha string
		var checkErr error
		if isAutoPlan {
			sha, checkErr = h.storePlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, env)
		} else {
			sha, recoveredApplyOwnedCheckState, checkErr = h.storeManualPlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, env)
		}
		if checkErr != nil {
			h.logger.Error("failed to store plan check record", "repo", repo, "pr", pr, "env", env, "error", checkErr)
		}
		if sha != "" {
			headSHA = sha
		}

		commentData := buildPlanCommentData(schemaResult, planResp, env, tenant, requestedBy)
		commentData.RecoveredApplyOwnedCheckState = recoveredApplyOwnedCheckState
		multiEnvData.Plans[env] = &commentData
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

func (h *Handler) postFailingAggregateForMultiEnvSetupError(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, database string, err error) {
	prInfo, fetchErr := client.FetchPullRequest(ctx, repo, pr)
	if fetchErr != nil {
		h.logger.Error("failed to fetch PR for multi-env setup failure aggregate", "repo", repo, "pr", pr, "database", database, "error", fetchErr)
		return
	}
	userError := userFacingError(err)
	h.logger.Warn("multi-env plan setup failed; posting failing aggregate",
		"repo", repo, "pr", pr, "head_sha", prInfo.HeadSHA, "database", database, "error", err)
	h.postFailingAggregates(ctx, client, repo, pr, prInfo.HeadSHA,
		h.aggregateMessagesForAllEnvironments(userError))
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

	logFields := []any{
		"repo", repo, "pr", pr, "environment", environment,
		"database", databaseName, "action", commandName, "error", err,
	}

	ctx := context.Background()

	var dbNotFoundErr *ghclient.DatabaseNotFoundError
	if errors.As(err, &dbNotFoundErr) {
		h.logger.Warn("schema request: database not found", logFields...)
		metrics.RecordSchemaRequestError(ctx, repo, commandName, databaseName, environment, "database_not_found")
		h.postComment(repo, pr, installationID, templates.RenderDatabaseNotFound(data))
		return
	}

	var configNotAuthorizedErr *schemaConfigOutsideAllowedDirsError
	if errors.As(err, &configNotAuthorizedErr) {
		data.DatabaseName = configNotAuthorizedErr.Database
		data.SchemaPath = configNotAuthorizedErr.SchemaPath
		h.logger.Warn("schema request: config outside allowed_dirs",
			"repo", repo, "pr", pr, "environment", environment,
			"database", data.DatabaseName, "database_type", configNotAuthorizedErr.DatabaseType,
			"schema_path", data.SchemaPath, "action", commandName, "error", err)
		metrics.RecordSchemaRequestError(ctx, repo, commandName, data.DatabaseName, environment, "config_not_authorized")
		h.postComment(repo, pr, installationID, templates.RenderConfigNotAuthorized(data))
		return
	}

	if errors.Is(err, ghclient.ErrInvalidConfig) {
		h.logger.Warn("schema request: invalid config", logFields...)
		metrics.RecordSchemaRequestError(ctx, repo, commandName, databaseName, environment, "invalid_config")
		h.postComment(repo, pr, installationID, templates.RenderInvalidConfig(data))
		return
	}

	if errors.Is(err, ghclient.ErrNoConfig) {
		h.logger.Warn("schema request: no config found", logFields...)
		metrics.RecordSchemaRequestError(ctx, repo, commandName, databaseName, environment, "no_config")
		h.postComment(repo, pr, installationID, templates.RenderNoConfig(data))
		return
	}

	if errors.Is(err, ghclient.ErrMultipleConfigs) {
		h.logger.Warn("schema request: multiple configs found", logFields...)
		metrics.RecordSchemaRequestError(ctx, repo, commandName, databaseName, environment, "multiple_configs")
		data.AvailableDatabases = templates.FormatAvailableDatabases(err.Error())
		h.postComment(repo, pr, installationID, templates.RenderMultipleConfigs(data))
		return
	}

	h.logger.Error("schema request failed", logFields...)
	metrics.RecordSchemaRequestError(ctx, repo, commandName, databaseName, environment, "unexpected")
	data.ErrorDetail = err.Error()
	h.postComment(repo, pr, installationID, templates.RenderGenericError(data))
}

// buildPlanCommentData converts plan results into template data.
func buildPlanCommentData(schema *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment, tenant, requestedBy string) templates.PlanCommentData {
	data := templates.PlanCommentData{
		Database:     schema.Database,
		Environment:  environment,
		Tenant:       tenant,
		HeadSHA:      schema.HeadSHA,
		Repository:   schema.Repository,
		RequestedBy:  requestedBy,
		DatabaseType: schema.Type,
		IsMySQL:      schema.Type == "mysql",
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
