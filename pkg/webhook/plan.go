package webhook

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// handlePlanCommand handles the "schemabot plan -e <env>" command.
func (h *Handler) handlePlanCommand(w http.ResponseWriter, repo string, pr int, environment, databaseName string, installationID int64, requestedBy string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := h.ghClient.ForInstallation(installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client", "error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to initialize GitHub client")
		return
	}

	// Discover config and fetch schema files from PR
	schemaResult, err := client.CreateSchemaRequestFromPR(ctx, repo, pr, environment, databaseName)
	if err != nil {
		h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, "plan", err)
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
		Target:      schemaResult.Target,
	}

	// Execute plan via the service
	planResp, err := h.service.ExecutePlan(ctx, planReq)
	if err != nil {
		h.logger.Error("plan execution failed", "repo", repo, "pr", pr, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Timestamp:   time.Now().UTC().Format("2006-01-02 15:04:05"),
			Environment: environment,
			CommandName: "plan",
			ErrorDetail: err.Error(),
		}))
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "plan failed"})
		return
	}

	// Build plan comment data
	commentData := buildPlanCommentData(schemaResult, planResp, environment, requestedBy)

	// Post plan comment
	h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))

	// Create check run
	h.createPlanCheckRun(ctx, client, repo, pr, schemaResult, planResp, environment, installationID)

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

	client, err := h.ghClient.ForInstallation(installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client", "error", err)
		return
	}

	// Find config to get environments
	var environments []string
	if databaseName != "" {
		config, _, findErr := client.FindConfigByDatabaseName(ctx, repo, pr, databaseName)
		if findErr != nil {
			h.handleSchemaRequestError(repo, pr, installationID, "", databaseName, requestedBy, "plan", findErr)
			return
		}
		environments = config.GetEnvironments()
	} else {
		config, _, findErr := client.FindConfigForPR(ctx, repo, pr)
		if findErr != nil {
			h.handleSchemaRequestError(repo, pr, installationID, "", databaseName, requestedBy, "plan", findErr)
			return
		}
		environments = config.GetEnvironments()
	}

	// Collect plans for all environments
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
			Target:      schemaResult.Target,
		}

		planResp, err := h.service.ExecutePlan(ctx, planReq)
		if err != nil {
			h.logger.Error("plan execution failed", "repo", repo, "pr", pr, "env", env, "error", err)
			multiEnvData.Errors[env] = userFacingError(err)
			continue
		}

		commentData := buildPlanCommentData(schemaResult, planResp, env, requestedBy)
		multiEnvData.Plans[env] = &commentData

		// Create check run per environment
		h.createPlanCheckRun(ctx, client, repo, pr, schemaResult, planResp, env, installationID)
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

	// Extract unsafe changes using shared logic
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

	// Add lint warnings (error-severity results are shown via UnsafeChanges instead)
	for _, w := range planResp.LintWarnings() {
		data.LintWarnings = append(data.LintWarnings, templates.LintWarningData{
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
