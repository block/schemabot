package webhook

import (
	"context"
	"errors"
	"fmt"
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
func (h *Handler) handlePlanCommand(w http.ResponseWriter, repo string, pr int, environment, databaseName, tenant string, installationID int64, requestedBy string, commentID int64) {
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

	ackedEarly := h.acknowledgeCommandEarlyIfOwned(ctx, client, repo, pr, databaseName, tenant, installationID, commentID)

	// Discover config and fetch schema files from PR
	schemaResult, err := h.createManagedSchemaRequestFromPR(ctx, client, repo, pr, environment, databaseName, action.Plan)
	if err != nil {
		if h.skipUnownedUnscopedCommand(repo, tenant, err) {
			h.logger.Debug("unscoped fan-out plan touches no schema this deployment owns; staying silent",
				"repo", repo, "pr", pr, "environment", environment, "error", err)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "unowned unscoped command skipped"})
			return
		}
		h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.Plan, err)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "schema request error handled"})
		return
	}
	if err := h.attachServerEnvironments(schemaResult, environment); err != nil {
		h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.Plan, err)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "schema request error handled"})
		return
	}
	if !ackedEarly {
		h.acknowledgeCommandActPoint(repo, pr, installationID, CommandResult{Tenant: tenant, CommentID: commentID})
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
	planProto, planResp, err := h.executePlanProtoWithTransientRetry(ctx, planReq, repo, pr)
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

	// Roll up every deployment's diff against the reviewed plan so drift on a
	// non-primary deployment fails the check closed at review time.
	drift := h.reviewTimeDrift(ctx, planReq, planProto, planResp.Deployment, repo, pr)

	// Build plan comment data
	commentData := buildPlanCommentData(schemaResult, planResp, environment, tenant, requestedBy)

	metrics.RecordPlan(ctx, repo, schemaResult.Database, deployment, environment, "success")

	// Store per-database check record and update aggregate
	headSHA, recoveredApplyOwnedCheckState, checkErr := h.storeManualPlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment, drift)
	if checkErr != nil {
		h.logger.Error("failed to store plan check record", "repo", repo, "pr", pr, "database", schemaResult.Database, "deployment", deployment, "environment", environment, "error", checkErr)
	}
	commentData.RecoveredApplyOwnedCheckState = recoveredApplyOwnedCheckState

	// Post plan comment
	h.postComment(repo, pr, installationID, templates.RenderPlanComment(commentData))

	// When drift blocked the check but its record could not be persisted, the
	// aggregate must not be recomputed from stale (possibly passing) stored rows.
	// Post a failing aggregate carrying the drift block from the in-memory result
	// so the gate still blocks closed. This is best-effort visibility: with the
	// per-database row unstored, a later recompute has no durable drift row to
	// read, so the store error is logged above and not treated as a safe update.
	if drift.blocks() && checkErr != nil {
		h.postFailingAggregatesWithBlock(ctx, client, repo, pr, schemaResult.HeadSHA, map[string]string{environment: drift.summary}, reviewTimeDeploymentDriftBlock)
	} else if headSHA != "" {
		h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
	}

	h.writeJSON(w, http.StatusOK, map[string]string{
		"message": "plan generated successfully",
		"plan_id": planResp.PlanID,
	})
}

// handleMultiEnvPlan runs plan for all configured environments and posts a single combined comment.
// When isAutoPlan is true and no environments have changes or errors, the comment is skipped to reduce PR noise.
// commentID is the command comment to acknowledge once discovery commits this
// deployment to acting; auto-plans pass zero (no comment to acknowledge).
func (h *Handler) handleMultiEnvPlan(repo string, pr int, databaseName, tenant string, installationID int64, requestedBy string, isAutoPlan bool, postPlanComment bool, commentID int64) {
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

	// A user-issued plan on a PR with no managed schema changes converges the
	// checks (or explains apply-owned state) instead of searching the whole
	// repo for a config. Auto-plan skips this: it is only dispatched for
	// configs already discovered from the PR's changed files.
	if !isAutoPlan {
		if handled, err := h.handleNoManagedSchemaChangesForCommand(ctx, client, repo, pr, installationID, action.Plan, "", databaseName, requestedBy); err != nil {
			h.logger.Error("failed to check whether plan command needs schema change reconciliation", "repo", repo, "pr", pr, "database", databaseName, "error", err)
			h.postCommandError(repo, pr, installationID, action.Plan, "", requestedBy, err.Error())
			return
		} else if handled {
			return
		}
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
	// Ownership is only fully decided once the discovered database resolves in
	// this deployment's registry: config discovery alone can pass on a repo with
	// no schema-dir allowlist even when the database belongs to another
	// deployment, and acknowledging there would promise work a fan-out silent
	// skip never does. The registry lookups above are in-memory, so the
	// acknowledgment is still immediate.
	h.acknowledgeCommandActPoint(repo, pr, installationID, CommandResult{Tenant: tenant, CommentID: commentID})
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
	// Environments whose drift-blocking check record could not be persisted, kept
	// so a failing aggregate can be posted from the in-memory drift result rather
	// than letting the post-loop aggregate recompute from stale stored rows.
	driftBlockUnstored := map[string]string{}
	multiEnvData := templates.MultiEnvPlanCommentData{
		RequestedBy:  requestedBy,
		Tenant:       tenant,
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

		planProto, planResp, err := h.executePlanProtoWithTransientRetry(ctx, planReq, repo, pr)
		if err != nil {
			h.logger.Error("plan execution failed", "repo", repo, "pr", pr, "env", env, "error", err)
			multiEnvData.Errors[env] = userFacingError(err)
			continue
		}

		// Roll up every deployment's diff against the reviewed plan so drift on a
		// non-primary deployment fails the check closed at review time.
		drift := h.reviewTimeDrift(ctx, planReq, planProto, planResp.Deployment, repo, pr)

		// Store per-database check record per environment
		var recoveredApplyOwnedCheckState bool
		var sha string
		var checkErr error
		if isAutoPlan {
			sha, checkErr = h.storePlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, env, drift)
		} else {
			sha, recoveredApplyOwnedCheckState, checkErr = h.storeManualPlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, env, drift)
		}
		if checkErr != nil {
			h.logger.Error("failed to store plan check record", "repo", repo, "pr", pr, "env", env, "error", checkErr)
			if drift.blocks() {
				driftBlockUnstored[env] = drift.summary
			}
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

	// For any environment whose drift-blocking check record could not be
	// persisted, post a failing aggregate from the in-memory drift result after
	// the stored-row aggregate sync so a stale passing row cannot leave the gate
	// open. Posted last so it wins over updateAggregateCheck for these envs.
	if len(driftBlockUnstored) > 0 && multiEnvData.HeadSHA != "" {
		h.postFailingAggregatesWithBlock(ctx, client, repo, pr, multiEnvData.HeadSHA, driftBlockUnstored, reviewTimeDeploymentDriftBlock)
	} else if len(driftBlockUnstored) > 0 {
		h.logger.Warn("deployment drift blocked one or more environments but no head SHA is known; the fallback failing aggregate was not posted, so an operator must re-run plan to re-establish the merge-gate block",
			"repo", repo,
			"pr", pr,
			"environments", len(driftBlockUnstored))
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

	if !postPlanComment {
		h.logger.Info("auto-plan refreshed checks without posting plan comment", "repo", repo, "pr", pr, "database", multiEnvData.Database)
		return
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

// shardedUnsafeChanges collects unsafe per-shard changes, grouped by (table,
// reason) so a change present on several shards lists them together rather than
// repeating. Returns nil when the plan carries no per-shard changes (the
// non-sharded path uses the namespace-level unsafe view instead).
func shardedUnsafeChanges(shards []*apitypes.ShardPlanResponse) []templates.UnsafeChangeData {
	if len(shards) == 0 {
		return nil
	}
	type key struct{ table, reason string }
	var order []key
	byKey := make(map[key]*templates.UnsafeChangeData)
	for _, sp := range shards {
		if sp == nil {
			continue
		}
		for _, t := range sp.Changes {
			unsafeChange, ok := t.UnsafeChange()
			if !ok {
				continue
			}
			k := key{table: unsafeChange.Table, reason: unsafeChange.Reason}
			uc := byKey[k]
			if uc == nil {
				uc = &templates.UnsafeChangeData{Table: unsafeChange.Table, Reason: unsafeChange.Reason}
				byKey[k] = uc
				order = append(order, k)
			}
			uc.Shards = append(uc.Shards, sp.Shard)
		}
	}
	out := make([]templates.UnsafeChangeData, 0, len(order))
	for _, k := range order {
		out = append(out, *byKey[k])
	}
	return out
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

	// Per-shard changes, grouped by keyspace, so a sharded keyspace can show what
	// applies to which shard rather than the collapsed namespace-level view.
	shardsByKeyspace := make(map[string][]templates.KeyspaceShardChange)
	var malformedShardErrors []string
	for _, sp := range planResp.Shards {
		if sp == nil {
			continue
		}
		shard := templates.KeyspaceShardChange{Shard: sp.Shard}
		for _, t := range sp.Changes {
			if t == nil || t.DDL == "" {
				continue
			}
			shard.Statements = append(shard.Statements, t.DDL)
		}
		if len(shard.Statements) == 0 {
			if len(sp.Changes) > 0 {
				// The shard reported changes but none produced usable DDL — the
				// plan is incomplete for this shard. Surface it as an error rather
				// than dropping the shard, which would silently hide the divergent
				// state this view exists to show.
				malformedShardErrors = append(malformedShardErrors, fmt.Sprintf(
					"shard %q in keyspace %q reported %d change(s) with no DDL — plan is incomplete for this shard",
					sp.Shard, sp.Namespace, len(sp.Changes)))
				continue
			}
			// A shard with zero changes already matches the desired schema while
			// sibling shards change; carry it as a satisfied (no-change) group so a
			// partially-applied keyspace renders its divergent "already applied vs
			// will change" state instead of hiding the shard.
			shard.Satisfied = true
		}
		shardsByKeyspace[sp.Namespace] = append(shardsByKeyspace[sp.Namespace], shard)
	}

	// Build keyspace changes from namespace-grouped plan response
	for _, sc := range planResp.Changes {
		ksData := templates.KeyspaceChangeData{
			Keyspace: sc.Namespace,
			Shards:   shardsByKeyspace[sc.Namespace],
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

	// Unsafe changes. For a sharded plan, derive them from the per-shard changes
	// so an unsafe change confined to one shard (e.g. a column drop on a single
	// drifted shard) is still flagged with the shard it applies to — the
	// collapsed namespace-level Changes can omit it. Otherwise use the
	// namespace-level view.
	if unsafe := shardedUnsafeChanges(planResp.Shards); len(unsafe) > 0 {
		data.HasUnsafeChanges = true
		data.UnsafeChanges = unsafe
	} else if unsafeChanges := planResp.UnsafeChanges(); len(unsafeChanges) > 0 {
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

	// Add errors. Malformed-shard errors (changes reported with no usable DDL)
	// are surfaced alongside the plan's own errors so the operator sees the plan
	// is incomplete rather than reading a silently-shortened shard list.
	data.Errors = append(data.Errors, planResp.Errors...)
	data.Errors = append(data.Errors, malformedShardErrors...)

	return data
}
