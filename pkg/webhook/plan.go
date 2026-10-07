package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/ui"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// handlePlanCommand handles the "schemabot plan -e <env>" command.
func (h *Handler) handlePlanCommand(w http.ResponseWriter, repo string, pr int, environment, databaseName, tenant string, installationID int64, deliveryID, requestedBy string, commentID int64) {
	ctx, cancel, client, err := h.commandBootstrap(context.Background(), repo, installationID)
	if err != nil {
		h.logger.Error("plan: failed to bootstrap command", "error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to initialize GitHub client")
		return
	}
	defer cancel()

	// Fix checks stuck at "in_progress" from crashed applies
	if err := h.reconcileStaleChecks(ctx, client, repo, pr); err != nil {
		h.logger.Error("failed to reconcile stale status checks", "repo", repo, "pr", pr, "error", err)
		h.postCommandError(repo, pr, installationID, action.Plan, environment, requestedBy, "Failed to reconcile stale status checks. Retry, and see server logs if it persists.")
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "status check reconciliation failed"})
		return
	}

	if h.answerUnregisteredDatabase(repo, pr, installationID, environment, databaseName, tenant, requestedBy, action.Plan, false) {
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "database not configured handled"})
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

	ackedEarly := h.acknowledgeCommandEarlyIfOwned(ctx, client, repo, pr, databaseName, tenant, installationID, deliveryID, commentID)

	// Discover config and fetch schema files from PR
	schemaResult, err := h.createManagedSchemaRequestFromPR(ctx, client, repo, pr, environment, databaseName, action.Plan)
	if err != nil {
		if h.silentDiscoveryFailureOnUnscopedFanOut(repo, environment, tenant, err) {
			h.logger.Debug("unscoped fan-out plan resolves to no schema this deployment answers for; staying silent",
				"repo", repo, "pr", pr, "environment", environment, "database", databaseName, "error", err)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "unowned unscoped command skipped"})
			return
		}
		// Answering the failure is acting on the command: the deployment that
		// posts the answer is the one that acknowledges.
		if !ackedEarly {
			h.acknowledgeCommandActPoint(repo, pr, installationID, CommandResult{Tenant: tenant, CommentID: commentID})
		}
		h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.Plan, err, false)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "schema request error handled"})
		return
	}
	if err := h.attachServerEnvironments(schemaResult, environment); err != nil {
		h.handleSchemaRequestError(repo, pr, installationID, environment, databaseName, requestedBy, action.Plan, err, false)
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

	// A user-issued plan reads the resolved database's live schema, so it
	// requires that database's configured principals.
	if h.planForResolvedDatabaseBlocked(ctx, repo, pr, installationID, requestedBy, schemaResult.Database, environment) {
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "plan blocked by actor authorization"})
		return
	}

	// Build PlanRequest in the format expected by the API service
	prNumber := int32(pr)
	deployment := ""
	if resolvedTarget, err := h.service.Config().ResolvePrimaryDatabaseTarget(schemaResult.Database, environment); err != nil {
		h.logger.Warn("plan logs carry no deployment because target resolution failed",
			"repo", repo,
			"pr", pr,
			"database", schemaResult.Database,
			"environment", environment,
			"error", err)
	} else {
		deployment = resolvedTarget.Deployment
	}
	planReq := api.PlanRequest{
		Database:          schemaResult.Database,
		Environment:       environment,
		Type:              schemaResult.Type,
		SchemaFiles:       schemaResult.SchemaFiles,
		Repository:        repo,
		PullRequest:       &prNumber,
		HeadSHA:           &schemaResult.HeadSHA,
		SchemaPath:        schemaResult.SchemaPath,
		IgnoredNamespaces: schemaResult.IgnoredNamespaces,
		IgnoreTables:      schemaResult.IgnoreTables,
		SourceTrusted:     true,
	}

	// Execute plan via the service
	planProto, planResp, err := h.executePlanProtoWithTransientRetry(ctx, planReq, repo, pr)
	if planRefusedByNamespacePlacement(err) {
		h.logger.Warn("plan refused by namespace placement; storing a failing check for the environment",
			"repo", repo, "pr", pr, "database", schemaResult.Database, "deployment", deployment, "environment", environment, "head_sha", schemaResult.HeadSHA, "error", err)
		h.failClosedOnNamespacePlacement(ctx, client, repo, pr, schemaResult, environment)
		h.postCommandError(repo, pr, installationID, action.Plan, environment, requestedBy, userFacingError(err))
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "plan refused by namespace placement"})
		return
	}
	if err != nil {
		h.logger.Error("plan execution failed", "repo", repo, "pr", pr, "database", schemaResult.Database, "deployment", deployment, "environment", environment, "error", err)
		userError := userFacingError(err)
		h.postFailingAggregates(ctx, client, repo, pr, schemaResult.HeadSHA, map[string]string{
			environment: userError,
		})
		h.postCommandError(repo, pr, installationID, action.Plan, environment, requestedBy, userError)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "plan failed"})
		return
	}

	// Roll up every deployment's diff against the primary plan so drift on a
	// non-primary deployment fails the check closed at review time.
	drift, driftPreview := h.reviewTimeDrift(ctx, planReq, planProto, plannedPrimaryMember(planResp), repo, pr)

	// Build plan comment data
	commentData := buildPlanCommentData(schemaResult, planResp, environment, tenant, requestedBy, h.agentHint(), h.cliName())
	commentData.ScopedDatabase = databaseName
	commentData.DeploymentDrift = driftPreview
	h.annotateMemberApplyRefusal(ctx, &commentData, planResp, environment, drift, repo, pr)
	h.annotateAttributedChanges(ctx, client, &commentData, planResp, driftPreview, repo, pr, environment)

	// Store per-database check record and update aggregate
	headSHA, recoveredApplyOwnedCheckState, checkErr := h.storeManualPlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment, drift)
	if checkErr != nil {
		h.logger.Error("failed to store plan check record", "repo", repo, "pr", pr, "database", schemaResult.Database, "deployment", deployment, "environment", environment, "error", checkErr)
	}
	commentData.RecoveredApplyOwnedCheckState = recoveredApplyOwnedCheckState

	// Post plan comment
	h.postTrackedPlanComment(repo, pr, installationID, planCommentSlot{
		Database:     schemaResult.Database,
		DatabaseType: schemaResult.Type,
		Environments: []string{environment},
		HeadSHA:      schemaResult.HeadSHA,
		UpToDate: planCommentUpToDate(templates.MultiEnvPlanCommentData{
			Environments: []string{environment},
			Plans:        map[string]*templates.PlanCommentData{environment: &commentData},
		}, drift.work.pending > 0),
	}, templates.RenderPlanComment(commentData))

	// When drift blocked the check, or any rollout member has work (the primary
	// included), but the record could not be persisted, the aggregate must not be recomputed from
	// stale (possibly passing) stored rows. Post a failing aggregate from the
	// in-memory result so the gate still blocks closed. This is best-effort
	// visibility: with the per-database row unstored, a later recompute has no
	// durable row to read, so the store error is logged above and not treated as
	// a safe update.
	if checkErr != nil && rolloutStillPending(drift) {
		h.failClosedOnUnstoredRollout(ctx, client, repo, pr, schemaResult.HeadSHA, environment, drift)
	} else if headSHA != "" {
		h.settleChecksReplacedByNewTypeBeforeFold(ctx, client, repo, pr, headSHA, schemaResult.Database, schemaResult.Type)
		h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
	}

	h.writeJSON(w, http.StatusOK, map[string]string{
		"message": "plan generated successfully",
		"plan_id": planResp.PlanID,
	})
}

// planRefusedByNamespacePlacement reports whether an environment's plan was
// refused for a reason its namespace placement owns: the targets entries and
// the schema files disagree on where a namespace lives, or the plan proposed
// dropping tables in a namespace the target's entry does not select (or those
// drops could not be checked). Each reproduces on every plan until the server
// config, the schema files or the planning deployment changes, not when the PR
// head moves, and leaves the environment with no plan to fold. So its stored
// check must fail closed rather than keep an older passing row a later fold
// would read (MG-12).
func planRefusedByNamespacePlacement(err error) bool {
	if api.NamespacePlacementRefused(err) {
		return true
	}
	return api.UnselectedTableDropRefused(err)
}

// failClosedOnNamespacePlacement fails one environment's check closed after
// namespace placement refused its plan. The refusal is stored as the
// environment's check row, so a later fold from stored check state, from any
// plan of any environment, reads it rather than an older passing row (MG-12).
// A row that cannot be stored is posted as a failing aggregate carrying the
// placement block instead, so no later fold reads an older passing row in the
// refusal's place; that block lifts when the check is re-run or a new commit
// re-plans every environment (namespacePlacementUnstoredSummary).
func (h *Handler) failClosedOnNamespacePlacement(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schemaResult *ghclient.SchemaRequestResult, environment string) {
	headSHA, err := h.storeNamespacePlacementCheck(ctx, client, repo, pr, schemaResult, environment)
	if err != nil {
		h.logger.Error("failed to store namespace placement check record; posting a failing aggregate carrying the placement block instead, which holds the gate closed until the check is re-run or a commit is pushed",
			"repo", repo, "pr", pr, "environment", environment, "database", schemaResult.Database, "head_sha", schemaResult.HeadSHA, "error", err)
		h.postFailingAggregatesWithBlock(ctx, client, repo, pr, schemaResult.HeadSHA,
			map[string]string{environment: namespacePlacementUnstoredSummary}, namespacePlacementRefusedBlock)
		return
	}
	h.settleChecksReplacedByNewTypeBeforeFold(ctx, client, repo, pr, headSHA, schemaResult.Database, schemaResult.Type)
	h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
}

// planForResolvedDatabaseBlocked enforces actor authorization once a
// user-issued plan has resolved which database it targets. A plan reads the
// database's live schema, so it requires the same configured principals as
// the mutating commands — the database's operator teams/users and the
// instance admins. A plan that resolves no managed database (the stuck-check
// rescue on a PR with no schema changes) never reaches this gate, and databases
// not configured on this deployment defer to the downstream ownership
// handling so fan-out deployments stay silent. When blocked, the
// authorization path has already posted the rejection comment.
func (h *Handler) planForResolvedDatabaseBlocked(ctx context.Context, repo string, pr int, installationID int64, requestedBy, databaseName, environment string) bool {
	if databaseName == "" {
		h.logger.Debug("plan resolved no database; no authorization gate", "repo", repo, "pr", pr)
		return false
	}
	dbConfig := h.service.Config().Database(databaseName)
	if dbConfig == nil {
		h.logger.Debug("plan database is not configured on this deployment; deferring to downstream ownership handling",
			"repo", repo, "pr", pr, "database", databaseName)
		return false
	}
	authzClient, err := h.actorAuthorizationClient(repo, pr, installationID, requestedBy, databaseName, environment, action.Plan)
	if err != nil {
		// The plan handler is not a durable core, so there is no driver to
		// classify the cause; the gate has already logged the failure and
		// posted the authorization-unavailable comment, and the plan is
		// blocked (fail closed).
		return true
	}
	if authzClient == nil {
		h.logger.Debug("PR command authorization disabled; plan proceeds ungated",
			"repo", repo, "pr", pr, "database", databaseName, "requested_by", requestedBy)
		return false
	}
	// The plan handler is not a durable core, so an authorization evaluation
	// failure blocks the plan the same as a merit denial (fail closed); the
	// gate has already logged and posted the distinction.
	blocked, authErr := h.enforcePRCommandActorAuthorization(ctx, authzClient, repo, pr, installationID, requestedBy, databaseName, dbConfig.Type, environment, action.Plan, false)
	return authErr != nil || blocked
}

// handleMultiEnvPlan runs plan for all configured environments and posts a single combined comment.
// When isAutoPlan is true and there is genuinely nothing to show, the comment is skipped to reduce
// PR noise — which is narrower than "no environment has changes": a rollout still converging plans
// no changes for the target that was reviewed and is not a no-op for the fleet.
// The skip only holds while no plan comment from a prior head is visible: once
// the PR shows a plan answer, the no-changes comment posts and supersedes it.
// commentID is the command comment to acknowledge once discovery commits this
// deployment to acting; auto-plans pass zero (no comment to acknowledge).
// commandScopeDatabases is how many databases a bare command offered by this
// comment would reach, which decides whether its commands have to name theirs;
// only an auto-plan reads it. unmanagedSchema lists the schema configs the PR
// also changes that this environment-scoped deployment does not manage; the
// comment names them, and posts even when it has no plan to show.
func (h *Handler) handleMultiEnvPlan(repo string, pr int, databaseName, tenant string, installationID int64, requestedBy string, isAutoPlan bool, commandScopeDatabases int, postPlanComment bool, commentID int64, unmanagedSchema []templates.UnmanagedSchemaConfigNoticeData) {
	ctx, cancel, client, err := h.commandBootstrap(context.Background(), repo, installationID)
	if err != nil {
		h.logger.Error("multi-env plan: failed to bootstrap command", "error", err)
		return
	}
	defer cancel()

	// Fix checks stuck at "in_progress" from crashed applies
	if err := h.reconcileStaleChecks(ctx, client, repo, pr); err != nil {
		h.logger.Error("failed to reconcile stale status checks", "repo", repo, "pr", pr, "error", err)
		h.postCommandError(repo, pr, installationID, action.Plan, "", requestedBy, "Failed to reconcile stale status checks. Retry, and see server logs if it persists.")
		return
	}

	// The registry gate reads databaseName as the command's -d value. An
	// auto-plan passes the database discovered from the PR instead, and its
	// answer for an unconfigured database is the failing aggregate the
	// multi-environment setup below posts, which is what tells branch
	// protection the plan cannot succeed.
	if !isAutoPlan && h.answerUnregisteredDatabase(repo, pr, installationID, "", databaseName, tenant, requestedBy, action.Plan, false) {
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
			if h.silentDiscoveryFailureOnUnscopedFanOut(repo, "", tenant, findErr) {
				h.logger.Debug("unscoped fan-out plan targets a database not found by this deployment's discovery; staying silent",
					"repo", repo, "pr", pr, "database", databaseName, "error", findErr)
				return
			}
			h.acknowledgeCommandActPoint(repo, pr, installationID, CommandResult{Tenant: tenant, CommentID: commentID})
			h.handleSchemaRequestError(repo, pr, installationID, "", databaseName, requestedBy, action.Plan, findErr, false)
			return
		}
		if !h.configPathManagedByRepo(ctx, repo, pr, "", config, configDir, action.Plan) {
			unownedErr := h.unownedDiscoveredConfigError(repo, config, configDir)
			if h.silentDiscoveryFailureOnUnscopedFanOut(repo, "", tenant, unownedErr) {
				h.logger.Debug("unscoped fan-out plan touches no schema this deployment owns; staying silent",
					"repo", repo, "pr", pr, "database", databaseName, "error", unownedErr)
				return
			}
			h.acknowledgeCommandActPoint(repo, pr, installationID, CommandResult{Tenant: tenant, CommentID: commentID})
			h.handleSchemaRequestError(repo, pr, installationID, "", databaseName, requestedBy, action.Plan, unownedErr, false)
			return
		}
		schemaDatabase = config.Database
	} else {
		config, _, findErr := h.resolveUnscopedManagedConfig(ctx, client, repo, pr, "", action.Plan)
		if findErr != nil {
			if h.silentDiscoveryFailureOnUnscopedFanOut(repo, "", tenant, findErr) {
				h.logger.Debug("unscoped fan-out plan touches no schema this deployment owns; staying silent",
					"repo", repo, "pr", pr, "error", findErr)
				return
			}
			// Answering the failure is acting on the command: the deployment
			// that posts the answer is the one that acknowledges.
			h.acknowledgeCommandActPoint(repo, pr, installationID, CommandResult{Tenant: tenant, CommentID: commentID})
			h.handleSchemaRequestError(repo, pr, installationID, "", databaseName, requestedBy, action.Plan, findErr, false)
			return
		}
		schemaDatabase = config.Database
	}
	// A user-issued plan reads the resolved database's live schema, so it
	// requires that database's configured principals. Auto-plans are
	// system-triggered with no actor to authorize.
	if !isAutoPlan && h.planForResolvedDatabaseBlocked(ctx, repo, pr, installationID, requestedBy, schemaDatabase, "") {
		return
	}

	configuredEnvironments, envErr := h.configuredDatabaseEnvironments(schemaDatabase)
	if envErr != nil {
		if isAutoPlan {
			h.postFailingAggregateForMultiEnvSetupError(ctx, client, repo, pr, schemaDatabase, envErr)
		}
		h.handleSchemaRequestError(repo, pr, installationID, "", databaseName, requestedBy, action.Plan, envErr, false)
		return
	}
	environments, envErr := h.allowedDatabaseEnvironments(schemaDatabase)
	if envErr != nil {
		if isAutoPlan {
			h.postFailingAggregateForMultiEnvSetupError(ctx, client, repo, pr, schemaDatabase, envErr)
		}
		h.handleSchemaRequestError(repo, pr, installationID, "", databaseName, requestedBy, action.Plan, envErr, false)
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
	// Environments whose check record could not be persisted while some rollout
	// member, the primary included, has work (MG-12), kept for the same reason.
	pendingWorkUnstored := map[string]string{}
	// Environments whose namespace placement refusal could not be persisted,
	// kept for the same reason and posted under the placement block's reason.
	placementBlockUnstored := map[string]string{}
	// Whether any environment's rollout round found a member with work. The
	// primary's plan alone cannot say, so auto-plan reads this before skipping
	// its comment.
	rolloutHasWork := false
	multiEnvData := templates.MultiEnvPlanCommentData{
		RequestedBy:    requestedBy,
		Tenant:         tenant,
		ScopedDatabase: planCommentDatabaseFlag(databaseName, schemaDatabase, isAutoPlan, commandScopeDatabases),
		AgentHint:      h.agentHint(),
		Environments:   environments,
		Plans:          make(map[string]*templates.PlanCommentData),
		Errors:         make(map[string]string),
	}
	if len(unmanagedSchema) > 0 {
		multiEnvData.UnmanagedSchema = unmanagedSchema
		multiEnvData.UnmanagedEnvironments = h.service.Config().OrderedEnvironments(allowedEnvironments)
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
			Database:          schemaResult.Database,
			Environment:       env,
			Type:              schemaResult.Type,
			SchemaFiles:       schemaResult.SchemaFiles,
			Repository:        repo,
			PullRequest:       &prNumber,
			HeadSHA:           &schemaResult.HeadSHA,
			SchemaPath:        schemaResult.SchemaPath,
			IgnoredNamespaces: schemaResult.IgnoredNamespaces,
			IgnoreTables:      schemaResult.IgnoreTables,
			SourceTrusted:     true,
		}

		planProto, planResp, err := h.executePlanProtoWithTransientRetry(ctx, planReq, repo, pr)
		if planRefusedByNamespacePlacement(err) {
			h.logger.Warn("plan refused by namespace placement; storing a failing check for the environment",
				"repo", repo, "pr", pr, "env", env, "database", schemaResult.Database, "head_sha", schemaResult.HeadSHA, "error", err)
			multiEnvData.Errors[env] = userFacingError(err)
			sha, checkErr := h.storeNamespacePlacementCheck(ctx, client, repo, pr, schemaResult, env)
			if checkErr != nil {
				h.logger.Error("failed to store namespace placement check record; posting a failing aggregate carrying the placement block instead, which holds the gate closed until the check is re-run or a commit is pushed",
					"repo", repo, "pr", pr, "env", env, "database", schemaResult.Database, "head_sha", schemaResult.HeadSHA, "error", checkErr)
				placementBlockUnstored[env] = namespacePlacementUnstoredSummary
			}
			if sha != "" {
				headSHA = sha
			}
			continue
		}
		if err != nil {
			h.logger.Error("plan execution failed", "repo", repo, "pr", pr, "env", env, "error", err)
			multiEnvData.Errors[env] = userFacingError(err)
			continue
		}

		// Roll up every deployment's diff against the primary plan so drift on a
		// non-primary deployment fails the check closed at review time.
		drift, driftPreview := h.reviewTimeDrift(ctx, planReq, planProto, plannedPrimaryMember(planResp), repo, pr)

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
			} else if drift.work.pending > 0 {
				pendingWorkUnstored[env] = drift.work.unstoredSummary()
			}
		}
		if drift.work.pending > 0 {
			rolloutHasWork = true
		}
		if sha != "" {
			headSHA = sha
		}

		commentData := buildPlanCommentData(schemaResult, planResp, env, tenant, requestedBy, h.agentHint(), h.cliName())
		commentData.ScopedDatabase = planCommentDatabaseFlag(databaseName, schemaDatabase, isAutoPlan, commandScopeDatabases)
		h.annotateAttributedChanges(ctx, client, &commentData, planResp, driftPreview, repo, pr, env)
		commentData.RecoveredApplyOwnedCheckState = recoveredApplyOwnedCheckState
		commentData.DeploymentDrift = driftPreview
		h.annotateMemberApplyRefusal(ctx, &commentData, planResp, env, drift, repo, pr)
		multiEnvData.Plans[env] = &commentData
	}

	// When every environment fails before a schema request resolves, no
	// successful result seeded the comment's database identity. Fall back to
	// the config-resolved database and this deployment's registry type so the
	// failure comment names what failed to plan instead of rendering an empty
	// database and a defaulted type.
	if multiEnvData.Database == "" {
		multiEnvData.Database = schemaDatabase
		if db := h.service.Config().Database(schemaDatabase); db != nil {
			multiEnvData.DatabaseType = db.Type
			multiEnvData.IsMySQL = db.Type == storage.DatabaseTypeMySQL
		}
	}

	// Update aggregate check once after all environments are planned.
	// If all environments errored, no check records were stored and headSHA
	// is empty. Post a failing aggregate so branch protection isn't stuck
	// waiting for a check that will never arrive.
	if headSHA != "" {
		h.settleChecksReplacedByNewTypeBeforeFold(ctx, client, repo, pr, headSHA, multiEnvData.Database, multiEnvData.DatabaseType)
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
	if len(placementBlockUnstored) > 0 && multiEnvData.HeadSHA != "" {
		h.postFailingAggregatesWithBlock(ctx, client, repo, pr, multiEnvData.HeadSHA, placementBlockUnstored, namespacePlacementRefusedBlock)
	} else if len(placementBlockUnstored) > 0 {
		h.logger.Warn("namespace placement refused one or more environments' plans but no head SHA is known; the fallback failing aggregate was not posted, so an operator must re-run plan to re-establish the merge-gate block",
			"repo", repo,
			"pr", pr,
			"environments", len(placementBlockUnstored))
	}
	if len(pendingWorkUnstored) > 0 && multiEnvData.HeadSHA != "" {
		h.postFailingAggregates(ctx, client, repo, pr, multiEnvData.HeadSHA, pendingWorkUnstored)
	} else if len(pendingWorkUnstored) > 0 {
		h.logger.Warn("a rollout target still needs the change in one or more environments but no head SHA is known; the fallback failing aggregate was not posted, so an operator must re-run plan to re-establish the merge-gate block",
			"repo", repo,
			"pr", pr,
			"environments", len(pendingWorkUnstored))
	}

	slot := planCommentSlot{
		Database:     multiEnvData.Database,
		DatabaseType: multiEnvData.DatabaseType,
		Environments: multiEnvData.Environments,
		HeadSHA:      multiEnvData.HeadSHA,
		UpToDate:     planCommentUpToDate(multiEnvData, rolloutHasWork),
	}

	// The caller asked not to re-post: either the push left the schema inputs
	// unchanged, or this is the re-plan after a terminal apply, where the
	// operator asked for an apply rather than a plan. The visible plan comment
	// stays the PR's answer unless its outcome no longer matches this plan.
	if !postPlanComment {
		if !h.priorHeadPlanCommentNeedsReplacing(ctx, client, repo, pr, slot, true) {
			h.logger.Info("auto-plan refreshed checks without posting plan comment because the visible plan comment still answers for this plan",
				"repo", repo, "pr", pr, "database", slot.Database,
				"database_type", slot.DatabaseType, "head_sha", slot.HeadSHA, "up_to_date", slot.UpToDate)
			return
		}
		h.logger.Info("auto-plan posting plan comment because the visible plan comment no longer matches the plan outcome",
			"repo", repo, "pr", pr, "database", slot.Database,
			"database_type", slot.DatabaseType, "head_sha", slot.HeadSHA, "up_to_date", slot.UpToDate)
	} else if isAutoPlan && slot.UpToDate && len(unmanagedSchema) == 0 {
		// Auto-plan skips the comment only when there is genuinely nothing to
		// show and the PR has never shown a plan. Schema this deployment does
		// not manage keeps the comment: on an environment-scoped deployment the
		// comment is the PR's only mention of it. Once the PR shows a plan
		// answer it keeps showing a current one: a prior head's comment is
		// superseded by posting this head's no-changes comment, never by
		// removing it and leaving nothing.
		if !h.priorHeadPlanCommentNeedsReplacing(ctx, client, repo, pr, slot, false) {
			h.logger.Info("auto-plan: no changes, errors, or drift detected and no plan comment from a prior head is visible; skipping comment",
				"repo", repo, "pr", pr, "database", slot.Database,
				"database_type", slot.DatabaseType, "head_sha", slot.HeadSHA)
			return
		}
		h.logger.Info("auto-plan: no changes, errors, or drift detected; posting the no-changes plan comment to supersede the plan comment from a prior head",
			"repo", repo, "pr", pr, "database", slot.Database,
			"database_type", slot.DatabaseType, "head_sha", slot.HeadSHA)
	}

	// Post a single combined comment
	h.postTrackedPlanComment(repo, pr, installationID, slot, templates.RenderMultiEnvPlanComment(multiEnvData))
}

// planCommentUpToDate reports whether the plan shows nothing to act on: no
// changes on any rollout member, no errors in any environment or plan, and no
// deployment drift. A member with work keeps the check pending, and a drifted
// or unverifiable deployment fails it closed, even when every primary plan is
// a clean no-op, so neither is up to date.
func planCommentUpToDate(data templates.MultiEnvPlanCommentData, rolloutHasWork bool) bool {
	if rolloutHasWork || len(data.Errors) > 0 || templates.AnyEnvHasDriftToShow(data) {
		return false
	}
	for _, plan := range data.Plans {
		if plan != nil && (len(plan.Changes) > 0 || len(plan.Errors) > 0) {
			return false
		}
	}
	return true
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

// handleSchemaRequestError maps schema request errors to GitHub comments. It
// reports whether the error is a recognized user-facing rejection — the
// command's answer, which the same input will always reproduce — as opposed to
// an unexpected failure (for example a transient GitHub read error) that a
// durable driver may re-drive. suppressRetryComments silences only the
// unexpected-failure fallback comment on durable attempts, where the driver
// retries and posts the single terminal answer instead; recognized rejections
// always comment because they are the command's answer.
func (h *Handler) handleSchemaRequestError(repo string, pr int, installationID int64, environment, databaseName, requestedBy, commandName string, err error, suppressRetryComments bool) bool {
	data := templates.SchemaErrorData{
		RequestedBy:  requestedBy,
		Timestamp:    time.Now().UTC().Format("2006-01-02 15:04:05"),
		Environment:  environment,
		Environments: h.deploymentEnvironmentScope(environment),
		DatabaseName: databaseName,
		CommandName:  commandName,
	}

	if config := h.config(); config != nil {
		data.ExperimentalStrataEnabled = config.ExperimentalStrataEnabled
	}

	logFields := []any{
		"repo", repo, "pr", pr, "environment", environment,
		"database", databaseName, "action", commandName, "error", err,
	}

	ctx := context.Background()

	var dbNotFoundErr *ghclient.DatabaseNotFoundError
	if errors.As(err, &dbNotFoundErr) {
		// A configured database that accepts no changes from this repository
		// has no directory the scoped search may probe, so the miss is the
		// server's policy, not a search result, and the comment says so
		// instead of blaming a schemabot.yaml that was never looked for.
		if dbNotFoundErr.RepositoryNotAccepted() {
			h.logger.Warn("schema request: database accepts no schema changes from this repository", logFields...)
			metrics.RecordSchemaRequestError(ctx, repo, commandName, databaseName, environment, "database_repo_not_allowed")
			h.postComment(repo, pr, installationID, templates.RenderDatabaseRepoNotAllowed(data))
			return true
		}
		data.SearchedDirs = dbNotFoundErr.SearchedDirs
		h.logger.Warn("schema request: database not found", append(logFields, "searched_dirs", dbNotFoundErr.SearchedDirs)...)
		metrics.RecordSchemaRequestError(ctx, repo, commandName, databaseName, environment, "database_not_found")
		h.postComment(repo, pr, installationID, templates.RenderDatabaseNotFound(data))
		return true
	}

	var configNotAuthorizedErr *schemaConfigOutsideAllowedDirsError
	if errors.As(err, &configNotAuthorizedErr) {
		data.DatabaseName = configNotAuthorizedErr.Database
		data.SchemaPath = configNotAuthorizedErr.SchemaPath
		// Another deployment serving a different environment may manage the
		// directory, so the comment names the deployment making the claim.
		data.Deployment, data.DeploymentEnvironments = h.deploymentIdentity()
		h.logger.Warn("schema request: config outside allowed_dirs",
			"repo", repo, "pr", pr, "environment", environment,
			"database", data.DatabaseName, "database_type", configNotAuthorizedErr.DatabaseType,
			"schema_path", data.SchemaPath, "action", commandName, "error", err)
		metrics.RecordSchemaRequestError(ctx, repo, commandName, data.DatabaseName, environment, "config_not_authorized")
		h.postComment(repo, pr, installationID, templates.RenderConfigNotAuthorized(data))
		return true
	}

	if errors.Is(err, ghclient.ErrInvalidConfig) {
		h.logger.Warn("schema request: invalid config", logFields...)
		metrics.RecordSchemaRequestError(ctx, repo, commandName, databaseName, environment, "invalid_config")
		h.postComment(repo, pr, installationID, templates.RenderInvalidConfig(data))
		return true
	}

	if errors.Is(err, ghclient.ErrNoConfig) {
		h.logger.Warn("schema request: no config found", logFields...)
		metrics.RecordSchemaRequestError(ctx, repo, commandName, databaseName, environment, "no_config")
		h.postComment(repo, pr, installationID, templates.RenderNoConfig(data))
		return true
	}

	if errors.Is(err, ghclient.ErrMultipleConfigs) {
		h.logger.Warn("schema request: multiple configs found", logFields...)
		metrics.RecordSchemaRequestError(ctx, repo, commandName, databaseName, environment, "multiple_configs")
		data.AvailableDatabases = templates.FormatAvailableDatabases(err.Error())
		h.postComment(repo, pr, installationID, templates.RenderMultipleConfigs(data))
		return true
	}

	var notRegisteredErr *databaseNotRegisteredError
	if errors.As(err, &notRegisteredErr) {
		for _, cfg := range notRegisteredErr.Configs {
			data.UnregisteredConfigs = append(data.UnregisteredConfigs, templates.UnregisteredSchemaConfigData{
				Database:   cfg.Database,
				SchemaPath: cfg.SchemaPath,
			})
		}
		// The claim is about this deployment's registry only, so the comment
		// names the deployment making it.
		data.Deployment, data.DeploymentEnvironments = h.deploymentIdentity()
		// A single config names its database on the metric; several have no
		// one database to name, the same as Multiple Databases Detected.
		metricDatabase := databaseName
		if len(notRegisteredErr.Configs) == 1 {
			metricDatabase = notRegisteredErr.Configs[0].Database
		}
		h.logger.Warn("schema request: database not registered on this deployment and schema directory under no expected participant's paths",
			"repo", repo, "pr", pr, "environment", environment,
			"deployment", data.Deployment,
			"databases", notRegisteredErr.Databases(), "schema_paths", notRegisteredErr.SchemaPaths(),
			"action", commandName, "error", err)
		metrics.RecordSchemaRequestError(ctx, repo, commandName, metricDatabase, environment, "database_not_registered")
		h.postComment(repo, pr, installationID, templates.RenderDatabaseNotRegistered(data))
		return true
	}

	var dbNotConfiguredErr *api.DatabaseNotConfiguredError
	if errors.As(err, &dbNotConfiguredErr) {
		// The registry miss may name a database discovered from the PR's own
		// config rather than the -d value (which an unscoped command leaves
		// empty), so the comment, the log, and the metric all carry the one
		// the error names.
		data.DatabaseName = dbNotConfiguredErr.Database
		h.logger.Warn("schema request: database not configured on this server",
			"repo", repo, "pr", pr, "environment", environment,
			"database", data.DatabaseName, "action", commandName, "error", err)
		metrics.RecordSchemaRequestError(ctx, repo, commandName, data.DatabaseName, environment, "database_not_configured")
		h.postComment(repo, pr, installationID, templates.RenderDatabaseNotConfigured(data))
		return true
	}

	// Config discovery on a truncated repository tree is incomplete discovery,
	// not the command's answer: the caller keeps it retryable, exactly like an
	// unexpected error, but the comment explains what the size of the
	// repository means for discovery instead of quoting the raw error. A
	// truncated listing below an already discovered config (schema-file
	// loading) is a different failure and keeps the generic comment.
	if errors.Is(err, ghclient.ErrConfigDiscoveryTruncated) {
		h.logger.Error("schema request: repository tree truncated; config discovery incomplete", logFields...)
		metrics.RecordSchemaRequestError(ctx, repo, commandName, databaseName, environment, "config_discovery_truncated")
		if !suppressRetryComments {
			h.postComment(repo, pr, installationID, templates.RenderRepositoryTreeTruncated(data))
		}
		return false
	}

	var envNotConfiguredErr *environmentNotConfiguredError
	if errors.As(err, &envNotConfiguredErr) {
		h.logger.Warn("schema request: environment not configured on this server", logFields...)
		metrics.RecordSchemaRequestError(ctx, repo, commandName, databaseName, environment, "environment_not_configured")
		data.ErrorDetail = err.Error()
		h.postComment(repo, pr, installationID, templates.RenderGenericError(data))
		return true
	}

	h.logger.Error("schema request failed", logFields...)
	metrics.RecordSchemaRequestError(ctx, repo, commandName, databaseName, environment, "unexpected")
	if !suppressRetryComments {
		data.ErrorDetail = err.Error()
		h.postComment(repo, pr, installationID, templates.RenderGenericError(data))
	}
	return false
}

// shardedUnsafeChanges collects unsafe per-shard changes, grouped by (table,
// reason) so a change present on several shards lists them together rather than
// repeating. The group's statement and change type are the first shard's: the
// reason already names what the change destroys, so shards whose statements
// differ only in drift still describe one drop. Returns nil when the plan
// carries no per-shard changes (the non-sharded path uses the namespace-level
// unsafe view instead).
func shardedUnsafeChanges(shards []*apitypes.ShardPlanResponse) []templates.UnsafeChangeData {
	if len(shards) == 0 {
		return nil
	}
	total := plannedShardCount(shards)
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
				uc = &templates.UnsafeChangeData{Table: unsafeChange.Table, Reason: unsafeChange.Reason, DDL: unsafeChange.DDL, ChangeType: unsafeChange.ChangeType, TotalShards: total}
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

// plannedShardCount counts the shards the plan actually covers, so a shard
// list rendered against it states coverage over what was planned rather than
// over slots that carried no plan.
func plannedShardCount(shards []*apitypes.ShardPlanResponse) int {
	total := 0
	for _, sp := range shards {
		if sp != nil {
			total++
		}
	}
	return total
}

// msgDeferCutoverAllDirect rejects --defer-cutover on a plan whose every
// change the policy routes to direct execution: a direct statement has no
// cutover to defer, so the flag is refused instead of silently ignored.
const msgDeferCutoverAllDirect = "`--defer-cutover` has no effect on this plan: every change runs directly as native DDL, which has no cutover to defer. Re-run without the flag."

// msgDeferCutoverAllDirectConfirm rejects --defer-cutover at confirm time on
// an all-direct plan. The rejection preserves the pending confirmation — the
// lock still pins the plan the operator confirmed against — so the recovery
// is re-running apply-confirm without the flag, not restarting from apply.
// The format verb takes the environment for the coached command.
const msgDeferCutoverAllDirectConfirm = "`--defer-cutover` has no effect on this plan: every change runs directly as native DDL, which has no cutover to defer. The pending confirmation is preserved — re-run `schemabot apply-confirm -e %s` without the flag."

// shardedDirectChanges collects direct-execution per-shard changes, grouped by
// (table, reason) so a change present on several shards lists them together
// rather than repeating. Returns nil when the plan carries no per-shard
// changes (the non-sharded path uses the namespace-level view instead).
func shardedDirectChanges(shards []*apitypes.ShardPlanResponse) []templates.DirectChangeData {
	if len(shards) == 0 {
		return nil
	}
	total := plannedShardCount(shards)
	type key struct{ table, reason string }
	var order []key
	byKey := make(map[key]*templates.DirectChangeData)
	for _, sp := range shards {
		if sp == nil {
			continue
		}
		for _, t := range sp.Changes {
			if !t.DirectExecution() {
				continue
			}
			k := key{table: t.TableName, reason: t.ModeReason}
			dc := byKey[k]
			if dc == nil {
				dc = &templates.DirectChangeData{Table: t.TableName, Reason: t.ModeReason, TotalShards: total}
				byKey[k] = dc
				order = append(order, k)
			}
			dc.Shards = append(dc.Shards, sp.Shard)
		}
	}
	out := make([]templates.DirectChangeData, 0, len(order))
	for _, k := range order {
		out = append(out, *byKey[k])
	}
	return out
}

// shardedBlockedChanges collects blocked per-shard changes, grouped by (table,
// reason) so a change present on several shards lists them together rather
// than repeating. Returns nil when the plan carries no per-shard changes (the
// non-sharded path uses the namespace-level blocked view instead).
func shardedBlockedChanges(shards []*apitypes.ShardPlanResponse) []templates.BlockedChangeData {
	if len(shards) == 0 {
		return nil
	}
	total := plannedShardCount(shards)
	type key struct{ table, reason string }
	var order []key
	byKey := make(map[key]*templates.BlockedChangeData)
	for _, sp := range shards {
		if sp == nil {
			continue
		}
		for _, t := range sp.Changes {
			if !t.EngineBlocked() {
				continue
			}
			k := key{table: t.TableName, reason: t.ModeReason}
			bc := byKey[k]
			if bc == nil {
				bc = &templates.BlockedChangeData{Table: t.TableName, Reason: t.ModeReason, TotalShards: total}
				byKey[k] = bc
				order = append(order, k)
			}
			bc.Shards = append(bc.Shards, sp.Shard)
		}
	}
	out := make([]templates.BlockedChangeData, 0, len(order))
	for _, k := range order {
		out = append(out, *byKey[k])
	}
	return out
}

// splitExistingCopies sorts the target's unfinished copies by what the apply
// will do to them. The sections are opposite promises to the operator — one
// says the work survives, the other says it is destroyed — so a disposition
// this build does not recognize is shown as a discard: warning about work that
// in fact survives costs a second look, while promising survival to work that
// is destroyed costs the copy.
//
// Surviving work splits again by whether it is still being made. Both are kept,
// but only one of them stopped, and a copy still running is the one an operator
// can watch progressing while they read the comment — telling them it will be
// picked up where it stopped invites them to go looking for a stall that is not
// there.
func splitExistingCopies(copies []*apitypes.ExistingCopyResponse) (discarded, adopted, running []templates.ExistingCopyData) {
	for _, c := range copies {
		if c == nil {
			continue
		}
		entry := templates.ExistingCopyData{
			Namespace: c.Namespace,
			Tables:    c.Tables,
			Reason:    c.Reason,
			Statement: c.Statement,
			Running:   c.Running,
		}
		if c.AgeSeconds > 0 {
			entry.Age = ui.FormatHumanDuration(time.Duration(c.AgeSeconds) * time.Second)
		}
		switch c.Disposition {
		case apitypes.ExistingCopyAdopt:
			if c.Running {
				running = append(running, entry)
				continue
			}
			adopted = append(adopted, entry)
		case apitypes.ExistingCopyDiscard:
			// A running copy still lands in the destructive section: the work is
			// destroyed whether or not it is live, and moving it out would hide a
			// discard behind a reassuring heading. The entry carries Running so it
			// reads "(still copying)" rather than dating live work as stale.
			discarded = append(discarded, entry)
		default:
			// Reaching here means a deployment reported a disposition this build
			// has no name for, so the comment warns about work that may in fact
			// survive. Without this line the ⚠️ section is indistinguishable from
			// a real discard and there is nothing to reconcile it against.
			slog.Warn("comment discloses an unfinished copy as discarded because the deployment reported a disposition this build does not recognize",
				"namespace", c.Namespace, "tables", c.Tables, "disposition", c.Disposition)
			discarded = append(discarded, entry)
		}
	}
	return discarded, adopted, running
}

// setNamespaceWork records on a keyspace's comment data the namespace-level
// work the engine planned beside its DDL: a VSchema change to show, with its
// rendered diff, and a finalize. The plan and rollback comments both read it
// through here so they describe the same plan the same way.
func setNamespaceWork(ks *templates.KeyspaceChangeData, sc *apitypes.SchemaChangeResponse) {
	if sc.ShowsVSchemaChange() {
		ks.VSchemaChanged = true
		ks.VSchemaDiff = sc.Metadata[apitypes.VSchemaDiffMetadataKey]
	}
	ks.Finalize = sc.NeedsFinalizer()
}

// planCommentDatabaseFlag returns the database a plan comment's copy-paste
// commands name, empty when they are to stay unscoped.
//
// An operator who scoped their command with -d is answered with the same scope,
// and an unscoped one with an unscoped command. An auto-plan has no operator to
// answer, so it names a database only where a bare command would not resolve to
// the one its comment is about: commandScopeDatabases counts what the pasted
// command itself would reach, which is a wider set than the comment planned,
// and above one the comment has to name its database or offer a command that
// comes back ambiguous, or that acts on a database the comment never mentioned.
// Scoping the single-database case too would put a flag in front of every
// operator on every pull request to serve the few that need it.
func planCommentDatabaseFlag(requestedDatabase, resolvedDatabase string, isAutoPlan bool, commandScopeDatabases int) string {
	if !isAutoPlan {
		return requestedDatabase
	}
	if commandScopeDatabases > 1 {
		return resolvedDatabase
	}
	return ""
}

// planTableRef names a table within a plan namespace.
type planTableRef struct{ namespace, table string }

// shardDDLByTable collects every shard's DDL for each table, so a sharded
// namespace's size lines are decided from what each shard runs rather than
// from the one statement the namespace view keeps per table.
func shardDDLByTable(shards []*apitypes.ShardPlanResponse) map[planTableRef][]string {
	byTable := make(map[planTableRef][]string)
	for _, sp := range shards {
		if sp == nil {
			continue
		}
		for _, t := range sp.Changes {
			if t == nil || t.DDL == "" {
				continue
			}
			ref := planTableRef{sp.Namespace, t.TableName}
			byTable[ref] = append(byTable[ref], t.DDL)
		}
	}
	return byTable
}

// planCollationChanges lists the collation changes the engine reported for a
// namespace's table changes, in plan order.
func planCollationChanges(sc *apitypes.SchemaChangeResponse) []templates.CollationChangeData {
	var changes []templates.CollationChangeData
	for _, t := range sc.TableChanges {
		for _, c := range t.CollationChanges {
			changes = append(changes, templates.CollationChangeData{
				Table:          t.TableName,
				Column:         c.Column,
				From:           c.From,
				To:             c.To,
				Case:           engine.ComparisonChange(c.Case),
				TrailingSpaces: engine.ComparisonChange(c.TrailingSpaces),
				CanMergeValues: c.CanMergeValues,
				UniqueIndexes:  c.UniqueIndexes,
			})
		}
	}
	return changes
}

// planTableSizes lists the size estimate of each existing table a namespace's
// plan copies, rebuilds, or scans. Metadata-only changes get no size line,
// since a size beside them would be noise on the plan, and neither do tables
// the plan creates, which have no data yet. A table is listed once however
// many statements change it, since each statement carries the whole table's
// estimate.
func planTableSizes(schema *ghclient.SchemaRequestResult, sc *apitypes.SchemaChangeResponse, shardDDL map[planTableRef][]string) []templates.TableSizeData {
	var sizes []templates.TableSizeData
	listed := make(map[string]bool)
	for _, t := range sc.TableChanges {
		logAttrs := []any{"repo", schema.Repository, "database", schema.Database, "namespace", sc.Namespace, "table", t.TableName}
		if listed[t.TableName] {
			slog.Debug("table already has a size line; skipping its further statements", logAttrs...)
			continue
		}
		if ddl.OpToStatementType(t.ChangeType) == ddl.StatementCreateTable {
			slog.Debug("table is created by this plan; it has no size to show", logAttrs...)
			continue
		}
		ddls := append([]string{t.DDL}, shardDDL[planTableRef{sc.Namespace, t.TableName}]...)
		if !ddl.TableCostScalesWithSize(schema.Type, ddls, logAttrs...) {
			slog.Debug("table's changes are metadata-only; it gets no size line", logAttrs...)
			continue
		}
		listed[t.TableName] = true
		sizes = append(sizes, templates.TableSizeData{
			Table:          t.TableName,
			ShardCount:     t.ShardCount,
			EstimatedBytes: t.EstimatedBytes,
		})
	}
	return sizes
}

// buildPlanCommentData converts plan results into template data.
func buildPlanCommentData(schema *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment, tenant, requestedBy, agentHint, cliName string) templates.PlanCommentData {
	data := templates.PlanCommentData{
		Database:          schema.Database,
		Environment:       environment,
		Tenant:            tenant,
		AgentHint:         agentHint,
		CLIName:           cliName,
		HeadSHA:           schema.HeadSHA,
		Repository:        schema.Repository,
		RequestedBy:       requestedBy,
		DatabaseType:      schema.Type,
		IsMySQL:           schema.Type == "mysql",
		IgnoredNamespaces: schema.IgnoredNamespaces,
		PlanID:            planResp.PlanID,
	}
	for _, group := range planResp.ExemptTables {
		if group == nil {
			continue
		}
		data.ExemptTables = append(data.ExemptTables, templates.ExemptTablesData{
			Namespace: group.Namespace,
			Tables:    group.Tables,
			Reason:    group.Reason,
		})
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
				// plan is incomplete for this shard. Surface it as an error so the
				// omission is never silent. The shard is left out of the keyspace's
				// shard list, so when every shard of a keyspace lands here,
				// keyspaceStatements reads the keyspace as unsharded and the
				// adjacent summary comes from the namespace-level view; the error
				// beside it is what tells the operator so.
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
	shardDDL := shardDDLByTable(planResp.Shards)
	for _, sc := range planResp.Changes {
		ksData := templates.KeyspaceChangeData{
			Keyspace:   sc.Namespace,
			Shards:     shardsByKeyspace[sc.Namespace],
			TableSizes: planTableSizes(schema, sc, shardDDL),
		}
		ksData.CollationChanges = planCollationChanges(sc)
		for _, t := range sc.TableChanges {
			ksData.Statements = append(ksData.Statements, t.DDL)
		}
		setNamespaceWork(&ksData, sc)
		data.Changes = append(data.Changes, ksData)
	}

	// Unsafe changes. For a sharded plan, derive table-level entries from the
	// per-shard changes so an unsafe change confined to one shard (e.g. a column
	// drop on a single drifted shard) is still flagged with the shard it applies
	// to — the collapsed namespace-level Changes can omit it. Otherwise use the
	// namespace-level table view. VSchema removals live only on the
	// namespace-level change, so they are appended in both views.
	unsafe := shardedUnsafeChanges(planResp.Shards)
	if len(unsafe) == 0 {
		for _, sc := range planResp.Changes {
			if sc == nil {
				continue
			}
			for _, t := range sc.TableChanges {
				if uc, ok := t.UnsafeChange(); ok {
					unsafe = append(unsafe, templates.UnsafeChangeData{
						Table:      uc.Table,
						Reason:     uc.Reason,
						DDL:        uc.DDL,
						ChangeType: uc.ChangeType,
					})
				}
			}
		}
	}
	for _, sc := range planResp.Changes {
		if sc == nil {
			continue
		}
		for _, uc := range sc.VSchemaUnsafeChanges() {
			unsafe = append(unsafe, templates.UnsafeChangeData{
				Table:            uc.Table,
				Reason:           uc.Reason,
				DDL:              uc.DDL,
				ChangeType:       uc.ChangeType,
				VSchemaNamespace: sc.Namespace,
			})
		}
	}
	if len(unsafe) > 0 {
		data.HasUnsafeChanges = true
		data.UnsafeChanges = unsafe
	}

	// Blocked changes — the apply commands will reject these. Like the
	// unsafe view, a sharded plan derives them per shard so a blocked change
	// confined to one shard names the shard it applies to.
	if blocked := shardedBlockedChanges(planResp.Shards); len(blocked) > 0 {
		data.BlockedChanges = blocked
	} else {
		for _, sc := range planResp.Changes {
			if sc == nil {
				continue
			}
			for _, t := range sc.TableChanges {
				if !t.EngineBlocked() {
					continue
				}
				data.BlockedChanges = append(data.BlockedChanges, templates.BlockedChangeData{
					Table:  t.TableName,
					Reason: t.ModeReason,
				})
			}
		}
	}

	// Direct-execution changes — the policy routes these to native MySQL DDL,
	// derived the same way as the blocked view.
	if direct := shardedDirectChanges(planResp.Shards); len(direct) > 0 {
		data.DirectChanges = direct
	} else {
		for _, sc := range planResp.Changes {
			if sc == nil {
				continue
			}
			for _, t := range sc.TableChanges {
				if !t.DirectExecution() {
					continue
				}
				data.DirectChanges = append(data.DirectChanges, templates.DirectChangeData{
					Table:  t.TableName,
					Reason: t.ModeReason,
				})
			}
		}
	}

	data.AllChangesDirect = planResp.AllChangesDirect()

	data.DiscardedCopies, data.AdoptedCopies, data.RunningCopies = splitExistingCopies(planResp.ExistingCopies)

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

// settleChecksReplacedByNewTypeBeforeFold settles the rows a planned database
// left under an old type, ahead of the plan's aggregate fold. A row that
// cannot be settled keeps blocking the aggregate, so the fold still runs.
func (h *Handler) settleChecksReplacedByNewTypeBeforeFold(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, databaseName, databaseType string) {
	if err := h.settleChecksReplacedByNewType(ctx, client, repo, pr, headSHA, databaseName, databaseType); err != nil {
		h.logger.Error("checks under the database's old type keep blocking the aggregate because they could not be settled",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", databaseName, "database_type", databaseType, "error", err)
	}
}
