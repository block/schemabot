package webhook

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// appDispatchHeadCheckTimeout bounds each inter-dispatch PR head re-read. The
// dispatch loop can outlive the bootstrap command context — each database's
// core runs under its own fresh deadline — so every head check derives a fresh
// bounded context from the dispatch parent instead of reusing the aging
// bootstrap context, which could otherwise expire mid-loop and halt a healthy
// dispatch with a spurious deadline error.
const appDispatchHeadCheckTimeout = 10 * time.Second

// appExpansion is the fail-closed result of resolving `--app` to concrete
// databases: the databases the command will run against (in name order, each
// paired with the stored plan that qualified it) and the app databases left
// out of this run, with reasons.
type appExpansion struct {
	databases []string
	// planByDatabase holds the newest stored plan for this PR and environment
	// per participating database; the source-policy re-check reads its
	// repository and schema path.
	planByDatabase map[string]*storage.Plan
	skipped        []templates.AppScopedSkippedDatabase
}

// appScopedApplyCore handles apply and apply-confirm commands carrying
// `--app`. It expands the app to concrete databases and fails closed before
// any database is touched:
//
//  1. The app must name at least one configured database.
//  2. Only databases with a stored plan for this PR and environment
//     participate; app databases without one are skipped with a note (a PR
//     that never touched a database's schema directory never planned it).
//  3. Zero participating databases is a rejection, not a no-op.
//  4. Every participating database's source policy must permit the plan's
//     repository and schema path; one denial rejects the whole command.
//  5. The actor must be authorized for every participating database; one
//     denial rejects the whole command.
//
// Once every gate passes it posts an expansion summary, then runs the
// single-database command core once per database in name order — each
// database gets its own apply, lock, check, and progress comments, exactly as
// a `-d` command would. A database whose own gates block (lock conflict,
// checks, prior environment) posts its usual comment without stopping its
// siblings: past this point each database's outcome is independently visible
// and independently retryable.
//
// The (retry, err) disposition mirrors the single-database cores: a transient
// failure in the pre-flight or in any database's core marks the whole command
// retryable, and a re-drive re-runs every database — databases that already
// dispatched are protected by their own lock and active-apply guards.
func (h *Handler) appScopedApplyCore(parent context.Context, repo string, pr int, environment string, installationID int64, requestedBy string, result CommandResult, commandName string) (bool, error) {
	app := result.App
	ctx, cancel, client, err := h.commandBootstrap(parent, repo, installationID)
	if err != nil {
		if errors.Is(err, errGitHubAppResolution) {
			h.logger.Error("app-scoped command blocked: cannot resolve GitHub App client for repo",
				"repo", repo, "pr", pr, "app", app, "environment", environment, "command", commandName, "error", err)
			return false, fmt.Errorf("app-scoped %s command bootstrap %s#%d: %w", commandName, repo, pr, err)
		}
		h.logger.Error("app-scoped command: failed to bootstrap command",
			"repo", repo, "pr", pr, "app", app, "environment", environment, "command", commandName, "error", err)
		return true, fmt.Errorf("app-scoped %s command bootstrap %s#%d: %w", commandName, repo, pr, err)
	}
	defer cancel()

	rejectExpansion := func(reason string) {
		h.postComment(repo, pr, installationID, templates.RenderAppScopedExpansionError(templates.AppScopedExpansionErrorData{
			App:         app,
			Environment: environment,
			CommandName: commandName,
			RequestedBy: requestedBy,
			Reason:      reason,
		}))
	}

	// App-scoped preflight — source policy and all-or-nothing actor
	// authorization — is evaluated against this deployment's config only. On an
	// aggregate repo an unscoped command fans out to every deployment, so one
	// deployment could pass its preflight and dispatch while a sibling denies,
	// breaking the all-or-nothing guarantee fleet-wide. Fail closed: unscoped
	// app commands are rejected on aggregate repos. A `-t`-scoped command
	// addresses a single deployment, where the guarantee holds, so it proceeds.
	if result.Tenant == "" && h.service.Config().AggregateRoleForRepo(repo) != "" {
		if h.silentUsageErrorOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("app-scoped command deferred: unscoped app command on an aggregate participant; the leader posts the rejection",
				"repo", repo, "pr", pr, "app", app, "environment", environment, "command", commandName,
				"requested_by", requestedBy)
			return false, nil
		}
		h.logger.Warn("app-scoped command rejected: unscoped app command on an aggregate repo cannot coordinate preflight across deployments",
			"repo", repo, "pr", pr, "app", app, "environment", environment, "command", commandName,
			"requested_by", requestedBy)
		rejectExpansion("this repository fans commands out to multiple SchemaBot deployments, and app preflight cannot be coordinated across them — target one deployment with `-t <tenant>`")
		return false, nil
	}

	appDatabases, err := h.service.Config().DatabasesForApp(app)
	if err != nil {
		h.logger.Warn("app-scoped command rejected: app matches no configured databases",
			"repo", repo, "pr", pr, "app", app, "environment", environment, "command", commandName,
			"requested_by", requestedBy, "error", err)
		rejectExpansion(fmt.Sprintf("no configured database declares `app: %s` on this SchemaBot instance", app))
		return false, nil
	}

	expansion, err := h.expandAppDatabases(ctx, repo, pr, environment, app, appDatabases)
	if err != nil {
		h.logger.Error("app-scoped command: failed to load stored plans for expansion",
			"repo", repo, "pr", pr, "app", app, "environment", environment, "command", commandName, "error", err)
		if !result.SuppressRetryComments {
			h.postCommandError(repo, pr, installationID, commandName, environment, requestedBy,
				"Failed to resolve the app's databases. Retry, and see server logs if it persists.")
		}
		return true, fmt.Errorf("app-scoped %s expansion %s#%d app %s: %w", commandName, repo, pr, app, err)
	}
	if len(expansion.databases) == 0 {
		h.logger.Warn("app-scoped command rejected: no database in the app has a stored plan for this PR",
			"repo", repo, "pr", pr, "app", app, "environment", environment, "command", commandName,
			"requested_by", requestedBy, "app_databases", len(appDatabases))
		rejectExpansion(fmt.Sprintf("no database in the app has a stored `%s` plan for this PR — run `schemabot plan -e %s` first", environment, environment))
		return false, nil
	}

	// Source policy re-check for every participating database. The stored plan
	// already passed source policy when it was created, but config can change
	// between plan and apply, so one denial now rejects the whole command.
	for _, database := range expansion.databases {
		plan := expansion.planByDatabase[database]
		if err := h.service.Config().AuthorizePlanSource(api.PlanSourcePolicyRequest{
			Database:    database,
			Repository:  plan.Repository,
			PullRequest: plan.PullRequest,
			SchemaPath:  plan.SchemaPath,
		}); err != nil {
			h.logger.Warn("app-scoped command rejected by source policy",
				"repo", repo, "pr", pr, "app", app, "environment", environment, "command", commandName,
				"database", database, "schema_path", plan.SchemaPath, "requested_by", requestedBy, "error", err)
			rejectExpansion(appSourcePolicyRejection(database, err))
			return false, nil
		}
	}

	if blocked, err := h.enforceAppScopedActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, app, environment, commandName, expansion.databases, result.SuppressRetryComments); err != nil {
		return true, err
	} else if blocked {
		return false, nil
	}

	// Pin the PR head for the whole dispatch. Every database must apply the
	// same commit: without the pin, a commit landing mid-loop would let earlier
	// databases apply one head and later databases another, and each would pass
	// its own single-database freshness checks. The uncached fetch matters —
	// the pin exists to observe head movement.
	pinnedPR, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		h.logger.Error("app-scoped command: failed to pin the PR head before dispatch",
			"repo", repo, "pr", pr, "app", app, "environment", environment, "command", commandName, "error", err)
		if !result.SuppressRetryComments {
			h.postCommandError(repo, pr, installationID, commandName, environment, requestedBy,
				"Failed to read the PR's current commit. Retry, and see server logs if it persists.")
		}
		return true, fmt.Errorf("app-scoped %s pin PR head %s#%d app %s: %w", commandName, repo, pr, app, err)
	}
	pinnedSHA := pinnedPR.HeadSHA

	h.logger.Info("app-scoped command expansion authorized; dispatching per database",
		"repo", repo, "pr", pr, "app", app, "environment", environment, "command", commandName,
		"requested_by", requestedBy, "head_sha", pinnedSHA,
		"databases", expansion.databases, "skipped", len(expansion.skipped))
	h.postComment(repo, pr, installationID, templates.RenderAppScopedDispatch(templates.AppScopedDispatchData{
		App:         app,
		Environment: environment,
		RequestedBy: requestedBy,
		PinnedSHA:   pinnedSHA,
		Databases:   expansion.databases,
		Skipped:     expansion.skipped,
	}))

	// Per-database runs re-enter the single-database cores, so the per-database
	// result must not carry the app or the cores would re-expand it. The pinned
	// head rides along so each core independently rejects if the PR advances —
	// the loop's own re-check narrows the window, the core check closes it.
	perDatabase := result
	perDatabase.App = ""
	perDatabase.ExpectedHeadSHA = pinnedSHA

	var retry bool
	var coreErrs []error
	for i, database := range expansion.databases {
		if i > 0 {
			if halted, haltErr := h.haltDispatchIfHeadMoved(parent, client, repo, pr, installationID, app, environment, commandName, pinnedSHA, expansion.databases[i:]); haltErr != nil {
				coreErrs = append(coreErrs, haltErr)
				retry = true
				break
			} else if halted {
				break
			}
		}
		var dbRetry bool
		var dbErr error
		switch commandName {
		case action.Apply:
			dbRetry, dbErr = h.applyCommandCore(parent, repo, pr, environment, database, installationID, requestedBy, perDatabase)
		case action.ApplyConfirm:
			dbRetry, dbErr = h.applyConfirmCommandCore(parent, repo, pr, environment, database, installationID, requestedBy, perDatabase)
		default:
			return false, fmt.Errorf("app-scoped dispatch %s#%d app %s: command %q does not support --app", repo, pr, app, commandName)
		}
		if dbErr != nil {
			coreErrs = append(coreErrs, fmt.Errorf("database %s: %w", database, dbErr))
		}
		retry = retry || dbRetry
	}
	if len(coreErrs) > 0 {
		return retry, fmt.Errorf("app-scoped %s %s#%d app %s: %w", commandName, repo, pr, app, errors.Join(coreErrs...))
	}
	// A database core can ask for a retry without an error; honor it the same
	// way the durable driver does for a single-database command. A re-drive
	// re-runs every database, and already-dispatched siblings are protected by
	// their own lock and active-apply guards.
	return retry, nil
}

// haltDispatchIfHeadMoved re-reads the PR head between per-database dispatches
// and halts the remaining databases when it no longer matches the head pinned
// at dispatch start, posting a comment naming what was not started. It fails
// closed: when the head cannot be re-read, the dispatch halts with an error
// rather than continuing on an unverified pin. Each check runs under its own
// fresh deadline derived from parent (see appDispatchHeadCheckTimeout).
func (h *Handler) haltDispatchIfHeadMoved(parent context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, app, environment, commandName, pinnedSHA string, remaining []string) (bool, error) {
	ctx, cancel := context.WithTimeout(parent, appDispatchHeadCheckTimeout)
	defer cancel()
	currentPR, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		h.logger.Error("app-scoped dispatch halted: cannot verify the pinned PR head",
			"repo", repo, "pr", pr, "app", app, "environment", environment, "command", commandName,
			"head_sha", pinnedSHA, "remaining_databases", remaining, "error", err)
		return false, fmt.Errorf("verify pinned PR head %s during app-scoped dispatch: %w", pinnedSHA, err)
	}
	if currentPR.HeadSHA == pinnedSHA {
		return false, nil
	}
	h.logger.Warn("app-scoped dispatch halted: PR head advanced mid-dispatch; remaining databases will not start",
		"repo", repo, "pr", pr, "app", app, "environment", environment, "command", commandName,
		"pinned_sha", pinnedSHA, "current_sha", currentPR.HeadSHA, "remaining_databases", remaining)
	h.postComment(repo, pr, installationID, templates.RenderAppScopedDispatchHalted(templates.AppScopedDispatchHaltedData{
		App:         app,
		Environment: environment,
		CommandName: commandName,
		PinnedSHA:   pinnedSHA,
		CurrentSHA:  currentPR.HeadSHA,
		NotStarted:  remaining,
	}))
	return true, nil
}

// expandAppDatabases applies the participation filter: of the app's configured
// databases, only those with the target environment configured and a stored
// plan for this PR and environment take part in the command. Everything else
// is recorded as skipped with a reason the dispatch summary can show.
func (h *Handler) expandAppDatabases(ctx context.Context, repo string, pr int, environment, app string, appDatabases []string) (appExpansion, error) {
	plans, err := h.service.Storage().Plans().GetByPR(ctx, repo, pr)
	if err != nil {
		return appExpansion{}, fmt.Errorf("load stored plans for %s#%d: %w", repo, pr, err)
	}
	newestPlan := make(map[string]*storage.Plan, len(appDatabases))
	for _, plan := range plans {
		if plan.Environment != environment {
			continue
		}
		current, ok := newestPlan[plan.Database]
		if !ok || plan.CreatedAt.After(current.CreatedAt) || (plan.CreatedAt.Equal(current.CreatedAt) && plan.ID > current.ID) {
			newestPlan[plan.Database] = plan
		}
	}

	expansion := appExpansion{planByDatabase: make(map[string]*storage.Plan, len(appDatabases))}
	for _, database := range appDatabases {
		dbConfig := h.service.Config().Database(database)
		if dbConfig == nil {
			// DatabasesForApp only returns configured databases, so a nil config
			// here means the config was swapped mid-command; skip rather than
			// guessing at the database's environments.
			h.logger.Warn("app database disappeared from config during expansion; skipping it",
				"repo", repo, "pr", pr, "app", app, "environment", environment, "database", database)
			expansion.skipped = append(expansion.skipped, templates.AppScopedSkippedDatabase{
				Database: database, Reason: "no longer configured on this SchemaBot instance",
			})
			continue
		}
		if _, ok := dbConfig.Environments[environment]; !ok {
			h.logger.Debug("app database does not configure the target environment; skipping it",
				"repo", repo, "pr", pr, "app", app, "environment", environment, "database", database)
			expansion.skipped = append(expansion.skipped, templates.AppScopedSkippedDatabase{
				Database: database, Reason: fmt.Sprintf("environment `%s` is not configured", environment),
			})
			continue
		}
		plan, ok := newestPlan[database]
		if !ok {
			h.logger.Debug("app database has no stored plan for this PR; skipping it",
				"repo", repo, "pr", pr, "app", app, "environment", environment, "database", database)
			expansion.skipped = append(expansion.skipped, templates.AppScopedSkippedDatabase{
				Database: database, Reason: "no plan for this PR",
			})
			continue
		}
		expansion.databases = append(expansion.databases, database)
		expansion.planByDatabase[database] = plan
	}
	return expansion, nil
}

// enforceAppScopedActorAuthorization evaluates PR-command actor authorization
// against every database in the expansion, denying the whole command on the
// first denial. This is deliberately all-or-nothing: an app-scoped command
// must never partially apply because the actor operates only a subset of the
// app's databases.
func (h *Handler) enforceAppScopedActorAuthorization(
	ctx context.Context,
	client *ghclient.InstallationClient,
	repo string,
	pr int,
	installationID int64,
	requestedBy string,
	app string,
	environment string,
	commandName string,
	databases []string,
	suppressRetryComments bool,
) (blocked bool, err error) {
	for _, database := range databases {
		authResult, err := h.authorizePRCommandActor(ctx, client, requestedBy, repo, database)
		status := actorAuthorizationMetricStatus(authResult, err)
		metrics.RecordPRCommandActorAuthorization(ctx, metricActionKey(commandName), database, environment, repo, status, authResult.Reason)

		if err != nil {
			h.logger.Warn("app-scoped command stopped by actor authorization error",
				"repo", repo, "pr", pr, "app", app, "database", database,
				"environment", environment, "command", commandName,
				"requested_by", requestedBy, "reason", authResult.Reason, "error", err)
			if !suppressRetryComments {
				h.postComment(repo, pr, installationID, templates.RenderPRCommandAuthorizationUnavailable(templates.ActorAuthorizationCommentData{
					RequestedBy: requestedBy,
					CommandName: commandName,
					Database:    database,
					Environment: environment,
				}))
			}
			return false, fmt.Errorf("app-scoped actor authorization for %s command %s#%d app %s database %s: %w", commandName, repo, pr, app, database, err)
		}
		if !authResult.Allowed {
			h.logger.Warn("app-scoped command blocked by actor authorization; denying every database in the app",
				"repo", repo, "pr", pr, "app", app, "database", database,
				"environment", environment, "command", commandName,
				"requested_by", requestedBy, "reason", authResult.Reason)
			operatorPrincipals, otherPrincipals := h.service.Config().PRCommandAuthorizedPrincipals(repo, database)
			h.postComment(repo, pr, installationID, templates.RenderAppScopedApplyNotAuthorized(templates.AppScopedNotAuthorizedData{
				RequestedBy:        requestedBy,
				CommandName:        commandName,
				App:                app,
				Environment:        environment,
				Database:           database,
				OperatorPrincipals: operatorPrincipals,
				OtherPrincipals:    otherPrincipals,
			}))
			return true, nil
		}
		if authResult.Reason == api.ActorAuthReasonDisabled {
			h.logger.Debug("skipping app-scoped actor authorization because it is disabled",
				"repo", repo, "pr", pr, "app", app, "environment", environment,
				"command", commandName, "requested_by", requestedBy)
			return false, nil
		}
	}
	h.logger.Info("app-scoped actor authorization allowed for every database in the expansion",
		"repo", repo, "pr", pr, "app", app, "environment", environment,
		"command", commandName, "requested_by", requestedBy, "databases", databases)
	return false, nil
}

// appSourcePolicyRejection renders the server-authored reason line for a
// source-policy denial during app expansion. Source-policy messages are
// config-derived and safe to show; any other error stays in the server logs.
func appSourcePolicyRejection(database string, err error) string {
	if policyErr, ok := errors.AsType[*api.SourcePolicyError](err); ok {
		return fmt.Sprintf("database `%s` denied the PR's source: %s", database, policyErr.Message)
	}
	return fmt.Sprintf("source policy verification failed for database `%s`; see server logs", database)
}
