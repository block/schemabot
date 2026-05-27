package webhook

import (
	"context"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/webhook/templates"
)

func (h *Handler) enforcePRCommandActorAuthorization(
	ctx context.Context,
	client *ghclient.InstallationClient,
	repo string,
	pr int,
	installationID int64,
	requestedBy string,
	database string,
	databaseType string,
	environment string,
	commandName string,
) bool {
	result, err := h.authorizePRCommandActor(ctx, client, requestedBy, database)
	status := actorAuthorizationMetricStatus(result, err)
	metrics.RecordPRCommandActorAuthorization(ctx, metricActionKey(commandName), database, environment, repo, status, result.Reason)

	if err != nil {
		h.logger.Warn("PR command blocked by actor authorization error",
			"repo", repo, "pr", pr, "database", database,
			"database_type", databaseType, "environment", environment,
			"command", commandName, "requested_by", requestedBy,
			"reason", result.Reason, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderPRCommandAuthorizationUnavailable(templates.ActorAuthorizationCommentData{
			RequestedBy: requestedBy,
			CommandName: commandName,
			Database:    database,
			Environment: environment,
		}))
		return true
	}
	if !result.Allowed {
		h.logger.Warn("PR command blocked by actor authorization",
			"repo", repo, "pr", pr, "database", database,
			"database_type", databaseType, "environment", environment,
			"command", commandName, "requested_by", requestedBy,
			"reason", result.Reason)
		h.postComment(repo, pr, installationID, templates.RenderPRCommandNotAuthorized(templates.ActorAuthorizationCommentData{
			RequestedBy: requestedBy,
			CommandName: commandName,
			Database:    database,
			Environment: environment,
		}))
		return true
	}
	if result.Reason == api.ActorAuthReasonDisabled {
		h.logger.Debug("skipping PR command actor authorization because it is disabled",
			"repo", repo, "pr", pr, "database", database,
			"database_type", databaseType, "environment", environment,
			"command", commandName, "requested_by", requestedBy)
		return false
	}
	h.logger.Debug("PR command actor authorization allowed",
		"repo", repo, "pr", pr, "database", database,
		"database_type", databaseType, "environment", environment,
		"command", commandName, "requested_by", requestedBy,
		"reason", result.Reason, "matched_principal", result.MatchedPrincipal)
	return false
}

func (h *Handler) authorizePRCommandActor(
	ctx context.Context,
	client *ghclient.InstallationClient,
	actor string,
	database string,
) (api.ActorAuthorizationResult, error) {
	// Without server config, SchemaBot cannot know the trusted actor policy.
	if h.service == nil {
		return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonMissingServerConfig}, fmt.Errorf("server config is unavailable")
	}
	config := h.service.Config()
	// The feature is opt-in; disabled auth preserves existing PR command behavior.
	if !config.PRCommandAuthorizationEnabled() {
		return api.ActorAuthorizationResult{Allowed: true, Reason: api.ActorAuthReasonDisabled}, nil
	}

	// GitHub should provide a comment actor. Missing actor identity is unsafe for
	// a mutating PR command, so deny instead of guessing.
	if strings.TrimSpace(actor) == "" {
		return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonMissingActor}, nil
	}

	return h.authorizeConfiguredDatabaseActor(ctx, client, actor, database)
}

func (h *Handler) authorizeConfiguredDatabaseActor(
	ctx context.Context,
	client *ghclient.InstallationClient,
	actor string,
	database string,
) (api.ActorAuthorizationResult, error) {
	// Without server config, SchemaBot cannot know the trusted actor policy.
	if h.service == nil {
		return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonMissingServerConfig}, fmt.Errorf("server config is unavailable")
	}
	config := h.service.Config()

	// GitHub should provide a principal. Missing identity is unsafe for a
	// mutating PR command or a review gate approval, so deny instead of guessing.
	if strings.TrimSpace(actor) == "" {
		return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonMissingActor}, nil
	}

	// Authorization is scoped to the resolved server-side database config.
	dbConfig := config.Database(database)
	if dbConfig == nil {
		return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonMissingDatabaseConfig}, nil
	}

	authConfig := config.PRCommandAuthorization
	teamCount := len(authConfig.AdminTeams) + len(dbConfig.OperatorTeams)
	principalCount := teamCount + len(authConfig.AdminUsers) + len(dbConfig.OperatorUsers)
	// Actor auth is enabled but no admin/operator principals exist for this
	// database. Fail closed instead of treating an empty policy as "allow all".
	if principalCount == 0 {
		return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonNoConfiguredPrincipal}, nil
	}

	// Global admin teams have the highest precedence and can approve any
	// configured database. A non-member result falls through to admin users.
	if len(authConfig.AdminTeams) > 0 {
		if client == nil {
			return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonGitHubError}, fmt.Errorf("github client is nil")
		}
		matched, principal, err := actorInAnyTeam(ctx, client, authConfig.AdminTeams, actor)
		if err != nil {
			return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonGitHubError}, err
		}
		if matched {
			return api.ActorAuthorizationResult{
				Allowed:          true,
				Reason:           api.ActorAuthReasonAllowedAdminTeam,
				MatchedPrincipal: principal,
			}, nil
		}
	}

	// Global admin users are checked after admin teams and before any
	// database-scoped operator policy.
	if matched, principal := matchedUserPrincipal(authConfig.AdminUsers, actor); matched {
		return api.ActorAuthorizationResult{
			Allowed:          true,
			Reason:           api.ActorAuthReasonAllowedAdminUser,
			MatchedPrincipal: principal,
		}, nil
	}

	// Database operator teams authorize only the database currently being
	// mutated by this PR command.
	if len(dbConfig.OperatorTeams) > 0 {
		if client == nil {
			return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonGitHubError}, fmt.Errorf("github client is nil")
		}
		matched, principal, err := actorInAnyTeam(ctx, client, dbConfig.OperatorTeams, actor)
		if err != nil {
			return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonGitHubError}, err
		}
		if matched {
			return api.ActorAuthorizationResult{
				Allowed:          true,
				Reason:           api.ActorAuthReasonAllowedOperatorTeam,
				MatchedPrincipal: principal,
			}, nil
		}
	}

	// Database operator users are the final allowlist and are scoped to this
	// database.
	if matched, principal := matchedUserPrincipal(dbConfig.OperatorUsers, actor); matched {
		return api.ActorAuthorizationResult{
			Allowed:          true,
			Reason:           api.ActorAuthReasonAllowedOperatorUser,
			MatchedPrincipal: principal,
		}, nil
	}

	// No configured user or team authorized this actor.
	return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonNotAuthorized}, nil
}

func actorInAnyTeam(ctx context.Context, client *ghclient.InstallationClient, teams []string, actor string) (bool, string, error) {
	for _, team := range teams {
		org, slug, err := api.ParseGitHubTeamPrincipal(team)
		if err != nil {
			return false, "", fmt.Errorf("invalid configured GitHub team %q: %w", team, err)
		}
		member, err := client.IsTeamMember(ctx, org, slug, actor)
		if err != nil {
			return false, "", err
		}
		if member {
			return true, team, nil
		}
	}
	return false, "", nil
}

func matchedUserPrincipal(allowedUsers []string, actor string) (bool, string) {
	for _, user := range allowedUsers {
		if strings.EqualFold(user, actor) {
			return true, user
		}
	}
	return false, ""
}

func actorAuthorizationMetricStatus(result api.ActorAuthorizationResult, err error) string {
	if err != nil {
		return "error"
	}
	if result.Reason == api.ActorAuthReasonDisabled {
		return "skipped"
	}
	if result.Allowed {
		return "allowed"
	}
	return "denied"
}
