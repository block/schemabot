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
	if h.service == nil {
		return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonMissingServerConfig}, fmt.Errorf("server config is unavailable")
	}
	config := h.service.Config()
	if !config.PRCommandAuthorizationEnabled() {
		return api.ActorAuthorizationResult{Allowed: true, Reason: api.ActorAuthReasonDisabled}, nil
	}

	if strings.TrimSpace(actor) == "" {
		return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonMissingActor}, nil
	}

	dbConfig := config.Database(database)
	if dbConfig == nil {
		return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonMissingDatabaseConfig}, nil
	}

	authConfig := config.PRCommandAuthorization
	if matched, principal := matchedUserPrincipal(authConfig.AdminUsers, actor); matched {
		return api.ActorAuthorizationResult{
			Allowed:          true,
			Reason:           api.ActorAuthReasonAllowedAdminUser,
			MatchedPrincipal: principal,
		}, nil
	}
	if matched, principal := matchedUserPrincipal(dbConfig.OperatorUsers, actor); matched {
		return api.ActorAuthorizationResult{
			Allowed:          true,
			Reason:           api.ActorAuthReasonAllowedOperatorUser,
			MatchedPrincipal: principal,
		}, nil
	}

	teamCount := len(authConfig.AdminTeams) + len(dbConfig.OperatorTeams)
	principalCount := teamCount + len(authConfig.AdminUsers) + len(dbConfig.OperatorUsers)
	if principalCount == 0 {
		return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonNoConfiguredPrincipal}, nil
	}
	if teamCount == 0 {
		return api.ActorAuthorizationResult{Reason: api.ActorAuthReasonNotAuthorized}, nil
	}
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

	matched, principal, err = actorInAnyTeam(ctx, client, dbConfig.OperatorTeams, actor)
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
