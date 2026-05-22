package api

import (
	"fmt"
	"strings"
)

const (
	ActorAuthReasonDisabled              = "disabled"
	ActorAuthReasonAllowedAdminTeam      = "allowed_admin_team"
	ActorAuthReasonAllowedAdminUser      = "allowed_admin_user"
	ActorAuthReasonAllowedOperatorTeam   = "allowed_operator_team"
	ActorAuthReasonAllowedOperatorUser   = "allowed_operator_user"
	ActorAuthReasonMissingActor          = "missing_actor"
	ActorAuthReasonMissingServerConfig   = "missing_server_config"
	ActorAuthReasonMissingDatabaseConfig = "missing_database_config"
	ActorAuthReasonNoConfiguredPrincipal = "no_configured_principal"
	ActorAuthReasonNotAuthorized         = "not_authorized"
	ActorAuthReasonGitHubError           = "github_error"
	ActorAuthReasonUnknown               = "unknown"
)

// ActorAuthorizationResult describes the decision for a GitHub user running an
// apply/apply-confirm PR comment command. Reason is stable for metrics;
// MatchedPrincipal names the user or team that granted access on allow paths.
type ActorAuthorizationResult struct {
	Allowed          bool
	Reason           string
	MatchedPrincipal string
}

// PRCommandAuthorizationEnabled returns true when apply/apply-confirm PR
// comment commands must pass actor authorization.
func (c *ServerConfig) PRCommandAuthorizationEnabled() bool {
	return c != nil && c.PRCommandAuthorization.Enabled
}

// ParseGitHubTeamPrincipal splits a configured team principal in "org/team-slug"
// form. Team principals are optional; callers should only parse values that were
// explicitly configured.
func ParseGitHubTeamPrincipal(value string) (org, slug string, err error) {
	if strings.TrimSpace(value) != value {
		return "", "", fmt.Errorf("value has leading or trailing whitespace")
	}
	parts := strings.Split(value, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("value must be in org/team-slug form")
	}
	return parts[0], parts[1], nil
}

func validatePRCommandAuthorization(config PRCommandAuthorizationConfig) error {
	if err := validateGitHubTeamPrincipals("pr_command_authorization.admin_teams", config.AdminTeams); err != nil {
		return err
	}
	if err := validateGitHubUsers("pr_command_authorization.admin_users", config.AdminUsers); err != nil {
		return err
	}
	return nil
}

func validateDatabaseActorAuthorization(database string, dbConfig DatabaseConfig) error {
	if err := validateGitHubTeamPrincipals("databases."+database+".operator_teams", dbConfig.OperatorTeams); err != nil {
		return err
	}
	if err := validateGitHubUsers("databases."+database+".operator_users", dbConfig.OperatorUsers); err != nil {
		return err
	}
	return nil
}

func validateGitHubTeamPrincipals(field string, teams []string) error {
	seen := make(map[string]struct{}, len(teams))
	for _, team := range teams {
		if _, _, err := ParseGitHubTeamPrincipal(team); err != nil {
			return fmt.Errorf("%s contains invalid team %q: %w", field, team, err)
		}
		key := strings.ToLower(team)
		if _, ok := seen[key]; ok {
			return fmt.Errorf("%s contains duplicate value %q", field, team)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateGitHubUsers(field string, users []string) error {
	seen := make(map[string]struct{}, len(users))
	for _, user := range users {
		trimmed := strings.TrimSpace(user)
		if trimmed == "" {
			return fmt.Errorf("%s contains an empty value", field)
		}
		if trimmed != user {
			return fmt.Errorf("%s contains value %q with leading or trailing whitespace", field, user)
		}
		if strings.Contains(user, "/") {
			return fmt.Errorf("%s contains invalid user %q: users must not contain /", field, user)
		}
		key := strings.ToLower(user)
		if _, ok := seen[key]; ok {
			return fmt.Errorf("%s contains duplicate value %q", field, user)
		}
		seen[key] = struct{}{}
	}
	return nil
}
