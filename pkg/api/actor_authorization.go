package api

import (
	"fmt"
	"strings"
)

const (
	ActorAuthReasonDisabled              = "disabled"
	ActorAuthReasonAllowedAdminTeam      = "allowed_admin_team"
	ActorAuthReasonAllowedAdminUser      = "allowed_admin_user"
	ActorAuthReasonAllowedRepoAdminTeam  = "allowed_repo_admin_team"
	ActorAuthReasonAllowedRepoAdminUser  = "allowed_repo_admin_user"
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

// ReviewPolicyEnabled returns true when apply/apply-confirm PR comments must
// pass the review gate.
func (c *ServerConfig) ReviewPolicyEnabled() bool {
	return c != nil && c.ReviewPolicy.Enabled
}

// ReviewPolicyIncludesDatabaseOperators returns whether database-scoped
// operator principals should satisfy the review gate. Defaults to true.
func (c *ServerConfig) ReviewPolicyIncludesDatabaseOperators() bool {
	return c == nil || c.ReviewPolicy.IncludeDatabaseOperators == nil || *c.ReviewPolicy.IncludeDatabaseOperators
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

func validateReviewPolicy(config ReviewPolicyConfig) error {
	if err := validateGitHubTeamPrincipals("review_policy.admin_teams", config.AdminTeams); err != nil {
		return err
	}
	if err := validateGitHubUsers("review_policy.admin_users", config.AdminUsers); err != nil {
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

func validateRepoActorAuthorization(repo string, repoConfig RepoConfig) error {
	if err := validateGitHubTeamPrincipals("repos."+repo+".admin_teams", repoConfig.AdminTeams); err != nil {
		return err
	}
	if err := validateGitHubUsers("repos."+repo+".admin_users", repoConfig.AdminUsers); err != nil {
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

// PRCommandAuthorizedPrincipals returns the GitHub principals (teams as
// "org/team", plain user logins) allowed to run mutating PR commands for
// database via repo, in the order the authorizer consults them: the
// deployment-wide admin teams/users, the repository's admin teams/users, then
// the database's operator teams/users. Rejection comments surface this list so
// a blocked user knows who to ask instead of guessing.
func (c *ServerConfig) PRCommandAuthorizedPrincipals(repo, database string) []string {
	if c == nil {
		return nil
	}
	var principals []string
	seen := map[string]bool{}
	add := func(items []string) {
		for _, item := range items {
			// GitHub logins and team slugs are case-insensitive, and operators
			// sometimes configure them with a leading "@" — normalize both so
			// the rendered list never repeats one principal in two spellings.
			display := strings.TrimPrefix(strings.TrimSpace(item), "@")
			if display == "" || seen[strings.ToLower(display)] {
				continue
			}
			seen[strings.ToLower(display)] = true
			principals = append(principals, display)
		}
	}
	add(c.PRCommandAuthorization.AdminTeams)
	add(c.PRCommandAuthorization.AdminUsers)
	repoAdminTeams, repoAdminUsers := c.RepoAdmins(repo)
	add(repoAdminTeams)
	add(repoAdminUsers)
	if db, ok := c.Databases[database]; ok {
		add(db.OperatorTeams)
		add(db.OperatorUsers)
	}
	return principals
}
