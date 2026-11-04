package api

import (
	"fmt"
	"maps"
	"slices"
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

// ReviewPolicyEnabled returns true when the instance-wide review policy is on,
// so apply/apply-confirm PR comments for every database must pass the review
// gate. A database can also opt in on its own; ReviewPolicyEnabledForDatabase
// covers both.
func (c *ServerConfig) ReviewPolicyEnabled() bool {
	return c != nil && c.ReviewPolicy.Enabled
}

// ReviewPolicyEnabledForDatabase returns true when apply/apply-confirm PR
// comments for database must pass the review gate: the instance-wide review
// policy is on, or the database opts in with its own review_policy.
func (c *ServerConfig) ReviewPolicyEnabledForDatabase(database string) bool {
	if c.ReviewPolicyEnabled() {
		return true
	}
	if c == nil {
		return false
	}
	dbConfig := c.Database(database)
	return dbConfig != nil && dbConfig.ReviewPolicy.Enabled
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

// validateDatabaseReviewPolicy rejects a database that opts into the review
// gate unless every PR it accepts has a configured principal who can approve
// it. Without one, the gate fails that PR's every apply with an error instead
// of asking for a review.
func (c *ServerConfig) validateDatabaseReviewPolicy(database string, dbConfig DatabaseConfig) error {
	if !dbConfig.ReviewPolicy.Enabled {
		return nil
	}
	if c.ReviewPolicyIncludesDatabaseOperators() && (len(dbConfig.OperatorTeams) > 0 || len(dbConfig.OperatorUsers) > 0) {
		return nil
	}
	if len(c.ReviewPolicy.AdminTeams) > 0 || len(c.ReviewPolicy.AdminUsers) > 0 {
		return nil
	}
	// Otherwise each repo the database accepts needs admins of its own: a
	// repo's admins approve only that repo's PRs, and CODEOWNERS approve only
	// a PR whose files they own, so neither covers every PR.
	const fix = "set the database's operator_teams or operator_users (with review_policy.include_database_operators left on), or review_policy.admin_teams or admin_users"
	repos := dbConfig.AllowedRepos
	if len(repos) == 0 || slices.Contains(repos, "*") {
		// The database accepts every repository SchemaBot serves, which is
		// every repository when none are configured.
		if len(c.Repos) == 0 {
			return fmt.Errorf("databases.%s.review_policy.enabled requires someone who can approve every PR, but the database accepts any repository and no repos are configured: %s", database, fix)
		}
		repos = slices.Sorted(maps.Keys(c.Repos))
	}
	for _, repo := range repos {
		if teams, users := c.RepoAdmins(repo); len(teams) == 0 && len(users) == 0 {
			return fmt.Errorf("databases.%s.review_policy.enabled requires someone who can approve every PR, but repos.%s, which the database accepts schema changes from, has no admin_teams or admin_users: set them, or %s", database, repo, fix)
		}
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
// database via repo, split into the database's own operator teams/users and
// the broader principals (deployment-wide admins, then the repository's
// admins). Rejection comments surface the two groups so a blocked user pings
// the database's operators first; a principal configured in both groups is
// listed only as an operator.
func (c *ServerConfig) PRCommandAuthorizedPrincipals(repo, database string) (operators, others []string) {
	if c == nil {
		return nil, nil
	}
	seen := map[string]bool{}
	add := func(dst []string, items []string) []string {
		for _, item := range items {
			// GitHub logins and team slugs are case-insensitive, and operators
			// sometimes configure them with a leading "@" — normalize both so
			// the rendered list never repeats one principal in two spellings.
			display := strings.TrimPrefix(strings.TrimSpace(item), "@")
			if display == "" || seen[strings.ToLower(display)] {
				continue
			}
			seen[strings.ToLower(display)] = true
			dst = append(dst, display)
		}
		return dst
	}
	if db, ok := c.DatabaseConfigs()[database]; ok {
		operators = add(operators, db.OperatorTeams)
		operators = add(operators, db.OperatorUsers)
	}
	others = add(others, c.PRCommandAuthorization.AdminTeams)
	others = add(others, c.PRCommandAuthorization.AdminUsers)
	repoAdminTeams, repoAdminUsers := c.RepoAdmins(repo)
	others = add(others, repoAdminTeams)
	others = add(others, repoAdminUsers)
	return operators, others
}
