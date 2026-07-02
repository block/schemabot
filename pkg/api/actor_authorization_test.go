package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseGitHubTeamPrincipal(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		wantOrg  string
		wantSlug string
		wantErr  string
	}{
		{
			name:     "valid org and team slug",
			value:    "octocat/db-operators",
			wantOrg:  "octocat",
			wantSlug: "db-operators",
		},
		{
			name:    "missing slash",
			value:   "db-operators",
			wantErr: "org/team-slug",
		},
		{
			name:    "extra slash",
			value:   "octocat/platform/db-operators",
			wantErr: "org/team-slug",
		},
		{
			name:    "leading whitespace",
			value:   " octocat/db-operators",
			wantErr: "whitespace",
		},
		{
			name:    "empty org",
			value:   "/db-operators",
			wantErr: "org/team-slug",
		},
		{
			name:    "empty team",
			value:   "octocat/",
			wantErr: "org/team-slug",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			org, slug, err := ParseGitHubTeamPrincipal(tt.value)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantOrg, org)
			assert.Equal(t, tt.wantSlug, slug)
		})
	}
}

func TestServerConfigValidatePRCommandAuthorization(t *testing.T) {
	baseConfig := func() ServerConfig {
		return ServerConfig{
			Databases: map[string]DatabaseConfig{
				"orders": {
					Type: "mysql",
					Environments: map[string]EnvironmentConfig{
						"staging": {DSN: "root@tcp(localhost)/orders"},
					},
				},
			},
		}
	}

	tests := []struct {
		name    string
		mutate  func(*ServerConfig)
		wantErr string
	}{
		{
			name: "valid admin and database operators",
			mutate: func(cfg *ServerConfig) {
				cfg.PRCommandAuthorization = PRCommandAuthorizationConfig{
					Enabled:    true,
					AdminTeams: []string{"octocat/schema-admins"},
					AdminUsers: []string{"hubot"},
				}
				cfg.Databases["orders"] = DatabaseConfig{
					Type: "mysql",
					Environments: map[string]EnvironmentConfig{
						"staging": {DSN: "root@tcp(localhost)/orders"},
					},
					OperatorTeams: []string{"octocat/orders-operators"},
					OperatorUsers: []string{"mona"},
				}
			},
		},
		{
			name: "users only is valid",
			mutate: func(cfg *ServerConfig) {
				cfg.PRCommandAuthorization = PRCommandAuthorizationConfig{
					Enabled:    true,
					AdminUsers: []string{"hubot"},
				}
				db := cfg.Databases["orders"]
				db.OperatorUsers = []string{"mona"}
				cfg.Databases["orders"] = db
			},
		},
		{
			name: "invalid admin team",
			mutate: func(cfg *ServerConfig) {
				cfg.PRCommandAuthorization = PRCommandAuthorizationConfig{
					Enabled:    true,
					AdminTeams: []string{"schema-admins"},
				}
			},
			wantErr: "pr_command_authorization.admin_teams",
		},
		{
			name: "duplicate admin users are rejected case-insensitively",
			mutate: func(cfg *ServerConfig) {
				cfg.PRCommandAuthorization = PRCommandAuthorizationConfig{
					Enabled:    true,
					AdminUsers: []string{"Mona", "mona"},
				}
			},
			wantErr: "duplicate",
		},
		{
			name: "invalid database operator team",
			mutate: func(cfg *ServerConfig) {
				db := cfg.Databases["orders"]
				db.OperatorTeams = []string{"orders-operators"}
				cfg.Databases["orders"] = db
			},
			wantErr: "databases.orders.operator_teams",
		},
		{
			name: "duplicate database operator users are rejected case-insensitively",
			mutate: func(cfg *ServerConfig) {
				db := cfg.Databases["orders"]
				db.OperatorUsers = []string{"Mona", "mona"}
				cfg.Databases["orders"] = db
			},
			wantErr: "duplicate",
		},
		{
			name: "valid repo admins",
			mutate: func(cfg *ServerConfig) {
				cfg.Repos = map[string]RepoConfig{
					"octocat/hello-world": {
						AdminTeams: []string{"octocat/repo-admins"},
						AdminUsers: []string{"kara"},
					},
				}
			},
		},
		{
			name: "invalid repo admin team",
			mutate: func(cfg *ServerConfig) {
				cfg.Repos = map[string]RepoConfig{
					"octocat/hello-world": {AdminTeams: []string{"repo-admins"}},
				}
			},
			wantErr: "repos.octocat/hello-world.admin_teams",
		},
		{
			name: "invalid repo admin user",
			mutate: func(cfg *ServerConfig) {
				cfg.Repos = map[string]RepoConfig{
					"octocat/hello-world": {AdminUsers: []string{"octocat/kara"}},
				}
			},
			wantErr: "repos.octocat/hello-world.admin_users",
		},
		{
			name: "username cannot contain a slash",
			mutate: func(cfg *ServerConfig) {
				cfg.PRCommandAuthorization = PRCommandAuthorizationConfig{
					Enabled:    true,
					AdminUsers: []string{"octocat/mona"},
				}
			},
			wantErr: "must not contain /",
		},
		{
			name: "valid review policy admins",
			mutate: func(cfg *ServerConfig) {
				cfg.ReviewPolicy = ReviewPolicyConfig{
					Enabled:    true,
					AdminTeams: []string{"octocat/db-admins"},
					AdminUsers: []string{"mona"},
				}
			},
		},
		{
			name: "invalid review policy admin team",
			mutate: func(cfg *ServerConfig) {
				cfg.ReviewPolicy = ReviewPolicyConfig{
					Enabled:    true,
					AdminTeams: []string{"db-admins"},
				}
			},
			wantErr: "review_policy.admin_teams",
		},
		{
			name: "invalid review policy admin user",
			mutate: func(cfg *ServerConfig) {
				cfg.ReviewPolicy = ReviewPolicyConfig{
					Enabled:    true,
					AdminUsers: []string{"octocat/mona"},
				}
			},
			wantErr: "review_policy.admin_users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
