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

// The authorized principals for a database are split into the database's own
// operators and the broader admins (deployment-wide, then per-repo) — the two
// groups surfaced on command-rejection comments so a blocked user pings the
// operators first. A principal configured in both groups lists only as an
// operator.
func TestPRCommandAuthorizedPrincipals(t *testing.T) {
	cfg := &ServerConfig{
		PRCommandAuthorization: PRCommandAuthorizationConfig{
			Enabled:    true,
			AdminTeams: []string{"octocat/db-admins"},
			AdminUsers: []string{"kara"},
		},
		Repos: map[string]RepoConfig{
			"octocat/hello-world": {
				AdminTeams: []string{"octocat/repo-admins", "octocat/db-admins"},
				AdminUsers: []string{"mona"},
			},
		},
		Databases: map[string]DatabaseConfig{
			"orders": {
				OperatorTeams: []string{"octocat/orders-operators", "octocat/db-admins"},
				OperatorUsers: []string{"kara", "lee"},
			},
		},
	}

	operators, others := cfg.PRCommandAuthorizedPrincipals("octocat/hello-world", "orders")
	assert.Equal(t,
		[]string{"octocat/orders-operators", "octocat/db-admins", "kara", "lee"},
		operators,
		"the database's operator teams then users lead")
	assert.Equal(t,
		[]string{"octocat/repo-admins", "mona"},
		others,
		"admins already listed as operators are not repeated in the fallback group")

	operators, others = cfg.PRCommandAuthorizedPrincipals("octocat/other-repo", "orders")
	assert.Equal(t,
		[]string{"octocat/orders-operators", "octocat/db-admins", "kara", "lee"},
		operators)
	assert.Empty(t, others,
		"a repo with no config entry contributes no repo admins, and the deployment admins are all operator duplicates")

	operators, others = cfg.PRCommandAuthorizedPrincipals("octocat/hello-world", "unknown")
	assert.Empty(t, operators, "an unknown database has no operators")
	assert.Equal(t,
		[]string{"octocat/db-admins", "kara", "octocat/repo-admins", "mona"},
		others,
		"an unknown database still lists the deployment and repo admins")

	normalized := &ServerConfig{
		PRCommandAuthorization: PRCommandAuthorizationConfig{
			Enabled:    true,
			AdminTeams: []string{"@octocat/db-admins"},
			AdminUsers: []string{"Kara"},
		},
		Databases: map[string]DatabaseConfig{
			"orders": {
				OperatorTeams: []string{"octocat/DB-Admins"},
				OperatorUsers: []string{"kara", " lee "},
			},
		},
	}
	operators, others = normalized.PRCommandAuthorizedPrincipals("octocat/hello-world", "orders")
	assert.Equal(t,
		[]string{"octocat/DB-Admins", "kara", "lee"},
		operators,
		"leading @, surrounding whitespace, and case variants collapse to one entry per principal")
	assert.Empty(t, others)

	var nilCfg *ServerConfig
	operators, others = nilCfg.PRCommandAuthorizedPrincipals("octocat/hello-world", "orders")
	assert.Nil(t, operators)
	assert.Nil(t, others)
}

// command_apps lists App slugs. A malformed entry, or one naming a trusted
// sibling SchemaBot App (whose comments are SchemaBot's own and would loop if
// run as commands), is a startup error rather than a grant that never matches.
func TestServerConfigValidateCommandApps(t *testing.T) {
	baseConfig := func(apps ...string) ServerConfig {
		return ServerConfig{
			Databases: map[string]DatabaseConfig{
				"orders": {
					Type: "mysql",
					Environments: map[string]EnvironmentConfig{
						"staging": {DSN: "root@tcp(localhost)/orders"},
					},
					CommandApps: apps,
				},
			},
		}
	}

	tests := []struct {
		name    string
		cfg     ServerConfig
		wantErr string
	}{
		{
			name: "valid App slug",
			cfg:  baseConfig("deploy-gate"),
		},
		{
			name:    "bot login instead of slug",
			cfg:     baseConfig("deploy-gate[bot]"),
			wantErr: `databases.orders.command_apps contains "deploy-gate[bot]": list the App slug, without the [bot] suffix of its login`,
		},
		{
			name:    "duplicate slug",
			cfg:     baseConfig("deploy-gate", "Deploy-Gate"),
			wantErr: `databases.orders.command_apps contains duplicate value "Deploy-Gate"`,
		},
		{
			name:    "empty slug",
			cfg:     baseConfig(""),
			wantErr: "databases.orders.command_apps contains an empty value",
		},
		{
			name:    "leading fullwidth at sign",
			cfg:     baseConfig("＠deploy-gate"),
			wantErr: `databases.orders.command_apps contains "＠deploy-gate", which is not a GitHub App slug`,
		},
		{
			name:    "leading at sign",
			cfg:     baseConfig("@deploy-gate"),
			wantErr: `databases.orders.command_apps contains "@deploy-gate", which is not a GitHub App slug`,
		},
		{
			name:    "internal whitespace",
			cfg:     baseConfig("deploy gate"),
			wantErr: `databases.orders.command_apps contains "deploy gate", which is not a GitHub App slug`,
		},
		{
			name: "slug with digits and underscore",
			cfg:  baseConfig("deploy_gate-2"),
		},
		{
			name: "trusted sibling App in the legacy github block",
			cfg: func() ServerConfig {
				cfg := baseConfig("schemabot-staging")
				cfg.GitHub.TrustedCheckAppSlugs = []string{"schemabot-staging"}
				return cfg
			}(),
			wantErr: `databases.orders.command_apps contains "schemabot-staging", which is in trusted-check-app-slugs`,
		},
		{
			name: "trusted sibling App in an apps entry",
			cfg: func() ServerConfig {
				cfg := baseConfig("Schemabot-Staging")
				cfg.Apps = map[string]GitHubAppConfig{
					"app-a": {AppID: "12345", PrivateKey: "env:PK", WebhookSecret: "env:WS", TrustedCheckAppSlugs: []string{"schemabot-staging"}},
				}
				cfg.Repos = map[string]RepoConfig{"org/repo": {GitHubApp: "app-a"}}
				return cfg
			}(),
			wantErr: `databases.orders.command_apps contains "Schemabot-Staging", which is in trusted-check-app-slugs`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

// CommandAppSlug decides whether a bot comment is parsed at all, so it admits
// an App listed by any database that accepts the repo; IsCommandApp then
// decides per database.
func TestCommandAppLookups(t *testing.T) {
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders":   {CommandApps: []string{"deploy-gate"}, AllowedRepos: []string{"octocat/schemas"}},
			"payments": {},
		},
	}

	slug, ok := cfg.CommandAppSlug("octocat/schemas", "Deploy-Gate[bot]")
	assert.True(t, ok)
	assert.Equal(t, "Deploy-Gate", slug)

	_, ok = cfg.CommandAppSlug("octocat/other", "deploy-gate[bot]")
	assert.False(t, ok, "no database that lists the App accepts the repo")
	_, ok = cfg.CommandAppSlug("octocat/schemas", "deploy-gate")
	assert.False(t, ok, "a login without the [bot] suffix is not an App")
	_, ok = cfg.CommandAppSlug("octocat/schemas", "[bot]")
	assert.False(t, ok)

	assert.True(t, cfg.IsCommandApp("octocat/schemas", "orders", "deploy-gate[bot]"))
	assert.False(t, cfg.IsCommandApp("octocat/schemas", "payments", "deploy-gate[bot]"))
	assert.False(t, cfg.IsCommandApp("octocat/other", "orders", "deploy-gate[bot]"))
	assert.False(t, cfg.IsCommandApp("octocat/schemas", "missing", "deploy-gate[bot]"))

	var nilConfig *ServerConfig
	assert.False(t, nilConfig.IsCommandApp("octocat/schemas", "orders", "deploy-gate[bot]"))

	// The review gate asks whether a login is a command App anywhere, so a
	// listing for orders counts for every repository and database.
	assert.True(t, cfg.IsConfiguredCommandApp("Deploy-Gate[bot]"))
	assert.False(t, cfg.IsConfiguredCommandApp("deploy-gate"), "a login without the [bot] suffix is not an App")
	assert.False(t, cfg.IsConfiguredCommandApp("other-app[bot]"))
	assert.False(t, cfg.IsConfiguredCommandApp("[bot]"))
	assert.False(t, nilConfig.IsConfiguredCommandApp("deploy-gate[bot]"))
}
