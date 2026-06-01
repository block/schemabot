package api

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

func TestLoadServerConfig(t *testing.T) {
	// Create temp config file
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	content := `
databases:
  testapp:
    type: mysql
    environments:
      staging:
        target: testapp-staging
        deployment: default
      production:
        target: testapp-production
        deployment: default
tern_deployments:
  default:
    staging: "localhost:9090"
    production: "localhost:9091"
repos:
  org/repo: {}
default_reviewers:
  - team/schema-reviewers
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err, "write config file")

	// Set env var
	t.Setenv("SCHEMABOT_CONFIG_FILE", configPath)

	cfg, err := LoadServerConfig()
	require.NoError(t, err, "LoadServerConfig")

	assert.Equal(t, 1, len(cfg.TernDeployments))
	assert.Equal(t, "localhost:9090", cfg.TernDeployments["default"]["staging"])
}

func TestLoadServerConfig_NoEnvVar(t *testing.T) {
	t.Setenv("SCHEMABOT_CONFIG_FILE", "")

	_, err := LoadServerConfig()
	assert.Error(t, err, "expected error when SCHEMABOT_CONFIG_FILE not set")
}

func TestLoadServerConfigFromFile(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	content := `
databases:
  testapp:
    type: mysql
    environments:
      production:
        target: testapp-production
        deployment: default
      staging:
        target: testapp-staging
        deployment: secondary
tern_deployments:
  default:
    production: "tern-prod:9090"
  secondary:
    staging: "tern-staging:9090"
repos:
  org/repo: {}
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err, "write config file")

	cfg, err := LoadServerConfigFromFile(configPath)
	require.NoError(t, err, "LoadServerConfigFromFile")

	assert.Equal(t, 2, len(cfg.TernDeployments))
	assert.Contains(t, cfg.Repos, "org/repo")
}

func TestLoadServerConfigFromFile_DSNFrom(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	content := `
storage:
  dsn_from:
    config_ref: file:/run/secrets/storage-config.yaml
    username: schemabot_user
    password_ref: file:/run/secrets/storage-password
databases:
  testapp:
    type: mysql
    environments:
      staging:
        dsn_from:
          config_ref: file:/run/secrets/testapp-config.yaml
          username: testapp_user
          password_ref: file:/run/secrets/testapp-password
          config_paths:
            host: endpoints.primary.host
            port: endpoints.primary.port
            database: endpoints.primary.database
          params:
            parseTime: "true"
`
	require.NoError(t, os.WriteFile(configPath, []byte(content), 0644), "write config file")

	cfg, err := LoadServerConfigFromFile(configPath)
	require.NoError(t, err)
	require.NotNil(t, cfg.Storage.DSNFrom)
	assert.Equal(t, "schemabot_user", cfg.Storage.DSNFrom.Username)
	require.NotNil(t, cfg.Databases["testapp"].Environments["staging"].DSNFrom)
	targetDSNFrom := cfg.Databases["testapp"].Environments["staging"].DSNFrom
	assert.Equal(t, "endpoints.primary.host", targetDSNFrom.ConfigPaths.Host)
	assert.Equal(t, "endpoints.primary.port", targetDSNFrom.ConfigPaths.Port)
	assert.Equal(t, "endpoints.primary.database", targetDSNFrom.ConfigPaths.Database)
	assert.Equal(t, map[string]string{"parseTime": "true"}, targetDSNFrom.Params)
}

func TestLoadServerConfigFromFile_NotFound(t *testing.T) {
	_, err := LoadServerConfigFromFile("/nonexistent/config.yaml")
	assert.Error(t, err, "expected error for nonexistent file")
}

func TestLoadServerConfigFromFile_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	err := os.WriteFile(configPath, []byte("invalid: yaml: content:"), 0644)
	require.NoError(t, err, "write config file")

	_, err = LoadServerConfigFromFile(configPath)
	assert.Error(t, err, "expected error for invalid YAML")
}

func TestLoadServerConfigFromFile_RejectsUnknownFields(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{
			name: "top-level field",
			content: `
tern_deployments:
  default:
    staging: "localhost:9090"
default_tern_deployment: default
`,
		},
		{
			name: "repo deployment routing",
			content: `
tern_deployments:
  default:
    staging: "localhost:9090"
repos:
  org/repo:
    default_tern_deployment: default
`,
		},
		{
			name: "database environment field",
			content: `
databases:
  testdb:
    type: mysql
    environments:
      staging:
        dsn: "root@tcp(localhost:3306)/testdb"
        extra_field: ignored
`,
		},
		{
			name: "dsn_from field",
			content: `
databases:
  testdb:
    type: mysql
    environments:
      staging:
        dsn_from:
          config_ref: file:/run/secrets/database.yaml
          username: test_user
          password_ref: file:/run/secrets/password
          extra_field: ignored
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			configPath := filepath.Join(dir, "config.yaml")
			err := os.WriteFile(configPath, []byte(tt.content), 0644)
			require.NoError(t, err, "write config file")

			_, err = LoadServerConfigFromFile(configPath)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "field")
		})
	}
}

func TestServerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ServerConfig
		wantErr bool
	}{
		{
			name: "tern deployments without databases",
			cfg: ServerConfig{
				TernDeployments: TernConfig{
					"default": {"production": "localhost:9090"},
				},
			},
			wantErr: true,
		},
		{
			name: "empty deployments",
			cfg: ServerConfig{
				TernDeployments: TernConfig{},
			},
			wantErr: true,
		},
		{
			name: "deployment with no environments",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type: "mysql",
						Environments: map[string]EnvironmentConfig{
							"staging": {Target: "cluster-staging-001", Deployment: "default"},
						},
					},
				},
				TernDeployments: TernConfig{
					"default": {},
				},
			},
			wantErr: true,
		},
		{
			name: "deployment with empty address",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type: "mysql",
						Environments: map[string]EnvironmentConfig{
							"staging": {Target: "cluster-staging-001", Deployment: "default"},
						},
					},
				},
				TernDeployments: TernConfig{
					"default": {"production": ""},
				},
			},
			wantErr: true,
		},
		{
			name: "repo allowlist does not affect deployment validation",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type: "mysql",
						Environments: map[string]EnvironmentConfig{
							"production": {Target: "cluster-production-001", Deployment: "default"},
						},
					},
				},
				TernDeployments: TernConfig{
					"default":   {"production": "localhost:9090"},
					"secondary": {"staging": "localhost:9091"},
				},
				Repos: map[string]RepoConfig{
					"org/repo": {},
				},
			},
			wantErr: false,
		},
		{
			name: "databases only (local mode)",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type:         "mysql",
						Environments: map[string]EnvironmentConfig{"staging": {DSN: "root@tcp(localhost)/mydb"}},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid required checks",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type:         "mysql",
						Environments: map[string]EnvironmentConfig{"staging": {DSN: "root@tcp(localhost)/mydb"}},
					},
				},
				RequiredChecks: []string{"Required Review", "CI / lint"},
			},
			wantErr: false,
		},
		{
			name: "required checks reject empty values",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type:         "mysql",
						Environments: map[string]EnvironmentConfig{"staging": {DSN: "root@tcp(localhost)/mydb"}},
					},
				},
				RequiredChecks: []string{"Required Review", ""},
			},
			wantErr: true,
		},
		{
			name: "required checks reject duplicate values",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type:         "mysql",
						Environments: map[string]EnvironmentConfig{"staging": {DSN: "root@tcp(localhost)/mydb"}},
					},
				},
				RequiredChecks: []string{"Required Review", "Required Review"},
			},
			wantErr: true,
		},
		{
			name: "required checks reject leading and trailing whitespace",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type:         "mysql",
						Environments: map[string]EnvironmentConfig{"staging": {DSN: "root@tcp(localhost)/mydb"}},
					},
				},
				RequiredChecks: []string{"Required Review "},
			},
			wantErr: true,
		},
		{
			name: "remote database target",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type: "mysql",
						Environments: map[string]EnvironmentConfig{
							"staging": {Target: "cluster-staging-001", Deployment: "tenant-a"},
						},
					},
				},
				TernDeployments: TernConfig{
					"tenant-a": {"staging": "localhost:9090"},
				},
			},
			wantErr: false,
		},
		{
			name: "remote database target missing target",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type: "mysql",
						Environments: map[string]EnvironmentConfig{
							"staging": {Deployment: "tenant-a"},
						},
					},
				},
				TernDeployments: TernConfig{
					"tenant-a": {"staging": "localhost:9090"},
				},
			},
			wantErr: true,
		},
		{
			name: "remote database target missing deployment",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type: "mysql",
						Environments: map[string]EnvironmentConfig{
							"staging": {Target: "cluster-staging-001"},
						},
					},
				},
				TernDeployments: TernConfig{
					"tenant-a": {"staging": "localhost:9090"},
				},
			},
			wantErr: true,
		},
		{
			name: "remote database target references deployment without environment endpoint",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type: "mysql",
						Environments: map[string]EnvironmentConfig{
							"production": {Target: "cluster-production-001", Deployment: "tenant-a"},
						},
					},
				},
				TernDeployments: TernConfig{
					"tenant-a": {"staging": "localhost:9090"},
				},
			},
			wantErr: true,
		},
		{
			name: "database environment cannot mix local dsn and remote target",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type: "mysql",
						Environments: map[string]EnvironmentConfig{
							"staging": {DSN: "root@tcp(localhost)/mydb", Target: "cluster-staging-001", Deployment: "tenant-a"},
						},
					},
				},
				TernDeployments: TernConfig{
					"tenant-a": {"staging": "localhost:9090"},
				},
			},
			wantErr: true,
		},
		{
			name: "both databases and tern_deployments (hybrid mode)",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"local-db": {
						Type:         "mysql",
						Environments: map[string]EnvironmentConfig{"staging": {DSN: "root@tcp(localhost)/localdb"}},
					},
				},
				TernDeployments: TernConfig{
					"default": {"staging": "localhost:9090"},
				},
			},
			wantErr: false,
		},
		{
			name: "hybrid mode: repo allowlist",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"local-db": {
						Type:         "mysql",
						Environments: map[string]EnvironmentConfig{"staging": {DSN: "root@tcp(localhost)/localdb"}},
					},
				},
				TernDeployments: TernConfig{
					"remote-cluster": {"staging": "localhost:9090"},
				},
				Repos: map[string]RepoConfig{
					"org/repo": {},
				},
			},
			wantErr: false,
		},
		{
			name:    "missing databases",
			cfg:     ServerConfig{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err, "Validate() should have returned an error")
			} else {
				assert.NoError(t, err, "Validate() should not have returned an error")
			}
		})
	}
}

func TestServerConfig_ValidateRejectsLocalRemoteRouteCollision(t *testing.T) {
	cfg := ServerConfig{
		Databases: map[string]DatabaseConfig{
			"primary": {
				Type: "mysql",
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/primary"},
				},
			},
			"payments": {
				Type: "mysql",
				Environments: map[string]EnvironmentConfig{
					"staging": {Target: "payments-staging", Deployment: "primary"},
				},
			},
		},
		TernDeployments: TernConfig{
			"primary": {"staging": "localhost:9090"},
		},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), `database "primary" environment "staging" uses a local dsn`)
	assert.Contains(t, err.Error(), `tern_deployments also defines deployment "primary"`)
}

func TestServerConfig_ValidateSourcePolicy(t *testing.T) {
	baseConfig := func(allowedDirs []string) ServerConfig {
		return ServerConfig{
			Databases: map[string]DatabaseConfig{
				"payments": {
					Type: storage.DatabaseTypeMySQL,
					Environments: map[string]EnvironmentConfig{
						"staging": {DSN: "root@tcp(localhost)/payments"},
					},
					AllowedRepos: []string{"octocat/hello-world"},
					AllowedDirs:  allowedDirs,
				},
			},
		}
	}

	tests := []struct {
		name       string
		config     ServerConfig
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:    "valid source policy",
			config:  baseConfig([]string{"schema/payments", "db/payments/"}),
			wantErr: false,
		},
		{
			name:       "empty schema dir",
			config:     baseConfig([]string{""}),
			wantErr:    true,
			wantErrMsg: "path is empty",
		},
		{
			name:       "absolute schema dir",
			config:     baseConfig([]string{"/schema/payments"}),
			wantErr:    true,
			wantErrMsg: "repo-relative",
		},
		{
			name:       "escaping schema dir",
			config:     baseConfig([]string{"../payments"}),
			wantErr:    true,
			wantErrMsg: "must not escape",
		},
		{
			name:       "duplicate normalized schema dir",
			config:     baseConfig([]string{"schema/payments", "schema/payments/"}),
			wantErr:    true,
			wantErrMsg: "duplicate",
		},
		{
			name: "duplicate repo",
			config: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"payments": {
						Type: storage.DatabaseTypeMySQL,
						Environments: map[string]EnvironmentConfig{
							"staging": {DSN: "root@tcp(localhost)/payments"},
						},
						AllowedRepos: []string{"octocat/hello-world", "octocat/hello-world"},
					},
				},
			},
			wantErr:    true,
			wantErrMsg: "duplicate",
		},
		{
			name: "empty repo after trimming whitespace",
			config: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"payments": {
						Type: storage.DatabaseTypeMySQL,
						Environments: map[string]EnvironmentConfig{
							"staging": {DSN: "root@tcp(localhost)/payments"},
						},
						AllowedRepos: []string{" "},
					},
				},
			},
			wantErr:    true,
			wantErrMsg: "empty value",
		},
		{
			name: "repo with surrounding whitespace",
			config: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"payments": {
						Type: storage.DatabaseTypeMySQL,
						Environments: map[string]EnvironmentConfig{
							"staging": {DSN: "root@tcp(localhost)/payments"},
						},
						AllowedRepos: []string{"octocat/hello-world "},
					},
				},
			},
			wantErr:    true,
			wantErrMsg: "leading or trailing whitespace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestServerConfig_AuthorizePlanSource(t *testing.T) {
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"payments": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/payments"},
				},
				AllowedRepos: []string{"octocat/hello-world"},
				AllowedDirs:  []string{"schema/payments"},
			},
			"open": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/open"},
				},
			},
		},
	}

	t.Run("missing server config blocks trusted source", func(t *testing.T) {
		var nilConfig *ServerConfig
		err := nilConfig.AuthorizePlanSource(PlanSourcePolicyRequest{
			Database:    "payments",
			Repository:  "octocat/hello-world",
			PullRequest: 1,
			SchemaPath:  "schema/payments",
		})

		require.Error(t, err)
		var policyErr *SourcePolicyError
		require.True(t, errors.As(err, &policyErr), "expected SourcePolicyError")
		assert.Equal(t, SourcePolicyReasonMissingServerConfig, policyErr.Reason)
	})

	tests := []struct {
		name       string
		req        PlanSourcePolicyRequest
		wantReason string
	}{
		{
			name: "database without source policy allows trusted source",
			req: PlanSourcePolicyRequest{
				Database:    "open",
				Repository:  "octocat/hello-world",
				PullRequest: 1,
				SchemaPath:  "schema/open",
			},
		},
		{
			name: "missing database config blocks trusted source",
			req: PlanSourcePolicyRequest{
				Database:    "missing",
				Repository:  "octocat/hello-world",
				PullRequest: 1,
				SchemaPath:  "schema/payments",
			},
			wantReason: SourcePolicyReasonMissingDatabaseConfig,
		},
		{
			name: "trusted source is allowed",
			req: PlanSourcePolicyRequest{
				Database:    "payments",
				Repository:  "octocat/hello-world",
				PullRequest: 1,
				SchemaPath:  "schema/payments",
			},
		},
		{
			name: "trusted descendant dir is allowed",
			req: PlanSourcePolicyRequest{
				Database:    "payments",
				Repository:  "octocat/hello-world",
				PullRequest: 1,
				SchemaPath:  "schema/payments/archive",
			},
		},
		{
			name: "missing repository is blocked",
			req: PlanSourcePolicyRequest{
				Database:    "payments",
				PullRequest: 1,
				SchemaPath:  "schema/payments",
			},
			wantReason: SourcePolicyReasonMissingRepository,
		},
		{
			name: "missing pull request is blocked",
			req: PlanSourcePolicyRequest{
				Database:   "payments",
				Repository: "octocat/hello-world",
				SchemaPath: "schema/payments",
			},
			wantReason: SourcePolicyReasonMissingPullRequest,
		},
		{
			name: "missing schema path is blocked",
			req: PlanSourcePolicyRequest{
				Database:    "payments",
				Repository:  "octocat/hello-world",
				PullRequest: 1,
			},
			wantReason: SourcePolicyReasonMissingSchemaPath,
		},
		{
			name: "unauthorized repo is blocked",
			req: PlanSourcePolicyRequest{
				Database:    "payments",
				Repository:  "octocat/orders",
				PullRequest: 1,
				SchemaPath:  "schema/payments",
			},
			wantReason: SourcePolicyReasonUnauthorizedRepo,
		},
		{
			name: "sibling directory is blocked",
			req: PlanSourcePolicyRequest{
				Database:    "payments",
				Repository:  "octocat/hello-world",
				PullRequest: 1,
				SchemaPath:  "schema/payments-archive",
			},
			wantReason: SourcePolicyReasonUnauthorizedSchemaDir,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cfg.AuthorizePlanSource(tt.req)
			if tt.wantReason == "" {
				assert.NoError(t, err)
				return
			}

			require.Error(t, err)
			var policyErr *SourcePolicyError
			require.True(t, errors.As(err, &policyErr), "expected SourcePolicyError")
			assert.Equal(t, tt.wantReason, policyErr.Reason)
		})
	}
}

func TestServerConfig_ResolveDatabaseTarget(t *testing.T) {
	cfg := ServerConfig{
		Databases: map[string]DatabaseConfig{
			"localdb": {
				Type: "mysql",
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/localdb"},
				},
			},
			"structuredlocaldb": {
				Type: "mysql",
				Environments: map[string]EnvironmentConfig{
					"staging": {
						DSNFrom: &DSNFromConfig{
							ConfigRef:   "file:/run/secrets/database.yaml",
							Username:    "test_user",
							PasswordRef: "file:/run/secrets/password",
						},
					},
				},
			},
			"remotedb": {
				Type: "vitess",
				Environments: map[string]EnvironmentConfig{
					"production": {Target: "cluster-production-001", Deployment: "tenant-a"},
				},
			},
		},
		TernDeployments: TernConfig{
			"tenant-a": {"production": "localhost:9090"},
		},
	}

	local, err := cfg.ResolveDatabaseTarget("localdb", "staging")
	require.NoError(t, err)
	assert.Equal(t, "mysql", local.DatabaseType)
	assert.Equal(t, "localdb", local.Deployment)
	assert.Equal(t, "localdb", local.Target)

	structuredLocal, err := cfg.ResolveDatabaseTarget("structuredlocaldb", "staging")
	require.NoError(t, err)
	assert.Equal(t, "mysql", structuredLocal.DatabaseType)
	assert.Equal(t, "structuredlocaldb", structuredLocal.Deployment)
	assert.Equal(t, "structuredlocaldb", structuredLocal.Target)

	remote, err := cfg.ResolveDatabaseTarget("remotedb", "production")
	require.NoError(t, err)
	assert.Equal(t, "vitess", remote.DatabaseType)
	assert.Equal(t, "tenant-a", remote.Deployment)
	assert.Equal(t, "cluster-production-001", remote.Target)

	_, err = cfg.ResolveDatabaseTarget("missing", "staging")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not configured")
}

func TestServerConfig_EnvironmentIsolatedConfigMaps(t *testing.T) {
	stagingConfig := ServerConfig{
		AllowedEnvironments: []string{"staging"},
		Databases: map[string]DatabaseConfig{
			"payments": {
				Type: "mysql",
				Environments: map[string]EnvironmentConfig{
					"staging": {Target: "payments-staging-target", Deployment: "primary"},
				},
			},
		},
		TernDeployments: TernConfig{
			"primary": {"staging": "tern-staging:9090"},
		},
	}
	require.NoError(t, stagingConfig.Validate())

	staging, err := stagingConfig.ResolveDatabaseTarget("payments", "staging")
	require.NoError(t, err)
	assert.Equal(t, "primary", staging.Deployment)
	assert.Equal(t, "payments-staging-target", staging.Target)

	_, err = stagingConfig.ResolveDatabaseTarget("payments", "production")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "production")

	productionConfig := ServerConfig{
		AllowedEnvironments: []string{"production"},
		Databases: map[string]DatabaseConfig{
			"payments": {
				Type: "mysql",
				Environments: map[string]EnvironmentConfig{
					"production": {Target: "payments-production-target", Deployment: "primary"},
				},
			},
		},
		TernDeployments: TernConfig{
			"primary": {"production": "tern-production:9090"},
		},
	}
	require.NoError(t, productionConfig.Validate())

	production, err := productionConfig.ResolveDatabaseTarget("payments", "production")
	require.NoError(t, err)
	assert.Equal(t, "primary", production.Deployment)
	assert.Equal(t, "payments-production-target", production.Target)
}

func TestServerConfig_OrderedEnvironments(t *testing.T) {
	t.Run("default order ignores client order", func(t *testing.T) {
		cfg := ServerConfig{}

		got := cfg.OrderedEnvironments([]string{"production", "staging"})

		assert.Equal(t, []string{"staging", "production"}, got)
	})

	t.Run("custom order", func(t *testing.T) {
		cfg := ServerConfig{EnvironmentOrder: []string{"sandbox", "staging", "production"}}

		got := cfg.OrderedEnvironments([]string{"production", "sandbox", "staging"})

		assert.Equal(t, []string{"sandbox", "staging", "production"}, got)
	})

	t.Run("unknown environments are deterministic", func(t *testing.T) {
		cfg := ServerConfig{}

		got := cfg.OrderedEnvironments([]string{"qa", "production", "dev", "staging"})

		assert.Equal(t, []string{"staging", "production", "dev", "qa"}, got)
	})

	t.Run("invalid order rejects duplicates", func(t *testing.T) {
		cfg := ServerConfig{
			EnvironmentOrder: []string{"staging", "staging"},
			Databases: map[string]DatabaseConfig{
				"testdb": {
					Type: "mysql",
					Environments: map[string]EnvironmentConfig{
						"staging": {DSN: "root@tcp(localhost)/testdb"},
					},
				},
			},
		}

		err := cfg.Validate()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate")
	})
}

func TestLoadServerConfigFromFile_InvalidConfig(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	// Valid YAML but invalid config (no deployments)
	content := `
repos:
  org/repo: {}
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err, "write config file")

	_, err = LoadServerConfigFromFile(configPath)
	assert.Error(t, err, "expected error for invalid config")
}

func TestDSNFromConfig_Resolve(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "database.yaml")
	passwordPath := filepath.Join(dir, "password")
	require.NoError(t, os.WriteFile(configPath, []byte(`
connections:
  primary:
    host: db.example.com
    port: 3307
    database: appdb
`), 0644))
	require.NoError(t, os.WriteFile(passwordPath, []byte("p@ss/word\n"), 0600))

	dsn, err := (&DSNFromConfig{
		ConfigRef:   "file:" + configPath,
		Username:    "app_ddl",
		PasswordRef: "file:" + passwordPath,
		ConfigPaths: DSNFromConfigPaths{
			Host:     "connections.primary.host",
			Port:     "connections.primary.port",
			Database: "connections.primary.database",
		},
		Params: map[string]string{
			"parseTime": "true",
		},
	}).Resolve()
	require.NoError(t, err)

	cfg, err := gomysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "tcp", cfg.Net)
	assert.Equal(t, "db.example.com:3307", cfg.Addr)
	assert.Equal(t, "app_ddl", cfg.User)
	assert.Equal(t, "p@ss/word", cfg.Passwd)
	assert.Equal(t, "appdb", cfg.DBName)
	assert.True(t, cfg.ParseTime)
}

func TestDSNFromConfig_ResolveDefaultPaths(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "database.yaml")
	passwordPath := filepath.Join(dir, "password")
	require.NoError(t, os.WriteFile(configPath, []byte(`
host: 127.0.0.1:3307
database: appdb
`), 0644))
	require.NoError(t, os.WriteFile(passwordPath, []byte("secret\n"), 0600))

	dsn, err := (&DSNFromConfig{
		ConfigRef:   "file:" + configPath,
		Username:    "app_user",
		PasswordRef: "file:" + passwordPath,
	}).Resolve()
	require.NoError(t, err)

	cfg, err := gomysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:3307", cfg.Addr)
	assert.Equal(t, "appdb", cfg.DBName)
	assert.Equal(t, "app_user", cfg.User)
	assert.Equal(t, "secret", cfg.Passwd)
}

func TestDSNFromConfig_ResolveErrors(t *testing.T) {
	t.Run("missing configured host path", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "database.yaml")
		passwordPath := filepath.Join(dir, "password")
		require.NoError(t, os.WriteFile(configPath, []byte(`
connection:
  database: appdb
`), 0644))
		require.NoError(t, os.WriteFile(passwordPath, []byte("secret\n"), 0600))

		_, err := (&DSNFromConfig{
			ConfigRef:   "file:" + configPath,
			Username:    "app_user",
			PasswordRef: "file:" + passwordPath,
			ConfigPaths: DSNFromConfigPaths{
				Host:     "connection.host",
				Database: "connection.database",
			},
		}).Resolve()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "read database config host")
		assert.Contains(t, err.Error(), "path not found")
	})

	t.Run("host path must contain string", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "database.yaml")
		passwordPath := filepath.Join(dir, "password")
		require.NoError(t, os.WriteFile(configPath, []byte(`
host: 1234
database: appdb
`), 0644))
		require.NoError(t, os.WriteFile(passwordPath, []byte("secret\n"), 0600))

		_, err := (&DSNFromConfig{
			ConfigRef:   "file:" + configPath,
			Username:    "app_user",
			PasswordRef: "file:" + passwordPath,
		}).Resolve()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "must contain a string")
	})

	t.Run("port path must contain integer", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "database.yaml")
		passwordPath := filepath.Join(dir, "password")
		require.NoError(t, os.WriteFile(configPath, []byte(`
host: db.example.com
port: not-a-port
database: appdb
`), 0644))
		require.NoError(t, os.WriteFile(passwordPath, []byte("secret\n"), 0600))

		_, err := (&DSNFromConfig{
			ConfigRef:   "file:" + configPath,
			Username:    "app_user",
			PasswordRef: "file:" + passwordPath,
		}).Resolve()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "must contain an integer")
	})
}

func TestServerConfig_StorageDSNFromConfig(t *testing.T) {
	dir := t.TempDir()
	databaseConfigPath := filepath.Join(dir, "storage.yaml")
	passwordPath := filepath.Join(dir, "password")
	require.NoError(t, os.WriteFile(databaseConfigPath, []byte(`
host: storage.example.com
database: schemabot
`), 0644))
	require.NoError(t, os.WriteFile(passwordPath, []byte("secret\n"), 0600))

	cfg := ServerConfig{
		Storage: StorageConfig{
			DSNFrom: &DSNFromConfig{
				ConfigRef:   "file:" + databaseConfigPath,
				Username:    "schemabot_user",
				PasswordRef: "file:" + passwordPath,
			},
		},
	}

	dsn, err := cfg.StorageDSN()
	require.NoError(t, err)

	mysqlCfg, err := gomysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "storage.example.com:3306", mysqlCfg.Addr)
	assert.Equal(t, "schemabot", mysqlCfg.DBName)
	assert.Equal(t, "schemabot_user", mysqlCfg.User)
	assert.Equal(t, "secret", mysqlCfg.Passwd)
}

func TestServerConfig_ValidateDSNFrom(t *testing.T) {
	t.Run("database cannot set both dsn and dsn_from", func(t *testing.T) {
		cfg := ServerConfig{
			Databases: map[string]DatabaseConfig{
				"testapp": {
					Type: storage.DatabaseTypeMySQL,
					Environments: map[string]EnvironmentConfig{
						"staging": {
							DSN: "root@tcp(localhost:3306)/testapp",
							DSNFrom: &DSNFromConfig{
								ConfigRef:   "file:/secrets/database.yaml",
								Username:    "testapp_user",
								PasswordRef: "file:/secrets/password",
							},
						},
					},
				},
			},
		}

		err := cfg.Validate()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot configure both dsn and dsn_from")
	})

	t.Run("database dsn_from requires config ref", func(t *testing.T) {
		cfg := ServerConfig{
			Databases: map[string]DatabaseConfig{
				"testapp": {
					Type: storage.DatabaseTypeMySQL,
					Environments: map[string]EnvironmentConfig{
						"staging": {
							DSNFrom: &DSNFromConfig{
								Username:    "testapp_user",
								PasswordRef: "file:/secrets/password",
							},
						},
					},
				},
			},
		}

		err := cfg.Validate()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing config_ref")
	})

	t.Run("storage cannot set both dsn and dsn_from", func(t *testing.T) {
		cfg := ServerConfig{
			Storage: StorageConfig{
				DSN: "root@tcp(localhost:3306)/schemabot",
				DSNFrom: &DSNFromConfig{
					ConfigRef:   "file:/secrets/database.yaml",
					Username:    "schemabot_user",
					PasswordRef: "file:/secrets/password",
				},
			},
			Databases: map[string]DatabaseConfig{
				"testapp": {
					Type: storage.DatabaseTypeMySQL,
					Environments: map[string]EnvironmentConfig{
						"staging": {DSN: "root@tcp(localhost:3306)/testapp"},
					},
				},
			},
		}

		err := cfg.Validate()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "storage cannot configure both dsn and dsn_from")
	})
}

func TestGitHubConfig_YAMLKeys(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	content := `
databases:
  testapp:
    type: mysql
    environments:
      staging:
        dsn: "root:pass@tcp(localhost:3306)/testapp"
github:
  app-id: "123456"
  private-key: "my-private-key"
  webhook-secret: "my-webhook-secret"
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadServerConfigFromFile(configPath)
	require.NoError(t, err)

	assert.Equal(t, "123456", cfg.GitHub.AppID)
	assert.Equal(t, "my-private-key", cfg.GitHub.PrivateKey)
	assert.Equal(t, "my-webhook-secret", cfg.GitHub.WebhookSecret)
}

func TestGitHubConfig_Configured(t *testing.T) {
	t.Run("not configured when empty", func(t *testing.T) {
		g := GitHubConfig{}
		assert.False(t, g.Configured())
	})

	t.Run("not configured with only private key", func(t *testing.T) {
		g := GitHubConfig{PrivateKey: "some-key"}
		assert.False(t, g.Configured())
	})

	t.Run("not configured with only app id", func(t *testing.T) {
		g := GitHubConfig{AppID: "123"}
		assert.False(t, g.Configured())
	})

	t.Run("configured with both", func(t *testing.T) {
		g := GitHubConfig{AppID: "123", PrivateKey: "some-key"}
		assert.True(t, g.Configured())
	})

	t.Run("not configured when file reference does not exist", func(t *testing.T) {
		nonexistent := filepath.Join(t.TempDir(), "nonexistent-key.pem")
		g := GitHubConfig{AppID: "123", PrivateKey: "file:" + nonexistent}
		assert.False(t, g.Configured())
	})
}

func TestGitHubConfig_ResolveAppID(t *testing.T) {
	t.Run("resolves numeric string", func(t *testing.T) {
		g := GitHubConfig{AppID: "456789"}
		assert.Equal(t, int64(456789), g.ResolveAppID())
	})

	t.Run("returns 0 for empty", func(t *testing.T) {
		g := GitHubConfig{}
		assert.Equal(t, int64(0), g.ResolveAppID())
	})

	t.Run("returns 0 for non-numeric", func(t *testing.T) {
		g := GitHubConfig{AppID: "not-a-number"}
		assert.Equal(t, int64(0), g.ResolveAppID())
	})

	t.Run("falls back to env var", func(t *testing.T) {
		t.Setenv("GITHUB_APP_ID", "999")
		g := GitHubConfig{}
		assert.Equal(t, int64(999), g.ResolveAppID())
	})

	t.Run("config takes precedence over env var", func(t *testing.T) {
		t.Setenv("GITHUB_APP_ID", "999")
		g := GitHubConfig{AppID: "123"}
		assert.Equal(t, int64(123), g.ResolveAppID())
	})
}

func TestGitHubConfig_ResolvePrivateKey(t *testing.T) {
	t.Run("resolves direct value", func(t *testing.T) {
		g := GitHubConfig{PrivateKey: "my-private-key"}
		key, err := g.ResolvePrivateKey()
		require.NoError(t, err)
		assert.Equal(t, "my-private-key", key)
	})

	t.Run("resolves env reference", func(t *testing.T) {
		t.Setenv("TEST_PK", "env-private-key")
		g := GitHubConfig{PrivateKey: "env:TEST_PK"}
		key, err := g.ResolvePrivateKey()
		require.NoError(t, err)
		assert.Equal(t, "env-private-key", key)
	})
}

func TestServerConfig_IsRepoAllowed(t *testing.T) {
	t.Run("nil repos allows all", func(t *testing.T) {
		cfg := ServerConfig{Repos: nil}
		assert.True(t, cfg.IsRepoAllowed("org/any-repo"))
		assert.True(t, cfg.IsRepoAllowed(""))
	})

	t.Run("empty repos allows all", func(t *testing.T) {
		cfg := ServerConfig{Repos: map[string]RepoConfig{}}
		assert.True(t, cfg.IsRepoAllowed("org/any-repo"))
	})

	t.Run("populated repos allows listed repo", func(t *testing.T) {
		cfg := ServerConfig{
			Repos: map[string]RepoConfig{
				"org/allowed-repo": {},
			},
		}
		assert.True(t, cfg.IsRepoAllowed("org/allowed-repo"))
	})

	t.Run("populated repos rejects unlisted repo", func(t *testing.T) {
		cfg := ServerConfig{
			Repos: map[string]RepoConfig{
				"org/allowed-repo": {},
			},
		}
		assert.False(t, cfg.IsRepoAllowed("org/other-repo"))
	})

	t.Run("multiple repos allows any listed", func(t *testing.T) {
		cfg := ServerConfig{
			Repos: map[string]RepoConfig{
				"org/repo-a": {},
				"org/repo-b": {},
			},
		}
		assert.True(t, cfg.IsRepoAllowed("org/repo-a"))
		assert.True(t, cfg.IsRepoAllowed("org/repo-b"))
		assert.False(t, cfg.IsRepoAllowed("org/repo-c"))
	})

	t.Run("nil receiver allows all", func(t *testing.T) {
		var cfg *ServerConfig
		assert.True(t, cfg.IsRepoAllowed("org/any-repo"))
	})

	t.Run("local mode repos as allowlist", func(t *testing.T) {
		cfg := ServerConfig{
			Databases: map[string]DatabaseConfig{
				"payments": {Type: "mysql"},
			},
			Repos: map[string]RepoConfig{
				"myorg/payments-service": {},
			},
		}
		assert.True(t, cfg.IsRepoAllowed("myorg/payments-service"))
		assert.False(t, cfg.IsRepoAllowed("myorg/other-repo"))
	})
}

func TestServerConfig_IsEnvironmentAllowed(t *testing.T) {
	t.Run("nil allowed_environments allows all", func(t *testing.T) {
		cfg := ServerConfig{AllowedEnvironments: nil}
		assert.True(t, cfg.IsEnvironmentAllowed("staging"))
		assert.True(t, cfg.IsEnvironmentAllowed("production"))
		assert.True(t, cfg.IsEnvironmentAllowed(""))
	})

	t.Run("empty allowed_environments allows all", func(t *testing.T) {
		cfg := ServerConfig{AllowedEnvironments: []string{}}
		assert.True(t, cfg.IsEnvironmentAllowed("staging"))
		assert.True(t, cfg.IsEnvironmentAllowed("production"))
	})

	t.Run("listed environment is allowed", func(t *testing.T) {
		cfg := ServerConfig{AllowedEnvironments: []string{"staging"}}
		assert.True(t, cfg.IsEnvironmentAllowed("staging"))
	})

	t.Run("unlisted environment is rejected", func(t *testing.T) {
		cfg := ServerConfig{AllowedEnvironments: []string{"staging"}}
		assert.False(t, cfg.IsEnvironmentAllowed("production"))
	})

	t.Run("multiple environments", func(t *testing.T) {
		cfg := ServerConfig{AllowedEnvironments: []string{"staging", "production"}}
		assert.True(t, cfg.IsEnvironmentAllowed("staging"))
		assert.True(t, cfg.IsEnvironmentAllowed("production"))
		assert.False(t, cfg.IsEnvironmentAllowed("sandbox"))
	})

	t.Run("nil receiver allows all", func(t *testing.T) {
		var cfg *ServerConfig
		assert.True(t, cfg.IsEnvironmentAllowed("staging"))
		assert.True(t, cfg.IsEnvironmentAllowed(""))
	})

	t.Run("YAML deserialization", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "config.yaml")
		content := `
databases:
  testapp:
    type: mysql
    environments:
      staging:
        dsn: "root@tcp(localhost:3306)/testapp"
allowed_environments:
  - staging
`
		err := os.WriteFile(configPath, []byte(content), 0644)
		require.NoError(t, err)

		cfg, err := LoadServerConfigFromFile(configPath)
		require.NoError(t, err)

		assert.True(t, cfg.IsEnvironmentAllowed("staging"))
		assert.False(t, cfg.IsEnvironmentAllowed("production"))
	})
}

func TestServerConfig_ShouldRequirePassingChecks(t *testing.T) {
	t.Run("nil receiver defaults to true", func(t *testing.T) {
		var cfg *ServerConfig
		assert.True(t, cfg.ShouldRequirePassingChecks())
	})

	t.Run("nil field defaults to true", func(t *testing.T) {
		cfg := &ServerConfig{}
		assert.True(t, cfg.ShouldRequirePassingChecks())
	})

	t.Run("explicitly true", func(t *testing.T) {
		cfg := &ServerConfig{RequirePassingChecks: new(true)}
		assert.True(t, cfg.ShouldRequirePassingChecks())
	})

	t.Run("explicitly false", func(t *testing.T) {
		cfg := &ServerConfig{RequirePassingChecks: new(false)}
		assert.False(t, cfg.ShouldRequirePassingChecks())
	})

	t.Run("YAML deserialization", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "config.yaml")
		content := `
databases:
  testapp:
    type: mysql
    environments:
      staging:
        dsn: "root@tcp(localhost:3306)/testapp"
require_passing_checks: false
`
		err := os.WriteFile(configPath, []byte(content), 0644)
		require.NoError(t, err)

		cfg, err := LoadServerConfigFromFile(configPath)
		require.NoError(t, err)

		assert.False(t, cfg.ShouldRequirePassingChecks())
	})
}

func TestServerConfig_ReviewPolicy(t *testing.T) {
	t.Run("nil receiver defaults to false", func(t *testing.T) {
		var cfg *ServerConfig
		assert.False(t, cfg.ReviewPolicyEnabled())
	})

	t.Run("disabled by default", func(t *testing.T) {
		cfg := &ServerConfig{}
		assert.False(t, cfg.ReviewPolicyEnabled())
	})

	t.Run("explicitly enabled", func(t *testing.T) {
		cfg := &ServerConfig{ReviewPolicy: ReviewPolicyConfig{Enabled: true}}
		assert.True(t, cfg.ReviewPolicyEnabled())
	})

	t.Run("database operators included by default", func(t *testing.T) {
		cfg := &ServerConfig{ReviewPolicy: ReviewPolicyConfig{Enabled: true}}
		assert.True(t, cfg.ReviewPolicyIncludesDatabaseOperators())
	})

	t.Run("database operators can be disabled", func(t *testing.T) {
		cfg := &ServerConfig{
			ReviewPolicy: ReviewPolicyConfig{
				Enabled:                  true,
				IncludeDatabaseOperators: new(false),
			},
		}
		assert.False(t, cfg.ReviewPolicyIncludesDatabaseOperators())
	})

	t.Run("YAML deserialization", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "config.yaml")
		content := `
databases:
  testapp:
    type: mysql
    environments:
      staging:
        dsn: "root@tcp(localhost:3306)/testapp"
review_policy:
  enabled: true
  admin_teams:
    - octocat/admins
  admin_users:
    - mona
  include_database_operators: false
  include_codeowners: true
`
		err := os.WriteFile(configPath, []byte(content), 0644)
		require.NoError(t, err)

		cfg, err := LoadServerConfigFromFile(configPath)
		require.NoError(t, err)

		assert.True(t, cfg.ReviewPolicyEnabled())
		assert.Equal(t, []string{"octocat/admins"}, cfg.ReviewPolicy.AdminTeams)
		assert.Equal(t, []string{"mona"}, cfg.ReviewPolicy.AdminUsers)
		assert.False(t, cfg.ReviewPolicyIncludesDatabaseOperators())
		assert.True(t, cfg.ReviewPolicy.IncludeCodeowners)
	})
}

func TestServerConfig_IsCheckRequired(t *testing.T) {
	t.Run("nil receiver requires all checks", func(t *testing.T) {
		var cfg *ServerConfig
		assert.True(t, cfg.IsCheckRequired("CI / lint"))
	})

	t.Run("empty list requires all checks", func(t *testing.T) {
		cfg := &ServerConfig{}
		assert.True(t, cfg.IsCheckRequired("CI / lint"))
	})

	t.Run("configured list matches exact names only", func(t *testing.T) {
		cfg := &ServerConfig{RequiredChecks: []string{"Required Review", "CI / lint"}}
		assert.True(t, cfg.IsCheckRequired("Required Review"))
		assert.True(t, cfg.IsCheckRequired("CI / lint"))
		assert.False(t, cfg.IsCheckRequired("required review"))
		assert.False(t, cfg.IsCheckRequired("CI / tests"))
	})

	t.Run("YAML deserialization", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "config.yaml")
		content := `
databases:
  testapp:
    type: mysql
    environments:
      staging:
        dsn: "root@tcp(localhost:3306)/testapp"
required_checks:
  - "Required Review"
  - "CI / lint"
`
		err := os.WriteFile(configPath, []byte(content), 0644)
		require.NoError(t, err)

		cfg, err := LoadServerConfigFromFile(configPath)
		require.NoError(t, err)

		assert.Equal(t, []string{"Required Review", "CI / lint"}, cfg.RequiredChecks)
		assert.True(t, cfg.IsCheckRequired("Required Review"))
		assert.False(t, cfg.IsCheckRequired("CI / tests"))
	})
}

func TestGitHubConfig_ResolveWebhookSecret(t *testing.T) {
	t.Run("resolves direct value", func(t *testing.T) {
		g := GitHubConfig{WebhookSecret: "my-secret"}
		secret, err := g.ResolveWebhookSecret()
		require.NoError(t, err)
		assert.Equal(t, "my-secret", secret)
	})

	t.Run("resolves env reference", func(t *testing.T) {
		t.Setenv("TEST_WS", "env-webhook-secret")
		g := GitHubConfig{WebhookSecret: "env:TEST_WS"}
		secret, err := g.ResolveWebhookSecret()
		require.NoError(t, err)
		assert.Equal(t, "env-webhook-secret", secret)
	})
}
