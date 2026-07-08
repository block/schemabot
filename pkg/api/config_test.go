package api

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/pendingdrops"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
)

// TestServerConfig_OnFailure verifies the rollout-continuation resolver: it
// defaults to halt when unset or unconfigured, and honours an explicit
// continue so a terminal-failed deployment no longer halts later ones.
func TestServerConfig_OnFailure(t *testing.T) {
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"payments": {
				Type: "mysql",
				Environments: map[string]EnvironmentConfig{
					"unset":    {Target: "payments", Deployment: "payments-a"},
					"halt":     {Deployments: map[string]DeploymentTarget{"payments-a": {Target: "payments"}}, OnFailure: storage.OnFailureHalt},
					"continue": {Deployments: map[string]DeploymentTarget{"payments-a": {Target: "payments"}}, OnFailure: storage.OnFailureContinue},
					"pause":    {Deployments: map[string]DeploymentTarget{"payments-a": {Target: "payments"}}, OnFailure: storage.OnFailurePause},
				},
			},
		},
	}

	assert.Equal(t, storage.OnFailureHalt, cfg.OnFailure("payments", "unset"), "unset defaults to halting")
	assert.Equal(t, storage.OnFailureHalt, cfg.OnFailure("payments", "halt"), "explicit halt halts")
	assert.Equal(t, storage.OnFailureContinue, cfg.OnFailure("payments", "continue"), "explicit continue does not halt")
	assert.Equal(t, storage.OnFailurePause, cfg.OnFailure("payments", "pause"), "explicit pause resolves to pause")
	assert.Equal(t, storage.OnFailureHalt, cfg.OnFailure("payments", "missing-env"), "unconfigured env defaults to halting")
	assert.Equal(t, storage.OnFailureHalt, cfg.OnFailure("missing-db", "continue"), "unconfigured database defaults to halting")
}

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
  org/repo:
    enable_checks: false
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err, "write config file")

	cfg, err := LoadServerConfigFromFile(configPath)
	require.NoError(t, err, "LoadServerConfigFromFile")

	assert.Equal(t, 2, len(cfg.TernDeployments))
	assert.Contains(t, cfg.Repos, "org/repo")
	assert.False(t, cfg.AreChecksEnabled("org/repo"))
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
			// A data plane configured with a target_resolver resolves targets
			// dynamically and needs no database registry.
			name: "etre target_resolver without databases is valid",
			cfg: ServerConfig{
				TargetResolver: TargetResolverConfig{
					Etre: []EtreConfig{{
						Addr:         "http://etre:8080",
						DatabaseType: storage.DatabaseTypeMySQL,
						EntityType:   "aurora_cluster",
						TargetLabel:  "dsid",
						MySQL:        EtreMySQLConfig{HostField: "writer_endpoint"},
						Credentials: EtreCredentialsConfig{
							Username:    "spirit",
							PasswordRef: "env:DDL_PASSWORD",
						},
					}},
				},
			},
			wantErr: false,
		},
		{
			// The static-inventory backend is also exempt from the databases
			// requirement.
			name: "static target_resolver without databases is valid",
			cfg: ServerConfig{
				TargetResolver: TargetResolverConfig{
					Targets: map[string]inventory.StaticTarget{
						"dsid-orders-prod": {DatabaseType: "mysql", DSN: "root@tcp(localhost:3306)/"},
					},
				},
			},
			wantErr: false,
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
			name: "required checks reject the default aggregate check base name",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type:         "mysql",
						Environments: map[string]EnvironmentConfig{"staging": {DSN: "root@tcp(localhost)/mydb"}},
					},
				},
				RequiredChecks: []string{DefaultGitHubCheckName},
			},
			wantErr: true,
		},
		{
			name: "required checks reject the environment-scoped aggregate check name",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type:         "mysql",
						Environments: map[string]EnvironmentConfig{"staging": {DSN: "root@tcp(localhost)/mydb"}},
					},
				},
				RequiredChecks: []string{DefaultGitHubCheckName + " (staging)"},
			},
			wantErr: true,
		},
		{
			name: "required checks reject a custom aggregate check base name",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type:         "mysql",
						Environments: map[string]EnvironmentConfig{"staging": {DSN: "root@tcp(localhost)/mydb"}},
					},
				},
				GitHub:         GitHubConfig{CheckName: "Acme SchemaBot"},
				RequiredChecks: []string{"Acme SchemaBot (production)"},
			},
			wantErr: true,
		},
		{
			name: "required checks allow a name that merely shares the aggregate prefix",
			cfg: ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type:         "mysql",
						Environments: map[string]EnvironmentConfig{"staging": {DSN: "root@tcp(localhost)/mydb"}},
					},
				},
				RequiredChecks: []string{DefaultGitHubCheckName + " Lint"},
			},
			wantErr: false,
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

// A required_checks entry that names SchemaBot's own aggregate Check Run would
// be silently unenforced: SchemaBot checks are excluded from the passing-checks
// gate, so the gate would treat the named check as always satisfied. Config
// validation rejects such an entry and the error names the offending entry and
// the aggregate base so the operator can correct the typo.
func TestServerConfig_ValidateRejectsAggregateRequiredCheck(t *testing.T) {
	baseConfig := func(required []string) *ServerConfig {
		return &ServerConfig{
			Databases: map[string]DatabaseConfig{
				"mydb": {
					Type:         "mysql",
					Environments: map[string]EnvironmentConfig{"staging": {DSN: "root@tcp(localhost)/mydb"}},
				},
			},
			RequiredChecks: required,
		}
	}

	t.Run("default aggregate base", func(t *testing.T) {
		err := baseConfig([]string{"CI / lint", DefaultGitHubCheckName}).Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "required_checks entry \"SchemaBot\"")
		assert.Contains(t, err.Error(), "would never be enforced")
	})

	t.Run("environment-scoped aggregate name", func(t *testing.T) {
		err := baseConfig([]string{DefaultGitHubCheckName + " (production)"}).Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "required_checks entry \"SchemaBot (production)\"")
	})

	t.Run("multi-app aggregate base", func(t *testing.T) {
		cfg := baseConfig([]string{"Acme Bot (staging)"})
		cfg.Apps = map[string]GitHubAppConfig{
			"acme": {
				AppID:         "123",
				PrivateKey:    "key",
				WebhookSecret: "secret",
				CheckName:     "Acme Bot",
			},
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "required_checks entry \"Acme Bot (staging)\"")
		assert.Contains(t, err.Error(), "base \"Acme Bot\"")
	})

	t.Run("name sharing the aggregate prefix is allowed", func(t *testing.T) {
		err := baseConfig([]string{DefaultGitHubCheckName + " Lint"}).Validate()
		assert.NoError(t, err)
	})
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

// A non-empty but unparseable revert_window_duration is a startup config error
// so a typo (e.g. "30 minutes") fails closed instead of silently reverting to
// the engine default window.
func TestServerConfig_ValidateRejectsInvalidRevertWindowDuration(t *testing.T) {
	cfg := ServerConfig{
		Databases: map[string]DatabaseConfig{
			"mydb": {
				Type: "vitess",
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/mydb", RevertWindowDuration: "30 minutes"},
				},
			},
		},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), `database "mydb" environment "staging" revert_window_duration "30 minutes"`)
	assert.Contains(t, err.Error(), "not a valid duration")
}

// A revert_window_duration that parses but is non-positive ("0", "-5m") is
// rejected so a meaningless window fails closed at config load instead of
// silently reverting to the engine default window.
func TestServerConfig_ValidateRejectsNonPositiveRevertWindowDuration(t *testing.T) {
	for _, value := range []string{"0", "0s", "-5m"} {
		t.Run(value, func(t *testing.T) {
			cfg := ServerConfig{
				Databases: map[string]DatabaseConfig{
					"mydb": {
						Type: "vitess",
						Environments: map[string]EnvironmentConfig{
							"staging": {DSN: "root@tcp(localhost)/mydb", RevertWindowDuration: value},
						},
					},
				},
			}

			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), `database "mydb" environment "staging" revert_window_duration "`+value+`"`)
			assert.Contains(t, err.Error(), "must be positive")
		})
	}
}

// A well-formed revert_window_duration parses to the configured window.
func TestServerConfig_ValidateAcceptsValidRevertWindowDuration(t *testing.T) {
	cfg := ServerConfig{
		Databases: map[string]DatabaseConfig{
			"mydb": {
				Type: "vitess",
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/mydb", RevertWindowDuration: "4h"},
				},
			},
		},
	}

	require.NoError(t, cfg.Validate())

	parsed, err := time.ParseDuration(cfg.Databases["mydb"].Environments["staging"].RevertWindowDuration)
	require.NoError(t, err)
	assert.Equal(t, 4*time.Hour, parsed)
}

// An empty revert_window_duration is valid and leaves the engine default in
// place — only a non-empty unparseable value is rejected.
func TestServerConfig_ValidateAllowsEmptyRevertWindowDuration(t *testing.T) {
	cfg := ServerConfig{
		Databases: map[string]DatabaseConfig{
			"mydb": {
				Type: "vitess",
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/mydb"},
				},
			},
		},
	}

	require.NoError(t, cfg.Validate())
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

func TestServerConfig_RepoSchemaDirAllowlist(t *testing.T) {
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
			"ledger": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/ledger"},
				},
				AllowedRepos: []string{"octocat/hello-world"},
				AllowedDirs:  []string{"services/ledger/schema"},
			},
			"docs": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/docs"},
				},
				AllowedRepos: []string{"octocat/docs"},
				AllowedDirs:  []string{"schema/docs"},
			},
			"open": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/open"},
				},
				AllowedDirs: []string{"shared/open"},
			},
			"unscoped": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/unscoped"},
				},
				AllowedRepos: []string{"octocat/no-dirs"},
			},
		},
	}

	tests := []struct {
		name          string
		repo          string
		schemaPath    string
		wantAllowlist bool
		wantAllowed   bool
	}{
		{
			name:          "repo with allowed dirs allows configured path",
			repo:          "octocat/hello-world",
			schemaPath:    "schema/payments",
			wantAllowlist: true,
			wantAllowed:   true,
		},
		{
			name:          "repo with allowed dirs allows descendant path",
			repo:          "octocat/hello-world",
			schemaPath:    "services/ledger/schema/tables",
			wantAllowlist: true,
			wantAllowed:   true,
		},
		{
			name:          "repo with allowed dirs rejects sibling path",
			repo:          "octocat/hello-world",
			schemaPath:    "schema/payments_archive",
			wantAllowlist: true,
			wantAllowed:   false,
		},
		{
			name:          "repo with allowed dirs rejects local fixture path",
			repo:          "octocat/hello-world",
			schemaPath:    "schema/local/testapp",
			wantAllowlist: true,
			wantAllowed:   false,
		},
		{
			name:          "repo with allowed dirs rejects path owned by another repo",
			repo:          "octocat/hello-world",
			schemaPath:    "schema/docs",
			wantAllowlist: true,
			wantAllowed:   false,
		},
		{
			name:          "database without allowed repos applies to any repo",
			repo:          "octocat/another-repo",
			schemaPath:    "shared/open",
			wantAllowlist: true,
			wantAllowed:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantAllowlist, cfg.RepoHasSchemaDirAllowlist(tt.repo))
			assert.Equal(t, tt.wantAllowed, cfg.SchemaPathAllowedForRepo(tt.repo, tt.schemaPath))
		})
	}

	noDirCfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"unscoped": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/unscoped"},
				},
				AllowedRepos: []string{"octocat/no-dirs"},
			},
		},
	}

	assert.False(t, noDirCfg.RepoHasSchemaDirAllowlist("octocat/no-dirs"))
	assert.False(t, noDirCfg.SchemaPathAllowedForRepo("octocat/no-dirs", "anything/testapp"))
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

func TestServerConfig_ResolveDatabaseTargets(t *testing.T) {
	cfg := ServerConfig{
		Databases: map[string]DatabaseConfig{
			"localdb": {
				Type: "mysql",
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/localdb"},
				},
			},
			"scalardb": {
				Type: "vitess",
				Environments: map[string]EnvironmentConfig{
					"production": {Target: "cluster-production-001", Deployment: "tenant-a"},
				},
			},
			"multidb": {
				Type: "mysql",
				Environments: map[string]EnvironmentConfig{
					"production": {
						Deployments: map[string]DeploymentTarget{
							"payments-c": {Target: "payments"},
							"payments-a": {Target: "payments"},
							"payments-b": {Target: "payments"},
						},
					},
				},
			},
		},
		TernDeployments: TernConfig{
			"tenant-a":   {"production": "localhost:9090"},
			"payments-a": {"production": "tern-a:9090"},
			"payments-b": {"production": "tern-b:9090"},
			"payments-c": {"production": "tern-c:9090"},
		},
	}
	// This test exercises the resolver directly on a hand-built config so it can
	// cover multi-deployment resolution; deployments-map validation is covered
	// separately by TestServerConfig_DeploymentsMapValidation.

	t.Run("local DSN returns single element", func(t *testing.T) {
		got, err := cfg.ResolveDatabaseTargets("localdb", "staging")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, routing.ExecutionTarget{DatabaseType: "mysql", Deployment: "localdb", Target: "localdb"}, got[0])
	})

	t.Run("scalar remote returns single element", func(t *testing.T) {
		got, err := cfg.ResolveDatabaseTargets("scalardb", "production")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, routing.ExecutionTarget{DatabaseType: "vitess", Deployment: "tenant-a", Target: "cluster-production-001"}, got[0])
	})

	t.Run("deployments map returns sorted slice", func(t *testing.T) {
		got, err := cfg.ResolveDatabaseTargets("multidb", "production")
		require.NoError(t, err)
		assert.Equal(t, []routing.ExecutionTarget{
			{DatabaseType: "mysql", Deployment: "payments-a", Target: "payments"},
			{DatabaseType: "mysql", Deployment: "payments-b", Target: "payments"},
			{DatabaseType: "mysql", Deployment: "payments-c", Target: "payments"},
		}, got)
	})

	t.Run("implements routing interface", func(t *testing.T) {
		var resolver routing.Resolver = &cfg
		got, err := resolver.ResolveTargets(t.Context(), routing.Request{Database: "scalardb", Environment: "production"})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "tenant-a", got[0].Deployment)
		assert.Equal(t, "cluster-production-001", got[0].Target)
	})

	t.Run("unknown database errors", func(t *testing.T) {
		_, err := cfg.ResolveDatabaseTargets("missing", "production")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not configured")
	})

	t.Run("unknown environment errors", func(t *testing.T) {
		_, err := cfg.ResolveDatabaseTargets("multidb", "staging")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "staging")
	})

	t.Run("ResolveDatabaseTarget errors on multi-deployment", func(t *testing.T) {
		_, err := cfg.ResolveDatabaseTarget("multidb", "production")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ResolveDatabaseTargets")
	})

	t.Run("ResolveDatabaseTarget works for scalar remote", func(t *testing.T) {
		got, err := cfg.ResolveDatabaseTarget("scalardb", "production")
		require.NoError(t, err)
		assert.Equal(t, "tenant-a", got.Deployment)
		assert.Equal(t, "cluster-production-001", got.Target)
	})

	t.Run("ResolvePrimaryDatabaseTarget returns the lead deployment for multi-deployment", func(t *testing.T) {
		got, err := cfg.ResolvePrimaryDatabaseTarget("multidb", "production")
		require.NoError(t, err)
		assert.Equal(t, routing.ExecutionTarget{DatabaseType: "mysql", Deployment: "payments-a", Target: "payments"}, got)
	})

	t.Run("ResolvePrimaryDatabaseTarget returns the single target for scalar remote", func(t *testing.T) {
		got, err := cfg.ResolvePrimaryDatabaseTarget("scalardb", "production")
		require.NoError(t, err)
		assert.Equal(t, routing.ExecutionTarget{DatabaseType: "vitess", Deployment: "tenant-a", Target: "cluster-production-001"}, got)
	})

	t.Run("ResolvePrimaryDatabaseTarget errors on unknown database", func(t *testing.T) {
		_, err := cfg.ResolvePrimaryDatabaseTarget("missing", "production")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not configured")
	})
}

// TestServerConfig_ResolveDatabaseTargets_BypassValidate covers the cases
// where a caller constructs a ServerConfig directly without going through
// Validate() (tests, hot-reload paths, etc.). The resolver must still return
// a clear error rather than falling through to scalar routing with a
// misleading "missing server-side target" message.
func TestServerConfig_ResolveDatabaseTargets_BypassValidate(t *testing.T) {
	makeCfg := func(env EnvironmentConfig) *ServerConfig {
		return &ServerConfig{
			Databases: map[string]DatabaseConfig{
				"payments": {
					Type:         "mysql",
					Environments: map[string]EnvironmentConfig{"production": env},
				},
			},
		}
	}

	t.Run("explicitly empty deployments map errors clearly", func(t *testing.T) {
		cfg := makeCfg(EnvironmentConfig{Deployments: map[string]DeploymentTarget{}})
		_, err := cfg.ResolveDatabaseTargets("payments", "production")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "deployments map is empty")
	})

	t.Run("empty map key errors clearly", func(t *testing.T) {
		cfg := makeCfg(EnvironmentConfig{Deployments: map[string]DeploymentTarget{
			"": {Target: "payments"},
		}})
		_, err := cfg.ResolveDatabaseTargets("payments", "production")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty key")
	})

	t.Run("entry with empty target errors clearly", func(t *testing.T) {
		cfg := makeCfg(EnvironmentConfig{Deployments: map[string]DeploymentTarget{
			"payments-a": {Target: ""},
		}})
		_, err := cfg.ResolveDatabaseTargets("payments", "production")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `deployment "payments-a" missing target`)
	})
}

// TestServerConfig_ResolveDatabaseTargets_DeploymentOrder exercises the
// resolver directly on hand-built multi-deployment configs so it can cover
// deployment_order ordering and the permutation checks the resolver enforces.
func TestServerConfig_ResolveDatabaseTargets_DeploymentOrder(t *testing.T) {
	makeCfg := func(env EnvironmentConfig) *ServerConfig {
		return &ServerConfig{
			Databases: map[string]DatabaseConfig{
				"payments": {
					Type:         "mysql",
					Environments: map[string]EnvironmentConfig{"production": env},
				},
			},
		}
	}
	deployments := map[string]DeploymentTarget{
		"payments-a": {Target: "payments"},
		"payments-b": {Target: "payments"},
		"payments-c": {Target: "payments"},
	}
	resolvedOrder := func(t *testing.T, targets []routing.ExecutionTarget) []string {
		t.Helper()
		order := make([]string, 0, len(targets))
		for _, rt := range targets {
			order = append(order, rt.Deployment)
		}
		return order
	}

	t.Run("explicit deployment_order controls rollout order", func(t *testing.T) {
		cfg := makeCfg(EnvironmentConfig{
			Deployments:     deployments,
			DeploymentOrder: []string{"payments-c", "payments-a", "payments-b"},
		})
		got, err := cfg.ResolveDatabaseTargets("payments", "production")
		require.NoError(t, err)
		assert.Equal(t, []string{"payments-c", "payments-a", "payments-b"}, resolvedOrder(t, got))
	})

	t.Run("empty deployment_order falls back to alphabetical", func(t *testing.T) {
		cfg := makeCfg(EnvironmentConfig{Deployments: deployments})
		got, err := cfg.ResolveDatabaseTargets("payments", "production")
		require.NoError(t, err)
		assert.Equal(t, []string{"payments-a", "payments-b", "payments-c"}, resolvedOrder(t, got))
	})

	t.Run("primary target is the first deployment in explicit rollout order", func(t *testing.T) {
		cfg := makeCfg(EnvironmentConfig{
			Deployments:     deployments,
			DeploymentOrder: []string{"payments-c", "payments-a", "payments-b"},
		})
		got, err := cfg.ResolvePrimaryDatabaseTarget("payments", "production")
		require.NoError(t, err)
		assert.Equal(t, "payments-c", got.Deployment)
		assert.Equal(t, "payments", got.Target)
	})

	t.Run("primary target falls back to alphabetical first when order unset", func(t *testing.T) {
		cfg := makeCfg(EnvironmentConfig{Deployments: deployments})
		got, err := cfg.ResolvePrimaryDatabaseTarget("payments", "production")
		require.NoError(t, err)
		assert.Equal(t, "payments-a", got.Deployment)
	})

	t.Run("deployment_order missing a deployment errors", func(t *testing.T) {
		cfg := makeCfg(EnvironmentConfig{
			Deployments:     deployments,
			DeploymentOrder: []string{"payments-a", "payments-b"},
		})
		_, err := cfg.ResolveDatabaseTargets("payments", "production")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `deployment_order is missing deployment "payments-c"`)
	})

	t.Run("deployment_order with duplicate entry errors", func(t *testing.T) {
		cfg := makeCfg(EnvironmentConfig{
			Deployments:     deployments,
			DeploymentOrder: []string{"payments-a", "payments-a", "payments-b", "payments-c"},
		})
		_, err := cfg.ResolveDatabaseTargets("payments", "production")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `deployment_order has duplicate entry "payments-a"`)
	})

	t.Run("deployment_order with unknown deployment errors", func(t *testing.T) {
		cfg := makeCfg(EnvironmentConfig{
			Deployments:     deployments,
			DeploymentOrder: []string{"payments-a", "payments-b", "payments-c", "payments-z"},
		})
		_, err := cfg.ResolveDatabaseTargets("payments", "production")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `deployment_order references unknown deployment "payments-z"`)
	})
}

// validateDeploymentOrder must report an empty deployments map key with the
// same clear error used elsewhere, rather than the confusing
// "missing deployment \"\"" that fell out of the permutation check. This guards
// the Validate() path, which calls validateDeploymentOrder before its own
// empty-key check.
func TestValidateDeploymentOrder_EmptyMapKey(t *testing.T) {
	err := validateDeploymentOrder(
		map[string]DeploymentTarget{
			"":           {Target: "payments"},
			"payments-a": {Target: "payments"},
		},
		[]string{"payments-a"},
		"database \"payments\" environment \"production\"",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has a deployments map entry with an empty key")
	assert.NotContains(t, err.Error(), `missing deployment ""`)
}

// TestServerConfig_CutoverPolicyFor verifies the cutover-policy resolver:
// it defaults to rolling (today's serial rollout) when unset or unconfigured,
// and honours an explicit barrier policy.
func TestServerConfig_CutoverPolicyFor(t *testing.T) {
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"payments": {
				Type: "mysql",
				Environments: map[string]EnvironmentConfig{
					"policy-unset":    {Target: "payments", Deployment: "payments-a"},
					"policy-rolling":  {Deployments: map[string]DeploymentTarget{"payments-a": {Target: "payments"}}, CutoverPolicy: storage.CutoverPolicyRolling},
					"policy-barrier":  {Deployments: map[string]DeploymentTarget{"payments-a": {Target: "payments"}}, CutoverPolicy: storage.CutoverPolicyBarrier},
					"policy-parallel": {Deployments: map[string]DeploymentTarget{"payments-a": {Target: "payments"}}, CutoverPolicy: storage.CutoverPolicyParallel},
				},
			},
		},
	}

	assert.Equal(t, storage.CutoverPolicyRolling, cfg.CutoverPolicyFor("payments", "policy-unset"), "unset defaults to rolling")
	assert.Equal(t, storage.CutoverPolicyRolling, cfg.CutoverPolicyFor("payments", "policy-rolling"), "explicit rolling is rolling")
	assert.Equal(t, storage.CutoverPolicyBarrier, cfg.CutoverPolicyFor("payments", "policy-barrier"), "explicit barrier is barrier")
	assert.Equal(t, storage.CutoverPolicyParallel, cfg.CutoverPolicyFor("payments", "policy-parallel"), "explicit parallel is parallel")
	assert.Equal(t, storage.CutoverPolicyRolling, cfg.CutoverPolicyFor("payments", "missing-env"), "unconfigured env defaults to rolling")
	assert.Equal(t, storage.CutoverPolicyRolling, cfg.CutoverPolicyFor("missing-db", "policy-barrier"), "unconfigured database defaults to rolling")
}

func TestServerConfig_DeploymentsMapValidation(t *testing.T) {
	baseTern := TernConfig{
		"payments-a": {"production": "tern-a:9090"},
		"payments-b": {"production": "tern-b:9090"},
	}

	cases := []struct {
		name       string
		envConfig  EnvironmentConfig
		tern       TernConfig
		wantErrSub string
	}{
		{
			name: "valid single-entry deployments map",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
				},
			},
			tern: baseTern,
		},
		{
			name: "multi-entry deployments map is accepted",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
					"payments-b": {Target: "payments"},
				},
			},
			tern: baseTern,
		},
		{
			name: "multi-entry deployments map with deployment_order and barrier cutover is accepted",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
					"payments-b": {Target: "payments"},
				},
				DeploymentOrder: []string{"payments-a", "payments-b"},
				CutoverPolicy:   storage.CutoverPolicyBarrier,
			},
			tern: baseTern,
		},
		{
			name: "mixing scalar and map is rejected",
			envConfig: EnvironmentConfig{
				Target:     "payments",
				Deployment: "payments-a",
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
				},
			},
			tern:       baseTern,
			wantErrSub: "cannot configure both scalar target/deployment and a deployments map",
		},
		{
			name: "empty deployments map is rejected",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{},
			},
			tern:       baseTern,
			wantErrSub: "deployments map is empty",
		},
		{
			name: "entry with empty target is rejected",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: ""},
				},
			},
			tern:       baseTern,
			wantErrSub: `deployment "payments-a" missing target`,
		},
		{
			name: "unknown deployment is rejected",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"unknown-deployment": {Target: "payments"},
				},
			},
			tern:       baseTern,
			wantErrSub: `references unknown deployment "unknown-deployment"`,
		},
		{
			name: "deployment without endpoint for env is rejected",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
				},
			},
			tern: TernConfig{
				"payments-a": {"staging": "tern-a:9090"},
			},
			wantErrSub: `deployment "payments-a" has no endpoint`,
		},
		{
			name: "local DSN together with deployments map is rejected",
			envConfig: EnvironmentConfig{
				DSN: "root@tcp(localhost)/payments",
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
				},
			},
			tern:       baseTern,
			wantErrSub: "cannot configure both local DSN and target/deployment(s)",
		},
		{
			name: "single-entry deployments map with matching deployment_order",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
				},
				DeploymentOrder: []string{"payments-a"},
			},
			tern: baseTern,
		},
		{
			name: "deployment_order referencing unknown deployment is rejected",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
				},
				DeploymentOrder: []string{"payments-z"},
			},
			tern:       baseTern,
			wantErrSub: `deployment_order references unknown deployment "payments-z"`,
		},
		{
			name: "deployment_order without a deployments map is rejected",
			envConfig: EnvironmentConfig{
				Target:          "payments",
				Deployment:      "payments-a",
				DeploymentOrder: []string{"payments-a"},
			},
			tern:       baseTern,
			wantErrSub: "sets deployment_order without a deployments map",
		},
		{
			name: "cutover_policy without a deployments map is rejected",
			envConfig: EnvironmentConfig{
				Target:        "payments",
				Deployment:    "payments-a",
				CutoverPolicy: storage.CutoverPolicyBarrier,
			},
			tern:       baseTern,
			wantErrSub: "sets cutover_policy without a deployments map",
		},
		{
			name: "invalid cutover_policy value is rejected",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
				},
				CutoverPolicy: "warp-speed",
			},
			tern:       baseTern,
			wantErrSub: `invalid cutover_policy "warp-speed"`,
		},
		{
			name: "cutover_policy rolling with a deployments map is accepted",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
				},
				CutoverPolicy: storage.CutoverPolicyRolling,
			},
			tern: baseTern,
		},
		{
			name: "cutover_policy barrier with a deployments map is accepted",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
				},
				CutoverPolicy: storage.CutoverPolicyBarrier,
			},
			tern: baseTern,
		},
		{
			name: "cutover_policy parallel with a deployments map is accepted",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
				},
				CutoverPolicy: storage.CutoverPolicyParallel,
			},
			tern: baseTern,
		},
		{
			name: "on_failure without a deployments map is rejected",
			envConfig: EnvironmentConfig{
				Target:     "payments",
				Deployment: "payments-a",
				OnFailure:  storage.OnFailureContinue,
			},
			tern:       baseTern,
			wantErrSub: "sets on_failure without a deployments map",
		},
		{
			name: "on_failure halt with a deployments map is accepted",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
				},
				OnFailure: storage.OnFailureHalt,
			},
			tern: baseTern,
		},
		{
			name: "on_failure continue with a deployments map is accepted",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
				},
				OnFailure: storage.OnFailureContinue,
			},
			tern: baseTern,
		},
		{
			name: "invalid on_failure value is rejected",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
				},
				OnFailure: "warp-speed",
			},
			tern:       baseTern,
			wantErrSub: `invalid on_failure "warp-speed"`,
		},
		{
			name: "on_failure pause with a deployments map is accepted",
			envConfig: EnvironmentConfig{
				Deployments: map[string]DeploymentTarget{
					"payments-a": {Target: "payments"},
				},
				OnFailure: storage.OnFailurePause,
			},
			tern: baseTern,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ServerConfig{
				Databases: map[string]DatabaseConfig{
					"payments": {
						Type: "mysql",
						Environments: map[string]EnvironmentConfig{
							"production": tc.envConfig,
						},
					},
				},
				TernDeployments: tc.tern,
			}
			err := cfg.Validate()
			if tc.wantErrSub == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErrSub)
		})
	}
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

func TestServerConfig_AreChecksEnabled(t *testing.T) {
	t.Run("nil config defaults to enabled", func(t *testing.T) {
		var cfg *ServerConfig
		assert.True(t, cfg.AreChecksEnabled("org/repo"))
	})

	t.Run("nil repos defaults to enabled", func(t *testing.T) {
		cfg := ServerConfig{Repos: nil}
		assert.True(t, cfg.AreChecksEnabled("org/repo"))
	})

	t.Run("listed repo without override defaults to enabled", func(t *testing.T) {
		cfg := ServerConfig{
			Repos: map[string]RepoConfig{
				"org/repo": {},
			},
		}
		assert.True(t, cfg.AreChecksEnabled("org/repo"))
	})

	t.Run("unlisted repo defaults to enabled", func(t *testing.T) {
		enableChecks := false
		cfg := ServerConfig{
			Repos: map[string]RepoConfig{
				"org/repo": {EnableChecks: &enableChecks},
			},
		}
		assert.True(t, cfg.AreChecksEnabled("org/other-repo"))
	})

	t.Run("explicit false disables checks", func(t *testing.T) {
		enableChecks := false
		cfg := ServerConfig{
			Repos: map[string]RepoConfig{
				"org/repo": {EnableChecks: &enableChecks},
			},
		}
		assert.False(t, cfg.AreChecksEnabled("org/repo"))
	})

	t.Run("explicit true enables checks", func(t *testing.T) {
		enableChecks := true
		cfg := ServerConfig{
			Repos: map[string]RepoConfig{
				"org/repo": {EnableChecks: &enableChecks},
			},
		}
		assert.True(t, cfg.AreChecksEnabled("org/repo"))
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

func TestServerConfig_ShouldRespondToTenant(t *testing.T) {
	t.Run("untargeted commands are allowed", func(t *testing.T) {
		cfg := ServerConfig{Tenant: "alpha"}
		assert.True(t, cfg.ShouldRespondToTenant(""))
	})

	t.Run("matching tenant is allowed", func(t *testing.T) {
		cfg := ServerConfig{Tenant: "alpha"}
		assert.True(t, cfg.ShouldRespondToTenant("alpha"))
	})

	t.Run("different tenant is rejected", func(t *testing.T) {
		cfg := ServerConfig{Tenant: "alpha"}
		assert.False(t, cfg.ShouldRespondToTenant("beta"))
	})

	t.Run("unset tenant rejects targeted commands", func(t *testing.T) {
		cfg := ServerConfig{}
		assert.False(t, cfg.ShouldRespondToTenant("alpha"))
	})

	t.Run("nil receiver rejects targeted commands", func(t *testing.T) {
		var cfg *ServerConfig
		assert.False(t, cfg.ShouldRespondToTenant("alpha"))
	})
}

func TestServerConfig_ValidateTenantName(t *testing.T) {
	baseConfig := func() *ServerConfig {
		return &ServerConfig{
			Databases: map[string]DatabaseConfig{
				"testapp": {
					Type: "mysql",
					Environments: map[string]EnvironmentConfig{
						"staging": {DSN: "root@tcp(localhost:3306)/testapp"},
					},
				},
			},
		}
	}

	t.Run("valid tenant name", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Tenant = "alpha_1-prod"
		require.NoError(t, cfg.Validate())
	})

	t.Run("tenant with whitespace is rejected", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Tenant = "alpha beta"
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tenant must start with a letter or number")
	})

	t.Run("tenant with command-unsafe punctuation is rejected", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Tenant = "alpha@example"
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tenant must start with a letter or number")
	})

	t.Run("tenant that looks like a flag is rejected", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Tenant = "--alpha"
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tenant must start with a letter or number")
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

func TestServerConfig_ShouldClaimOperations(t *testing.T) {
	t.Run("nil receiver defaults to true", func(t *testing.T) {
		var cfg *ServerConfig
		assert.True(t, cfg.ShouldClaimOperations())
	})

	t.Run("nil field defaults to true", func(t *testing.T) {
		cfg := &ServerConfig{}
		assert.True(t, cfg.ShouldClaimOperations())
	})

	t.Run("explicitly true", func(t *testing.T) {
		cfg := &ServerConfig{OperatorClaimOperations: new(true)}
		assert.True(t, cfg.ShouldClaimOperations())
	})

	t.Run("explicitly false falls back to apply-level claiming", func(t *testing.T) {
		cfg := &ServerConfig{OperatorClaimOperations: new(false)}
		assert.False(t, cfg.ShouldClaimOperations())
	})

	t.Run("YAML omits the key and defaults to true", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "config.yaml")
		content := `
databases:
  testapp:
    type: mysql
    environments:
      staging:
        dsn: "root@tcp(localhost:3306)/testapp"
`
		err := os.WriteFile(configPath, []byte(content), 0644)
		require.NoError(t, err)

		cfg, err := LoadServerConfigFromFile(configPath)
		require.NoError(t, err)

		assert.True(t, cfg.ShouldClaimOperations())
	})

	t.Run("YAML can opt back into apply-level claiming", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "config.yaml")
		content := `
databases:
  testapp:
    type: mysql
    environments:
      staging:
        dsn: "root@tcp(localhost:3306)/testapp"
operator_claim_operations: false
`
		err := os.WriteFile(configPath, []byte(content), 0644)
		require.NoError(t, err)

		cfg, err := LoadServerConfigFromFile(configPath)
		require.NoError(t, err)

		assert.False(t, cfg.ShouldClaimOperations())
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

// validMultiAppDB returns a minimal valid Databases map for tests focused on
// the apps:/github_app: surface — every Validate() call needs a non-empty
// Databases registry.
func validMultiAppDB() map[string]DatabaseConfig {
	return map[string]DatabaseConfig{
		"app": {
			Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{
				"staging": {DSN: "root@tcp(localhost)/app"},
			},
		},
	}
}

// TestServerConfig_AppsValidation covers the back-compat matrix that gates
// the multi-App config shape and its interaction with the legacy single-App
// GitHub field and per-repo github_app routing.
func TestServerConfig_AppsValidation(t *testing.T) {
	validApp := GitHubAppConfig{
		AppID:         "12345",
		PrivateKey:    "env:DOES_NOT_MATTER_FOR_PARSE",
		WebhookSecret: "env:DOES_NOT_MATTER_FOR_PARSE",
	}

	cases := []struct {
		name       string
		github     GitHubConfig
		apps       map[string]GitHubAppConfig
		repos      map[string]RepoConfig
		wantErrSub string
	}{
		{
			name:   "legacy single-app github: still works",
			github: GitHubConfig{AppID: "42", PrivateKey: "env:PK", WebhookSecret: "env:WS"},
			repos:  map[string]RepoConfig{"org/repo": {}},
		},
		{
			name: "single-app github: with trusted check app slugs is accepted",
			github: GitHubConfig{
				AppID: "42", PrivateKey: "env:PK", WebhookSecret: "env:WS",
				TrustedCheckAppSlugs: []string{"schemabot-staging"},
			},
			repos: map[string]RepoConfig{"org/repo": {}},
		},
		{
			name: "single-app github: with empty trusted check app slug is rejected",
			github: GitHubConfig{
				AppID: "42", PrivateKey: "env:PK", WebhookSecret: "env:WS",
				TrustedCheckAppSlugs: []string{""},
			},
			repos:      map[string]RepoConfig{"org/repo": {}},
			wantErrSub: "github.trusted-check-app-slugs contains an empty value",
		},
		{
			name: "app with duplicate trusted check app slugs is rejected",
			apps: map[string]GitHubAppConfig{
				"app-a": {
					AppID: "12345", PrivateKey: "env:PK", WebhookSecret: "env:WS",
					TrustedCheckAppSlugs: []string{"schemabot-staging", "schemabot-staging"},
				},
			},
			repos:      map[string]RepoConfig{"org/repo": {GitHubApp: "app-a"}},
			wantErrSub: `app "app-a" trusted-check-app-slugs contains duplicate value "schemabot-staging"`,
		},
		{
			name: "well-formed multi-app config is accepted",
			apps: map[string]GitHubAppConfig{
				"app-a": validApp,
				"app-b": validApp,
			},
			repos: map[string]RepoConfig{
				"org-a/repo-x": {GitHubApp: "app-a"},
				"org-b/repo-y": {GitHubApp: "app-b"},
			},
		},
		{
			name:  "single-entry apps: is accepted",
			apps:  map[string]GitHubAppConfig{"only": validApp},
			repos: map[string]RepoConfig{"org/repo": {GitHubApp: "only"}},
		},
		{
			name:       "github: and apps: together is rejected",
			github:     GitHubConfig{AppID: "42", PrivateKey: "env:PK", WebhookSecret: "env:WS"},
			apps:       map[string]GitHubAppConfig{"app-a": validApp},
			repos:      map[string]RepoConfig{"org/repo": {GitHubApp: "app-a"}},
			wantErrSub: "github: and apps: are mutually exclusive",
		},
		{
			name:       "apps: present but empty is rejected",
			apps:       map[string]GitHubAppConfig{},
			wantErrSub: "apps: is configured but contains no entries",
		},
		{
			name: "app missing app-id is rejected",
			apps: map[string]GitHubAppConfig{
				"app-a": {PrivateKey: "env:PK", WebhookSecret: "env:WS"},
			},
			repos:      map[string]RepoConfig{"org/repo": {GitHubApp: "app-a"}},
			wantErrSub: `app "app-a" missing app-id`,
		},
		{
			name: "app missing private-key is rejected",
			apps: map[string]GitHubAppConfig{
				"app-a": {AppID: "42", WebhookSecret: "env:WS"},
			},
			repos:      map[string]RepoConfig{"org/repo": {GitHubApp: "app-a"}},
			wantErrSub: `app "app-a" missing private-key`,
		},
		{
			name: "app missing webhook-secret is rejected",
			apps: map[string]GitHubAppConfig{
				"app-a": {AppID: "42", PrivateKey: "env:PK"},
			},
			repos:      map[string]RepoConfig{"org/repo": {GitHubApp: "app-a"}},
			wantErrSub: `app "app-a" missing webhook-secret`,
		},
		{
			name: "repo without github_app under apps: is rejected",
			apps: map[string]GitHubAppConfig{"app-a": validApp},
			repos: map[string]RepoConfig{
				"org/repo": {},
			},
			wantErrSub: `repository "org/repo" is missing github_app`,
		},
		{
			name: "repo with unknown github_app is rejected",
			apps: map[string]GitHubAppConfig{"app-a": validApp},
			repos: map[string]RepoConfig{
				"org/repo": {GitHubApp: "app-z"},
			},
			wantErrSub: `references unknown github_app "app-z"`,
		},
		{
			name: "github_app on repo without apps: is rejected",
			repos: map[string]RepoConfig{
				"org/repo": {GitHubApp: "app-a"},
			},
			wantErrSub: `sets github_app "app-a" but apps: is not configured`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ServerConfig{
				GitHub:    tc.github,
				Apps:      tc.apps,
				Repos:     tc.repos,
				Databases: validMultiAppDB(),
			}
			err := cfg.Validate()
			if tc.wantErrSub == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErrSub)
		})
	}
}

func TestServerConfig_ResolveGitHubAppForRepo(t *testing.T) {
	validApp := GitHubAppConfig{
		AppID:         "42",
		PrivateKey:    "env:PK",
		WebhookSecret: "env:WS",
	}

	t.Run("multi-app resolves to named app", func(t *testing.T) {
		cfg := &ServerConfig{
			Apps: map[string]GitHubAppConfig{
				"app-a": validApp,
				"app-b": {AppID: "99", PrivateKey: "env:PK2", WebhookSecret: "env:WS2"},
			},
			Repos: map[string]RepoConfig{
				"org-a/repo-x": {GitHubApp: "app-a"},
				"org-b/repo-y": {GitHubApp: "app-b"},
			},
		}
		got, err := cfg.ResolveGitHubAppForRepo("org-a/repo-x")
		require.NoError(t, err)
		assert.Equal(t, "app-a", got.Name)
		assert.Equal(t, "42", got.Config.AppID)

		got, err = cfg.ResolveGitHubAppForRepo("org-b/repo-y")
		require.NoError(t, err)
		assert.Equal(t, "app-b", got.Name)
		assert.Equal(t, "99", got.Config.AppID)
	})

	t.Run("multi-app errors for repo not in repos", func(t *testing.T) {
		cfg := &ServerConfig{
			Apps: map[string]GitHubAppConfig{"app-a": validApp},
			Repos: map[string]RepoConfig{
				"org-a/repo-x": {GitHubApp: "app-a"},
			},
		}
		_, err := cfg.ResolveGitHubAppForRepo("org-z/unknown")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not declared in the repos config")
	})

	t.Run("multi-app errors when repo missing github_app", func(t *testing.T) {
		// Construct directly (bypassing Validate) to exercise the resolver's
		// defensive path.
		cfg := &ServerConfig{
			Apps:  map[string]GitHubAppConfig{"app-a": validApp},
			Repos: map[string]RepoConfig{"org/repo": {}},
		}
		_, err := cfg.ResolveGitHubAppForRepo("org/repo")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing github_app")
	})

	t.Run("multi-app errors when github_app references unknown app", func(t *testing.T) {
		cfg := &ServerConfig{
			Apps:  map[string]GitHubAppConfig{"app-a": validApp},
			Repos: map[string]RepoConfig{"org/repo": {GitHubApp: "app-z"}},
		}
		_, err := cfg.ResolveGitHubAppForRepo("org/repo")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `unknown github_app "app-z"`)
	})

	t.Run("legacy single-app resolves to default name", func(t *testing.T) {
		t.Setenv("PK", "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----")
		cfg := &ServerConfig{
			GitHub: GitHubConfig{
				AppID:         "42",
				PrivateKey:    "env:PK",
				WebhookSecret: "env:WS",
			},
		}
		got, err := cfg.ResolveGitHubAppForRepo("any/repo")
		require.NoError(t, err)
		assert.Equal(t, "default", got.Name)
		assert.Equal(t, "42", got.Config.AppID)
	})

	t.Run("legacy single-app errors when not configured", func(t *testing.T) {
		cfg := &ServerConfig{}
		_, err := cfg.ResolveGitHubAppForRepo("any/repo")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no GitHub App is configured")
	})

	t.Run("nil receiver errors", func(t *testing.T) {
		var cfg *ServerConfig
		_, err := cfg.ResolveGitHubAppForRepo("any/repo")
		require.Error(t, err)
	})
}

func TestServerConfig_GitHubCheckNameBaseForRepo(t *testing.T) {
	t.Run("legacy single-app defaults", func(t *testing.T) {
		cfg := &ServerConfig{}
		assert.Equal(t, DefaultGitHubCheckName, cfg.GitHubCheckNameBaseForRepo("any/repo"))
	})

	t.Run("legacy single-app trims configured name", func(t *testing.T) {
		cfg := &ServerConfig{GitHub: GitHubConfig{CheckName: "  SchemaBot X  "}}
		assert.Equal(t, "SchemaBot X", cfg.GitHubCheckNameBaseForRepo("any/repo"))
	})

	t.Run("multi-app uses repository app name", func(t *testing.T) {
		cfg := &ServerConfig{
			Apps: map[string]GitHubAppConfig{
				"app-a": {CheckName: "SchemaBot X"},
				"app-b": {CheckName: "SchemaBot Y"},
			},
			Repos: map[string]RepoConfig{
				"org-a/repo-x": {GitHubApp: "app-a"},
				"org-b/repo-y": {GitHubApp: "app-b"},
			},
		}

		assert.Equal(t, "SchemaBot X", cfg.GitHubCheckNameBaseForRepo("org-a/repo-x"))
		assert.Equal(t, "SchemaBot Y", cfg.GitHubCheckNameBaseForRepo("org-b/repo-y"))
	})

	t.Run("multi-app falls back for unknown repository", func(t *testing.T) {
		cfg := &ServerConfig{
			Apps:  map[string]GitHubAppConfig{"app-a": {CheckName: "SchemaBot X"}},
			Repos: map[string]RepoConfig{"org/repo": {GitHubApp: "app-a"}},
		}
		assert.Equal(t, DefaultGitHubCheckName, cfg.GitHubCheckNameBaseForRepo("org/unknown"))
	})
}

func TestServerConfig_ResolveGitHubAppsByID(t *testing.T) {
	t.Run("multi-app config resolves each entry's app-id", func(t *testing.T) {
		cfg := &ServerConfig{
			Apps: map[string]GitHubAppConfig{
				"app-a": {AppID: "1001", PrivateKey: "x", WebhookSecret: "y"},
				"app-b": {AppID: "1002", PrivateKey: "x", WebhookSecret: "y"},
			},
		}
		got, err := cfg.ResolveGitHubAppsByID()
		require.NoError(t, err)
		assert.Len(t, got, 2)
		assert.Equal(t, "app-a", got[1001].Name)
		assert.Equal(t, "app-b", got[1002].Name)
	})

	t.Run("legacy single-app config resolves under default name", func(t *testing.T) {
		// Configured() requires the private key to resolve non-empty, so
		// inline the key material via env: so the test is hermetic.
		t.Setenv("RESOLVE_BY_ID_LEGACY_PK", "private-key-bytes")
		cfg := &ServerConfig{
			GitHub: GitHubConfig{AppID: "42", PrivateKey: "env:RESOLVE_BY_ID_LEGACY_PK", WebhookSecret: "y"},
		}
		got, err := cfg.ResolveGitHubAppsByID()
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "default", got[42].Name)
	})

	t.Run("duplicate app-id across two Apps fails closed", func(t *testing.T) {
		cfg := &ServerConfig{
			Apps: map[string]GitHubAppConfig{
				"app-a": {AppID: "1001", PrivateKey: "x", WebhookSecret: "y"},
				"app-b": {AppID: "1001", PrivateKey: "x", WebhookSecret: "y"},
			},
		}
		_, err := cfg.ResolveGitHubAppsByID()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resolve to the same app-id 1001")
	})

	t.Run("empty app-id fails closed", func(t *testing.T) {
		cfg := &ServerConfig{
			Apps: map[string]GitHubAppConfig{
				"app-a": {AppID: "", PrivateKey: "x", WebhookSecret: "y"},
			},
		}
		_, err := cfg.ResolveGitHubAppsByID()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty or unparseable app-id")
	})

	t.Run("nil receiver errors", func(t *testing.T) {
		var cfg *ServerConfig
		_, err := cfg.ResolveGitHubAppsByID()
		require.Error(t, err)
	})
}

// TestLoadServerConfigFromFile_MultiApp end-to-end-parses the new YAML shape
// to confirm the apps: map and per-repo github_app: field round-trip through
// the YAML decoder (including KnownFields(true)) and that LoadServerConfigFromFile
// accepts a well-formed multi-App configuration.
func TestLoadServerConfigFromFile_MultiApp(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	content := `
apps:
  app-a:
    app-id: "env:APP_A_ID"
    private-key: "env:APP_A_PK"
    webhook-secret: "env:APP_A_WS"
    check-name: "SchemaBot X"
  app-b:
    app-id: "env:APP_B_ID"
    private-key: "env:APP_B_PK"
    webhook-secret: "env:APP_B_WS"

databases:
  testapp:
    type: mysql
    environments:
      staging:
        dsn: "root@tcp(localhost)/testapp"

repos:
  org-a/repo-x:
    github_app: app-a
  org-b/repo-y:
    github_app: app-b
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	// Parse-only round-trip: confirm the YAML field tags decode correctly,
	// independent of the Validate() hard-block.
	raw, err := os.ReadFile(configPath)
	require.NoError(t, err)
	var parsed ServerConfig
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&parsed))
	require.Contains(t, parsed.Apps, "app-a")
	require.Contains(t, parsed.Apps, "app-b")
	assert.Equal(t, "env:APP_A_ID", parsed.Apps["app-a"].AppID)
	assert.Equal(t, "SchemaBot X", parsed.Apps["app-a"].CheckName)
	assert.Equal(t, "app-a", parsed.Repos["org-a/repo-x"].GitHubApp)
	assert.Equal(t, "app-b", parsed.Repos["org-b/repo-y"].GitHubApp)

	// Full load goes through Validate() and must accept the multi-App shape.
	cfg, err := LoadServerConfigFromFile(configPath)
	require.NoError(t, err)
	require.Contains(t, cfg.Apps, "app-a")
	require.Contains(t, cfg.Apps, "app-b")
	assert.Equal(t, "app-a", cfg.Repos["org-a/repo-x"].GitHubApp)
	assert.Equal(t, "app-b", cfg.Repos["org-b/repo-y"].GitHubApp)
}

func TestPendingDropsConfig(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }

	t.Run("enabled by default", func(t *testing.T) {
		cfg := ServerConfig{}
		assert.True(t, cfg.PendingDropsEnabled())
	})

	t.Run("explicit disable", func(t *testing.T) {
		cfg := ServerConfig{PendingDrops: PendingDropsConfig{Enabled: boolPtr(false)}}
		assert.False(t, cfg.PendingDropsEnabled())
		assert.False(t, cfg.PendingDropsCleanupEnabled())
	})

	t.Run("cleanup enabled by default", func(t *testing.T) {
		cfg := ServerConfig{}
		assert.True(t, cfg.PendingDropsCleanupEnabled())
	})

	t.Run("cleanup can be disabled without disabling quarantine", func(t *testing.T) {
		cfg := ServerConfig{PendingDrops: PendingDropsConfig{CleanupEnabled: boolPtr(false)}}
		assert.True(t, cfg.PendingDropsEnabled())
		assert.False(t, cfg.PendingDropsCleanupEnabled())
	})

	t.Run("default retention", func(t *testing.T) {
		cfg := ServerConfig{}
		retention, err := cfg.PendingDropsRetention()
		require.NoError(t, err)
		assert.Equal(t, pendingdrops.DefaultRetention, retention)
	})

	t.Run("custom retention", func(t *testing.T) {
		cfg := ServerConfig{PendingDrops: PendingDropsConfig{Retention: "48h"}}
		retention, err := cfg.PendingDropsRetention()
		require.NoError(t, err)
		assert.Equal(t, 48*time.Hour, retention)
	})

	t.Run("invalid retention rejected", func(t *testing.T) {
		cfg := ServerConfig{PendingDrops: PendingDropsConfig{Retention: "soon"}}
		_, err := cfg.PendingDropsRetention()
		assert.ErrorContains(t, err, "pending_drops.retention")
	})

	t.Run("non-positive retention rejected", func(t *testing.T) {
		cfg := ServerConfig{PendingDrops: PendingDropsConfig{Retention: "-1h"}}
		_, err := cfg.PendingDropsRetention()
		assert.ErrorContains(t, err, "must be positive")
	})

	t.Run("validate rejects invalid retention", func(t *testing.T) {
		cfg := ServerConfig{
			Databases: map[string]DatabaseConfig{
				"mydb": {
					Type: "mysql",
					Environments: map[string]EnvironmentConfig{
						"staging": {DSN: "root:pass@tcp(localhost:3306)/mydb"},
					},
				},
			},
			PendingDrops: PendingDropsConfig{Retention: "not-a-duration"},
		}
		err := cfg.Validate()
		assert.ErrorContains(t, err, "pending_drops.retention")
	})

	t.Run("validate ignores invalid retention when pending drops disabled", func(t *testing.T) {
		cfg := ServerConfig{
			Databases: map[string]DatabaseConfig{
				"mydb": {
					Type: "mysql",
					Environments: map[string]EnvironmentConfig{
						"staging": {DSN: "root:pass@tcp(localhost:3306)/mydb"},
					},
				},
			},
			PendingDrops: PendingDropsConfig{Enabled: boolPtr(false), Retention: "not-a-duration"},
		}
		assert.NoError(t, cfg.Validate())
	})
}

func TestSupportChannelConfig(t *testing.T) {
	validConfig := func() ServerConfig {
		return ServerConfig{
			Databases: map[string]DatabaseConfig{
				"mydb": {
					Type: "mysql",
					Environments: map[string]EnvironmentConfig{
						"staging": {DSN: "root:pass@tcp(localhost:3306)/mydb"},
					},
				},
			},
		}
	}

	t.Run("disabled by default", func(t *testing.T) {
		cfg := validConfig()
		require.NoError(t, cfg.Validate())
		assert.False(t, cfg.SupportChannel.Enabled())
	})

	t.Run("valid support channel", func(t *testing.T) {
		cfg := validConfig()
		cfg.SupportChannel = SupportChannelConfig{Name: "#schema-help", URL: "https://example.com/schema-help"}
		require.NoError(t, cfg.Validate())
		assert.True(t, cfg.SupportChannel.Enabled())
	})

	t.Run("name requires url", func(t *testing.T) {
		cfg := validConfig()
		cfg.SupportChannel = SupportChannelConfig{Name: "#schema-help"}
		err := cfg.Validate()
		assert.ErrorContains(t, err, "support_channel.url is required")
	})

	t.Run("url requires name", func(t *testing.T) {
		cfg := validConfig()
		cfg.SupportChannel = SupportChannelConfig{URL: "https://example.com/schema-help"}
		err := cfg.Validate()
		assert.ErrorContains(t, err, "support_channel.name is required")
	})

	t.Run("url must be absolute http", func(t *testing.T) {
		cfg := validConfig()
		cfg.SupportChannel = SupportChannelConfig{Name: "#schema-help", URL: "irc://example.com/schema-help"}
		err := cfg.Validate()
		assert.ErrorContains(t, err, "support_channel.url must use http or https")
	})

	t.Run("url must not include credentials", func(t *testing.T) {
		cfg := validConfig()
		cfg.SupportChannel = SupportChannelConfig{Name: "#schema-help", URL: "https://user:pass@example.com/schema-help"}
		err := cfg.Validate()
		assert.ErrorContains(t, err, "support_channel.url must not include credentials")
	})

	t.Run("url must not contain whitespace", func(t *testing.T) {
		cfg := validConfig()
		cfg.SupportChannel = SupportChannelConfig{Name: "#schema-help", URL: "https://example.com/schema help"}
		err := cfg.Validate()
		assert.ErrorContains(t, err, "support_channel.url contains whitespace or control characters")
	})

	t.Run("url must not contain markdown-unsafe delimiters", func(t *testing.T) {
		cfg := validConfig()
		cfg.SupportChannel = SupportChannelConfig{Name: "#schema-help", URL: "https://example.com/schema-help)"}
		err := cfg.Validate()
		assert.ErrorContains(t, err, "support_channel.url contains characters that are unsafe in Markdown links")
	})
}

func TestPendingDropsTargetsResolveEachPass(t *testing.T) {
	dsnPath := filepath.Join(t.TempDir(), "target.dsn")
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"mydb": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "file:" + dsnPath},
				},
			},
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := New(nil, cfg, nil, logger)

	targets, unresolved := svc.pendingDropsTargets(t.Context())
	assert.Empty(t, targets)
	assert.Equal(t, 1, unresolved)

	dsn := "root:testpassword@tcp(127.0.0.1:3306)/testdb?timeout=1s"
	require.NoError(t, os.WriteFile(dsnPath, []byte(dsn), 0o600))

	targets, unresolved = svc.pendingDropsTargets(t.Context())
	require.Len(t, targets, 1)
	assert.Equal(t, 0, unresolved)
	assert.Equal(t, pendingdrops.Target{Database: "mydb", Environment: "staging", DSN: dsn}, targets[0])
}

func TestSchemaDirHintsForRepo(t *testing.T) {
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"widgets": {
				AllowedRepos: []string{"octocat/hello-world"},
				AllowedDirs:  []string{"apps/widgets/schema", "apps/widgets/legacy/"},
			},
			"payments": {
				AllowedRepos: []string{"octocat/other-repo"},
				AllowedDirs:  []string{"payments/schema"},
			},
			"anyrepo": {
				AllowedDirs: []string{"shared/schema", "apps/widgets/schema"},
			},
			"unbounded": {
				AllowedRepos: []string{"octocat/hello-world"},
				AllowedDirs:  []string{"*", "", " ", "."},
			},
		},
	}

	hints, exhaustive := cfg.SchemaDirHintsForRepo("octocat/hello-world")

	assert.Equal(t, []string{"apps/widgets/legacy", "apps/widgets/schema", "shared/schema"}, hints)
	assert.False(t, exhaustive, "the unbounded database accepts configs outside every probe-able directory")
}

func TestSchemaDirHintsForRepoExhaustiveWhenAllDirsLiteral(t *testing.T) {
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"widgets": {
				AllowedRepos: []string{"octocat/hello-world"},
				AllowedDirs:  []string{"apps/widgets/schema"},
			},
			"payments": {
				AllowedRepos: []string{"octocat/other-repo"},
			},
		},
	}

	hints, exhaustive := cfg.SchemaDirHintsForRepo("octocat/hello-world")

	assert.Equal(t, []string{"apps/widgets/schema"}, hints)
	assert.True(t, exhaustive, "every repo-eligible database restricts configs to literal directories")
}

func TestSchemaDirHintsForRepoNotExhaustiveWhenDatabaseHasNoAllowedDirs(t *testing.T) {
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"widgets": {
				AllowedRepos: []string{"octocat/hello-world"},
				AllowedDirs:  []string{"apps/widgets/schema"},
			},
			"open": {
				AllowedRepos: []string{"octocat/hello-world"},
			},
		},
	}

	hints, exhaustive := cfg.SchemaDirHintsForRepo("octocat/hello-world")

	assert.Equal(t, []string{"apps/widgets/schema"}, hints)
	assert.False(t, exhaustive, "a database without allowed_dirs accepts a config anywhere in the repo")
}

func TestSchemaDirHintsForRepoNoMatches(t *testing.T) {
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"payments": {
				AllowedRepos: []string{"octocat/other-repo"},
				AllowedDirs:  []string{"payments/schema"},
			},
		},
	}

	hints, exhaustive := cfg.SchemaDirHintsForRepo("octocat/hello-world")
	assert.Empty(t, hints)
	assert.True(t, exhaustive, "no repo-eligible database means no policy-valid config location exists")
}
