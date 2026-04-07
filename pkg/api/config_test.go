package api

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadServerConfig(t *testing.T) {
	// Create temp config file
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	content := `
tern_deployments:
  default:
    staging: "localhost:9090"
    production: "localhost:9091"
repos:
  org/repo:
    default_tern_deployment: default
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
tern_deployments:
  default:
    production: "tern-prod:9090"
  secondary:
    staging: "tern-staging:9090"
repos:
  org/repo:
    default_tern_deployment: secondary
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err, "write config file")

	cfg, err := LoadServerConfigFromFile(configPath)
	require.NoError(t, err, "LoadServerConfigFromFile")

	assert.Equal(t, 2, len(cfg.TernDeployments))
	assert.Equal(t, "secondary", cfg.Repos["org/repo"].DefaultTernDeployment)
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

func TestServerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ServerConfig
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: ServerConfig{
				TernDeployments: TernConfig{
					"default": {"production": "localhost:9090"},
				},
			},
			wantErr: false,
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
				TernDeployments: TernConfig{
					"default": {},
				},
			},
			wantErr: true,
		},
		{
			name: "deployment with empty address",
			cfg: ServerConfig{
				TernDeployments: TernConfig{
					"default": {"production": ""},
				},
			},
			wantErr: true,
		},
		{
			name: "repo references unknown deployment",
			cfg: ServerConfig{
				TernDeployments: TernConfig{
					"default": {"production": "localhost:9090"},
				},
				Repos: map[string]RepoConfig{
					"org/repo": {DefaultTernDeployment: "nonexistent"},
				},
			},
			wantErr: true,
		},
		{
			name: "repo references valid deployment",
			cfg: ServerConfig{
				TernDeployments: TernConfig{
					"default":   {"production": "localhost:9090"},
					"secondary": {"staging": "localhost:9091"},
				},
				Repos: map[string]RepoConfig{
					"org/repo": {DefaultTernDeployment: "secondary"},
				},
			},
			wantErr: false,
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

func TestServerConfig_TernDeployment(t *testing.T) {
	cfg := ServerConfig{
		TernDeployments: TernConfig{
			"default":   {"production": "localhost:9090"},
			"secondary": {"staging": "localhost:9091"},
		},
		Repos: map[string]RepoConfig{
			"org/custom-repo": {DefaultTernDeployment: "secondary"},
		},
	}

	// Test repo with custom deployment
	dep := cfg.TernDeployment("org/custom-repo")
	assert.Equal(t, "secondary", dep)

	// Test repo without custom deployment (falls back to default)
	dep = cfg.TernDeployment("org/other-repo")
	assert.Equal(t, DefaultDeployment, dep)
}

func TestLoadServerConfigFromFile_InvalidConfig(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	// Valid YAML but invalid config (no deployments)
	content := `
repos:
  org/repo:
    default_tern_deployment: default
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err, "write config file")

	_, err = LoadServerConfigFromFile(configPath)
	assert.Error(t, err, "expected error for invalid config")
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
