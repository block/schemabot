//go:build !integration && !e2e

package commands

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/e2eutil"
)

func TestLoadCLIConfig_WithEnvironments(t *testing.T) {
	dir := e2eutil.WriteSchemaDir(t, "testapp", "mysql", map[string]string{
		"users.sql": "CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);",
	}, e2eutil.WithEnvironments(map[string]string{
		"staging":    "cash-aurora-staging-001",
		"production": "cash-aurora-production-001",
	}))

	cfg, err := LoadCLIConfig(dir)
	require.NoError(t, err)

	assert.Equal(t, "testapp", cfg.Database)
	assert.Equal(t, "mysql", cfg.Type)
	assert.Equal(t, "cash-aurora-staging-001", cfg.GetTarget("staging"))
	assert.Equal(t, "cash-aurora-production-001", cfg.GetTarget("production"))
	assert.Equal(t, "testapp", cfg.GetTarget("unknown"), "unknown env falls back to database name")
}

func TestLoadCLIConfig_WithoutEnvironments(t *testing.T) {
	dir := e2eutil.WriteSchemaDir(t, "testapp", "mysql", map[string]string{
		"users.sql": "CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);",
	})

	cfg, err := LoadCLIConfig(dir)
	require.NoError(t, err)

	assert.Equal(t, "testapp", cfg.Database)
	assert.Empty(t, cfg.Deployment, "deployment should be empty when not specified")
	assert.Equal(t, "testapp", cfg.GetTarget("staging"), "no environments falls back to database name")
}

func TestLoadCLIConfig_WithDeployment(t *testing.T) {
	dir := t.TempDir()
	content := "database: mydb\ntype: mysql\ndeployment: us-west\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "schemabot.yaml"), []byte(content), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "users.sql"), []byte("CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);"), 0644))

	cfg, err := LoadCLIConfig(dir)
	require.NoError(t, err)

	assert.Equal(t, "mydb", cfg.Database)
	assert.Equal(t, "us-west", cfg.Deployment)
}

func TestResolveEndpoint_ExplicitEndpoint(t *testing.T) {
	ep, err := resolveEndpoint("http://myserver:8080", "", "mydb")
	require.NoError(t, err)
	assert.Equal(t, "http://myserver:8080", ep)
}

func TestResolveEndpoint_ExplicitProfile(t *testing.T) {
	// Set up a temp config with a profile
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	configDir := filepath.Join(tmpDir, ".schemabot")
	require.NoError(t, os.MkdirAll(configDir, 0700))

	cfg := &client.Config{
		Profiles: map[string]client.Profile{
			"staging": {Endpoint: "http://staging:8080"},
		},
	}
	require.NoError(t, client.SaveConfig(cfg))

	ep, err := resolveEndpoint("", "staging", "mydb")
	require.NoError(t, err)
	assert.Equal(t, "http://staging:8080", ep)
}

func TestResolveEndpoint_DefaultProfileLocal(t *testing.T) {
	// Set up config with default_profile: local and a local database
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	configDir := filepath.Join(tmpDir, ".schemabot")
	require.NoError(t, os.MkdirAll(configDir, 0700))

	// Write config with default_profile: local
	configContent := "default_profile: local\nprofiles:\n  staging:\n    endpoint: http://staging:8080\n"
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "config.yaml"), []byte(configContent), 0600))

	// resolveEndpoint with no explicit flags should detect local profile.
	// No local databases configured, so it errors with a local-mode-specific message.
	_, err := resolveEndpoint("", "", "testdb")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "local mode")
}

func TestResolveEndpoint_ProfileLocalExplicit(t *testing.T) {
	// --profile local should trigger local mode even if default profile is different
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	configDir := filepath.Join(tmpDir, ".schemabot")
	require.NoError(t, os.MkdirAll(configDir, 0700))

	cfg := &client.Config{
		DefaultProfile: "staging",
		Profiles: map[string]client.Profile{
			"staging": {Endpoint: "http://staging:8080"},
		},
	}
	require.NoError(t, client.SaveConfig(cfg))

	// --profile local, even though default is staging
	_, err := resolveEndpoint("", "local", "testdb")
	// Should attempt local mode, not use the staging endpoint
	assert.NotContains(t, err.Error(), "staging")
}

func TestResolveEndpoint_EndpointOverridesLocalProfile(t *testing.T) {
	// Explicit --endpoint should override everything, even if default is local
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	configDir := filepath.Join(tmpDir, ".schemabot")
	require.NoError(t, os.MkdirAll(configDir, 0700))

	configContent := "default_profile: local\n"
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "config.yaml"), []byte(configContent), 0600))

	ep, err := resolveEndpoint("http://explicit:9090", "", "mydb")
	require.NoError(t, err)
	assert.Equal(t, "http://explicit:9090", ep)
}

func TestResolveEndpoint_ProfileOverridesDefaultLocal(t *testing.T) {
	// --profile staging overrides default_profile: local
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	configDir := filepath.Join(tmpDir, ".schemabot")
	require.NoError(t, os.MkdirAll(configDir, 0700))

	cfg := &client.Config{
		DefaultProfile: "local",
		Profiles: map[string]client.Profile{
			"staging": {Endpoint: "http://staging:8080"},
		},
	}
	require.NoError(t, client.SaveConfig(cfg))

	ep, err := resolveEndpoint("", "staging", "mydb")
	require.NoError(t, err)
	assert.Equal(t, "http://staging:8080", ep)
}
