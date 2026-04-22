package local

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpsertLocalEnvironment(t *testing.T) {
	// Use a temp dir for config
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)

	// First upsert creates the file and database entry
	err := UpsertLocalEnvironment("mydb", "mysql", "staging", LocalEnvironment{
		DSN: "root@tcp(localhost:3306)/mydb",
	})
	require.NoError(t, err)

	cfg, err := LoadCLIConfig()
	require.NoError(t, err)
	require.Contains(t, cfg.Local, "mydb")
	assert.Equal(t, "mysql", cfg.Local["mydb"].Type)
	require.Contains(t, cfg.Local["mydb"].Environments, "staging")
	assert.Equal(t, "root@tcp(localhost:3306)/mydb", cfg.Local["mydb"].Environments["staging"].DSN)

	// Second upsert adds another environment without overwriting the first
	err = UpsertLocalEnvironment("mydb", "mysql", "production", LocalEnvironment{
		DSN: "root@tcp(prod-host:3306)/mydb",
	})
	require.NoError(t, err)

	cfg, err = LoadCLIConfig()
	require.NoError(t, err)
	require.Contains(t, cfg.Local["mydb"].Environments, "staging")
	require.Contains(t, cfg.Local["mydb"].Environments, "production")
	assert.Equal(t, "root@tcp(localhost:3306)/mydb", cfg.Local["mydb"].Environments["staging"].DSN)
	assert.Equal(t, "root@tcp(prod-host:3306)/mydb", cfg.Local["mydb"].Environments["production"].DSN)

	// Third upsert adds a different database
	err = UpsertLocalEnvironment("otherdb", "vitess", "production", LocalEnvironment{
		Organization: "myorg",
		Token:        "env:PS_TOKEN",
	})
	require.NoError(t, err)

	cfg, err = LoadCLIConfig()
	require.NoError(t, err)
	require.Contains(t, cfg.Local, "mydb")
	require.Contains(t, cfg.Local, "otherdb")
	assert.Equal(t, "vitess", cfg.Local["otherdb"].Type)
	assert.Equal(t, "myorg", cfg.Local["otherdb"].Environments["production"].Organization)
}

func TestLoadCLIConfig_NoFile(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)

	cfg, err := LoadCLIConfig()
	require.NoError(t, err)
	assert.Empty(t, cfg.Local)
	assert.Empty(t, cfg.Profiles)
}

func TestLoadCLIConfig_PreservesProfiles(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)

	// Write a config with profiles
	configDir := filepath.Join(tmpDir, ".schemabot")
	require.NoError(t, os.MkdirAll(configDir, 0700))
	configContent := "default_profile: staging\nprofiles:\n  staging:\n    endpoint: http://localhost:8080\n"
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "config.yaml"), []byte(configContent), 0600))

	// Upsert a local database
	err := UpsertLocalEnvironment("mydb", "mysql", "local", LocalEnvironment{
		DSN: "root@tcp(localhost:3306)/mydb",
	})
	require.NoError(t, err)

	// Verify profiles are preserved
	cfg, err := LoadCLIConfig()
	require.NoError(t, err)
	assert.Equal(t, "staging", cfg.DefaultProfile)
	require.Contains(t, cfg.Profiles, "staging")
	assert.Equal(t, "http://localhost:8080", cfg.Profiles["staging"].Endpoint)
	require.Contains(t, cfg.Local, "mydb")
}
