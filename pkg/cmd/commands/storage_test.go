package commands

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeStorageTestConfig writes a minimal valid server config with the given
// storage section and returns its path.
func writeStorageTestConfig(t *testing.T, storageSection string) string {
	t.Helper()
	content := storageSection + `
databases:
  testapp:
    type: mysql
    environments:
      production:
        target: testapp-production
        deployment: default
tern_deployments:
  default:
    production: "tern-prod:9090"
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

func TestResolveStorageDSN_DirectDSNBypassesConfig(t *testing.T) {
	cmd := &ResyncIdentitySequencesCmd{DSN: "postgres://schemabot:test@localhost:5432/schemabot"}
	dsn, source, err := cmd.resolveStorageDSN()
	require.NoError(t, err)
	assert.Equal(t, cmd.DSN, dsn)
	assert.Equal(t, "--dsn flag", source)
}

func TestResolveStorageDSN_DSNAndConfigAreMutuallyExclusive(t *testing.T) {
	cmd := &ResyncIdentitySequencesCmd{DSN: "postgres://localhost/schemabot", Config: "/etc/schemabot/config.yaml"}
	_, _, err := cmd.resolveStorageDSN()
	require.ErrorContains(t, err, "mutually exclusive")
}

func TestResolveStorageDSN_NoSourceConfigured(t *testing.T) {
	t.Setenv("SCHEMABOT_CONFIG_FILE", "")
	cmd := &ResyncIdentitySequencesCmd{}
	_, _, err := cmd.resolveStorageDSN()
	require.ErrorContains(t, err, "no storage DSN source")
}

func TestResolveStorageDSN_ConfigResolvesPostgresStorageDSN(t *testing.T) {
	path := writeStorageTestConfig(t, `
storage:
  dialect: postgres
  dsn: postgres://schemabot:test@storage-host:5432/schemabot
`)
	cmd := &ResyncIdentitySequencesCmd{Config: path}
	dsn, source, err := cmd.resolveStorageDSN()
	require.NoError(t, err)
	assert.Equal(t, "postgres://schemabot:test@storage-host:5432/schemabot", dsn)
	assert.Contains(t, source, path)
	assert.NotContains(t, source, "test@storage-host", "the loggable source must not leak DSN credentials")
}

func TestResolveStorageDSN_ConfigFromEnvFallback(t *testing.T) {
	path := writeStorageTestConfig(t, `
storage:
  dialect: postgres
  dsn: postgres://schemabot:test@storage-host:5432/schemabot
`)
	t.Setenv("SCHEMABOT_CONFIG_FILE", path)
	cmd := &ResyncIdentitySequencesCmd{}
	dsn, source, err := cmd.resolveStorageDSN()
	require.NoError(t, err)
	assert.Equal(t, "postgres://schemabot:test@storage-host:5432/schemabot", dsn)
	assert.Contains(t, source, "$SCHEMABOT_CONFIG_FILE")
}

func TestResolveStorageDSN_RejectsNonPostgresStorageDialect(t *testing.T) {
	path := writeStorageTestConfig(t, `
storage:
  dsn: user:pass@tcp(storage-host:3306)/schemabot
`)
	cmd := &ResyncIdentitySequencesCmd{Config: path}
	_, _, err := cmd.resolveStorageDSN()
	require.ErrorContains(t, err, `only applies to "postgres" storage`)
}
