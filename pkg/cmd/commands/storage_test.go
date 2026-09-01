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
	flagDSN := "postgres://schemabot:test@localhost:5432/schemabot"
	dsn, source, err := resolveStorageDSN(flagDSN, "", "the identity sequence resync")
	require.NoError(t, err)
	assert.Equal(t, flagDSN, dsn)
	assert.Equal(t, "--dsn flag", source)
}

func TestResolveStorageDSN_DSNAndConfigAreMutuallyExclusive(t *testing.T) {
	_, _, err := resolveStorageDSN("postgres://localhost/schemabot", "/etc/schemabot/config.yaml", "the identity sequence resync")
	require.ErrorContains(t, err, "mutually exclusive")
}

func TestResolveStorageDSN_NoSourceConfigured(t *testing.T) {
	t.Setenv("SCHEMABOT_CONFIG_FILE", "")
	_, _, err := resolveStorageDSN("", "", "the identity sequence resync")
	require.ErrorContains(t, err, "no storage DSN source")
}

func TestResolveStorageDSN_ConfigResolvesPostgresStorageDSN(t *testing.T) {
	path := writeStorageTestConfig(t, `
storage:
  dialect: postgres
  dsn: postgres://schemabot:hunter2-distinctive@storage-host:5432/schemabot
`)
	dsn, source, err := resolveStorageDSN("", path, "the identity sequence resync")
	require.NoError(t, err)
	assert.Equal(t, "postgres://schemabot:hunter2-distinctive@storage-host:5432/schemabot", dsn)
	assert.Contains(t, source, path)
	assert.NotContains(t, source, "hunter2-distinctive", "the loggable source must not leak DSN credentials")
}

func TestResolveStorageDSN_ConfigFromEnvFallback(t *testing.T) {
	path := writeStorageTestConfig(t, `
storage:
  dialect: postgres
  dsn: postgres://schemabot:test@storage-host:5432/schemabot
`)
	t.Setenv("SCHEMABOT_CONFIG_FILE", path)
	dsn, source, err := resolveStorageDSN("", "", "the identity sequence resync")
	require.NoError(t, err)
	assert.Equal(t, "postgres://schemabot:test@storage-host:5432/schemabot", dsn)
	assert.Contains(t, source, "$SCHEMABOT_CONFIG_FILE")
}

func TestResolveStorageDSN_RejectsNonPostgresStorageDialect(t *testing.T) {
	path := writeStorageTestConfig(t, `
storage:
  dsn: user:pass@tcp(storage-host:3306)/schemabot
`)
	_, _, err := resolveStorageDSN("", path, "the identity sequence resync")
	require.ErrorContains(t, err, `only applies to "postgres" storage`)
}

func TestResolveStorageDSN_EmptyConfigDSN(t *testing.T) {
	t.Setenv("STORAGE_DSN", "")
	t.Setenv("MYSQL_DSN", "")
	path := writeStorageTestConfig(t, `
storage:
  dialect: postgres
`)
	_, _, err := resolveStorageDSN("", path, "the identity sequence resync")
	require.ErrorContains(t, err, "storage DSN not configured")
}

func TestResolveStorageDSN_WhitespaceDirectDSN(t *testing.T) {
	_, _, err := resolveStorageDSN("   ", "", "the identity sequence resync")
	require.ErrorContains(t, err, "storage DSN not configured")
}

func TestResolveStorageDSN_RejectsNonPostgresDirectDSN(t *testing.T) {
	_, _, err := resolveStorageDSN("user:pass@tcp(storage-host:3306)/schemabot", "", "the identity key canonicalization")
	require.ErrorContains(t, err, "not a PostgreSQL DSN")
	require.ErrorContains(t, err, `the identity key canonicalization only applies to "postgres" storage`)
	assert.NotContains(t, err.Error(), "pass", "the error must not echo DSN credentials")
}

func TestResolveStorageDSN_RejectsNonPostgresStorageDialectForCanonicalization(t *testing.T) {
	path := writeStorageTestConfig(t, `
storage:
  dsn: user:pass@tcp(storage-host:3306)/schemabot
`)
	_, _, err := resolveStorageDSN("", path, "the identity key canonicalization")
	require.ErrorContains(t, err, `the identity key canonicalization only applies to "postgres" storage`)
}

func TestResolveStorageDSN_ReportsEnvironmentSource(t *testing.T) {
	path := writeStorageTestConfig(t, `
storage:
  dialect: postgres
`)
	t.Setenv("STORAGE_DSN", "postgres://schemabot@storage-host:5432/schemabot")
	t.Setenv("MYSQL_DSN", "postgres://legacy@storage-host:5432/schemabot")
	_, source, err := resolveStorageDSN("", path, "the identity sequence resync")
	require.NoError(t, err)
	assert.Equal(t, "STORAGE_DSN environment variable", source)
}

func TestResyncIdentitySequencesCmd_PingFailure(t *testing.T) {
	cmd := &ResyncIdentitySequencesCmd{DSN: "postgres://user@127.0.0.1:1/db?sslmode=disable"}
	err := cmd.Run(t.Context(), &Globals{Version: "test"})
	require.ErrorContains(t, err, "ping storage database:")
}
