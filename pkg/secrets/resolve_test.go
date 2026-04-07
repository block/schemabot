package secrets

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolve_EmptyWithFallback(t *testing.T) {
	t.Setenv("TEST_FALLBACK_VAR", "fallback-value")

	result, err := Resolve("", "TEST_FALLBACK_VAR")
	require.NoError(t, err)
	assert.Equal(t, "fallback-value", result)
}

func TestResolve_EmptyNoFallback(t *testing.T) {
	result, err := Resolve("", "NONEXISTENT_VAR_12345")
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestResolve_EnvPrefix(t *testing.T) {
	t.Setenv("MY_SECRET_TOKEN", "secret-token-value")

	result, err := Resolve("env:MY_SECRET_TOKEN", "")
	require.NoError(t, err)
	assert.Equal(t, "secret-token-value", result)
}

func TestResolve_EnvPrefix_NotSet(t *testing.T) {
	result, err := Resolve("env:NONEXISTENT_VAR_67890", "")
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestResolve_FilePrefix(t *testing.T) {
	dir := t.TempDir()
	secretFile := filepath.Join(dir, "secret.txt")
	require.NoError(t, os.WriteFile(secretFile, []byte("file-secret-value\n"), 0600))

	result, err := Resolve("file:"+secretFile, "")
	require.NoError(t, err)
	assert.Equal(t, "file-secret-value", result)
}

func TestResolve_FilePrefix_NotFound(t *testing.T) {
	_, err := Resolve("file:/nonexistent/path/to/secret", "")
	require.Error(t, err)
}

func TestResolve_LiteralValue(t *testing.T) {
	result, err := Resolve("just-a-literal-value", "")
	require.NoError(t, err)
	assert.Equal(t, "just-a-literal-value", result)
}

func TestResolve_LiteralWithColon(t *testing.T) {
	// Values with colons that don't match known prefixes are returned as-is
	result, err := Resolve("host:port:path", "")
	require.NoError(t, err)
	assert.Equal(t, "host:port:path", result)
}

// Note: secretsmanager: tests would require mocking AWS SDK or integration tests
// The AWS functionality is tested via integration tests with LocalStack
