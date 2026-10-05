package secrets

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
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
// The AWS functionality is tested via integration tests with MiniStack

func TestValueFromGetSecretOutput(t *testing.T) {
	// The string form is used when present.
	got, err := ValueFromGetSecretOutput(&secretsmanager.GetSecretValueOutput{SecretString: aws.String("pw")}, "secret")
	require.NoError(t, err)
	assert.Equal(t, "pw", got)

	// A binary-only secret falls back to its bytes rather than being rejected.
	got, err = ValueFromGetSecretOutput(&secretsmanager.GetSecretValueOutput{SecretBinary: []byte("binary-pw")}, "secret")
	require.NoError(t, err)
	assert.Equal(t, "binary-pw", got)

	// An empty-but-present binary value round-trips as "" rather than erroring.
	got, err = ValueFromGetSecretOutput(&secretsmanager.GetSecretValueOutput{SecretBinary: []byte{}}, "secret")
	require.NoError(t, err)
	assert.Empty(t, got)

	// Neither form set is an error naming the secret.
	_, err = ValueFromGetSecretOutput(&secretsmanager.GetSecretValueOutput{}, "secret")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has no value")
}

func TestSecretJSONKey(t *testing.T) {
	const secret = `{"app-id": "123456", "app_id": 1234567, "big": 12345678901234567890, "enabled": true, "pk": "-----BEGIN", "nested": {"a": 1}, "absent": null}`

	t.Run("string value is returned as is", func(t *testing.T) {
		got, err := secretJSONKey(secret, "github-app", "app-id")
		require.NoError(t, err)
		assert.Equal(t, "123456", got)
	})

	t.Run("number value keeps every digit", func(t *testing.T) {
		got, err := secretJSONKey(secret, "github-app", "app_id")
		require.NoError(t, err)
		assert.Equal(t, "1234567", got)

		got, err = secretJSONKey(secret, "github-app", "big")
		require.NoError(t, err)
		assert.Equal(t, "12345678901234567890", got)
	})

	t.Run("other values are their JSON literal", func(t *testing.T) {
		got, err := secretJSONKey(secret, "github-app", "enabled")
		require.NoError(t, err)
		assert.Equal(t, "true", got)

		got, err = secretJSONKey(secret, "github-app", "nested")
		require.NoError(t, err)
		assert.Equal(t, `{"a":1}`, got)
	})

	t.Run("null value is an error rather than a stand-in string", func(t *testing.T) {
		_, err := secretJSONKey(secret, "github-app", "absent")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `key "absent" in secret "github-app" is null`)
	})

	t.Run("data after the object is rejected", func(t *testing.T) {
		for _, corrupt := range []string{
			`{"app-id": "1"} {"app-id": "2"}`,
			`{"app-id": "1"} junk`,
			`{"app-id": "1"}}`,
		} {
			_, err := secretJSONKey(corrupt, "github-app", "app-id")
			require.Error(t, err, corrupt)
			assert.Contains(t, err.Error(), `parse secret "github-app" as JSON: unexpected data after the JSON object`, corrupt)
		}
	})

	t.Run("trailing whitespace is not data", func(t *testing.T) {
		got, err := secretJSONKey(`{"app-id": "1"}`+"\n  \n", "github-app", "app-id")
		require.NoError(t, err)
		assert.Equal(t, "1", got)
	})

	t.Run("missing key names the key and the secret", func(t *testing.T) {
		_, err := secretJSONKey(secret, "github-app", "webhook-secret")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `key "webhook-secret" not found in secret "github-app"`)
	})

	t.Run("non-JSON secret names the secret", func(t *testing.T) {
		_, err := secretJSONKey("not json", "github-app", "app-id")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `parse secret "github-app" as JSON`)
	})
}
