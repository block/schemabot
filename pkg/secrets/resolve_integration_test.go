//go:build integration

package secrets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/block/schemabot/pkg/testutil"
)

// TestResolve_CustomResolver demonstrates how OSS users can register
// their own secret resolvers for custom backends (e.g., HashiCorp Vault,
// 1Password, custom key management systems).
func TestResolve_CustomResolver(t *testing.T) {
	// Simulate a custom secret backend (e.g., HashiCorp Vault)
	mockVault := map[string]string{
		"database/creds/myapp": "vault-db-password-123",
		"api/keys/stripe":      "sk_live_abc123",
	}

	// Register a custom "vault" resolver
	Register("vault", func(ref string) (string, error) {
		if val, ok := mockVault[ref]; ok {
			return val, nil
		}
		return "", fmt.Errorf("vault: secret not found: %s", ref)
	})
	t.Cleanup(func() {
		Unregister("vault")
	})

	t.Run("custom resolver returns secret", func(t *testing.T) {
		result, err := Resolve("vault:database/creds/myapp", "")
		require.NoError(t, err)
		assert.Equal(t, "vault-db-password-123", result)
	})

	t.Run("custom resolver with different path", func(t *testing.T) {
		result, err := Resolve("vault:api/keys/stripe", "")
		require.NoError(t, err)
		assert.Equal(t, "sk_live_abc123", result)
	})

	t.Run("custom resolver error propagates", func(t *testing.T) {
		_, err := Resolve("vault:nonexistent/path", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("built-in resolvers still work", func(t *testing.T) {
		t.Setenv("MY_TEST_SECRET", "env-value")
		result, err := Resolve("env:MY_TEST_SECRET", "")
		require.NoError(t, err)
		assert.Equal(t, "env-value", result)
	})
}

func TestResolve_SecretsManager_Integration(t *testing.T) {
	// Start MiniStack container
	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testutil.MiniStackContainerRequest(),
		Started:          true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	// Get MiniStack endpoint (port 4566 is the gateway for all services)
	host, err := testutil.ContainerHost(t.Context(), container)
	require.NoError(t, err)
	port, err := testutil.ContainerPort(t.Context(), container, "4566/tcp")
	require.NoError(t, err)
	endpoint := fmt.Sprintf("http://%s:%d", host, port)

	// Set environment for AWS SDK to use MiniStack
	t.Setenv("AWS_ENDPOINT_URL", endpoint)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")

	// Create a Secrets Manager client pointing to MiniStack
	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	// Create test secrets
	createTestSecret(t, client, "test-plain-secret", "my-secret-value")

	jsonSecret, err := json.Marshal(map[string]string{
		"username": "admin",
		"password": "super-secret-password",
	})
	require.NoError(t, err)
	createTestSecret(t, client, "test-json-secret", string(jsonSecret))

	t.Run("plain secret", func(t *testing.T) {
		result, err := Resolve("secretsmanager:test-plain-secret", "")
		require.NoError(t, err)
		assert.Equal(t, "my-secret-value", result)
	})

	t.Run("JSON secret - extract key", func(t *testing.T) {
		result, err := Resolve("secretsmanager:test-json-secret#password", "")
		require.NoError(t, err)
		assert.Equal(t, "super-secret-password", result)
	})

	t.Run("JSON secret - extract username", func(t *testing.T) {
		result, err := Resolve("secretsmanager:test-json-secret#username", "")
		require.NoError(t, err)
		assert.Equal(t, "admin", result)
	})

	t.Run("JSON secret - missing key", func(t *testing.T) {
		_, err := Resolve("secretsmanager:test-json-secret#nonexistent", "")
		require.Error(t, err)
	})

	t.Run("nonexistent secret", func(t *testing.T) {
		_, err := Resolve("secretsmanager:does-not-exist", "")
		require.Error(t, err)
	})

	// A per-request resolution is bounded by the caller's context: a cancelled
	// context fails the Secrets Manager fetch instead of waiting the background
	// timeout.
	t.Run("context cancellation is honored", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := ResolveContext(ctx, "secretsmanager:test-plain-secret", "")
		require.ErrorIs(t, err, context.Canceled)
	})
}

// createTestSecret creates a secret in MiniStack. The SDK retries a create
// whose response it lost under the same client request token, which AWS
// answers as the original request but MiniStack answers as a duplicate name.
// A duplicate name is accepted only when the stored secret holds the value
// this call meant to write, so a retried create passes and a genuine clash
// still fails.
func createTestSecret(t *testing.T, client *secretsmanager.Client, name, value string) {
	t.Helper()
	_, err := client.CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
		Name:         aws.String(name),
		SecretString: aws.String(value),
	})
	var exists *types.ResourceExistsException
	if !errors.As(err, &exists) {
		require.NoError(t, err, "create secret %s", name)
		return
	}
	stored, getErr := client.GetSecretValue(t.Context(), &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(name),
	})
	require.NoError(t, getErr, "read secret %s after its create reported it already exists", name)
	require.Equal(t, value, aws.ToString(stored.SecretString), "secret %s already exists with a different value", name)
}
