//go:build integration

package secrets

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"

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
	// Start LocalStack container
	container, err := localstack.Run(t.Context(),
		"localstack/localstack:3.0",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	// Get LocalStack endpoint (port 4566 is the gateway for all services)
	host, err := testutil.ContainerHost(t.Context(), container)
	require.NoError(t, err)
	port, err := testutil.ContainerPort(t.Context(), container, "4566/tcp")
	require.NoError(t, err)
	endpoint := fmt.Sprintf("http://%s:%d", host, port)

	// Set environment for AWS SDK to use LocalStack
	t.Setenv("AWS_ENDPOINT_URL", endpoint)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")

	// Create a Secrets Manager client pointing to LocalStack
	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	// Create test secrets
	_, err = client.CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
		Name:         aws.String("test-plain-secret"),
		SecretString: aws.String("my-secret-value"),
	})
	require.NoError(t, err)

	jsonSecret, _ := json.Marshal(map[string]string{
		"username": "admin",
		"password": "super-secret-password",
	})
	_, err = client.CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
		Name:         aws.String("test-json-secret"),
		SecretString: aws.String(string(jsonSecret)),
	})
	require.NoError(t, err)

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
}
