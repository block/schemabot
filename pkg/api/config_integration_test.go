//go:build integration

package api

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

func TestStorageDSN_StructuredFieldsWithSecretsManager(t *testing.T) {
	container, err := localstack.Run(t.Context(), "localstack/localstack:3.0")
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	host, err := testutil.ContainerHost(t.Context(), container)
	require.NoError(t, err)
	port, err := testutil.ContainerPort(t.Context(), container, "4566/tcp")
	require.NoError(t, err)
	endpoint := fmt.Sprintf("http://%s:%d", host, port)

	t.Setenv("AWS_ENDPOINT_URL", endpoint)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")

	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	// Simulate an RDS-managed secret with a password key
	rdsSecret, _ := json.Marshal(map[string]string{
		"username": "admin",
		"password": "rds-managed-password",
	})
	_, err = client.CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
		Name:         aws.String("rds/schemabot-cluster"),
		SecretString: aws.String(string(rdsSecret)),
	})
	require.NoError(t, err)

	serverCfg := &ServerConfig{
		Storage: StorageConfig{
			Host:     "db.example.com",
			Port:     "3306",
			Username: "admin",
			Password: "secretsmanager:rds/schemabot-cluster#password",
			Database: "schemabot",
		},
		TernDeployments: TernConfig{"default": {"prod": "localhost:9090"}},
	}
	require.NoError(t, serverCfg.Validate())

	dsn, err := serverCfg.StorageDSN()
	require.NoError(t, err)
	assert.Equal(t, "admin:rds-managed-password@tcp(db.example.com:3306)/schemabot?parseTime=true", dsn)
}
