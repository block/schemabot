//go:build e2e || integration

package testutil

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Endpoint returns the E2E_SCHEMABOT_URL environment variable.
func Endpoint(t *testing.T) string {
	t.Helper()
	v := os.Getenv("E2E_SCHEMABOT_URL")
	require.NotEmpty(t, v, "E2E_SCHEMABOT_URL not set")
	return v
}

// SchemabotDSN returns the E2E_SCHEMABOT_MYSQL_DSN environment variable.
func SchemabotDSN(t *testing.T) string {
	t.Helper()
	v := os.Getenv("E2E_SCHEMABOT_MYSQL_DSN")
	require.NotEmpty(t, v, "E2E_SCHEMABOT_MYSQL_DSN not set")
	return v
}

// TernStagingDSN returns the E2E_TERN_STAGING_MYSQL_DSN environment variable.
func TernStagingDSN(t *testing.T) string {
	t.Helper()
	v := os.Getenv("E2E_TERN_STAGING_MYSQL_DSN")
	require.NotEmpty(t, v, "E2E_TERN_STAGING_MYSQL_DSN not set")
	return v
}

// TernProductionDSN returns the E2E_TERN_PRODUCTION_MYSQL_DSN environment variable.
func TernProductionDSN(t *testing.T) string {
	t.Helper()
	v := os.Getenv("E2E_TERN_PRODUCTION_MYSQL_DSN")
	require.NotEmpty(t, v, "E2E_TERN_PRODUCTION_MYSQL_DSN not set")
	return v
}
