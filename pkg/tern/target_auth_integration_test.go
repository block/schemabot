//go:build integration

package tern

import (
	"log/slog"
	"testing"

	drivermysql "github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/targetauth"
)

// dsnWithPassword returns the DSN with its password replaced.
func dsnWithPassword(t *testing.T, dsn, password string) string {
	t.Helper()
	cfg, err := drivermysql.ParseDSN(dsn)
	require.NoError(t, err)
	cfg.Passwd = password
	return cfg.FormatDSN()
}

// dsnWithDatabase returns the DSN addressed to a different database.
func dsnWithDatabase(t *testing.T, dsn, database string) string {
	t.Helper()
	cfg, err := drivermysql.ParseDSN(dsn)
	require.NoError(t, err)
	cfg.DBName = database
	return cfg.FormatDSN()
}

func pullSchemaWithTargetDSN(t *testing.T, targetDSN string) error {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: targetDSN,
	}, nil, logger)
	require.NoError(t, err, "create client")
	defer utils.CloseAndLog(client)

	_, err = client.PullSchema(t.Context(), &ternv1.PullSchemaRequest{
		Type:        storage.DatabaseTypeMySQL,
		Environment: localClientTestEnvironment,
	})
	return err
}

func assertTargetAuthClassification(t *testing.T, err error, want targetauth.Classification) {
	t.Helper()
	require.Error(t, err)
	classification, ok := targetauth.ClassificationOf(err)
	assert.True(t, ok, "error should carry a target auth classification: %v", err)
	assert.Equal(t, want, classification)
}

// A schema pull whose target DSN names no database has to connect to discover
// the live namespaces. When the target rejects the configured credentials,
// that discovery fails with an error that still carries its authentication
// classification, so a caller can tell a credential problem apart from a
// transient connection failure without parsing the driver's message.
func TestLocalClient_PullSchemaNamespaceDiscoveryClassifiesTargetAuthFailure(t *testing.T) {
	_, dsn := setupMySQLContainer(t)

	err := pullSchemaWithTargetDSN(t, dsnWithPassword(t, dsnWithoutDatabase(t, dsn), "wrong"))

	assertTargetAuthClassification(t, err, targetauth.AuthInvalidCredentials)
}

// A schema pull whose target DSN names its database skips discovery and
// connects once per namespace. A target that rejects the credentials or does
// not have the named database fails that pull with an error that still
// carries its authentication classification.
func TestLocalClient_PullSchemaNamespaceClassifiesTargetAuthFailure(t *testing.T) {
	_, dsn := setupMySQLContainer(t)

	t.Run("wrong password", func(t *testing.T) {
		err := pullSchemaWithTargetDSN(t, dsnWithPassword(t, dsn, "wrong"))
		assertTargetAuthClassification(t, err, targetauth.AuthInvalidCredentials)
	})
	t.Run("missing database", func(t *testing.T) {
		err := pullSchemaWithTargetDSN(t, dsnWithDatabase(t, dsn, "target_auth_missing_db"))
		assertTargetAuthClassification(t, err, targetauth.AuthNoDatabase)
	})
}
