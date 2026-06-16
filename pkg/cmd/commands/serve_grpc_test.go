package commands

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// The data-plane gRPC server routes by opaque target when a target resolver is
// configured, so an operator running serve --grpc against a target inventory
// connects per request rather than binding to one database at startup.
func TestBuildGRPCTernClientRoutesWhenTargetResolverConfigured(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	config := &api.ServerConfig{
		TargetResolver: api.TargetResolverConfig{
			Targets: map[string]inventory.StaticTarget{
				"dsid-orders-prod": {
					DatabaseType: storage.DatabaseTypeMySQL,
					DSN:          "root@tcp(localhost:3306)/",
				},
			},
		},
	}

	client, err := buildGRPCTernClient(config, mysqlstore.New(nil), logger, "production")
	require.NoError(t, err)
	require.NotNil(t, client)
	_, ok := client.(*tern.TargetRouter)
	assert.True(t, ok, "expected a TargetRouter when target_resolver is configured")
}

// When a target_resolver.etre block is configured, the data plane routes
// through the Etre-backed resolver, resolving each target against Etre per
// request rather than binding to one database.
func TestBuildGRPCTernClientRoutesViaEtreResolver(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	config := &api.ServerConfig{
		TargetResolver: api.TargetResolverConfig{
			Etre: api.EtreConfig{
				Addr:        "https://etre.example",
				EntityType:  "cluster",
				TargetLabel: "dsid",
				HostField:   "writer_endpoint",
				Credentials: api.EtreCredentialsConfig{Username: "spirit", PasswordRef: "env:DDL_PASSWORD"},
			},
		},
	}

	client, err := buildGRPCTernClient(config, mysqlstore.New(nil), logger, "")
	require.NoError(t, err)
	_, ok := client.(*tern.TargetRouter)
	assert.True(t, ok, "expected a TargetRouter when target_resolver.etre is configured")
}

// Configuring both the Etre resolver and static targets is ambiguous until
// per-target overrides exist, so startup fails closed rather than silently
// picking one.
func TestBuildGRPCTernClientErrorsWhenEtreAndStaticBothConfigured(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	config := &api.ServerConfig{
		TargetResolver: api.TargetResolverConfig{
			Targets: map[string]inventory.StaticTarget{
				"dsid-orders-prod": {DatabaseType: storage.DatabaseTypeMySQL, DSN: "root@tcp(localhost:3306)/"},
			},
			Etre: api.EtreConfig{
				Addr: "https://etre.example", EntityType: "cluster", TargetLabel: "dsid",
				HostField: "writer_endpoint", Credentials: api.EtreCredentialsConfig{Username: "spirit", PasswordRef: "env:DDL_PASSWORD"},
			},
		},
	}

	_, err := buildGRPCTernClient(config, mysqlstore.New(nil), logger, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both etre and static")
}

// The Etre resolver's lazily-validated fields are checked at startup so a
// misconfiguration fails fast instead of on the first request.
func TestBuildEtreResolverValidatesConfig(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	base := api.EtreConfig{
		Addr: "https://etre.example", EntityType: "cluster", TargetLabel: "dsid",
		HostField: "writer_endpoint", Credentials: api.EtreCredentialsConfig{Username: "spirit", PasswordRef: "env:DDL_PASSWORD"},
	}

	_, err := buildEtreResolver(base, logger)
	require.NoError(t, err)

	noHost := base
	noHost.HostField = ""
	_, err = buildEtreResolver(noHost, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "host_field")

	noPassword := base
	noPassword.Credentials.PasswordRef = ""
	_, err = buildEtreResolver(noPassword, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "password_ref")

	noUsername := base
	noUsername.Credentials.Username = ""
	_, err = buildEtreResolver(noUsername, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "username")

	// A secret ref that resolves to empty (e.g. an unset env var) fails closed
	// with config context rather than a generic downstream error.
	emptyAddr := base
	emptyAddr.Addr = "env:UNSET_ETRE_ADDR"
	_, err = buildEtreResolver(emptyAddr, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "addr resolved to an empty value")
}

// Without a target resolver the data plane falls back to a single LocalClient
// bound to the first database configured for the environment, preserving the
// pre-router single-database serving mode.
func TestBuildGRPCTernClientFallsBackToSingleDatabase(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	config := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{
					"production": {DSN: "root@tcp(localhost:3306)/orders"},
				},
			},
		},
	}

	client, err := buildGRPCTernClient(config, mysqlstore.New(nil), logger, "production")
	require.NoError(t, err)
	require.NotNil(t, client)
	_, ok := client.(*tern.LocalClient)
	assert.True(t, ok, "expected a LocalClient when only databases are configured")
}

// In target-routing mode each request carries its own environment, so the
// server-level TERN_ENVIRONMENT is not required to start.
func TestBuildGRPCTernClientRoutesWithoutEnvironment(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	config := &api.ServerConfig{
		TargetResolver: api.TargetResolverConfig{
			Targets: map[string]inventory.StaticTarget{
				"dsid-orders-prod": {
					DatabaseType: storage.DatabaseTypeMySQL,
					DSN:          "root@tcp(localhost:3306)/",
				},
			},
		},
	}

	client, err := buildGRPCTernClient(config, mysqlstore.New(nil), logger, "")
	require.NoError(t, err)
	_, ok := client.(*tern.TargetRouter)
	assert.True(t, ok, "resolver mode should not require an environment")
}

// The single-database fallback requires an environment to select against.
func TestBuildGRPCTernClientErrorsWhenEnvMissingInFallback(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	config := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"orders": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{"production": {DSN: "root@tcp(localhost:3306)/orders"}},
			},
		},
	}

	_, err := buildGRPCTernClient(config, mysqlstore.New(nil), logger, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TERN_ENVIRONMENT")
}

// A single LocalClient serves exactly one database, so a config where multiple
// databases have a local DSN for the environment is ambiguous and fails closed
// rather than binding to a nondeterministic one.
func TestBuildGRPCTernClientErrorsOnAmbiguousFallback(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	config := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"orders": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{"production": {DSN: "root@tcp(localhost:3306)/orders"}},
			},
			"payments": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{"production": {DSN: "root@tcp(localhost:3306)/payments"}},
			},
		},
	}

	_, err := buildGRPCTernClient(config, mysqlstore.New(nil), logger, "production")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "orders")
	assert.Contains(t, err.Error(), "payments")
}

// With neither a target resolver nor a database for the environment, startup
// fails closed rather than serving a gRPC endpoint that can resolve nothing.
func TestBuildGRPCTernClientErrorsWhenNothingConfigured(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	_, err := buildGRPCTernClient(&api.ServerConfig{}, mysqlstore.New(nil), logger, "production")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "production")
}
