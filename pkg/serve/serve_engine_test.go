package serve

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/engine"
	postgresengine "github.com/block/schemabot/pkg/engine/postgres"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// WithEngine records an embedder-supplied engine factory keyed by database type,
// and the last registration for a type wins.
func TestWithEngineRecordsFactory(t *testing.T) {
	var o options

	first := func(tern.LocalConfig, *slog.Logger) (engine.Engine, error) { return nil, errors.New("first") }
	second := func(tern.LocalConfig, *slog.Logger) (engine.Engine, error) { return nil, errors.New("second") }
	WithEngine("customdb", first)(&o)
	WithEngine("customdb", second)(&o)

	require.Contains(t, o.engines, "customdb")
	_, err := o.engines["customdb"](tern.LocalConfig{}, slog.New(slog.DiscardHandler))
	assert.EqualError(t, err, "second", "the last registration for a type wins")
}

// The data-plane client factory threads embedder-supplied engine factories into
// every LocalConfig it builds, so a custom database type on the gRPC/router path
// resolves the registered engine. The factory is invoked for the custom type.
func TestGRPCLocalClientFactoryThreadsEngineFactories(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	engines := map[string]tern.EngineFactory{
		"customdb": func(tern.LocalConfig, *slog.Logger) (engine.Engine, error) {
			return nil, errors.New("sentinel: factory invoked")
		},
	}

	factory := grpcLocalClientFactory(&api.ServerConfig{}, nil, engines)
	_, err := factory(tern.LocalConfig{
		Database:  "resolute",
		Type:      "customdb",
		TargetDSN: "root@tcp(localhost:3306)/",
	}, mysqlstore.New(nil), logger)

	require.Error(t, err)
	assert.ErrorContains(t, err, "sentinel: factory invoked",
		"the embedder's engine factory must be threaded into the LocalConfig and invoked for the custom type")
}

// When the resolved config already carries engine factories for other types,
// the embedder registry is merged in rather than dropped, so a custom type still
// resolves its registered engine.
func TestGRPCLocalClientFactoryMergesIntoExistingFactories(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	engines := map[string]tern.EngineFactory{
		"customdb": func(tern.LocalConfig, *slog.Logger) (engine.Engine, error) {
			return nil, errors.New("sentinel: embedder factory invoked")
		},
	}

	factory := grpcLocalClientFactory(&api.ServerConfig{}, nil, engines)
	_, err := factory(tern.LocalConfig{
		Database:  "resolute",
		Type:      "customdb",
		TargetDSN: "root@tcp(localhost:3306)/",
		// A non-nil map for an unrelated type must not shadow the embedder registry.
		EngineFactories: map[string]tern.EngineFactory{
			"otherdb": func(tern.LocalConfig, *slog.Logger) (engine.Engine, error) { return nil, nil },
		},
	}, mysqlstore.New(nil), logger)

	require.Error(t, err)
	assert.ErrorContains(t, err, "sentinel: embedder factory invoked",
		"a non-nil per-config factory map must not drop the embedder registry")
}

// The data-plane client factory applies the server-level postgres ceiling to
// every LocalClient it builds, so a configured value governs native-safe DDL
// on the gRPC/router path instead of silently reverting to the default.
func TestGRPCLocalClientFactoryConfiguresPostgresTableSizeLimit(t *testing.T) {
	limit := int64(4 << 30)
	factory := grpcLocalClientFactory(&api.ServerConfig{
		Postgres: api.PostgresConfig{NativeSafeTableSizeLimitBytes: &limit},
	}, nil, nil)

	client, err := factory(tern.LocalConfig{
		Database:  "orders",
		Type:      storage.DatabaseTypePostgres,
		TargetDSN: "postgres://localhost:5432/orders",
	}, mysqlstore.New(nil), slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	lc, ok := client.(*tern.LocalClient)
	require.True(t, ok)
	eng, ok := lc.Engine().(*postgresengine.Engine)
	require.True(t, ok)
	assert.Equal(t, limit, eng.TableSizeLimit())
}

// The data-plane client factory applies the server-level concurrent index
// bound to every LocalClient it builds, so a configured value governs
// concurrent builds on the gRPC/router path instead of silently reverting to
// the default.
func TestGRPCLocalClientFactoryConfiguresPostgresConcurrentIndexMaxDuration(t *testing.T) {
	factory := grpcLocalClientFactory(&api.ServerConfig{
		Postgres: api.PostgresConfig{ConcurrentIndexMaxDuration: "36h"},
	}, nil, nil)

	client, err := factory(tern.LocalConfig{
		Database:  "orders",
		Type:      storage.DatabaseTypePostgres,
		TargetDSN: "postgres://localhost:5432/orders",
	}, mysqlstore.New(nil), slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	lc, ok := client.(*tern.LocalClient)
	require.True(t, ok)
	eng, ok := lc.Engine().(*postgresengine.Engine)
	require.True(t, ok)
	assert.Equal(t, 36*time.Hour, eng.ConcurrentIndexMaxDuration())
}

// Without a registered engine, the data-plane client factory fails closed for a
// custom database type rather than building a client with no engine.
func TestGRPCLocalClientFactoryFailsClosedForUnregisteredType(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	factory := grpcLocalClientFactory(&api.ServerConfig{}, nil, nil)
	_, err := factory(tern.LocalConfig{
		Database:  "resolute",
		Type:      "customdb",
		TargetDSN: "root@tcp(localhost:3306)/",
	}, mysqlstore.New(nil), logger)

	require.Error(t, err)
	assert.ErrorContains(t, err, "no engine registered",
		"a custom database type with no registered engine must fail closed")
}

// A server-wide direct execution policy reaches every MySQL target the data
// plane resolves per request, which is the only way those targets can carry
// one: they are resolved from an opaque identifier and have no per-database
// registration on this server to state a policy in.
func TestServerEngineMetadataAppliesServerDirectExecutionPolicy(t *testing.T) {
	config := &api.ServerConfig{
		DirectExecution: &api.DirectExecutionConfig{Enabled: true, MaxTableRows: 10000, LockAcquisitionTimeout: "10s"},
	}

	metadata, err := serverEngineMetadata(config, map[string]string{"organization": "acme"}, storage.DatabaseTypeMySQL)

	require.NoError(t, err)
	assert.Equal(t, "true", metadata[engine.MetadataDirectExecution])
	assert.Equal(t, "10000", metadata[engine.MetadataDirectExecutionMaxTableRows])
	assert.Equal(t, "10", metadata[engine.MetadataDirectExecutionLockAcquisitionTimeoutSeconds])
	assert.Equal(t, "acme", metadata["organization"], "the resolved target's own metadata survives the overlay")
}

// A resolved target that states any part of a direct execution policy states
// all of it: the server-wide policy is not merged in alongside, so the target
// can never enable direct execution under a row bound configured elsewhere.
func TestServerEngineMetadataLeavesATargetsOwnDirectExecutionPolicyWhole(t *testing.T) {
	config := &api.ServerConfig{
		DirectExecution: &api.DirectExecutionConfig{Enabled: true, MaxTableRows: 10000, LockAcquisitionTimeout: "10s"},
	}
	resolved := map[string]string{engine.MetadataDirectExecution: "true"}

	metadata, err := serverEngineMetadata(config, resolved, storage.DatabaseTypeMySQL)

	require.NoError(t, err)
	assert.Equal(t, "true", metadata[engine.MetadataDirectExecution])
	assert.NotContains(t, metadata, engine.MetadataDirectExecutionMaxTableRows,
		"a target stating its own policy must not inherit the server-wide row bound; the engine blocks a bound-less grant")
	assert.NotContains(t, metadata, engine.MetadataDirectExecutionLockAcquisitionTimeoutSeconds)
}

// With no policy configured, no direct execution key reaches the engine and
// refused statements stay blocked.
func TestServerEngineMetadataOmitsDirectExecutionByDefault(t *testing.T) {
	metadata, err := serverEngineMetadata(&api.ServerConfig{}, nil, storage.DatabaseTypeMySQL)

	require.NoError(t, err)
	assert.NotContains(t, metadata, engine.MetadataDirectExecution)
	assert.NotContains(t, metadata, engine.MetadataDirectExecutionMaxTableRows)
}

// The server-wide policy reaches only the engines that consume it, so a
// deployment that drives MySQL alongside other engines can state one policy
// without handing keys to engines that would ignore them.
func TestServerEngineMetadataSkipsDirectExecutionForOtherEngines(t *testing.T) {
	config := &api.ServerConfig{
		DirectExecution: &api.DirectExecutionConfig{Enabled: true, MaxTableRows: 10000},
	}

	metadata, err := serverEngineMetadata(config, nil, storage.DatabaseTypeVitess)

	require.NoError(t, err)
	assert.NotContains(t, metadata, engine.MetadataDirectExecution)
}

// Composing the metadata never mutates the resolved target's map, which the
// router clones per request from its cached inventory entry.
func TestServerEngineMetadataDoesNotMutateResolvedMetadata(t *testing.T) {
	config := &api.ServerConfig{
		DirectExecution: &api.DirectExecutionConfig{Enabled: true, MaxTableRows: 10000},
	}
	resolved := map[string]string{"organization": "acme"}

	_, err := serverEngineMetadata(config, resolved, storage.DatabaseTypeMySQL)

	require.NoError(t, err)
	assert.Equal(t, map[string]string{"organization": "acme"}, resolved)
}
