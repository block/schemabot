package serve

import (
	"errors"
	"log/slog"
	"testing"

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
