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

// An environment that opts out of a server-wide grant keeps its opt-out. The
// single-database gRPC path resolves that environment's policy into the
// metadata this composition receives, so an opt-out that rendered nothing
// would read as "unstated" here and be overlaid by the server-wide grant —
// executing a statement on a database whose configuration refused it.
func TestServerEngineMetadataKeepsAnEnvironmentsOptOutOverTheServerPolicy(t *testing.T) {
	config := &api.ServerConfig{
		DirectExecution: &api.DirectExecutionConfig{Enabled: true, MaxTableRows: 10000},
	}
	optedOut, err := config.DirectExecutionMetadata(
		&api.EnvironmentConfig{DirectExecution: &api.DirectExecutionConfig{Enabled: false}},
		storage.DatabaseTypeMySQL,
	)
	require.NoError(t, err)

	metadata, err := serverEngineMetadata(config, optedOut, storage.DatabaseTypeMySQL)

	require.NoError(t, err)
	assert.Equal(t, "false", metadata[engine.MetadataDirectExecution],
		"the server-wide grant must not overwrite a deliberate opt-out")
	assert.NotContains(t, metadata, engine.MetadataDirectExecutionMaxTableRows)
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

// The helper that composes the server policy is only worth having if the
// factory that builds every data-plane client actually delivers its result.
// This asserts the client's own metadata rather than the helper's return, so
// a factory that composes the policy and then discards it is caught.
func TestGRPCLocalClientFactoryDeliversTheDirectExecutionPolicy(t *testing.T) {
	factory := grpcLocalClientFactory(&api.ServerConfig{
		DirectExecution: &api.DirectExecutionConfig{Enabled: true, MaxTableRows: 10000, LockAcquisitionTimeout: "5s"},
	}, nil, nil)

	client, err := factory(tern.LocalConfig{
		Database:  "payments",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: "root@tcp(localhost:3306)/payments",
	}, mysqlstore.New(nil), slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	local, ok := client.(*tern.LocalClient)
	require.True(t, ok)
	metadata := local.Metadata()
	assert.Equal(t, "true", metadata[engine.MetadataDirectExecution])
	assert.Equal(t, "10000", metadata[engine.MetadataDirectExecutionMaxTableRows])
	assert.Equal(t, "5", metadata[engine.MetadataDirectExecutionLockAcquisitionTimeoutSeconds])
}

// A resolved target that states its own policy keeps it through the factory,
// so the client the data plane runs is the one the target's own opt-out or
// grant describes rather than the server's.
func TestGRPCLocalClientFactoryDeliversAResolvedTargetsOwnOptOut(t *testing.T) {
	factory := grpcLocalClientFactory(&api.ServerConfig{
		DirectExecution: &api.DirectExecutionConfig{Enabled: true, MaxTableRows: 10000},
	}, nil, nil)

	client, err := factory(tern.LocalConfig{
		Database:  "payments",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: "root@tcp(localhost:3306)/payments",
		Metadata:  map[string]string{engine.MetadataDirectExecution: "false"},
	}, mysqlstore.New(nil), slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	local, ok := client.(*tern.LocalClient)
	require.True(t, ok)
	metadata := local.Metadata()
	assert.Equal(t, "false", metadata[engine.MetadataDirectExecution])
	assert.NotContains(t, metadata, engine.MetadataDirectExecutionMaxTableRows)
}

// Single-database gRPC mode selects one local-DSN database and serves it
// directly. The environment's own policy has to be resolved against the
// server-wide one on the way in, or a grant stated once for the whole server
// never reaches the only database this process serves.
func TestBuildGRPCTernClientCarriesTheServerWideDirectExecutionPolicy(t *testing.T) {
	config := &api.ServerConfig{
		DirectExecution: &api.DirectExecutionConfig{Enabled: true, MaxTableRows: 10000, LockAcquisitionTimeout: "5s"},
		Databases: map[string]api.DatabaseConfig{
			"payments": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{"staging": {DSN: "root@tcp(localhost:3306)/payments"}},
			},
		},
	}

	client, err := buildGRPCTernClient(t.Context(), config, mysqlstore.New(nil), slog.New(slog.DiscardHandler), "staging", nil)
	require.NoError(t, err)

	local, ok := client.(*tern.LocalClient)
	require.True(t, ok)
	metadata := local.Metadata()
	assert.Equal(t, "true", metadata[engine.MetadataDirectExecution])
	assert.Equal(t, "10000", metadata[engine.MetadataDirectExecutionMaxTableRows])
	assert.Equal(t, "5", metadata[engine.MetadataDirectExecutionLockAcquisitionTimeoutSeconds])
}

// The single database this process serves keeps its own opt-out, which is the
// direction that matters: a grant laid over an environment that refused it is
// native blocking DDL on a table whose configuration said no.
func TestBuildGRPCTernClientKeepsTheEnvironmentsOptOut(t *testing.T) {
	config := &api.ServerConfig{
		DirectExecution: &api.DirectExecutionConfig{Enabled: true, MaxTableRows: 10000},
		Databases: map[string]api.DatabaseConfig{
			"payments": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{"staging": {
					DSN:             "root@tcp(localhost:3306)/payments",
					DirectExecution: &api.DirectExecutionConfig{Enabled: false},
				}},
			},
		},
	}

	client, err := buildGRPCTernClient(t.Context(), config, mysqlstore.New(nil), slog.New(slog.DiscardHandler), "staging", nil)
	require.NoError(t, err)

	local, ok := client.(*tern.LocalClient)
	require.True(t, ok)
	metadata := local.Metadata()
	assert.Equal(t, "false", metadata[engine.MetadataDirectExecution])
	assert.NotContains(t, metadata, engine.MetadataDirectExecutionMaxTableRows)
}

// An environment that states its own bound reaches the client with that
// bound, not the server's. The factory behind this path only knows the
// server-wide policy, so the environment's override has to be resolved here
// or the single database this process serves silently runs under the wrong
// blast-radius cap.
func TestBuildGRPCTernClientCarriesTheEnvironmentsOwnBound(t *testing.T) {
	config := &api.ServerConfig{
		DirectExecution: &api.DirectExecutionConfig{Enabled: true, MaxTableRows: 10000},
		Databases: map[string]api.DatabaseConfig{
			"payments": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{"staging": {
					DSN:             "root@tcp(localhost:3306)/payments",
					DirectExecution: &api.DirectExecutionConfig{Enabled: true, MaxTableRows: 500},
				}},
			},
		},
	}

	client, err := buildGRPCTernClient(t.Context(), config, mysqlstore.New(nil), slog.New(slog.DiscardHandler), "staging", nil)
	require.NoError(t, err)

	local, ok := client.(*tern.LocalClient)
	require.True(t, ok)
	assert.Equal(t, "500", local.Metadata()[engine.MetadataDirectExecutionMaxTableRows],
		"the override replaces the server-wide bound rather than being overlaid by it")
}
