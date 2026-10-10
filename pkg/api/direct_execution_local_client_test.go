package api

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// A database served from a local DSN builds its client in-process, so the
// server-wide policy has to be composed into that client's metadata the same
// way it is for a routed one. Reading only the environment's own block here
// would accept a server-wide grant at startup and never route a statement
// under it.
func TestLocalTernClientCarriesTheServerWideDirectExecutionPolicy(t *testing.T) {
	config := &ServerConfig{
		DirectExecution: &DirectExecutionConfig{Enabled: true, MaxTableRows: 10000, LockAcquisitionTimeout: "5s"},
		Databases: map[string]DatabaseConfig{
			"payments": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{"staging": {DSN: "root@tcp(localhost:3306)/payments"}},
			},
		},
	}
	svc := New(&mockStorage{}, config, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	client, err := svc.newLocalTernClient("payments/staging", "payments", storage.DatabaseTypeMySQL,
		config.Databases["payments"].Environments["staging"])
	require.NoError(t, err)

	local, ok := client.(*tern.LocalClient)
	require.True(t, ok)
	metadata := local.Metadata()
	assert.Equal(t, "true", metadata[engine.MetadataDirectExecution])
	assert.Equal(t, "10000", metadata[engine.MetadataDirectExecutionMaxTableRows])
	assert.Equal(t, "5", metadata[engine.MetadataDirectExecutionLockAcquisitionTimeoutSeconds])
}

// An environment that opts out keeps its opt-out on the in-process path: the
// override replaces the server-wide policy whole rather than merging with it,
// and the opt-out states itself so nothing downstream reads it as a database
// that named no policy.
func TestLocalTernClientKeepsAnEnvironmentsOptOutOverTheServerPolicy(t *testing.T) {
	envConfig := EnvironmentConfig{
		DSN:             "root@tcp(localhost:3306)/payments",
		DirectExecution: &DirectExecutionConfig{Enabled: false},
	}
	config := &ServerConfig{
		DirectExecution: &DirectExecutionConfig{Enabled: true, MaxTableRows: 10000},
		Databases: map[string]DatabaseConfig{
			"payments": {Type: storage.DatabaseTypeMySQL, Environments: map[string]EnvironmentConfig{"staging": envConfig}},
		},
	}
	svc := New(&mockStorage{}, config, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	client, err := svc.newLocalTernClient("payments/staging", "payments", storage.DatabaseTypeMySQL, envConfig)
	require.NoError(t, err)

	local, ok := client.(*tern.LocalClient)
	require.True(t, ok)
	metadata := local.Metadata()
	assert.Equal(t, "false", metadata[engine.MetadataDirectExecution])
	assert.NotContains(t, metadata, engine.MetadataDirectExecutionMaxTableRows,
		"an opt-out carries no bound for an enabled flag to pair with")
}
