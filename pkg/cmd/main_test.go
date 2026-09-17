package main

import (
	"io"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRollbackRequiresEnvironmentFlag(t *testing.T) {
	var cli CLI
	parser, err := kong.New(&cli,
		kong.Name("schemabot"),
		kong.Writers(io.Discard, io.Discard),
		kong.Vars{"cli_name": "schemabot"},
	)
	require.NoError(t, err)

	_, err = parser.Parse([]string{"rollback", "apply_abc123", "--auto-approve"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "-e")
}

func TestStorageResyncIdentitySequencesIsInvocable(t *testing.T) {
	var cli CLI
	parser, err := kong.New(&cli,
		kong.Name("schemabot"),
		kong.Writers(io.Discard, io.Discard),
		kong.Vars{"cli_name": "schemabot"},
	)
	require.NoError(t, err)

	_, err = parser.Parse([]string{"storage", "resync-identity-sequences", "--dsn", "postgres://user@localhost:5432/db"})
	require.NoError(t, err)
}

// A storage schema command routed through the API needs an endpoint, and with
// a local runtime selected that endpoint is one the CLI has to start first.
// The direct path needs none: it exists for when no server is up, so starting
// a runtime for it would be exactly the work the operator reached for --dsn to
// avoid. The maintenance commands never call the API at all.
func TestUsesLocalRuntime_StorageSubcommands(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "plan through the API", args: []string{"storage", "plan", "--release", "v1.4.0"}, want: true},
		{name: "plan of a data plane's storage", args: []string{"storage", "plan", "--release", "v1.4.0", "--deployment", "west", "-e", "production"}, want: true},
		{name: "apply through the API", args: []string{"storage", "apply"}, want: true},
		{name: "plan against a DSN", args: []string{"storage", "plan", "--release", "v1.4.0", "--dsn", "postgres://user@localhost:5432/db"}, want: false},
		{name: "apply against a server config", args: []string{"storage", "apply", "--config", "/etc/schemabot/config.yaml"}, want: false},
		{name: "a maintenance command", args: []string{"storage", "resync-identity-sequences", "--dsn", "postgres://user@localhost:5432/db"}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var cli CLI
			parser, err := kong.New(&cli,
				kong.Name("schemabot"),
				kong.Writers(io.Discard, io.Discard),
				kong.Vars{"cli_name": "schemabot"},
			)
			require.NoError(t, err)

			ctx, err := parser.Parse(tc.args)
			require.NoError(t, err)
			assert.Equal(t, tc.want, usesLocalRuntime(ctx.Command(), &cli))
		})
	}

	// The commands that already resolved an endpoint keep doing so.
	var cli CLI
	parser, err := kong.New(&cli,
		kong.Name("schemabot"),
		kong.Writers(io.Discard, io.Discard),
		kong.Vars{"cli_name": "schemabot"},
	)
	require.NoError(t, err)
	ctx, err := parser.Parse([]string{"status", "apply_abc123"})
	require.NoError(t, err)
	assert.True(t, usesLocalRuntime(ctx.Command(), &cli))
}
