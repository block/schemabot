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
