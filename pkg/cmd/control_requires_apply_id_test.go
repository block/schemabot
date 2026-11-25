package main

import (
	"context"
	"io"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

// A CLI control command that names no apply fails before it reaches the
// server, so it never acts on an apply the server would have to infer (CO-11).
// The command list comes from the storage registry, so a new control operation
// is held to the rule without anyone extending this test. Each command checks
// twice, in RequireApplyID and again in requireControlScope when it resolves
// its endpoint, and the endpoint is unroutable: only a command that skipped
// both checks would reach the network, and it would fail there with a
// different error.
func TestEveryCLIControlCommandRequiresAnApplyID(t *testing.T) {
	for _, op := range storage.ControlOperations() {
		t.Run(op.Command(), func(t *testing.T) {
			var cli CLI
			ctx, err := newCLIParserInto(t, &cli).Parse([]string{op.Command(), "-e", "staging", "--endpoint", "http://127.0.0.1:1"})
			require.NoError(t, err)
			ctx.BindTo(t.Context(), (*context.Context)(nil))

			err = ctx.Run(&cli.Globals)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "apply_id is required")
		})
	}

	// Rollback acts on the apply it reverses, and kong itself requires that
	// argument, so the command never runs without it.
	t.Run(action.Rollback, func(t *testing.T) {
		var cli CLI
		_, err := newCLIParserInto(t, &cli).Parse([]string{action.Rollback, "-e", "staging"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "<apply-id>")
	})
}

func newCLIParserInto(t *testing.T, cli *CLI) *kong.Kong {
	t.Helper()
	parser, err := kong.New(cli,
		kong.Name("schemabot"),
		kong.Writers(io.Discard, io.Discard),
		kong.Vars{"cli_name": "schemabot"},
	)
	require.NoError(t, err)
	return parser
}
