package commands

import (
	"bytes"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/client"
)

// The preflight both apply and rollback run does not refuse when it cannot
// tell whether the target is busy. The server guards the same conflict when
// the apply is created, so the command goes on, and the failed check is logged
// with the target it was about.
func TestRefuseActiveSchemaChange_ContinuesAndLogsWhenTheCheckFails(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	var err error
	out := captureStdout(func() {
		err = refuseActiveSchemaChange("shop", "production", false, func() (*client.ActiveSchemaChange, error) {
			return nil, errors.New("status unavailable")
		})
	})

	require.NoError(t, err)
	assert.Empty(t, out, "a failed check prints nothing to the operator's output")
	assert.Contains(t, logs.String(), "could not check for an active schema change, proceeding without it")
	assert.Contains(t, logs.String(), "database=shop")
	assert.Contains(t, logs.String(), "environment=production")
	assert.Contains(t, logs.String(), "status unavailable")
}

// A target with no active schema change, or one in a state that does not hold
// it, is not refused.
func TestRefuseActiveSchemaChange_ContinuesWhenNothingHoldsTheTarget(t *testing.T) {
	cases := map[string]*client.ActiveSchemaChange{
		"no active schema change":        nil,
		"an active change with no state": {ApplyID: "apply-1"},
		"a state that does not hold it":  {ApplyID: "apply-1", State: "revert_window"},
	}
	for name, active := range cases {
		t.Run(name, func(t *testing.T) {
			var err error
			out := captureStdout(func() {
				err = refuseActiveSchemaChange("shop", "production", false, func() (*client.ActiveSchemaChange, error) {
					return active, nil
				})
			})
			require.NoError(t, err)
			assert.Empty(t, out)
		})
	}
}
