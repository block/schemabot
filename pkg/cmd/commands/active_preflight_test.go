package commands

import (
	"bytes"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
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
	assert.Contains(t, logs.String(), "could not check for an active schema change; proceeding, and the server will refuse the apply if a schema change holds the database")
	assert.Contains(t, logs.String(), "database=shop")
	assert.Contains(t, logs.String(), "environment=production")
	assert.Contains(t, logs.String(), "status unavailable")
}

// A target with no active schema change is not refused.
func TestRefuseActiveSchemaChange_ContinuesWhenNothingHoldsTheTarget(t *testing.T) {
	cases := map[string]*client.ActiveSchemaChange{
		"no active schema change":        nil,
		"an active change with no state": {ApplyID: "apply-1"},
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

// The server refuses a new apply while any non-terminal apply holds the
// target, so the preflight refuses on every active state too, and the operator
// learns it before the plan, the prompt and the lock. A state with no wording
// of its own is named by its label, with what it does to the database when
// the state registry says.
func TestRefuseActiveSchemaChange_RefusesEveryHoldingState(t *testing.T) {
	cases := map[string]string{
		state.Apply.RevertWindow:            "A schema change is active (Revert window); it holds the database until it is reverted or skip-reverted.",
		state.Apply.FailedRetryable:         "A schema change is active (Retrying); it holds the database until it is retried or cancelled.",
		state.Apply.Reverting:               "A schema change is active (Reverting); it releases the database when the revert finishes, which it does on its own.",
		state.Apply.Paused:                  "A schema change is active (Paused).",
		state.Apply.Resuming:                "A schema change is active (Resuming).",
		state.Apply.Recovering:              "A schema change is active (Recovering).",
		state.Apply.SkippingRevert:          "A schema change is active (Skipping revert); it releases the database when the revert finishes, which it does on its own.",
		state.Apply.PreparingBranch:         "A schema change is active (Preparing branch).",
		state.Apply.ApplyingBranchChanges:   "A schema change is active (Applying changes to branch).",
		state.Apply.ValidatingBranch:        "A schema change is active (Validating branch).",
		state.Apply.CreatingDeployRequest:   "A schema change is active (Creating deploy request).",
		state.Apply.ValidatingDeployRequest: "A schema change is active (Validating deploy request).",
	}
	for activeState, want := range cases {
		t.Run(activeState, func(t *testing.T) {
			var err error
			out := captureStdout(func() {
				err = refuseActiveSchemaChange("shop", "production", false, func() (*client.ActiveSchemaChange, error) {
					return &client.ActiveSchemaChange{ApplyID: "apply-busy", State: activeState}, nil
				})
			})
			require.Error(t, err)
			assert.Contains(t, out, "Schema Change In Progress")
			assert.Contains(t, out, want)
			assert.Contains(t, out, "apply-busy")
		})
	}
}
