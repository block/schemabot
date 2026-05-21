package commands

import (
	"fmt"
	"log/slog"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

// SkipRevertCmd skips the revert window, finalizing a completed schema change.
type SkipRevertCmd struct {
	ControlFlags
}

// Run executes the skip-revert command.
func (cmd *SkipRevertCmd) Run(g *Globals) error {
	if err := cmd.RequireApplyID(); err != nil {
		return err
	}
	ep, err := cmd.Resolve(g)
	if err != nil {
		return fmt.Errorf("resolve endpoint: %w", err)
	}

	// Check current state
	result, err := client.GetProgress(ep, cmd.ApplyID)
	if err == nil {
		populateControlDisplayFields(&cmd.Database, result)
		if !state.IsState(result.State, state.Apply.RevertWindow) {
			return fmt.Errorf("cannot skip revert: apply is in state %q (expected revert_window)", result.State)
		}
	} else {
		slog.Warn("could not verify apply state before skip-revert, proceeding anyway",
			"apply_id", cmd.ApplyID, "environment", cmd.Environment, "error", err)
	}

	resp, err := client.CallSkipRevertAPI(ep, cmd.Environment, cmd.ApplyID)
	if err != nil {
		return fmt.Errorf("skip-revert failed: %w", err)
	}

	if resp.Accepted {
		fmt.Printf("Revert window skipped for %s — schema change finalized\n", formatControlTarget(cmd.ApplyID, cmd.Database, cmd.Environment))
	} else {
		fmt.Printf("Skip-revert not accepted: %s\n", resp.ErrorMessage)
	}
	return nil
}
