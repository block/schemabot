package commands

import (
	"fmt"
	"log/slog"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

// RevertCmd triggers a revert for a completed schema change during the revert window.
type RevertCmd struct {
	ControlFlags
}

// Run executes the revert command.
func (cmd *RevertCmd) Run(g *Globals) error {
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
		if !state.IsState(result.State, state.Apply.RevertWindow) {
			return fmt.Errorf("cannot revert: apply is in state %q (expected revert_window)", result.State)
		}
	} else {
		slog.Warn("could not verify apply state before revert, proceeding anyway", "error", err)
	}

	resp, err := client.CallRevertAPI(ep, cmd.Database, cmd.Environment)
	if err != nil {
		return fmt.Errorf("revert failed: %w", err)
	}

	if resp.Accepted {
		fmt.Printf("Revert initiated for %s/%s\n", cmd.Database, cmd.Environment)
	} else {
		fmt.Printf("Revert not accepted: %s\n", resp.ErrorMessage)
	}
	return nil
}
