package commands

import (
	"fmt"
	"log/slog"

	"github.com/block/schemabot/pkg/apitypes"
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
	var result *apitypes.ProgressResponse
	err = withLoading("Loading schema change progress...", true, func() error {
		var loadErr error
		result, loadErr = client.GetProgress(ep, cmd.ApplyID)
		return loadErr
	})
	if err == nil {
		populateControlDisplayFields(&cmd.Database, result)
		if !state.IsState(result.State, state.Apply.RevertWindow) {
			return fmt.Errorf("cannot revert: apply is in state %q (expected revert_window)", result.State)
		}
	} else {
		slog.Warn("could not verify apply state before revert, proceeding anyway",
			"apply_id", cmd.ApplyID, "environment", cmd.Environment, "error", err)
	}

	var resp *apitypes.ControlResponse
	err = withLoading("Requesting revert...", true, func() error {
		var revertErr error
		resp, revertErr = client.CallRevertAPI(ep, cmd.Environment, cmd.ApplyID)
		return revertErr
	})
	if err != nil {
		return fmt.Errorf("revert failed: %w", err)
	}

	if resp.Accepted {
		fmt.Printf("Revert initiated for %s\n", formatControlTarget(cmd.ApplyID, cmd.Database, cmd.Environment))
	} else {
		fmt.Printf("Revert not accepted: %s\n", resp.ErrorMessage)
	}
	return nil
}
