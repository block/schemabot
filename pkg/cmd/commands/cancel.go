package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
)

// CancelCmd cancels an in-progress schema change permanently.
type CancelCmd struct {
	ControlFlags
	AutoApprove bool `short:"y" help:"Skip confirmation prompt" name:"auto-approve"`
}

// Run executes the cancel command.
func (cmd *CancelCmd) Run(g *Globals) error {
	if err := cmd.RequireApplyID(); err != nil {
		return err
	}
	ep, err := cmd.Resolve(g)
	if err != nil {
		return err
	}

	var result *apitypes.ProgressResponse
	err = withLoading("Loading schema change progress...", true, func() error {
		var loadErr error
		result, loadErr = client.GetProgress(ep, cmd.ApplyID)
		return loadErr
	})
	if err != nil {
		return fmt.Errorf("get progress for apply %s: %w", cmd.ApplyID, err)
	}
	populateControlDisplayFields(&cmd.Database, result)

	curState := result.State
	if state.IsState(curState, state.NoActiveChange) || curState == "" {
		fmt.Printf("No active schema change for %s\n", formatControlTarget(cmd.ApplyID, cmd.Database, cmd.Environment))
		return nil
	}
	if state.IsTerminalApplyState(curState) && !state.IsState(curState, state.Apply.Stopped) {
		fmt.Printf("Schema change already terminal (state: %s) - nothing to cancel\n", curState)
		return nil
	}

	// Cancellation is permanent and non-resumable; require explicit confirmation
	// unless auto-approved. The server is the source of truth for which remaining
	// (non-terminal, or stopped) states are cancellable.
	if !cmd.AutoApprove {
		fmt.Printf("About to permanently cancel the schema change for %s (state: %s).\n",
			formatControlTarget(cmd.ApplyID, cmd.Database, cmd.Environment), curState)
		confirmed, err := confirmAction(
			"This cannot be resumed. Only 'yes' will be accepted: ",
			"\nCancel aborted.",
		)
		if err != nil {
			return err
		}
		if !confirmed {
			return nil
		}
	}

	var cancelResult *apitypes.CancelResponse
	err = withLoading("Cancelling schema change...", true, func() error {
		var cancelErr error
		cancelResult, cancelErr = client.CallCancelAPI(ep, cmd.Environment, cmd.ApplyID)
		return cancelErr
	})
	if err != nil {
		return err
	}
	if err := checkAccepted(cancelResponseWrapper{cancelResult}, "cancel"); err != nil {
		return err
	}

	templates.WriteCancelSuccess(templates.CancelData{
		Database:       cmd.Database,
		Environment:    cmd.Environment,
		CancelledCount: int(cancelResult.CancelledCount),
		SkippedCount:   int(cancelResult.SkippedCount),
	})
	return nil
}
