package commands

import (
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
)

// Bounds on how long cancel waits for the schema change to end once the server
// accepts the request. The server records the cancel and a driver consumes it
// on its next pass over the apply, which normally takes a few seconds.
const (
	cancelSettleTimeout      = 30 * time.Second
	cancelSettlePollInterval = time.Second
)

// CancelCmd cancels an in-progress schema change permanently.
type CancelCmd struct {
	ControlFlags
	AutoApprove bool `short:"y" help:"Skip confirmation prompt" name:"auto-approve"`

	// settleTimeout and settlePollInterval override the wait bounds above;
	// zero uses them.
	settleTimeout      time.Duration
	settlePollInterval time.Duration
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
	if applyHasEnded(curState) {
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

	var settled string
	lookupErr := withLoading("Waiting for the schema change to end...", true, func() error {
		var awaitErr error
		settled, awaitErr = cmd.awaitEnd(ep)
		return awaitErr
	})
	switch {
	case state.IsState(settled, state.Apply.Cancelled):
		templates.WriteCancelSuccess(templates.CancelData{
			Database:       cmd.Database,
			Environment:    cmd.Environment,
			CancelledCount: int(cancelResult.CancelledCount),
			SkippedCount:   int(cancelResult.SkippedCount),
		})
	case settled != "":
		templates.WriteCancelOvertaken(templates.CancelOvertakenData{
			Database:    cmd.Database,
			Environment: cmd.Environment,
			ApplyID:     cmd.ApplyID,
			State:       settled,
		})
	default:
		templates.WriteCancelPending(templates.CancelPendingData{
			Database:       cmd.Database,
			Environment:    cmd.Environment,
			ApplyID:        cmd.ApplyID,
			CancelledCount: int(cancelResult.CancelledCount),
			LookupErr:      lookupErr,
		})
	}
	return nil
}

// awaitEnd polls the apply until it ends or the settle timeout passes. It
// returns the state the apply ended in, or "" when it is still active at the
// deadline or its state could not be read, with the read error in that case.
// A stopped apply has not ended: the cancel of a stopped apply still waits
// for a driver to consume it.
func (cmd *CancelCmd) awaitEnd(ep string) (string, error) {
	timeout := cmd.settleTimeout
	if timeout == 0 {
		timeout = cancelSettleTimeout
	}
	interval := cmd.settlePollInterval
	if interval == 0 {
		interval = cancelSettlePollInterval
	}
	deadline := time.Now().Add(timeout)
	for {
		progress, err := client.GetProgress(ep, cmd.ApplyID)
		if err != nil {
			return "", fmt.Errorf("get progress for apply %s: %w", cmd.ApplyID, err)
		}
		if applyHasEnded(progress.State) {
			return progress.State, nil
		}
		if time.Now().Add(interval).After(deadline) {
			return "", nil
		}
		time.Sleep(interval)
	}
}

// applyHasEnded reports whether an apply is in a terminal state a cancel can
// no longer change. Stopped is terminal for a drive but not for a cancel,
// which still ends it.
func applyHasEnded(applyState string) bool {
	return state.IsTerminalApplyState(applyState) && !state.IsState(applyState, state.Apply.Stopped)
}
