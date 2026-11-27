package commands

import (
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
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

	var settled cancelSettlement
	lookupErr := withLoading("Waiting for the schema change to end...", true, func() error {
		var awaitErr error
		settled, awaitErr = cmd.awaitEnd(ep, result.ControlRequest(cancelOperation))
		return awaitErr
	})
	switch {
	case settled.declined != nil:
		templates.WriteCancelDeclined(templates.CancelDeclinedData{
			Database:    cmd.Database,
			Environment: cmd.Environment,
			ApplyID:     cmd.ApplyID,
			Reason:      settled.declined.ErrorMessage,
		})
	case state.IsState(settled.state, state.Apply.Cancelled):
		templates.WriteCancelSuccess(templates.CancelData{
			Database:       cmd.Database,
			Environment:    cmd.Environment,
			CancelledCount: int(cancelResult.CancelledCount),
			SkippedCount:   int(cancelResult.SkippedCount),
		})
	case settled.state != "":
		templates.WriteCancelOvertaken(templates.CancelOvertakenData{
			Database:    cmd.Database,
			Environment: cmd.Environment,
			ApplyID:     cmd.ApplyID,
			State:       settled.state,
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

// cancelOperation is the control operation name the progress API reports a
// cancel request under.
const cancelOperation = "cancel"

// cancelSettlement is how a cancel's wait ended: the state the apply ended
// in, or the cancel request the schema change declined while it runs on.
// Both are empty when the apply is still active at the deadline.
type cancelSettlement struct {
	state    string
	declined *apitypes.ControlRequestOutcome
}

// awaitEnd polls the apply until it ends, its cancel request is declined, or
// the settle timeout passes. before is the apply's cancel request as read
// before this cancel was sent, so a request settled by an earlier cancel is
// not mistaken for this one's answer. It returns the read error when the
// apply's state could not be read. A stopped apply has not ended: the cancel
// of a stopped apply still waits for a driver to consume it.
func (cmd *CancelCmd) awaitEnd(ep string, before *apitypes.ControlRequestOutcome) (cancelSettlement, error) {
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
			return cancelSettlement{}, fmt.Errorf("get progress for apply %s: %w", cmd.ApplyID, err)
		}
		if applyHasEnded(progress.State) {
			return cancelSettlement{state: progress.State}, nil
		}
		if req := progress.ControlRequest(cancelOperation); cancelDeclined(req, before) {
			return cancelSettlement{declined: req}, nil
		}
		if time.Now().Add(interval).After(deadline) {
			return cancelSettlement{}, nil
		}
		time.Sleep(interval)
	}
}

// cancelDeclined reports whether req is this cancel's request and the
// schema change declined it. The request is this cancel's when none was
// recorded before it was sent, when the one recorded then was still pending
// (the server answers a repeat cancel with that request), or when it carries
// a later revision than a request that had already settled.
func cancelDeclined(req, before *apitypes.ControlRequestOutcome) bool {
	if req == nil || req.Status != string(storage.ControlRequestFailed) {
		return false
	}
	if before == nil || before.Status == string(storage.ControlRequestPending) {
		return true
	}
	return req.Revision > before.Revision
}

// applyHasEnded reports whether an apply is in a terminal state a cancel can
// no longer change. Stopped is terminal for a drive but not for a cancel,
// which still ends it.
func applyHasEnded(applyState string) bool {
	return state.IsTerminalApplyState(applyState) && !state.IsState(applyState, state.Apply.Stopped)
}
