package commands

import (
	"context"
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
		settled, awaitErr = cmd.awaitEnd(ep, cancelRequestMatcher{
			accepted: cancelResult.ControlRequestRevision,
			before:   result.ControlRequest(cancelOperation),
		})
		return awaitErr
	})
	switch {
	case settled.state == "" && settled.request.declined():
		templates.WriteCancelDeclined(templates.CancelDeclinedData{
			Database:    cmd.Database,
			Environment: cmd.Environment,
			ApplyID:     cmd.ApplyID,
			Reason:      settled.request.ErrorMessage,
		})
	case state.IsState(settled.state, state.Apply.Cancelled):
		templates.WriteCancelSuccess(templates.CancelData{
			Database:       cmd.Database,
			Environment:    cmd.Environment,
			CancelledCount: int(cancelResult.CancelledCount),
			SkippedCount:   int(cancelResult.SkippedCount),
		})
	case settled.state != "" && settled.request.completed():
		templates.WriteCancelTookEffect(templates.CancelOvertakenData{
			Database:    cmd.Database,
			Environment: cmd.Environment,
			ApplyID:     cmd.ApplyID,
			State:       settled.state,
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
// in, if it ended, and this cancel's request as last read, if the progress
// answer named it. Both are empty when the wait read neither before its
// deadline.
type cancelSettlement struct {
	state   string
	request *cancelRequest
}

// cancelRequest is this cancel's request as the progress API reports it.
type cancelRequest struct {
	apitypes.ControlRequestOutcome
}

func (r *cancelRequest) declined() bool {
	return r != nil && r.Status == string(storage.ControlRequestFailed)
}

func (r *cancelRequest) completed() bool {
	return r != nil && r.Status == string(storage.ControlRequestCompleted)
}

func (r *cancelRequest) settled() bool {
	return r.declined() || r.completed()
}

// cancelRequestMatcher picks this cancel's request out of a progress answer.
// The cancel request row is re-armed in place for every later cancel, so a
// reported request is this cancel's only at the revision it was accepted at.
type cancelRequestMatcher struct {
	// accepted is the revision the server accepted this cancel at, or nil
	// when the server does not report one.
	accepted *int64
	// before is the apply's cancel request as read before this cancel was
	// sent, used only when accepted is nil.
	before *apitypes.ControlRequestOutcome
}

// match returns req when it is this cancel's request. With the accepted
// revision, that is req at exactly that revision: a later revision is a cancel
// someone else sent after this one settled, and its outcome is not this
// one's. Without it, req is this cancel's when none was recorded before it was
// sent, when the one recorded then was still pending (the server answers a
// repeat cancel with that request), or when it carries a later revision than
// one that had already settled.
func (m cancelRequestMatcher) match(req *apitypes.ControlRequestOutcome) *cancelRequest {
	if req == nil {
		return nil
	}
	if m.accepted != nil {
		if req.Revision != *m.accepted {
			return nil
		}
		return &cancelRequest{*req}
	}
	if m.before == nil || m.before.Status == string(storage.ControlRequestPending) || req.Revision > m.before.Revision {
		return &cancelRequest{*req}
	}
	return nil
}

// awaitEnd polls the apply until the cancel's outcome is known or the settle
// timeout passes, under one deadline that bounds every poll. The outcome is
// known when the apply ends cancelled, when this cancel's request settles, or
// when the apply ends and the progress answer names no request to wait on. An
// apply can end failed with the cancel still pending, as when one shard failed
// and the cancel ends the rest, so an ended apply waits for its request to
// settle. It returns the read error when the apply's state could not be read.
// A stopped apply has not ended: the cancel of a stopped apply still waits for
// a driver to consume it.
func (cmd *CancelCmd) awaitEnd(ep string, matcher cancelRequestMatcher) (cancelSettlement, error) {
	timeout := cmd.settleTimeout
	if timeout == 0 {
		timeout = cancelSettleTimeout
	}
	interval := cmd.settlePollInterval
	if interval == 0 {
		interval = cancelSettlePollInterval
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var last cancelSettlement
	for {
		progress, err := client.GetProgressCtx(ctx, ep, cmd.ApplyID)
		if err != nil {
			if ctx.Err() != nil {
				return last, nil
			}
			return last, fmt.Errorf("get progress for apply %s: %w", cmd.ApplyID, err)
		}
		last = cancelSettlement{request: matcher.match(progress.ControlRequest(cancelOperation))}
		if applyHasEnded(progress.State) {
			last.state = progress.State
		}
		if cancelOutcomeKnown(last, progress) {
			return last, nil
		}
		select {
		case <-ctx.Done():
			return last, nil
		case <-time.After(interval):
		}
	}
}

// cancelOutcomeKnown reports whether the wait can stop: the apply ended
// cancelled, this cancel's request settled, or the apply ended and the
// progress answer reports no cancel request to wait on.
func cancelOutcomeKnown(settled cancelSettlement, progress *apitypes.ProgressResponse) bool {
	if state.IsState(settled.state, state.Apply.Cancelled) {
		return true
	}
	if settled.request.settled() {
		return true
	}
	return settled.state != "" && progress.ControlRequest(cancelOperation) == nil
}

// applyHasEnded reports whether an apply is in a terminal state a cancel can
// no longer change. Stopped is terminal for a drive but not for a cancel,
// which still ends it.
func applyHasEnded(applyState string) bool {
	return state.IsTerminalApplyState(applyState) && !state.IsState(applyState, state.Apply.Stopped)
}
