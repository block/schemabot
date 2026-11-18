package commands

import (
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
)

// StopCmd stops an in-progress schema change.
type StopCmd struct {
	ControlFlags
}

// Run executes the stop command.
func (cmd *StopCmd) Run(g *Globals) error {
	if err := cmd.RequireApplyID(); err != nil {
		return err
	}
	ep, err := cmd.Resolve(g)
	if err != nil {
		return err
	}

	// Check current state first
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
	if state.IsState(curState, state.Apply.Completed) {
		fmt.Println("Schema change already complete - nothing to stop")
		return nil
	}
	// A stopped schema change still goes to the server: a start requested
	// moments ago may be queued behind it, and only the server can withdraw it.
	if !state.IsRunningApplyState(curState) && !state.IsState(curState, state.Apply.Stopped, state.Apply.Paused, state.Apply.CuttingOver, state.Apply.WaitingForDeploy, state.Apply.WaitingForCutover, state.Apply.Pending) {
		return fmt.Errorf("cannot stop schema change in state: %s", curState)
	}

	// Call stop API
	var stopResult *apitypes.StopResponse
	err = withLoading("Stopping schema change...", true, func() error {
		var stopErr error
		stopResult, stopErr = client.CallStopAPI(ep, cmd.Environment, cmd.ApplyID)
		return stopErr
	})
	if stopRefusedAsAlreadyStopped(err) {
		fmt.Println("Schema change already stopped")
		return nil
	}
	if err != nil {
		return err
	}

	if err := checkAccepted(stopResponseWrapper{stopResult}, "stop"); err != nil {
		return err
	}

	templates.WriteStopSuccess(templates.StopData{
		Database:       cmd.Database,
		Environment:    cmd.Environment,
		ApplyID:        cmd.ApplyID,
		StoppedCount:   int(stopResult.StoppedCount),
		SkippedCount:   int(stopResult.SkippedCount),
		StartWithdrawn: stopResult.Status == apitypes.ControlStatusStartWithdrawn,
	})
	return nil
}

// stopRefusedAsAlreadyStopped reports whether the server refused the stop
// because the schema change is already stopped with no start queued. Only the
// already_stopped code says that: a server that predates it refuses the same
// stop with a bare conflict even when a start is queued behind the stopped
// schema change and will still resume it, so a bare conflict stays an error.
func stopRefusedAsAlreadyStopped(err error) bool {
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode == apitypes.ErrCodeAlreadyStopped
}
