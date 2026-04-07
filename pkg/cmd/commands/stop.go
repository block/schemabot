package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/templates"
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
	result, err := client.GetProgress(ep, cmd.Database, cmd.Environment)
	if err != nil {
		// Check if this is a "not found" error - likely means no active schema change
		if client.IsNotFound(err) {
			fmt.Printf("No active schema change found for database '%s' environment '%s'\n", cmd.Database, cmd.Environment)
			return nil
		}
		return fmt.Errorf("get progress: %w", err)
	}

	curState := result.State
	if state.IsState(curState, StateNoActiveChange) || curState == "" {
		fmt.Printf("No active schema change for database '%s' environment '%s'\n", cmd.Database, cmd.Environment)
		return nil
	}
	if state.IsState(curState, StateCompleted) {
		fmt.Println("Schema change already complete - nothing to stop")
		return nil
	}
	if state.IsState(curState, StateStopped) {
		fmt.Println("Schema change already stopped")
		return nil
	}
	if !state.IsState(curState, StateRunning, StateCuttingOver, StateWaitingForCutover, StatePending) {
		return fmt.Errorf("cannot stop schema change in state: %s", curState)
	}

	// Always scope stop to the specific apply to avoid cross-apply contamination.
	autoResolveApplyID(&cmd.ApplyID, result)

	// Call stop API
	stopResult, err := client.CallStopAPI(ep, cmd.Database, cmd.Environment, cmd.ApplyID)
	if err != nil {
		return err
	}

	if err := checkAccepted(stopResponseWrapper{stopResult}, "stop"); err != nil {
		return err
	}

	templates.WriteStopSuccess(templates.StopData{
		Database:     cmd.Database,
		Environment:  cmd.Environment,
		ApplyID:      cmd.ApplyID,
		StoppedCount: int(stopResult.StoppedCount),
		SkippedCount: int(stopResult.SkippedCount),
	})
	return nil
}
