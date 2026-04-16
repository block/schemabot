package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

// VolumeCmd adjusts the speed of an in-progress schema change.
type VolumeCmd struct {
	ControlFlags
	Volume int  `short:"v" required:"" help:"Volume level 1-11: 1=slowest, 11=fastest"`
	Watch  bool `short:"w" help:"Watch progress until completion" default:"true" negatable:""`
}

// Run executes the volume command.
func (cmd *VolumeCmd) Run(g *Globals) error {
	if err := cmd.RequireApplyID(); err != nil {
		return err
	}
	if cmd.Volume < 1 || cmd.Volume > 11 {
		return fmt.Errorf("--volume must be between 1 and 11")
	}

	ep, err := cmd.Resolve(g)
	if err != nil {
		return err
	}

	// Check current state first
	result, err := client.GetProgress(ep, cmd.Database, cmd.Environment)
	if err != nil {
		return fmt.Errorf("get progress: %w", err)
	}

	curState := result.State
	if state.IsState(curState, state.Apply.Completed) {
		fmt.Println("Schema change already complete - cannot adjust volume")
		return nil
	}
	if state.IsState(curState, state.Apply.Failed) {
		return fmt.Errorf("schema change failed - cannot adjust volume")
	}
	if !state.IsState(curState, state.Apply.Running, state.Apply.CuttingOver, state.Apply.WaitingForCutover, state.Apply.Stopped) {
		return fmt.Errorf("cannot adjust volume in state: %s", curState)
	}

	// Auto-resolve apply_id from progress response
	autoResolveApplyID(&cmd.ApplyID, result)

	// Call volume API
	volumeResult, err := client.CallVolumeAPI(ep, cmd.Database, cmd.Environment, cmd.ApplyID, cmd.Volume)
	if err != nil {
		return err
	}

	if err := checkAccepted(volumeResponseWrapper{volumeResult}, "volume change"); err != nil {
		return err
	}

	fmt.Printf("Volume adjusted: %d → %d\n", int(volumeResult.PreviousVolume), int(volumeResult.NewVolume))

	if !cmd.Watch {
		printWatchInstructions(cmd.ApplyID, cmd.Database, cmd.Environment)
		return nil
	}

	return WatchApplyProgress(ep, cmd.Database, cmd.Environment, true)
}
