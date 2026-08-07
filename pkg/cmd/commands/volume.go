package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
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
	var result *apitypes.ProgressResponse
	err = withLoading("Loading schema change progress...", true, func() error {
		var loadErr error
		result, loadErr = client.GetProgress(ep, cmd.ApplyID)
		return loadErr
	})
	if err != nil {
		return fmt.Errorf("get progress: %w", err)
	}
	populateControlDisplayFields(&cmd.Database, result)

	curState := result.State
	if state.IsState(curState, state.Apply.Completed) {
		fmt.Println("Schema change already complete - cannot adjust volume")
		return nil
	}
	if state.IsState(curState, state.Apply.Failed) {
		return fmt.Errorf("schema change failed - cannot adjust volume")
	}
	if state.IsState(curState, state.Apply.Stopped) {
		return fmt.Errorf("schema change is stopped - start it first, then adjust volume")
	}
	if !state.IsState(curState, state.Apply.Running, state.Apply.RunningDegraded, state.Apply.CuttingOver, state.Apply.WaitingForCutover) {
		return fmt.Errorf("cannot adjust volume in state: %s", curState)
	}

	// Call volume API
	var volumeResult *apitypes.VolumeResponse
	err = withLoading("Queueing schema change volume adjustment...", true, func() error {
		var volumeErr error
		volumeResult, volumeErr = client.CallVolumeAPI(ep, cmd.Environment, cmd.ApplyID, cmd.Volume)
		return volumeErr
	})
	if err != nil {
		return err
	}

	if err := checkAccepted(volumeResponseWrapper{volumeResult}, "volume change"); err != nil {
		return err
	}

	fmt.Printf("Volume change to %d queued; the driver applies it at its next progress check.\n", int(volumeResult.NewVolume))

	if !cmd.Watch {
		printWatchInstructions(cmd.ApplyID, cmd.Database, cmd.Environment)
		return nil
	}

	return WatchApplyProgressByApplyID(ep, cmd.ApplyID, true)
}
