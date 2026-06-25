package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
)

// StartCmd resumes a stopped schema change.
type StartCmd struct {
	ControlFlags
	Watch bool `short:"w" help:"Watch progress until completion" default:"true" negatable:""`
}

// Run executes the start command.
func (cmd *StartCmd) Run(g *Globals) error {
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
		return fmt.Errorf("get progress: %w", err)
	}
	populateControlDisplayFields(&cmd.Database, result)

	curState := result.State
	if state.IsState(curState, state.Apply.Completed) {
		fmt.Println("Schema change already complete - nothing to start")
		return nil
	}
	if state.IsState(curState, state.Apply.Running, state.Apply.RunningDegraded, state.Apply.CuttingOver) {
		fmt.Println("Schema change already running")
		if cmd.Watch {
			return WatchApplyProgressByApplyID(ep, cmd.ApplyID, true)
		}
		return nil
	}
	if !state.IsState(curState, state.Apply.Stopped, state.Apply.WaitingForDeploy) {
		return fmt.Errorf("cannot start schema change in state: %s", curState)
	}

	// Call start API
	var startResult *apitypes.StartResponse
	err = withLoading("Starting schema change...", true, func() error {
		var startErr error
		startResult, startErr = client.CallStartAPI(ep, cmd.Environment, cmd.ApplyID)
		return startErr
	})
	if err != nil {
		return err
	}

	if err := checkAccepted(startResponseWrapper{startResult}, "start"); err != nil {
		return err
	}

	if !cmd.Watch {
		templates.WriteStartNoWatch(cmd.ApplyID, cmd.Database, cmd.Environment)
		return nil
	}

	templates.WriteStartSuccess(templates.StartData{
		Database:     cmd.Database,
		Environment:  cmd.Environment,
		ApplyID:      cmd.ApplyID,
		StartedCount: int(startResult.StartedCount),
		SkippedCount: int(startResult.SkippedCount),
	})
	fmt.Println()

	return WatchApplyProgressByApplyID(ep, cmd.ApplyID, true)
}
