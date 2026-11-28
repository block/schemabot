package commands

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
)

// StartCmd resumes a stopped schema change.
type StartCmd struct {
	ControlFlags
	Watch    bool   `short:"w" help:"Watch progress until completion" default:"true" negatable:""`
	Override string `help:"Why this change can't wait. It lets the change through a freeze, or asks a freeze approver to, as the server's override policy says" name:"override"`
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
	if state.IsRunningApplyState(curState) || state.IsState(curState, state.Apply.CuttingOver) {
		fmt.Println("Schema change already running")
		if cmd.Watch {
			return watchControlledApply(ep, cmd.ApplyID, cmd.Environment, true)
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
		startResult, startErr = client.CallStartAPIWithOverride(ep, cmd.Environment, cmd.ApplyID, cmd.Override)
		return startErr
	})
	err = reportChangeGate(err, cmd.Environment, "start", strings.TrimSpace(cmd.Override) != "", false)
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

	return watchControlledApply(ep, cmd.ApplyID, cmd.Environment, true)
}
