package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
)

// ReleaseCmd releases a rollout paused after an on_failure=pause deployment
// failure, letting the held later deployments proceed.
type ReleaseCmd struct {
	ControlFlags
	Watch bool `short:"w" help:"Watch progress until completion" default:"true" negatable:""`
}

// Run executes the release command.
func (cmd *ReleaseCmd) Run(g *Globals) error {
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
	if !state.IsState(curState, state.Apply.Paused) {
		return fmt.Errorf("cannot release schema change in state: %s (release only applies to a paused rollout)", curState)
	}

	// Call release API
	var releaseResult *apitypes.ReleaseResponse
	err = withLoading("Releasing paused rollout...", true, func() error {
		var releaseErr error
		releaseResult, releaseErr = client.CallReleaseAPI(ep, cmd.Environment, cmd.ApplyID)
		return releaseErr
	})
	if err != nil {
		return err
	}

	if err := checkAccepted(releaseResponseWrapper{releaseResult}, "release"); err != nil {
		return err
	}

	templates.WriteReleaseSuccess(templates.ReleaseData{
		Database:    cmd.Database,
		Environment: cmd.Environment,
		ApplyID:     cmd.ApplyID,
	})

	if !cmd.Watch {
		return nil
	}
	fmt.Println()
	return WatchApplyProgressByApplyID(ep, cmd.ApplyID, true)
}
