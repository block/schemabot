package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
)

// ReapplyCmd re-plans and reapplies a recent failed schema change.
type ReapplyCmd struct {
	ControlFlags
	Force bool `help:"Force reapply by taking over an existing database lock"`
	Watch bool `short:"w" help:"Watch progress until completion" default:"true" negatable:""`
}

// Run executes the reapply command.
func (cmd *ReapplyCmd) Run(g *Globals) error {
	if err := cmd.RequireApplyID(); err != nil {
		return err
	}
	ep, err := cmd.Resolve(g)
	if err != nil {
		return err
	}

	var reapplyResult *apitypes.ReapplyResponse
	err = withLoading("Re-planning failed schema change...", true, func() error {
		var reapplyErr error
		reapplyResult, reapplyErr = client.CallReapplyAPI(ep, cmd.Environment, cmd.ApplyID, cmd.Force)
		return reapplyErr
	})
	if err != nil {
		return err
	}
	if err := checkAccepted(reapplyResponseWrapper{reapplyResult}, "reapply"); err != nil {
		return err
	}

	printReapplyResult(reapplyResult)
	if !cmd.Watch || reapplyResult.ApplyID == "" {
		return nil
	}
	return WatchApplyProgressByApplyID(ep, reapplyResult.ApplyID, true)
}

func printReapplyResult(result *apitypes.ReapplyResponse) {
	if result == nil {
		return
	}
	switch result.Status {
	case apitypes.ReapplyStatusReapplied:
		fmt.Printf("Reapply queued for %s\n", result.ApplyID)
	default:
		fmt.Printf("Reapply accepted for %s\n", result.ApplyID)
	}
	if result.PlanID != "" {
		fmt.Printf("Plan: %s\n", result.PlanID)
	}
	if result.Message != "" {
		fmt.Println(result.Message)
	}
}
