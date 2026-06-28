package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
)

// RedriveCmd re-plans and redrives a recent failed schema change.
type RedriveCmd struct {
	ControlFlags
	Force bool `help:"Force redrive by taking over an existing database lock"`
	Watch bool `short:"w" help:"Watch progress until completion" default:"true" negatable:""`
}

// Run executes the redrive command.
func (cmd *RedriveCmd) Run(g *Globals) error {
	if err := cmd.RequireApplyID(); err != nil {
		return err
	}
	ep, err := cmd.Resolve(g)
	if err != nil {
		return err
	}

	var redriveResult *apitypes.RedriveResponse
	err = withLoading("Re-planning failed schema change...", true, func() error {
		var redriveErr error
		redriveResult, redriveErr = client.CallRedriveAPI(ep, cmd.Environment, cmd.ApplyID, cmd.Force)
		return redriveErr
	})
	if err != nil {
		return err
	}
	if err := checkAccepted(redriveResponseWrapper{redriveResult}, "redrive"); err != nil {
		return err
	}

	printRedriveResult(redriveResult)
	if !cmd.Watch || redriveResult.ApplyID == "" {
		return nil
	}
	return WatchApplyProgressByApplyID(ep, redriveResult.ApplyID, true)
}

func printRedriveResult(result *apitypes.RedriveResponse) {
	if result == nil {
		return
	}
	switch result.Status {
	case apitypes.RedriveStatusRedriven:
		fmt.Printf("Redrive queued for %s\n", result.ApplyID)
	default:
		fmt.Printf("Redrive accepted for %s\n", result.ApplyID)
	}
	if result.PlanID != "" {
		fmt.Printf("Plan: %s\n", result.PlanID)
	}
	if result.Message != "" {
		fmt.Println(result.Message)
	}
}
