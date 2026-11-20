package commands

import (
	"context"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

// CutoverCmd triggers cutover for a deferred schema change.
type CutoverCmd struct {
	ControlFlags
	Watch    bool   `short:"w" help:"Watch progress until completion" default:"true" negatable:""`
	Override string `help:"Reason to ask a freeze approver to let this change through a freeze" name:"override"`
}

// Run executes the cutover command.
func (cmd *CutoverCmd) Run(ctx context.Context, g *Globals) error {
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
	if err == nil {
		populateControlDisplayFields(&cmd.Database, result)
		if state.IsState(result.State, state.Apply.Completed) {
			fmt.Println("✓ Schema change already complete")
			tables := result.Tables
			if len(tables) > 0 {
				fmt.Println("Tables:")
				for _, tbl := range tables {
					if tbl.DDL != "" {
						fmt.Printf("  %s: %s\n", tbl.TableName, tbl.DDL)
					}
				}
			}
			return nil
		}
	}

	// Trigger cutover
	var cutoverResult *apitypes.ControlResponse
	err = withLoading("Requesting cutover...", true, func() error {
		var cutoverErr error
		cutoverResult, cutoverErr = client.CallCutoverAPIWithOverride(ep, cmd.Environment, cmd.ApplyID, cmd.Override)
		return cutoverErr
	})
	err = reportChangeGate(err, cmd.Environment, "cutover", strings.TrimSpace(cmd.Override) != "", false)
	if err != nil {
		return err
	}

	if err := checkAccepted(controlResponseWrapper{cutoverResult}, "cutover"); err != nil {
		return err
	}

	if cutoverResult.Status == apitypes.ControlStatusAlreadyInProgress {
		fmt.Println("✓ Cutover already in progress.")
	} else {
		fmt.Println("✓ Cutover requested successfully.")
	}
	fmt.Println("🔄 Cutting over...")

	if !cmd.Watch {
		printWatchInstructions(cmd.ApplyID, cmd.Database, cmd.Environment)
		return nil
	}

	// Watch progress until completion - cutover already triggered, so skip waiting instructions
	return WatchApplyProgressAfterCutover(ctx, ep, cmd.ApplyID, cmd.Environment)
}
