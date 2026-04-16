package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/state"
)

// CutoverCmd triggers cutover for a deferred schema change.
type CutoverCmd struct {
	ControlFlags
	Watch bool `short:"w" help:"Watch progress until completion" default:"true" negatable:""`
}

// Run executes the cutover command.
func (cmd *CutoverCmd) Run(g *Globals) error {
	if err := cmd.RequireApplyID(); err != nil {
		return err
	}
	ep, err := cmd.Resolve(g)
	if err != nil {
		return err
	}

	// Check current state first
	result, err := client.GetProgress(ep, cmd.Database, cmd.Environment)
	if err == nil {
		if state.IsState(result.State, state.Apply.Completed) {
			fmt.Println("✓ Schema change already complete")
			tables := ddl.FilterInternalTablesTyped(result.Tables)
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

	// Always scope cutover to the specific apply to avoid cross-apply contamination.
	if result != nil {
		autoResolveApplyID(&cmd.ApplyID, result)
	}

	// Trigger cutover
	cutoverResult, err := client.CallCutoverAPI(ep, cmd.Database, cmd.Environment, cmd.ApplyID)
	if err != nil {
		return err
	}

	if err := checkAccepted(controlResponseWrapper{cutoverResult}, "cutover"); err != nil {
		return err
	}

	fmt.Println("✓ Cutover requested successfully.")
	fmt.Println("🔄 Cutting over...")

	if !cmd.Watch {
		printWatchInstructions(cmd.ApplyID, cmd.Database, cmd.Environment)
		return nil
	}

	// Watch progress until completion - cutover already triggered, so skip waiting instructions
	return WatchApplyProgressAfterCutover(ep, cmd.Database, cmd.Environment)
}
