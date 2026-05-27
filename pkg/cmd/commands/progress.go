package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
)

// ProgressCmd watches schema change progress for a specific apply.
// Use "schemabot status -d <database> -e <env>" to check what's active on a database.
type ProgressCmd struct {
	ControlFlags
	Watch bool `short:"w" help:"Watch progress until completion" default:"true" negatable:""`
}

// Run executes the progress command.
func (cmd *ProgressCmd) Run(g *Globals) error {
	if cmd.ApplyID == "" {
		return fmt.Errorf("apply_id is required (use 'schemabot status -d <database>' to find active applies)")
	}

	ep, err := g.Resolve()
	if err != nil {
		return err
	}

	if cmd.Watch {
		return watchApplyProgressByApplyIDWithEnvironment(ep, cmd.ApplyID, cmd.Environment, true)
	}

	result, err := client.GetProgress(ep, cmd.ApplyID)
	if err != nil {
		return err
	}

	data := templates.ParseProgressResponse(result)
	templates.WriteProgress(data)
	return nil
}
