package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/templates"
)

// ProgressCmd gets schema change progress for a database.
type ProgressCmd struct {
	ControlFlags
	Watch bool `short:"w" help:"Watch progress until completion" default:"true" negatable:""`
}

// Run executes the progress command.
func (cmd *ProgressCmd) Run(g *Globals) error {
	if cmd.ApplyID == "" {
		if cmd.Database == "" {
			return fmt.Errorf("--database is required (or use --apply-id)")
		}
		if cmd.Environment == "" {
			return fmt.Errorf("--environment is required (or use --apply-id)")
		}
	}

	ep, err := g.Resolve()
	if err != nil {
		return err
	}

	// Apply ID path
	if cmd.ApplyID != "" {
		if cmd.Watch {
			return WatchApplyProgressByApplyID(ep, cmd.ApplyID, true)
		}

		result, err := client.GetProgressByApplyID(ep, cmd.ApplyID)
		if err != nil {
			return err
		}

		data := templates.ParseProgressResponse(result)
		templates.WriteProgress(data)
		return nil
	}

	// Database + environment path
	if cmd.Watch {
		return WatchApplyProgress(ep, cmd.Database, cmd.Environment, true)
	}

	result, err := client.GetProgress(ep, cmd.Database, cmd.Environment)
	if err != nil {
		return err
	}

	data := templates.ParseProgressResponse(result)
	templates.WriteProgress(data)

	return nil
}
