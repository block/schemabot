package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
)

// ProgressCmd watches schema change progress for a specific apply.
// Use "schemabot status -d <database> -e <env>" to check what's active on a database.
type ProgressCmd struct {
	ControlFlags
	Watch bool `short:"w" help:"Watch progress until completion" default:"true" negatable:""`
	JSON  bool `help:"Print one progress snapshot as JSON; never opens the watcher"`
}

// Run executes the progress command.
func (cmd *ProgressCmd) Run(g *Globals) error {
	if cmd.ApplyID == "" {
		return fmt.Errorf("apply_id is required (use '%s' to find active applies)", cliname.Command("status", "-d", "<database>"))
	}

	ep, err := g.Resolve()
	if err != nil {
		return err
	}

	// JSON is for a caller that reads the output, so it takes one snapshot.
	// The watcher is interactive, and its keys can stop the apply or run its cutover.
	if cmd.Watch && !cmd.JSON {
		return watchApplyProgressByApplyIDWithEnvironment(ep, cmd.ApplyID, cmd.Environment, true)
	}

	var result *apitypes.ProgressResponse
	err = withLoading("Loading schema change progress...", !cmd.JSON, func() error {
		var loadErr error
		result, loadErr = client.GetProgress(ep, cmd.ApplyID)
		return loadErr
	})
	if err != nil {
		return err
	}

	if cmd.JSON {
		return writeJSON(result)
	}

	data := templates.ParseProgressResponse(result)
	templates.WriteProgress(data)
	return nil
}
