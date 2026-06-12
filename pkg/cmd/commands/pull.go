package commands

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
)

// PullCmd returns live schema from a source environment without writing files.
type PullCmd struct {
	Database    string `short:"d" required:"" help:"Database name from SchemaBot server config"`
	Environment string `short:"e" required:"" help:"Source environment to pull from"`
	Type        string `help:"Database type" default:"mysql" enum:"mysql"`
}

// Run executes the pull command.
func (cmd *PullCmd) Run(g *Globals) error {
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}
	resp, err := client.CallPullSchemaAPI(ep, cmd.Database, cmd.Type, cmd.Environment)
	if err != nil {
		if outputSchemaPullRequestError("Pull", cmd.Database, cmd.Environment, err) {
			return ErrSilent
		}
		return fmt.Errorf("pull schema for database %s environment %s: %w", cmd.Database, cmd.Environment, err)
	}
	if err := writePullSchemaResponse(os.Stdout, resp); err != nil {
		return fmt.Errorf("write pull schema response: %w", err)
	}
	return nil
}

func writePullSchemaResponse(w io.Writer, resp *apitypes.PullSchemaResponse) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(resp)
}
