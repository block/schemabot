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
	Database      string   `short:"d" required:"" help:"Database name from SchemaBot server config"`
	Environment   string   `short:"e" required:"" help:"Source environment to pull from"`
	Type          string   `help:"Database type override; resolved from the server's registered config when omitted"`
	Namespaces    []string `name:"namespace" help:"Concrete live namespace to pull. Repeat for multiple namespaces. Omit to discover all non-reserved namespaces."`
	CatalogDetail string   `help:"Structured catalog detail to include" default:"basic" enum:"basic,detailed"`
}

// Run executes the pull command.
func (cmd *PullCmd) Run(g *Globals) error {
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}
	resp, err := client.CallPullSchemaAPIWithOptions(ep, cmd.Database, cmd.Type, cmd.Environment, client.PullSchemaOptions{
		Namespaces:    cmd.Namespaces,
		CatalogDetail: cmd.CatalogDetail,
	})
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
