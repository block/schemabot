package commands

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
)

// DatabasesCmd lists databases configured on the SchemaBot server.
type DatabasesCmd struct {
	Type string `help:"Only show databases of this type; the server validates the value against its configured database types"`
	Name string `help:"Only show databases whose name contains this string, case-insensitively; a family prefix like omnibus matches every shard"`
	JSON bool   `help:"Output as JSON"`
}

// Run executes the databases command.
func (cmd *DatabasesCmd) Run(g *Globals) error {
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}
	// The server treats a whitespace-only name filter as absent; trim here so
	// the rendered headers and empty-state message agree with what the server
	// actually filtered on.
	name := strings.TrimSpace(cmd.Name)

	var resp *apitypes.DatabaseListResponse
	err = withLoading("Loading databases...", !cmd.JSON, func() error {
		var loadErr error
		resp, loadErr = client.ListDatabases(ep, client.ListDatabasesOptions{Type: cmd.Type, Name: name})
		return loadErr
	})
	if err != nil {
		return fmt.Errorf("list databases: %w", err)
	}
	if cmd.JSON {
		return writeJSON(resp)
	}
	return writeDatabaseList(os.Stdout, resp, name)
}

func writeDatabaseList(w io.Writer, resp *apitypes.DatabaseListResponse, nameFilter string) error {
	if resp == nil || len(resp.Databases) == 0 {
		// An empty filtered list means no match, not an unconfigured server —
		// say so, or operators misread the deployment as empty.
		if nameFilter != "" {
			_, err := fmt.Fprintf(w, "No databases match --name %q.\n", nameFilter)
			return err
		}
		_, err := fmt.Fprintln(w, "No databases configured.")
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "DATABASE\tTYPE\tENVIRONMENTS\tDEPLOYMENTS"); err != nil {
		return err
	}
	for _, database := range resp.Databases {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
			database.Database,
			database.Type,
			databaseEnvironments(database.Environments),
			databaseDeployments(database.Environments),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func databaseEnvironments(environments []*apitypes.DatabaseEnvironmentResponse) string {
	names := make([]string, 0, len(environments))
	for _, environment := range environments {
		if environment == nil || environment.Environment == "" {
			continue
		}
		names = append(names, environment.Environment)
	}
	if len(names) == 0 {
		return "-"
	}
	return strings.Join(names, ", ")
}

func databaseDeployments(environments []*apitypes.DatabaseEnvironmentResponse) string {
	parts := make([]string, 0, len(environments))
	for _, environment := range environments {
		if environment == nil || len(environment.Deployments) == 0 {
			continue
		}
		deployments := append([]string(nil), environment.Deployments...)
		sort.Strings(deployments)
		parts = append(parts, fmt.Sprintf("%s: %s", environment.Environment, strings.Join(deployments, ", ")))
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, "; ")
}
