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
	"github.com/block/schemabot/pkg/storage"
)

// DatabasesCmd lists databases configured on the SchemaBot server.
type DatabasesCmd struct {
	Type string `help:"Database type filter (mysql, vitess, or strata)"`
	JSON bool   `help:"Output as JSON"`
}

// Run executes the databases command.
func (cmd *DatabasesCmd) Run(g *Globals) error {
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}
	if err := validateDatabaseListType(cmd.Type); err != nil {
		return err
	}

	resp, err := client.ListDatabases(ep, client.ListDatabasesOptions{Type: cmd.Type})
	if err != nil {
		return fmt.Errorf("list databases: %w", err)
	}
	if cmd.JSON {
		return writeJSON(resp)
	}
	return writeDatabaseList(os.Stdout, resp)
}

func validateDatabaseListType(databaseType string) error {
	switch databaseType {
	case "", storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess, storage.DatabaseTypeStrata:
		return nil
	default:
		return fmt.Errorf("--type must be %q, %q, or %q", storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess, storage.DatabaseTypeStrata)
	}
}

func writeDatabaseList(w io.Writer, resp *apitypes.DatabaseListResponse) error {
	if resp == nil || len(resp.Databases) == 0 {
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
