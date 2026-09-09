package commands

import (
	"fmt"
	"io"
	"os"
	"slices"
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
	App  string `help:"Only show databases belonging to this app; matches the whole app name, so every database of one application is returned together"`
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
	app := strings.TrimSpace(cmd.App)

	var resp *apitypes.DatabaseListResponse
	err = withLoading("Loading databases...", !cmd.JSON, func() error {
		var loadErr error
		resp, loadErr = client.ListDatabases(ep, client.ListDatabasesOptions{Type: cmd.Type, Name: name, App: app})
		return loadErr
	})
	if err != nil {
		return fmt.Errorf("list databases: %w", err)
	}
	if cmd.JSON {
		return writeJSON(resp)
	}
	return writeDatabaseList(os.Stdout, resp, name, app)
}

func writeDatabaseList(w io.Writer, resp *apitypes.DatabaseListResponse, nameFilter, appFilter string) error {
	if resp == nil || len(resp.Databases) == 0 {
		// An empty filtered list means no match, not an unconfigured server —
		// say so, or operators misread the deployment as empty.
		if filters := activeDatabaseFilters(nameFilter, appFilter); filters != "" {
			_, err := fmt.Fprintf(w, "No databases match %s.\n", filters)
			return err
		}
		_, err := fmt.Fprintln(w, "No databases configured.")
		return err
	}

	// The app column appears only once some database reports an app, so a
	// deployment that has not adopted the key is not given a column of dashes.
	showApp := slices.ContainsFunc(resp.Databases, func(database *apitypes.DatabaseResponse) bool {
		return database != nil && database.App != ""
	})

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	header := "DATABASE\tTYPE\tENVIRONMENTS\tDEPLOYMENTS"
	if showApp {
		header = "DATABASE\tAPP\tTYPE\tENVIRONMENTS\tDEPLOYMENTS"
	}
	if _, err := fmt.Fprintln(tw, header); err != nil {
		return err
	}
	for _, database := range resp.Databases {
		fields := []string{database.Database, database.Type, databaseEnvironments(database.Environments), databaseDeployments(database.Environments)}
		if showApp {
			fields = []string{database.Database, orDash(database.App), database.Type, databaseEnvironments(database.Environments), databaseDeployments(database.Environments)}
		}
		if _, err := fmt.Fprintln(tw, strings.Join(fields, "\t")); err != nil {
			return err
		}
	}
	return tw.Flush()
}

// activeDatabaseFilters describes the filters a caller actually supplied, so an
// empty result names the query that produced it rather than only the first
// filter that happened to be checked.
func activeDatabaseFilters(nameFilter, appFilter string) string {
	parts := make([]string, 0, 2)
	if nameFilter != "" {
		parts = append(parts, fmt.Sprintf("--name %q", nameFilter))
	}
	if appFilter != "" {
		parts = append(parts, fmt.Sprintf("--app %q", appFilter))
	}
	return strings.Join(parts, " and ")
}

func orDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
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
