package commands

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
)

// PullCmd returns live schema from a source environment without writing files.
type PullCmd struct {
	Database      string   `short:"d" required:"" help:"Database name from SchemaBot server config"`
	Environment   string   `short:"e" required:"" help:"Source environment to pull from"`
	Type          string   `help:"Database type override; resolved from the server's registered config when omitted"`
	Namespaces    []string `name:"namespace" help:"Concrete live namespace to pull. Repeat for multiple namespaces. Omit to discover all non-reserved namespaces."`
	Table         string   `help:"Only show tables whose name contains this string, case-insensitively; a prefix matches every table in its family. Applies in every namespace and output format."`
	CatalogDetail string   `help:"Structured catalog detail to include" default:"basic" enum:"basic,detailed"`
	Lint          bool     `help:"Run the schema linters over every pulled table and include the violations per namespace"`
	Output        string   `short:"o" help:"Output format: pretty renders the schema as readable SQL, json emits the API response after any --table filter" default:"pretty" enum:"pretty,json"`
}

// Run executes the pull command.
func (cmd *PullCmd) Run(g *Globals) error {
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}
	var resp *apitypes.PullSchemaResponse
	err = withLoading("Pulling live schema...", true, func() error {
		var pullErr error
		resp, pullErr = client.CallPullSchemaAPIWithOptions(ep, cmd.Database, cmd.Type, cmd.Environment, client.PullSchemaOptions{
			Namespaces:    cmd.Namespaces,
			CatalogDetail: cmd.CatalogDetail,
			Lint:          cmd.Lint,
		})
		return pullErr
	})
	if err != nil {
		if outputSchemaPullRequestError("Pull", cmd.Database, cmd.Environment, err) {
			return ErrSilent
		}
		return fmt.Errorf("pull schema for database %s environment %s: %w", cmd.Database, cmd.Environment, err)
	}
	if table := strings.TrimSpace(cmd.Table); table != "" {
		if err := filterPullSchemaTables(resp, table); err != nil {
			return err
		}
	}
	if cmd.Output == "json" {
		if err := writePullSchemaResponse(os.Stdout, resp); err != nil {
			return fmt.Errorf("write pull schema response: %w", err)
		}
		return nil
	}
	templates.WritePullSchema(resp)
	return nil
}

// maxTablesInFilterError caps how many available table names an unmatched
// --table error lists before falling back to a count, so a typo against a
// small database gets the correction inline while a huge one stays readable.
const maxTablesInFilterError = 20

// filterPullSchemaTables narrows a pulled schema to the tables whose name
// contains the filter, case-insensitively — the same matching the databases
// list uses for --name — so an operator can read one table without scrolling
// a full database dump, select a prefix family in one flag, or diff the same
// table across environments from two filtered pulls. The filter is a
// client-side projection over the API response: each namespace keeps only the
// matching tables with their catalog entries and lint findings, a namespace
// left with no match drops out entirely, and the table count reflects what
// remains. Artifacts are namespace-scoped rather than per-table, so a
// table-filtered pull omits them. A filter that matches no table in any
// pulled namespace is an error, so a typo never reads as an empty but
// successful pull.
func filterPullSchemaTables(resp *apitypes.PullSchemaResponse, filter string) error {
	needle := strings.ToLower(filter)
	matched := make(map[string]map[string]string, len(resp.Namespaces))
	var kept int32
	for name, ns := range resp.Namespaces {
		keptTables := make(map[string]string)
		for table, ddl := range ns.Tables {
			if strings.Contains(strings.ToLower(table), needle) {
				keptTables[table] = ddl
			}
		}
		matched[name] = keptTables
		kept += int32(len(keptTables))
	}
	if kept == 0 {
		return errNoTableMatches(resp, filter)
	}
	for name, ns := range resp.Namespaces {
		keptTables := matched[name]
		if len(keptTables) == 0 {
			delete(resp.Namespaces, name)
			continue
		}
		ns.Tables = keptTables
		ns.Artifacts = nil
		if ns.NamespaceCatalog != nil {
			ns.NamespaceCatalog.TableCount = int32(len(keptTables))
		}
		if ns.TableCatalog != nil {
			keptCatalog := make(map[string]*apitypes.TableCatalog, len(keptTables))
			for table, catalog := range ns.TableCatalog {
				if _, ok := keptTables[table]; ok {
					keptCatalog[table] = catalog
				}
			}
			ns.TableCatalog = keptCatalog
		}
		// A nil lint slice means lint was not requested; a filtered audit stays
		// explicitly non-nil so a clean selection still renders as clean.
		if ns.Lint != nil {
			keptLint := make([]*apitypes.LintViolationResponse, 0, len(ns.Lint))
			for _, v := range ns.Lint {
				if _, ok := keptTables[v.Table]; ok {
					keptLint = append(keptLint, v)
				}
			}
			ns.Lint = keptLint
		}
	}
	resp.TableCount = kept
	return nil
}

// errNoTableMatches names the filter, the database, and the environment, and
// lists the pulled table names when the list is short enough to read, so a
// typo is a one-round-trip fix.
func errNoTableMatches(resp *apitypes.PullSchemaResponse, filter string) error {
	var available []string
	for _, ns := range resp.Namespaces {
		for table := range ns.Tables {
			available = append(available, table)
		}
	}
	sort.Strings(available)
	available = slices.Compact(available)
	hint := fmt.Sprintf("%d tables available; run without --table to list them", len(available))
	if len(available) <= maxTablesInFilterError {
		hint = "available tables: " + strings.Join(available, ", ")
	}
	return fmt.Errorf("no table matching %q in database %s environment %s (%s)",
		filter, resp.Database, resp.Environment, hint)
}

func writePullSchemaResponse(w io.Writer, resp *apitypes.PullSchemaResponse) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(resp)
}
