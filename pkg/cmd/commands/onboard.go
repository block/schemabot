package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/storage"
)

// OnboardCmd pulls live schema into a new declarative schema directory.
type OnboardCmd struct {
	Database    string `short:"d" required:"" help:"Database name from SchemaBot server config"`
	Environment string `short:"e" required:"" help:"Source environment to pull from"`
	SchemaDir   string `short:"s" required:"" help:"Schema root to write schemabot.yaml and namespace directories" name:"schema_dir"`
	Type        string `help:"Database type" default:"mysql" enum:"mysql"`
	DryRun      bool   `help:"Preview files without writing them" name:"dry-run"`
	Force       bool   `help:"Overwrite existing generated files"`
}

// Run executes the onboard command.
func (cmd *OnboardCmd) Run(g *Globals) error {
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}
	if cmd.Type != storage.DatabaseTypeMySQL {
		return fmt.Errorf("onboard currently supports %s databases; got %s", storage.DatabaseTypeMySQL, cmd.Type)
	}

	resp, err := client.CallPullSchemaAPI(ep, cmd.Database, cmd.Type, cmd.Environment)
	if err != nil {
		return fmt.Errorf("pull schema for database %s environment %s: %w", cmd.Database, cmd.Environment, err)
	}
	plan, err := buildOnboardWritePlan(cmd.SchemaDir, resp)
	if err != nil {
		return err
	}
	if !cmd.DryRun {
		if err := plan.checkConflicts(cmd.Force); err != nil {
			return err
		}
	}

	fmt.Printf("Pulled %d tables from %s/%s.\n", resp.TableCount, resp.Database, resp.Environment)
	if cmd.DryRun {
		fmt.Println("Dry run: would write files:")
		for _, path := range plan.paths() {
			if fileExists(path) {
				fmt.Printf("  %s (exists)\n", path)
				continue
			}
			fmt.Printf("  %s\n", path)
		}
		return nil
	}

	if err := plan.write(); err != nil {
		return err
	}
	fmt.Println("Wrote declarative schema files:")
	for _, path := range plan.paths() {
		fmt.Printf("  %s\n", path)
	}
	fmt.Println()
	fmt.Println("Next: open a normal PR. SchemaBot plan comments and checks will reconcile other configured environments.")
	return nil
}

type onboardWritePlan struct {
	root  string
	files map[string]string
}

func buildOnboardWritePlan(schemaRoot string, resp *apitypes.PullSchemaResponse) (*onboardWritePlan, error) {
	if resp == nil {
		return nil, fmt.Errorf("pull schema response is empty")
	}
	if resp.Type != storage.DatabaseTypeMySQL {
		return nil, fmt.Errorf("onboard currently supports %s databases; got %s", storage.DatabaseTypeMySQL, resp.Type)
	}
	root := filepath.Clean(schemaRoot)
	files := map[string]string{
		"schemabot.yaml": fmt.Sprintf("database: %s\ntype: %s\n", resp.Database, resp.Type),
	}

	namespaces := make([]string, 0, len(resp.SchemaFiles))
	for namespace := range resp.SchemaFiles {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)
	for _, namespace := range namespaces {
		if err := validateRelativePathPart("namespace", namespace); err != nil {
			return nil, err
		}
		nsFiles := resp.SchemaFiles[namespace]
		if nsFiles == nil {
			return nil, fmt.Errorf("schema files for namespace %s are empty", namespace)
		}
		filenames := make([]string, 0, len(nsFiles.Files))
		for filename := range nsFiles.Files {
			filenames = append(filenames, filename)
		}
		sort.Strings(filenames)
		for _, filename := range filenames {
			if err := validateRelativePathPart("schema file", filename); err != nil {
				return nil, err
			}
			files[filepath.Join(namespace, filename)] = nsFiles.Files[filename]
		}
	}

	return &onboardWritePlan{root: root, files: files}, nil
}

func validateRelativePathPart(kind, value string) error {
	if value == "" {
		return fmt.Errorf("%s is empty", kind)
	}
	if filepath.IsAbs(value) || strings.Contains(value, "..") || strings.ContainsAny(value, `/\`) {
		return fmt.Errorf("%s %q must be a single relative path component", kind, value)
	}
	return nil
}

func (p *onboardWritePlan) paths() []string {
	paths := make([]string, 0, len(p.files))
	for relativePath := range p.files {
		paths = append(paths, filepath.Join(p.root, relativePath))
	}
	sort.Strings(paths)
	return paths
}

func (p *onboardWritePlan) checkConflicts(force bool) error {
	if force {
		return nil
	}
	var existing []string
	for _, path := range p.paths() {
		if _, err := os.Stat(path); err == nil {
			existing = append(existing, path)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("check output file %s: %w", path, err)
		}
	}
	if len(existing) > 0 {
		return fmt.Errorf("refusing to overwrite existing files (use --force to overwrite):\n  %s", strings.Join(existing, "\n  "))
	}
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func (p *onboardWritePlan) write() error {
	for relativePath, content := range p.files {
		path := filepath.Join(p.root, relativePath)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("create directory for %s: %w", path, err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
	}
	return nil
}
