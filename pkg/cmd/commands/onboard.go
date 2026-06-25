package commands

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/storage"
)

// OnboardCmd pulls live schema into a new declarative schema directory.
type OnboardCmd struct {
	Database          string   `short:"d" required:"" help:"Database name from SchemaBot server config"`
	Environment       string   `short:"e" required:"" help:"Source environment to pull from"`
	SchemaDir         string   `short:"s" required:"" help:"Schema root to write schemabot.yaml and namespace directories" name:"schema_dir"`
	Type              string   `help:"Database type override; resolved from the server's registered config when omitted"`
	Namespaces        []string `name:"namespace" help:"Concrete live namespace to onboard. Repeat for multiple namespaces. Omit to discover all non-reserved namespaces."`
	TemplateEnvSuffix bool     `help:"Write namespaces ending in _<environment> as _$ENV directories" name:"template-env-suffix"`
	DryRun            bool     `help:"Preview files without writing them" name:"dry-run"`
	Force             bool     `help:"Overwrite existing generated files"`
	SkipVerify        bool     `help:"Skip plan verification after writing files" name:"skip-verify"`
}

// Run executes the onboard command.
func (cmd *OnboardCmd) Run(g *Globals) error {
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}

	pullNamespaces, err := onboardPullNamespaces(cmd.Namespaces)
	if err != nil {
		return err
	}
	var resp *apitypes.PullSchemaResponse
	err = withLoading("Pulling live schema...", true, func() error {
		var pullErr error
		resp, pullErr = client.CallPullSchemaAPI(ep, cmd.Database, cmd.Type, cmd.Environment, pullNamespaces...)
		return pullErr
	})
	if err != nil {
		if outputSchemaPullRequestError("Onboard", cmd.Database, cmd.Environment, err) {
			return ErrSilent
		}
		return fmt.Errorf("pull schema for database %s environment %s: %w", cmd.Database, cmd.Environment, err)
	}
	if err := rewriteOnboardNamespaces(resp, cmd.Environment, cmd.TemplateEnvSuffix); err != nil {
		return err
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
			exists, statErr := fileStatusForDryRun(path)
			if statErr != nil {
				fmt.Printf("  %s (exists or inaccessible: %v)\n", path, statErr)
				continue
			}
			if exists {
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
	if !cmd.SkipVerify {
		fmt.Println()
		fmt.Println("Verifying pulled schema against the source environment...")
		if err := verifyOnboardPlan(ep, resp, cmd.SchemaDir); err != nil {
			return err
		}
		fmt.Println("Verified: pulled schema produces no schema changes in the source environment.")
	}
	fmt.Println()
	fmt.Printf("Onboarding complete for %s from %s.\n", resp.Database, resp.Environment)
	fmt.Println("Next: open a normal PR with these files. SchemaBot will reconcile other configured environments.")
	return nil
}

type onboardWritePlan struct {
	root  string
	files map[string]string
}

func onboardPullNamespaces(namespaces []string) ([]string, error) {
	if len(namespaces) == 0 {
		return nil, nil
	}
	pullNamespaces := make([]string, 0, len(namespaces))
	seen := make(map[string]struct{}, len(namespaces))
	for _, outputNamespace := range namespaces {
		if strings.TrimSpace(outputNamespace) != outputNamespace || outputNamespace == "" {
			return nil, fmt.Errorf("namespace %q must be non-empty and contain no leading or trailing whitespace", outputNamespace)
		}
		if err := validateRelativePathPart("namespace", outputNamespace); err != nil {
			return nil, err
		}
		if strings.Contains(outputNamespace, "$ENV") {
			return nil, fmt.Errorf("namespace %q must be a concrete live namespace; use --template-env-suffix to write _$ENV directories when a live namespace ends with _<environment>", outputNamespace)
		}
		if _, ok := seen[outputNamespace]; ok {
			return nil, fmt.Errorf("duplicate namespace %q", outputNamespace)
		}
		seen[outputNamespace] = struct{}{}
		pullNamespaces = append(pullNamespaces, outputNamespace)
	}
	return pullNamespaces, nil
}

func rewriteOnboardNamespaces(resp *apitypes.PullSchemaResponse, environment string, templateEnvSuffix bool) error {
	if resp == nil || len(resp.Namespaces) == 0 {
		return nil
	}
	rewritten := make(map[string]*apitypes.PulledNamespace, len(resp.Namespaces))
	for pullNamespace, pulled := range resp.Namespaces {
		outputNamespace := onboardOutputNamespace(pullNamespace, environment, templateEnvSuffix)
		if _, ok := rewritten[outputNamespace]; ok {
			return fmt.Errorf("multiple pulled namespaces resolve to output namespace %q", outputNamespace)
		}
		rewritten[outputNamespace] = pulled
	}
	resp.Namespaces = rewritten
	return nil
}

func onboardOutputNamespace(namespace, environment string, templateEnvSuffix bool) string {
	if !templateEnvSuffix {
		return namespace
	}
	environmentSuffix := "_" + environment
	if environment != "" && strings.HasSuffix(namespace, environmentSuffix) {
		return strings.TrimSuffix(namespace, environmentSuffix) + "_$ENV"
	}
	return namespace
}

func buildOnboardWritePlan(schemaRoot string, resp *apitypes.PullSchemaResponse) (*onboardWritePlan, error) {
	if strings.TrimSpace(schemaRoot) == "" {
		return nil, fmt.Errorf("schema root is required")
	}
	if resp == nil {
		return nil, fmt.Errorf("pull schema response is empty")
	}
	if strings.TrimSpace(resp.Database) == "" {
		return nil, fmt.Errorf("pull schema response database is empty")
	}
	if resp.Type != storage.DatabaseTypeMySQL && resp.Type != storage.DatabaseTypeVitess {
		return nil, fmt.Errorf("onboard currently supports %s and %s databases; got %s", storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess, resp.Type)
	}
	if len(resp.Namespaces) == 0 {
		return nil, fmt.Errorf("pull schema returned no tables for database %s environment %s", resp.Database, resp.Environment)
	}
	root := filepath.Clean(schemaRoot)
	files := map[string]string{
		"schemabot.yaml": fmt.Sprintf("database: %s\ntype: %s\n", resp.Database, resp.Type),
	}

	namespaces := make([]string, 0, len(resp.Namespaces))
	for namespace := range resp.Namespaces {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)
	for _, namespace := range namespaces {
		if err := validateRelativePathPart("namespace", namespace); err != nil {
			return nil, err
		}
		pulled := resp.Namespaces[namespace]
		if pulled == nil {
			return nil, fmt.Errorf("pulled namespace %s is empty", namespace)
		}
		if len(pulled.Tables) == 0 && len(pulled.Artifacts) == 0 {
			return nil, fmt.Errorf("pulled namespace %s contains no tables or artifacts", namespace)
		}
		tableNames := make([]string, 0, len(pulled.Tables))
		for tableName := range pulled.Tables {
			tableNames = append(tableNames, tableName)
		}
		sort.Strings(tableNames)
		for _, tableName := range tableNames {
			if err := validateRelativePathPart("table", tableName); err != nil {
				return nil, err
			}
			files[filepath.Join(namespace, tableName+".sql")] = pulled.Tables[tableName]
		}
		if vschema := pulled.Artifacts["vschema.json"]; vschema != "" {
			files[filepath.Join(namespace, "vschema.json")] = vschema
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
	paths := make([]string, 0, len(p.relativePaths()))
	for _, relativePath := range p.relativePaths() {
		paths = append(paths, filepath.Join(p.root, relativePath))
	}
	return paths
}

func (p *onboardWritePlan) relativePaths() []string {
	paths := make([]string, 0, len(p.files))
	for relativePath := range p.files {
		paths = append(paths, relativePath)
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

func fileStatusForDryRun(path string) (exists bool, statErr error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func (p *onboardWritePlan) write() error {
	written := make([]string, 0, len(p.files))
	for _, relativePath := range p.relativePaths() {
		path := filepath.Join(p.root, relativePath)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return formatOnboardWriteError("create directory for", path, written, err)
		}
		if err := os.WriteFile(path, []byte(p.files[relativePath]), 0o644); err != nil {
			return formatOnboardWriteError("write", path, written, err)
		}
		written = append(written, path)
	}
	return nil
}

func formatOnboardWriteError(operation, path string, written []string, err error) error {
	if len(written) == 0 {
		return fmt.Errorf("%s %s: %w", operation, path, err)
	}
	return fmt.Errorf("%s %s after writing files:\n  %s\nerror: %w", operation, path, strings.Join(written, "\n  "), err)
}

func verifyOnboardPlan(endpoint string, resp *apitypes.PullSchemaResponse, schemaDir string) error {
	var planResult *apitypes.PlanResponse
	err := withLoading("Verifying pulled schema...", true, func() error {
		var planErr error
		planResult, planErr = client.CallPlanAPI(endpoint, resp.Database, resp.Type, resp.Environment, schemaDir, "", 0)
		return planErr
	})
	if err != nil {
		if outputPlanRequestError(resp.Database, resp.Environment, err) {
			return ErrSilent
		}
		return fmt.Errorf("verify pulled schema for database %s environment %s: %w", resp.Database, resp.Environment, err)
	}
	return validateOnboardPlanResult(planResult)
}

func validateOnboardPlanResult(result *apitypes.PlanResponse) error {
	if result == nil {
		return fmt.Errorf("verify pulled schema: plan response is empty")
	}
	if len(result.Errors) > 0 {
		return fmt.Errorf("verify pulled schema: plan returned errors:\n  %s", strings.Join(result.Errors, "\n  "))
	}
	if hasResultChanges(result) {
		return fmt.Errorf("verify pulled schema: pulled files still produce schema changes in %s", result.Environment)
	}
	return nil
}

func outputSchemaPullRequestError(operation, database, environment string, err error) bool {
	var apiErr *client.APIError
	var connectionErr *client.ConnectionError
	if !errors.As(err, &apiErr) && !errors.As(err, &connectionErr) {
		return false
	}

	fmt.Printf("%s%s failed%s\n", templates.ANSIRed, operation, templates.ANSIReset)
	fmt.Printf("  Database: %s\n", database)
	fmt.Printf("  Environment: %s\n", environment)
	if apiErr != nil {
		fmt.Printf("  API status: HTTP %d\n", apiErr.Status)
		if apiErr.ErrorCode != "" {
			fmt.Printf("  Error code: %s\n", apiErr.ErrorCode)
		}
	}
	fmt.Printf("  Error: %s\n", err.Error())
	return true
}
