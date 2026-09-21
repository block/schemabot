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
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/repoconfig"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"gopkg.in/yaml.v3"
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
	LegacyBaseCommit  string   `help:"Full base-branch commit through which the generated schema accounts for legacy schema changes" name:"legacy-base-commit"`
	LegacyPaths       []string `help:"Repository-relative legacy schema path covered by the base commit. Repeat for multiple paths." name:"legacy-path"`
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
	preservedIgnores, err := preservedExclusions(cmd.SchemaDir)
	if err != nil {
		return err
	}
	legacyBaseline, err := resolveOnboardLegacyBaseline(cmd.SchemaDir, cmd.LegacyBaseCommit, cmd.LegacyPaths)
	if err != nil {
		return err
	}
	plan, err := buildOnboardWritePlanWithBaseline(cmd.SchemaDir, resp, preservedIgnores, legacyBaseline)
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
		strays, withheldStrays, strayErr := plan.strayFiles()
		if strayErr != nil {
			return strayErr
		}
		plan.printExclusionDisclosure()
		printStrayFileWarning(strays)
		printWithheldStrayFileWarning(withheldStrays)
		return nil
	}

	if err := plan.write(); err != nil {
		return err
	}
	fmt.Println("Wrote declarative schema files:")
	for _, path := range plan.paths() {
		fmt.Printf("  %s\n", path)
	}
	strays, withheldStrays, strayErr := plan.strayFiles()
	if strayErr != nil {
		return strayErr
	}
	plan.printExclusionDisclosure()
	printStrayFileWarning(strays)
	printWithheldStrayFileWarning(withheldStrays)
	if !cmd.SkipVerify {
		fmt.Println()
		fmt.Println("Verifying pulled schema against the source environment...")
		if err := verifyOnboardPlan(ep, cmd.Database, cmd.Environment, plan); err != nil {
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
	root         string
	databaseType string
	files        map[string]string
	// exclusions carries an existing config's ignore_namespaces and
	// ignore_tables through a rewrite, so re-onboarding does not drop the
	// exclusions an operator configured, and plan verification excludes the
	// same namespaces and withholds the same tables a real plan would.
	exclusions client.PlanExclusions
	// withheld names the live tables ignore_tables kept out of the written
	// files, by namespace. Onboard discloses them the way plan and apply
	// disclose a plan's exemptions, and tells a stray file describing an
	// absent table from one describing a table the config withholds.
	withheld []*apitypes.ExemptTablesResponse
}

// preservedExclusions returns the ignore_namespaces and ignore_tables of an
// existing schemabot.yaml under schemaRoot. A missing config is a fresh
// onboarding with nothing to preserve; an unreadable one is an error —
// rewriting it would silently drop whatever it configured.
func preservedExclusions(schemaRoot string) (client.PlanExclusions, error) {
	if _, err := os.Stat(filepath.Join(schemaRoot, "schemabot.yaml")); os.IsNotExist(err) {
		return client.PlanExclusions{}, nil
	}
	cfg, err := LoadCLIConfig(schemaRoot)
	if err != nil {
		return client.PlanExclusions{}, fmt.Errorf("read existing schemabot.yaml to preserve ignore_namespaces and ignore_tables: %w", err)
	}
	return cfg.PlanExclusions(), nil
}

// resolveOnboardLegacyBaseline requires a reviewed anchor for a new config and
// preserves it on an ordinary refresh. Passing either flag starts an explicit
// anchor update and therefore requires the complete pair.
func resolveOnboardLegacyBaseline(schemaRoot, baseCommit string, legacyPaths []string) (*repoconfig.LegacyBaseline, error) {
	configPath := filepath.Join(schemaRoot, "schemabot.yaml")
	_, statErr := os.Stat(configPath)
	configExists := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		return nil, fmt.Errorf("inspect existing schemabot.yaml for legacy_baseline: %w", statErr)
	}

	if baseCommit == "" && len(legacyPaths) == 0 {
		if !configExists {
			return nil, fmt.Errorf("fresh onboarding requires --legacy-base-commit and at least one --legacy-path")
		}
		cfg, err := LoadCLIConfig(schemaRoot)
		if err != nil {
			return nil, fmt.Errorf("read existing schemabot.yaml to preserve legacy_baseline: %w", err)
		}
		if err := cfg.LegacyBaseline.Validate(); err != nil {
			return nil, fmt.Errorf("existing schemabot.yaml cannot be refreshed without explicit legacy anchor flags: %w", err)
		}
		return cfg.LegacyBaseline, nil
	}
	if baseCommit == "" || len(legacyPaths) == 0 {
		return nil, fmt.Errorf("--legacy-base-commit and at least one --legacy-path must be supplied together")
	}
	baseline := &repoconfig.LegacyBaseline{
		Version:     repoconfig.LegacyBaselineVersion,
		BaseCommit:  baseCommit,
		LegacyPaths: legacyPaths,
	}
	if err := baseline.Validate(); err != nil {
		return nil, err
	}
	return baseline, nil
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

func buildOnboardWritePlan(schemaRoot string, resp *apitypes.PullSchemaResponse, exclusions client.PlanExclusions) (*onboardWritePlan, error) {
	return buildOnboardWritePlanWithBaseline(schemaRoot, resp, exclusions, nil)
}

func buildOnboardWritePlanWithBaseline(schemaRoot string, resp *apitypes.PullSchemaResponse, exclusions client.PlanExclusions, legacyBaseline *repoconfig.LegacyBaseline) (*onboardWritePlan, error) {
	if strings.TrimSpace(schemaRoot) == "" {
		return nil, fmt.Errorf("schema root is required")
	}
	if resp == nil {
		return nil, fmt.Errorf("pull schema response is empty")
	}
	if strings.TrimSpace(resp.Database) == "" {
		return nil, fmt.Errorf("pull schema response database is empty")
	}
	switch resp.Type {
	case storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess, storage.DatabaseTypePostgres:
	default:
		return nil, fmt.Errorf("onboard currently supports %s, %s, and %s databases; got %s", storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess, storage.DatabaseTypePostgres, resp.Type)
	}
	if len(resp.Namespaces) == 0 {
		return nil, fmt.Errorf("pull schema returned no tables for database %s environment %s", resp.Database, resp.Environment)
	}
	root := filepath.Clean(schemaRoot)
	configYAML, err := onboardConfigYAML(resp.Database, string(resp.Type), exclusions, legacyBaseline)
	if err != nil {
		return nil, err
	}
	files := map[string]string{
		"schemabot.yaml": configYAML,
	}

	namespaces := make([]string, 0, len(resp.Namespaces))
	for namespace := range resp.Namespaces {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)
	if err := rejectCaseCollisions("namespace", namespaces); err != nil {
		return nil, err
	}
	// A pull answers with the target's whole catalog: the request carries no
	// exclusions, so a table the config withholds from the planner comes back
	// like any other. Declaring it would state the contradiction the engines
	// refuse — a table both withheld from the planner and declared to it — and
	// re-onboarding an already-configured repository is where that happens,
	// because the entries it preserves name tables the pull just returned.
	ignored := engine.NewIgnoredTables(exclusions.Tables)
	var withheldGroups []*apitypes.ExemptTablesResponse

	for _, namespace := range namespaces {
		if err := validateRelativePathPart("namespace", namespace); err != nil {
			return nil, err
		}
		pulled := resp.Namespaces[namespace]
		if pulled == nil {
			return nil, fmt.Errorf("pulled namespace %s is empty", namespace)
		}
		tableNames := make([]string, 0, len(pulled.Tables))
		var withheld []string
		for tableName := range pulled.Tables {
			if ignored.Withholds(tableName) {
				withheld = append(withheld, tableName)
				continue
			}
			tableNames = append(tableNames, tableName)
		}
		sort.Strings(tableNames)
		if err := rejectCaseCollisions("table in "+namespace, tableNames); err != nil {
			return nil, err
		}
		// The filter above is exact, because an entry must never withhold a
		// table it does not name, while the engines refuse a declared-and-
		// ignored table with case folded. A live DATABASECHANGELOG under an
		// entry spelling it databasechangelog therefore survives the filter
		// and lands in a file that every later plan refuses. Refusing here,
		// against the tables about to be declared and with the engines' own
		// predicate, leaves the operator with no repository rather than one
		// no plan accepts.
		if err := ignored.RefuseDeclared(namespace, tableNames); err != nil {
			return nil, err
		}
		if group := ignored.Exemption(namespace, withheld); group != nil {
			withheldGroups = append(withheldGroups, &apitypes.ExemptTablesResponse{
				Namespace: group.Namespace,
				Tables:    group.Tables,
				Reason:    group.Reason,
			})
		}
		if len(tableNames) == 0 && len(pulled.Artifacts) == 0 {
			// Keep empty scope explicit: a comment-only SQL file declares no tables
			// while preserving the namespace for future plans and version control.
			// A namespace whose every table is withheld lands here too — the
			// namespace is still the plan's, it simply declares nothing.
			files[filepath.Join(namespace, "schema.sql")] = schema.EmptyNamespaceDeclaration
		}
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

	return &onboardWritePlan{
		root:         root,
		databaseType: resp.Type,
		files:        files,
		exclusions:   exclusions,
		withheld:     withheldGroups,
	}, nil
}

// Generated paths must remain distinct on case-insensitive filesystems too.
func rejectCaseCollisions(kind string, names []string) error {
	seen := make(map[string]string, len(names))
	for _, name := range names {
		folded := strings.ToLower(name)
		if previous, exists := seen[folded]; exists {
			return fmt.Errorf("%s names %q and %q would collide on a case-insensitive filesystem; schema files must be portable across checkouts, so choose names that differ beyond letter case before onboarding", kind, previous, name)
		}
		seen[folded] = name
	}
	return nil
}

// onboardConfig is the schemabot.yaml onboarding writes. An empty exclusion
// list is omitted rather than written as a bare key, which would read as a
// configured exclusion of nothing.
type onboardConfig struct {
	Database         string                     `yaml:"database"`
	Type             string                     `yaml:"type"`
	IgnoreNamespaces []string                   `yaml:"ignore_namespaces,omitempty"`
	IgnoreTables     []string                   `yaml:"ignore_tables,omitempty"`
	LegacyBaseline   *repoconfig.LegacyBaseline `yaml:"legacy_baseline,omitempty"`
}

// onboardConfigYAML renders the config for a freshly onboarded database. The
// document is encoded rather than formatted because the names in it come from
// the target's catalog: a name that needs YAML quoting to survive a load —
// one opening with a comment marker, say — would otherwise be written bare and
// read back as something else, silently dropping the exclusion it states and
// leaving the next plan to propose dropping its table.
func onboardConfigYAML(database, databaseType string, exclusions client.PlanExclusions, legacyBaseline *repoconfig.LegacyBaseline) (string, error) {
	var b strings.Builder
	enc := yaml.NewEncoder(&b)
	enc.SetIndent(2)
	if err := enc.Encode(onboardConfig{
		Database:         database,
		Type:             databaseType,
		IgnoreNamespaces: exclusions.Namespaces,
		IgnoreTables:     exclusions.Tables,
		LegacyBaseline:   legacyBaseline,
	}); err != nil {
		return "", fmt.Errorf("encode schemabot.yaml for database %s: %w", database, err)
	}
	if err := enc.Close(); err != nil {
		return "", fmt.Errorf("encode schemabot.yaml for database %s: %w", database, err)
	}
	return b.String(), nil
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

// strayFiles returns schema files in the pulled namespace directories that
// this pull did not write, split by why. The pull covers every table in each
// pulled namespace, so a leftover table file there describes a table absent in
// the target, and a leftover vschema.json (Vitess) proposes a VSchema the
// target doesn't have: verification will fail on either as a spurious change,
// and an onboard PR would propose applying it. The second list is the leftover
// files for tables ignore_tables withholds, whose table is present on the
// target and whose remedy is the opposite one.
func (p *onboardWritePlan) strayFiles() (strays, withheldStrays []string, err error) {
	withheldTableFiles := p.withheldTableFilePaths()
	planned := make(map[string]struct{}, len(p.files))
	namespaceDirs := make(map[string]struct{})
	for relativePath := range p.files {
		planned[relativePath] = struct{}{}
		if dir := filepath.Dir(relativePath); dir != "." {
			namespaceDirs[dir] = struct{}{}
		}
	}
	for dir := range namespaceDirs {
		path := filepath.Join(p.root, dir)
		entries, readErr := os.ReadDir(path)
		if readErr != nil {
			// A namespace directory that doesn't exist yet (dry run before any
			// write) has nothing to scan.
			if os.IsNotExist(readErr) {
				continue
			}
			return nil, nil, fmt.Errorf("scan namespace directory %s for stray files: %w", path, readErr)
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			// Only files the engine reads as schema inputs can go stray; other
			// files (docs, tooling) are ignored by plans and applies alike.
			// vschema.json is a schema input for Vitess only — MySQL planning
			// never reads it.
			isSchemaInput := strings.HasSuffix(name, ".sql") ||
				(name == "vschema.json" && p.databaseType == storage.DatabaseTypeVitess)
			if !isSchemaInput {
				continue
			}
			relativePath := filepath.Join(dir, name)
			if _, ok := planned[relativePath]; !ok {
				if withheldTableFiles[relativePath] {
					withheldStrays = append(withheldStrays, filepath.Join(p.root, relativePath))
					continue
				}
				strays = append(strays, filepath.Join(p.root, relativePath))
			}
		}
	}
	sort.Strings(strays)
	sort.Strings(withheldStrays)
	return strays, withheldStrays, nil
}

// printExclusionDisclosure reports what ignore_tables kept out of the written
// files. Onboard prints the server's own unfiltered table count, so without
// this an operator diffing the output against the catalog sees tables missing
// with no stated reason.
func (p *onboardWritePlan) printExclusionDisclosure() {
	templates.WriteExemptTables(p.withheld)
}

// withheldTableFilePaths returns the file each withheld table would have been
// written to. A repository onboarded before an entry was added still carries
// those files, and they are strays this pull deliberately did not write.
func (p *onboardWritePlan) withheldTableFilePaths() map[string]bool {
	paths := make(map[string]bool)
	for _, group := range p.withheld {
		for _, table := range group.Tables {
			paths[filepath.Join(group.Namespace, table+".sql")] = true
		}
	}
	return paths
}

func printStrayFileWarning(strays []string) {
	if len(strays) == 0 {
		return
	}
	fmt.Println()
	fmt.Printf("%sWarning:%s the schema root contains schema files this pull did not write. Verification will fail on them, and an onboard PR would propose the spurious changes they describe (recreating absent tables, or an unexpected VSchema change):\n", templates.ANSIYellow, templates.ANSIReset)
	for _, path := range strays {
		fmt.Printf("  %s\n", path)
	}
	fmt.Println("Delete the stray files, or restore the missing tables or VSchema in the target before onboarding.")
}

// printWithheldStrayFileWarning reports leftover files for tables ignore_tables
// now withholds. The table is on the target and deliberately unmanaged, so the
// generic stray remedy, restore it in the target, is the wrong instruction:
// the file states the contradiction the engines refuse, and deleting it is the
// only fix.
func printWithheldStrayFileWarning(strays []string) {
	if len(strays) == 0 {
		return
	}
	fmt.Println()
	fmt.Printf("%sWarning:%s the schema root declares tables that ignore_tables withholds. A table cannot be both withheld from the planner and declared to it, so every plan will refuse this repository until these files are gone:\n", templates.ANSIYellow, templates.ANSIReset)
	for _, path := range strays {
		fmt.Printf("  %s\n", path)
	}
	fmt.Println("Delete these files, or remove the ignore_tables entries that withhold them.")
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

func verifyOnboardPlan(endpoint, database, environment string, plan *onboardWritePlan) error {
	var planResult *apitypes.PlanResponse
	err := withLoading("Verifying pulled schema...", true, func() error {
		var planErr error
		planResult, _, planErr = client.CallPlanAPI(endpoint, database, plan.databaseType, environment, plan.root, "", 0, plan.exclusions, false)
		return planErr
	})
	if err != nil {
		if outputPlanRequestError(database, environment, err) {
			return ErrSilent
		}
		return fmt.Errorf("verify pulled schema for database %s environment %s: %w", database, environment, err)
	}
	if validateErr := validateOnboardPlanResult(planResult, database, environment); validateErr != nil {
		strays, withheldStrays, strayErr := plan.strayFiles()
		if strayErr != nil {
			return errors.Join(validateErr, strayErr)
		}
		if len(withheldStrays) > 0 {
			return fmt.Errorf("%w\nschema files declaring tables that ignore_tables withholds (delete them, or remove the entries that withhold them):\n  %s", validateErr, strings.Join(withheldStrays, "\n  "))
		}
		if len(strays) > 0 {
			return fmt.Errorf("%w\nstray schema files not written by this pull (delete them, or restore the missing tables or VSchema in the target):\n  %s", validateErr, strings.Join(strays, "\n  "))
		}
		return validateErr
	}
	return nil
}

func validateOnboardPlanResult(result *apitypes.PlanResponse, database, environment string) error {
	if result == nil {
		return fmt.Errorf("verify pulled schema for database %s environment %s: plan response is empty", database, environment)
	}
	if len(result.Errors) > 0 {
		return fmt.Errorf("verify pulled schema for database %s environment %s: plan returned errors:\n  %s", database, environment, strings.Join(result.Errors, "\n  "))
	}
	if hasResultChanges(result) {
		return fmt.Errorf("verify pulled schema for database %s environment %s: pulled files still produce schema changes:\n  %s", database, environment, strings.Join(describeOnboardPlanChanges(result), "\n  "))
	}
	return nil
}

// onboardVerifyDDLPreviewLimit bounds the single-line DDL preview in a failed
// verification's change listing so a full CREATE TABLE body doesn't drown the
// table-by-table summary.
const onboardVerifyDDLPreviewLimit = 120

// describeOnboardPlanChanges renders one line per planned change so a failed
// verification names the offending tables and DDL without a separate plan run.
func describeOnboardPlanChanges(result *apitypes.PlanResponse) []string {
	var lines []string
	for _, change := range result.Changes {
		if change == nil {
			continue
		}
		for _, tableChange := range change.TableChanges {
			if tableChange == nil {
				continue
			}
			lines = append(lines, fmt.Sprintf("%s/%s (%s): %s", change.Namespace, tableChange.TableName, strings.ToLower(tableChange.ChangeType), onboardDDLPreview(tableChange.DDL)))
		}
		if change.HasVSchemaChange() {
			lines = append(lines, fmt.Sprintf("%s: vschema change", change.Namespace))
		}
	}
	return lines
}

func onboardDDLPreview(ddl string) string {
	collapsed := strings.Join(strings.Fields(ddl), " ")
	runes := []rune(collapsed)
	if len(runes) > onboardVerifyDDLPreviewLimit {
		return string(runes[:onboardVerifyDDLPreviewLimit]) + "…"
	}
	return collapsed
}

func outputSchemaPullRequestError(operation, database, environment string, err error) bool {
	var apiErr *client.APIError
	var connectionErr *client.ConnectionError
	if !errors.As(err, &apiErr) && !errors.As(err, &connectionErr) {
		return false
	}

	failure := templates.SchemaPullFailure{
		Operation:   operation,
		Database:    database,
		Environment: environment,
		Message:     err.Error(),
	}
	if apiErr != nil {
		failure.Status = apiErr.Status
		failure.ErrorCode = apiErr.ErrorCode
	}
	templates.WriteSchemaPullFailure(failure)
	return true
}
