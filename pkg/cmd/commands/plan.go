package commands

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/templates"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/state"
)

// PlanCmd creates a schema change plan from schema files.
type PlanCmd struct {
	SchemaDir   string `short:"s" help:"Schema directory with schemabot.yaml and .sql files" default:"." name:"schema_dir"`
	Environment string `short:"e" help:"Target environment (omit to show all environments)"`
	Repository  string `help:"Repository name (optional, for tracking)"`
	PullRequest int    `help:"Pull request number (optional, for tracking)" name:"pull-request"`
	JSON        bool   `help:"Output as JSON"`
}

// Run executes the plan command.
func (cmd *PlanCmd) Run(g *Globals) error {
	// Load config from schema directory
	cfg, err := LoadCLIConfig(cmd.SchemaDir)
	if err != nil {
		if cmd.JSON {
			return client.ExitWithJSON("config_error", err.Error())
		}
		return err
	}

	ep, err := client.ResolveEndpointWithProfile(g.Endpoint, g.Profile)
	if err != nil {
		if cmd.JSON {
			return client.ExitWithJSON("config_error", err.Error())
		}
		return fmt.Errorf("resolve endpoint: %w", err)
	}
	if ep == "" {
		errMsg := "no endpoint configured (run 'schemabot configure' to set up a profile)"
		if cmd.JSON {
			return client.ExitWithJSON("invalid_request", errMsg)
		}
		return fmt.Errorf("%s", errMsg)
	}

	// If environment is not specified, get all environments and plan for each
	var environments []string
	if cmd.Environment == "" {
		envs, err := client.GetEnvironments(ep, cfg.Database, cfg.Deployment)
		if err != nil {
			if cmd.JSON {
				return client.ExitWithJSON("api_error", err.Error())
			}
			return err
		}
		environments = envs
	} else {
		environments = []string{cmd.Environment}
	}

	// Collect results for all environments
	allResults := make(map[string]*apitypes.PlanResponse)
	for _, env := range environments {
		opts := client.PlanOptions{Target: cfg.GetTarget(env), Deployment: cfg.Deployment}
		result, err := client.CallPlanAPI(ep, cfg.Database, cfg.Type, env, cfg.SchemaDir, cmd.Repository, cmd.PullRequest, opts)
		if err != nil {
			if cmd.JSON {
				return client.ExitWithJSON("api_error", err.Error())
			}
			return err
		}
		allResults[env] = result
	}

	if cmd.JSON {
		return writeJSON(allResults)
	}

	// Human-readable output for all environments
	outputMultiEnvPlanResult(allResults, cfg.Database, cfg.SchemaDir)
	return nil
}

// outputMultiEnvPlanResult prints plan results for multiple environments.
// If all environments have the same plan, it deduplicates and shows once.
func outputMultiEnvPlanResult(results map[string]*apitypes.PlanResponse, database, schemaDir string) {
	// Get first result to determine engine type
	var engine string
	for _, result := range results {
		engine = result.Engine
		break
	}

	isMySQL := !state.IsPlanetScaleEngine(engine)

	// Sort environments: staging first, production second, then alphabetically
	envOrder := make([]string, 0, len(results))
	for env := range results {
		envOrder = append(envOrder, env)
	}
	sortEnvironments(envOrder)

	// Check which environments have changes
	stagingResult := results["staging"]
	productionResult := results["production"]
	stagingHasChanges := hasResultChanges(stagingResult)
	productionHasChanges := hasResultChanges(productionResult)

	// Check if staging and production have identical plans
	bothConfigured := stagingResult != nil && productionResult != nil
	plansIdentical := bothConfigured && stagingHasChanges && productionHasChanges &&
		planFingerprint(stagingResult) == planFingerprint(productionResult)

	// Header box (title + database only, environment shown below)
	templates.WritePlanHeader(templates.PlanHeaderData{
		Database:   database,
		SchemaName: filepath.Base(schemaDir),
		IsMySQL:    isMySQL,
	})

	switch {
	case len(envOrder) == 1:
		// Single environment
		templates.WriteEnvironmentHeader(envOrder[0])
		writeEnvPlan(results[envOrder[0]])
	case plansIdentical:
		// Same plan across all environments
		var titled []string
		for _, env := range envOrder {
			titled = append(titled, cases.Title(language.English).String(env))
		}
		fmt.Printf("  %s%s%s\n\n", templates.ANSIBold, strings.Join(titled, " & "), templates.ANSIReset)
		writeEnvPlan(stagingResult)
	default:
		// Different plans — per-env sections
		for _, env := range envOrder {
			templates.WriteEnvironmentHeader(env)
			writeEnvPlan(results[env])
		}
	}
}

// writeEnvPlan writes the plan for a single environment result.
func writeEnvPlan(result *apitypes.PlanResponse) {
	if result == nil {
		fmt.Println("(not configured)")
		fmt.Println()
		return
	}
	writePlanBody(result, false)
}

// writePlanBody writes the plan body (errors, changes, unsafe warnings, lint, summary).
// Used by both writeEnvPlan (plan command) and OutputPlanResult (apply command).
// When isApply is true, the ⛔ unsafe warning is skipped (apply shows its own 🚨 warning).
func writePlanBody(result *apitypes.PlanResponse, isApply bool) {
	// Check for errors
	if len(result.Errors) > 0 {
		templates.WriteErrors(result.Errors)
		return
	}

	// Collect VSchema changes from metadata
	var vschemaChanges []templates.VSchemaChange
	for _, sc := range result.Changes {
		if diff, ok := sc.Metadata["vschema"]; ok && diff != "" {
			vschemaChanges = append(vschemaChanges, templates.VSchemaChange{
				Keyspace: sc.Namespace,
				Diff:     diff,
			})
		}
	}

	// Check if there are any changes (DDL or VSchema)
	tables := result.FlatTables()
	if len(tables) == 0 && len(vschemaChanges) == 0 {
		templates.WriteNoChanges()
		return
	}

	// Collect DDL changes (filter out internal Spirit tables), grouped by namespace
	namespaceMap := make(map[string][]templates.DDLChange)
	for _, tbl := range ddl.FilterInternalTablesTyped(tables) {
		ns := tbl.Namespace
		if ns == "" {
			ns = result.Database
		}
		namespaceMap[ns] = append(namespaceMap[ns], templates.DDLChange{
			ChangeType: tbl.ChangeType,
			TableName:  tbl.TableName,
			DDL:        tbl.DDL,
		})
	}

	// Flatten all changes for summary/lint
	var allChanges []templates.DDLChange
	for _, c := range namespaceMap {
		allChanges = append(allChanges, c...)
	}

	// Build VSchema diff map by keyspace for merging into namespace changes
	vsDiffByKS := make(map[string]string)
	for _, vc := range vschemaChanges {
		vsDiffByKS[vc.Keyspace] = vc.Diff
	}

	// Render DDL + VSchema changes grouped by namespace/keyspace
	isVitess := state.IsPlanetScaleEngine(result.Engine)
	if len(allChanges) > 0 || len(vschemaChanges) > 0 {
		// Collect all namespaces (from DDL and VSchema)
		allNamespaces := make(map[string]bool)
		for ns := range namespaceMap {
			allNamespaces[ns] = true
		}
		for _, vc := range vschemaChanges {
			allNamespaces[vc.Keyspace] = true
		}

		var nsChanges []templates.NamespaceChange
		for ns := range allNamespaces {
			nc := templates.NamespaceChange{
				Namespace: ns,
				Changes:   namespaceMap[ns],
			}
			if diff, ok := vsDiffByKS[ns]; ok {
				nc.VSchemaChanged = true
				nc.VSchemaDiff = diff
			}
			nsChanges = append(nsChanges, nc)
		}
		templates.WriteNamespaceChanges(nsChanges, !isVitess, result.Database)
	}

	// Check for unsafe changes and show with ⛔ (error level)
	// Skip in apply context — apply shows its own 🚨 warning via WriteUnsafeWarningAllowed
	unsafeChanges := result.UnsafeChanges()
	if len(unsafeChanges) > 0 && !isApply {
		templates.WriteUnsafeChangesWarning(unsafeChanges)
	}

	// Show non-unsafe lint violations with ⚠️
	lintViolations := result.LintNonErrors()
	if len(lintViolations) > 0 {
		templates.WriteLintViolations(lintViolations)
	}

	// Write summary
	switch {
	case len(vschemaChanges) > 0:
		templates.WritePlanSummaryWithVSchema(allChanges, vschemaChanges)
	default:
		templates.WritePlanSummary(allChanges)
	}
}

// hasResultChanges returns true if the result has schema changes (DDL or VSchema).
func hasResultChanges(result *apitypes.PlanResponse) bool {
	if result == nil {
		return false
	}
	if len(result.FlatTables()) > 0 {
		return true
	}
	for _, sc := range result.Changes {
		if sc.Metadata["vschema"] != "" {
			return true
		}
	}
	return false
}

// sortEnvironments sorts environments with staging first, production second, then alphabetically.
func sortEnvironments(envs []string) {
	priority := map[string]int{
		"staging":    0,
		"production": 1,
	}
	sort.Slice(envs, func(i, j int) bool {
		pi, oki := priority[envs[i]]
		pj, okj := priority[envs[j]]
		if !oki {
			pi = 100
		}
		if !okj {
			pj = 100
		}
		if pi != pj {
			return pi < pj
		}
		return envs[i] < envs[j]
	})
}

// planFingerprint creates a string fingerprint of a plan result for deduplication.
// Plans with identical DDL statements are considered the same.
func planFingerprint(result *apitypes.PlanResponse) string {
	// Check for errors first
	if len(result.Errors) > 0 {
		data, _ := json.Marshal(result.Errors)
		return "errors:" + string(data)
	}

	// Get DDL statements
	tables := result.FlatTables()
	if len(tables) == 0 {
		return "no-changes"
	}

	// Build fingerprint from DDL statements
	var ddls []string
	for _, tbl := range tables {
		ddls = append(ddls, tbl.DDL)
	}

	// Sort DDLs to make fingerprint order-independent
	sort.Strings(ddls)

	data, _ := json.Marshal(ddls)
	return string(data)
}

// OutputPlanResult prints the plan result in a format similar to PR comments.
func OutputPlanResult(result *apitypes.PlanResponse, database, environment, schemaDir string, isApply bool) {
	// Determine engine type for header
	isMySQL := !state.IsPlanetScaleEngine(result.Engine)

	// Header box + environment
	templates.WritePlanHeader(templates.PlanHeaderData{
		Database:   database,
		SchemaName: filepath.Base(schemaDir),
		IsMySQL:    isMySQL,
		IsApply:    isApply,
	})
	templates.WriteEnvironmentHeader(environment)

	// Body (shared with writeEnvPlan)
	writePlanBody(result, isApply)
}
