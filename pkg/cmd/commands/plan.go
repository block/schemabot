package commands

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
)

// PlanCmd creates a schema change plan from schema files.
type PlanCmd struct {
	SchemaDir   string `short:"s" help:"Schema directory with schemabot.yaml and .sql files" default:"." name:"schema_dir"`
	Environment string `short:"e" help:"Target environment (omit to show all environments)"`
	Target      string `help:"Plan only this rollout member of the environment: its target, or deployment/target when the name is ambiguous; requires -e" name:"target"`
	Repository  string `help:"Repository name (optional, for tracking)"`
	PullRequest int    `help:"Pull request number (optional, for tracking)" name:"pull-request"`
	JSON        bool   `help:"Output as JSON"`
}

// Run executes the plan command.
func (cmd *PlanCmd) Run(g *Globals) error {
	if cmd.Target != "" && cmd.Environment == "" {
		errMsg := "--target names a rollout member of one environment; pass -e with it"
		if cmd.JSON {
			return client.ExitWithJSON("invalid_request", errMsg)
		}
		return fmt.Errorf("%s", errMsg)
	}

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
		errMsg := fmt.Sprintf("no endpoint configured (run '%s configure' to set up a profile)", cliname.Name())
		if cmd.JSON {
			return client.ExitWithJSON("invalid_request", errMsg)
		}
		return fmt.Errorf("%s", errMsg)
	}

	// If environment is not specified, get all environments and plan for each
	var environments []string
	if cmd.Environment == "" {
		var envs []string
		err := withLoading("Loading environments...", !cmd.JSON, func() error {
			var loadErr error
			envs, loadErr = client.GetEnvironments(ep, cfg.Database)
			return loadErr
		})
		if err != nil {
			if cmd.JSON {
				return client.ExitWithJSON("api_error", err.Error())
			}
			if outputPlanRequestError(cfg.Database, "", err) {
				return ErrSilent
			}
			return err
		}
		environments = envs
	} else {
		environments = []string{cmd.Environment}
	}

	// Collect results for all environments
	allResults := make(map[string]*apitypes.PlanResponse)
	ignoredByEnv := make(map[string][]string)
	for _, env := range environments {
		var result *apitypes.PlanResponse
		err := withLoading("Generating schema change plan...", !cmd.JSON, func() error {
			var planErr error
			result, ignoredByEnv[env], planErr = client.CallPlanAPIForTarget(ep, cfg.Database, cfg.Type, env, cfg.SchemaDir, cmd.Repository, cmd.PullRequest, cfg.PlanExclusions(), false, cmd.Target)
			return planErr
		})
		if err != nil {
			if cmd.JSON {
				return client.ExitWithJSON("api_error", err.Error())
			}
			if outputPlanRequestError(cfg.Database, env, err) {
				return ErrSilent
			}
			return err
		}
		allResults[env] = result
	}

	if cmd.JSON {
		return writeJSON(allResults)
	}

	// Disclose ignore_namespaces once per distinct resolution: entries resolve
	// from config alone and differ between environments only when they use
	// {env} or $ENV, so several environments usually share one notice.
	disclosed := make(map[string]bool)
	for _, env := range environments {
		ignored := ignoredByEnv[env]
		unmatched := schema.UnmatchedIgnoreEntries(cfg.IgnoreNamespaces, env, ignored)
		key := strings.Join(ignored, ",") + "|" + strings.Join(unmatched, ",")
		if disclosed[key] {
			continue
		}
		disclosed[key] = true
		templates.WriteIgnoredNamespaces(ignored, unmatched)
	}

	// Human-readable output for all environments
	outputMultiEnvPlanResult(allResults, cfg.Database, cfg.SchemaDir)
	if cmd.Target != "" {
		writeNarrowedTo(allResults[cmd.Environment])
	}
	return nil
}

func outputPlanRequestError(database, environment string, err error) bool {
	var apiErr *client.APIError
	var connectionErr *client.ConnectionError
	if !errors.As(err, &apiErr) && !errors.As(err, &connectionErr) {
		return false
	}

	fmt.Printf("%sPlan failed%s\n", templates.ANSIRed, templates.ANSIReset)
	fmt.Printf("  Database: %s\n", database)
	if environment != "" {
		fmt.Printf("  Environment: %s\n", environment)
	}
	if apiErr != nil {
		fmt.Printf("  API status: HTTP %d\n", apiErr.Status)
		if apiErr.ErrorCode != "" {
			fmt.Printf("  Error code: %s\n", apiErr.ErrorCode)
		}
	}
	fmt.Printf("  Error: %s\n", err.Error())
	return true
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
	// A rollout's plan names its members, so it never reads as another
	// environment's plan.
	plansIdentical := bothConfigured && stagingHasChanges && productionHasChanges &&
		stagingResult.WholeRollout() == nil && productionResult.WholeRollout() == nil &&
		planFingerprint(stagingResult) == planFingerprint(productionResult)

	// Header box (title + database only, environment shown below)
	templates.WritePlanHeader(templates.PlanHeaderData{
		Engine:     engine,
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
// When isApply is true, the ⚠️ unsafe warning is skipped (apply shows its own 🚨 warning).
func writePlanBody(result *apitypes.PlanResponse, isApply bool) {
	// Check for errors
	if len(result.Errors) > 0 {
		templates.WriteErrors(result.Errors)
		return
	}
	if result.WholeRollout() != nil {
		writeRolloutPlanBody(result, isApply)
		return
	}
	writeChangesBody(result, isApply)
}

// writeRolloutPlanBody writes the plan of every member of a rollout the way
// the PR comment does: the members that need attention first, then one
// heading per distinct plan naming the members that run it, with that plan's
// changes under it. Groups with work lead; a group already at the desired
// schema says so in place of DDL. Lint results and exempt tables describe the
// schema files and the primary's live schema, so they are written once, after
// every group.
func writeRolloutPlanBody(result *apitypes.PlanResponse, isApply bool) {
	rollout := result.WholeRollout()
	noun := templates.RolloutNoun(rollout)
	templates.WriteRolloutAttention(noun, rollout.Attention)
	if len(rollout.Groups) > 1 {
		templates.WriteRolloutDivergence(noun)
	}
	plans := result.MemberPlans()
	order := make([]int, len(rollout.Groups))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int {
		return compareWorkFirst(plans[a].HasChanges(), plans[b].HasChanges())
	})
	for _, i := range order {
		templates.WriteRolloutGroupHeading(noun, rollout.Groups[i].Members, rollout.Members)
		writeChangesBody(plans[i], isApply)
	}
	if lint := result.LintNonErrors(); len(lint) > 0 {
		templates.WriteLintViolations(lint)
	}
	templates.WriteExemptTables(result.ExemptTables)
}

// compareWorkFirst orders a plan with work ahead of one without.
func compareWorkFirst(aHasWork, bHasWork bool) int {
	switch {
	case aHasWork == bHasWork:
		return 0
	case aHasWork:
		return -1
	default:
		return 1
	}
}

// writeChangesBody writes one plan's changes, unsafe warnings, lint, and
// summary.
func writeChangesBody(result *apitypes.PlanResponse, isApply bool) {
	// Collect VSchema changes from metadata, and the namespaces the engine
	// asked to finalize: a finalize is work the apply runs, so a plan made only
	// of finalizes must not read as "no changes". finalizeOnly holds those with
	// no VSchema change to show; the summary counts the ones with no DDL either.
	var vschemaChanges []templates.VSchemaChange
	finalize := map[string]bool{}
	finalizeOnly := map[string]bool{}
	for _, sc := range result.Changes {
		if sc.NeedsFinalizer() {
			finalize[renderedNamespace(sc.Namespace, result.Database)] = true
		}
		if sc.ShowsVSchemaChange() {
			vschemaChanges = append(vschemaChanges, templates.VSchemaChange{
				Keyspace: sc.Namespace,
				Diff:     sc.Metadata[apitypes.VSchemaDiffMetadataKey],
			})
		} else if sc.NeedsFinalizer() {
			finalizeOnly[renderedNamespace(sc.Namespace, result.Database)] = true
		}
	}

	// Check if there are any changes (DDL or VSchema)
	// The exempt-table disclosure renders on both branches: a clean result is
	// exactly where a reader needs to tell an exempted live table from one the
	// plan simply found declared.
	//
	// The DDL block and the summary below are both built from this one set, so
	// the summary counts what the block shows — and for a sharded plan that is
	// every distinct per-shard statement, the same set the PR comment counts.
	tables := result.RenderedTables()
	if len(tables) == 0 && len(vschemaChanges) == 0 && len(finalizeOnly) == 0 {
		templates.WriteNoChanges()
		templates.WriteExemptTables(result.ExemptTables)
		return
	}

	// Collect DDL changes (filter out internal Spirit tables), grouped by namespace
	namespaceMap := make(map[string][]templates.DDLChange)
	for _, tbl := range ddl.FilterInternalTablesTyped(tables) {
		ns := renderedNamespace(tbl.Namespace, result.Database)
		namespaceMap[ns] = append(namespaceMap[ns], templates.DDLChange{
			ChangeType: tbl.ChangeType,
			Namespace:  ns,
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
	if len(allChanges) > 0 || len(vschemaChanges) > 0 || len(finalizeOnly) > 0 {
		// Collect all namespaces (from DDL, VSchema, and finalizes)
		allNamespaces := make(map[string]bool)
		for ns := range namespaceMap {
			allNamespaces[ns] = true
		}
		for _, vc := range vschemaChanges {
			allNamespaces[vc.Keyspace] = true
		}
		for ns := range finalizeOnly {
			allNamespaces[ns] = true
		}

		var nsChanges []templates.NamespaceChange
		for ns := range allNamespaces {
			nc := templates.NamespaceChange{
				Namespace: ns,
				Changes:   namespaceMap[ns],
				Finalize:  finalize[ns],
			}
			if diff, ok := vsDiffByKS[ns]; ok {
				nc.VSchemaChanged = true
				nc.VSchemaDiff = diff
			}
			nsChanges = append(nsChanges, nc)
		}
		templates.WriteNamespaceChanges(nsChanges, !isVitess, result.Database, schema.DialectForDatabaseType(result.DatabaseType))
	}

	// A direct-execution change runs as native DDL that blocks writes to its
	// table for as long as it runs, so it is disclosed under the plan that runs
	// it, with the reason the policy routed it there.
	templates.WriteChangeNotice(glyph.Attention, "Direct execution: runs as native MySQL DDL, not through Spirit, and blocks writes to the table while it runs:", directChangeNotices(result))

	// Check for unsafe changes and show with ⚠️ (attention — the changes await consent)
	// Skip in apply context — apply shows its own 🚨 warning via WriteUnsafeWarningAllowed
	unsafeChanges := result.UnsafeChanges()
	if len(unsafeChanges) > 0 && !isApply {
		templates.WriteUnsafeChangesWarning(unsafeChanges)
	}

	// Show advisory (non-error) lint violations
	lintViolations := result.LintNonErrors()
	if len(lintViolations) > 0 {
		templates.WriteLintViolations(lintViolations)
	}

	// Write summary. A namespace with DDL finalizes as part of that work, so
	// only a finalize that is the namespace's only work is counted.
	finalizes := 0
	for ns := range finalizeOnly {
		if len(namespaceMap[ns]) == 0 {
			finalizes++
		}
	}
	switch {
	case len(vschemaChanges) > 0 || finalizes > 0:
		templates.WritePlanSummaryWithKeyspaceUpdates(allChanges, vschemaChanges, finalizes)
	default:
		templates.WritePlanSummary(allChanges)
	}
	templates.WriteExemptTables(result.ExemptTables)
}

// directChangeNotices lists the plan's direct-execution changes one per table
// and reason, so a statement that runs the same way on every shard is named
// once.
func directChangeNotices(result *apitypes.PlanResponse) []templates.UnsafeChange {
	var notices []templates.UnsafeChange
	seen := make(map[string]bool)
	for _, tc := range result.DirectChanges() {
		key := tc.TableName + "\x00" + tc.ModeReason
		if seen[key] {
			continue
		}
		seen[key] = true
		notices = append(notices, templates.UnsafeChange{Table: tc.TableName, Reason: tc.ModeReason, ChangeType: tc.ChangeType})
	}
	return notices
}

// hasResultChanges returns true if the result has schema changes (DDL or
// VSchema) on any member of its rollout.
func hasResultChanges(result *apitypes.PlanResponse) bool {
	return result != nil && result.RolloutHasChanges()
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
// Plans with identical DDL statements, VSchema updates, and exempt-table
// disclosures are considered the same; the disclosure is part of what the
// reader sees, so two environments that exempted different live tables render
// their own sections.
func planFingerprint(result *apitypes.PlanResponse) string {
	// Check for errors first
	if len(result.Errors) > 0 {
		data, _ := json.Marshal(result.Errors)
		return "errors:" + string(data)
	}

	var ddls []string
	for _, tbl := range result.RenderedTables() {
		ddls = append(ddls, tbl.DDL)
	}
	namespacesWithDDL := map[string]bool{}
	for _, tbl := range ddl.FilterInternalTablesTyped(result.RenderedTables()) {
		namespacesWithDDL[renderedNamespace(tbl.Namespace, result.Database)] = true
	}
	// A finalize beside DDL or a VSchema change renders as that work alone, so
	// only a finalize that is its namespace's only work distinguishes one
	// plan's output from another.
	var vschemas, finalizes []string
	for _, sc := range result.Changes {
		if sc.ShowsVSchemaChange() {
			vschemas = append(vschemas, sc.Namespace+":"+sc.Metadata[apitypes.VSchemaDiffMetadataKey])
		}
		if sc.NeedsFinalizer() && !sc.ShowsVSchemaChange() && !namespacesWithDDL[renderedNamespace(sc.Namespace, result.Database)] {
			finalizes = append(finalizes, sc.Namespace)
		}
	}
	if len(ddls) == 0 && len(vschemas) == 0 && len(finalizes) == 0 {
		return "no-changes"
	}

	var exempt []string
	for _, group := range result.ExemptTables {
		if group == nil || len(group.Tables) == 0 {
			continue
		}
		exempt = append(exempt, group.Namespace+":"+group.Reason+":"+strings.Join(group.Tables, ","))
	}

	// Sort to make the fingerprint order-independent
	sort.Strings(ddls)
	sort.Strings(vschemas)
	finalizes = slices.Compact(slices.Sorted(slices.Values(finalizes)))
	sort.Strings(exempt)

	data, _ := json.Marshal(struct {
		DDLs      []string `json:"ddls"`
		VSchemas  []string `json:"vschemas"`
		Finalizes []string `json:"finalizes"`
		Exempt    []string `json:"exempt"`
	}{ddls, vschemas, finalizes, exempt})
	return string(data)
}

// renderedNamespace is the namespace a plan's output lists a change under: a
// change with no namespace belongs to the database itself.
func renderedNamespace(namespace, database string) string {
	if namespace == "" {
		return database
	}
	return namespace
}

// OutputPlanResult prints the plan result in a format similar to PR comments.
func OutputPlanResult(result *apitypes.PlanResponse, database, environment, schemaDir string, isApply bool) {
	// Determine engine type for header
	isMySQL := !state.IsPlanetScaleEngine(result.Engine)

	// Header box + environment
	templates.WritePlanHeader(templates.PlanHeaderData{
		Engine:     result.Engine,
		Database:   database,
		SchemaName: filepath.Base(schemaDir),
		IsMySQL:    isMySQL,
		IsApply:    isApply,
	})
	templates.WriteEnvironmentHeader(environment)

	// Body (shared with writeEnvPlan)
	writePlanBody(result, isApply)
}
