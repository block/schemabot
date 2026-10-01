package commands

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
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
			result, ignoredByEnv[env], planErr = client.CallPlanAPIForTarget(ep, cfg.Database, cfg.Type, env, cfg.SchemaDir, cmd.Repository, cmd.PullRequest, cfg.PlanExclusions(), false, cmd.Target, true)
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
	// Sort environments: staging first, production second, then alphabetically
	envOrder := make([]string, 0, len(results))
	for env := range results {
		envOrder = append(envOrder, env)
	}
	sortEnvironments(envOrder)

	// The first configured environment, in that order, names the engine.
	var engine string
	for _, env := range envOrder {
		if result := results[env]; result != nil {
			engine = result.Engine
			break
		}
	}

	isMySQL := !state.IsPlanetScaleEngine(engine)

	// Check which environments have changes
	stagingResult := results["staging"]
	productionResult := results["production"]
	stagingHasChanges := hasResultChanges(stagingResult)
	productionHasChanges := hasResultChanges(productionResult)

	// Check if every environment's section would render the same as staging's,
	// so the combined section below never stands in for one that reads
	// differently.
	bothConfigured := stagingResult != nil && productionResult != nil
	plansIdentical := bothConfigured && stagingHasChanges && productionHasChanges &&
		everyPlanMatches(results, stagingResult)

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
		// Beside a primary plan that reported errors no other rollout member is
		// planned, and the rollout says so, so the errors are not read as the
		// whole rollout's verdict.
		if rollout := result.WholeRollout(); rollout != nil {
			templates.WriteRolloutAttention(templates.RolloutNoun(rollout), rollout.Attention)
		}
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
// schema says so in one line in place of DDL. Lint results, the plan summary,
// and exempt tables are written once, after every group: lint and exempt
// tables describe the schema files and the primary's live schema, and the
// summary counts what the whole rollout runs and on how many members, so the
// output closes on one summary as a single plan's does.
func writeRolloutPlanBody(result *apitypes.PlanResponse, isApply bool) {
	rollout := result.WholeRollout()
	noun := templates.RolloutNoun(rollout)
	templates.WriteRolloutAttention(noun, rollout.Attention)
	// plans, work, and rollout.Groups are index-parallel. A rollout with no
	// groups has no plan of its own to render, so the response's own changes,
	// which are the primary's, are never read as the rollout's.
	var plans []*apitypes.PlanResponse
	if len(rollout.Groups) > 0 {
		plans = result.MemberPlans()
	}
	work := make([]planWork, len(plans))
	for i, plan := range plans {
		work[i] = collectPlanWork(plan)
	}
	order := make([]int, len(rollout.Groups))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int {
		return compareWorkFirst(!work[a].empty(), !work[b].empty())
	})
	// A rollout known to have no work anywhere closes on the one no-changes
	// line a single plan does, so its groups carry no line of their own. A
	// member that needs attention has unknown work, never none (MG-12), so
	// while one does the rollout gets no such verdict and each settled group
	// says so under its own heading instead.
	settled := len(work) > 0 && !slices.ContainsFunc(work, func(w planWork) bool { return !w.empty() }) && len(rollout.Attention) == 0
	var rolloutWork []planWork
	for _, i := range order {
		opensOnHeader := !work[i].empty() && opensOnNamespaceHeader(plans[i], work[i])
		fmt.Print(templates.FormatRolloutGroupHeading(noun, rollout.Groups[i].Members, rollout.Members, opensOnHeader))
		if work[i].empty() {
			if !settled {
				templates.WriteRolloutGroupNoChanges()
			}
			continue
		}
		writeChangeDetail(plans[i], work[i], isApply)
		rolloutWork = append(rolloutWork, work[i])
	}
	if lint := result.LintNonErrors(); len(lint) > 0 {
		templates.WriteLintViolations(lint)
	}
	// The summary counts what the rollout runs the way the PR comment does,
	// each table once however many groups change it. The group headings
	// already say which members run what.
	switch {
	case len(rolloutWork) > 0:
		total := combinePlanWork(rolloutWork)
		templates.WritePlanSummaryWithKeyspaceUpdates(total.changes, total.vschemaChanges, total.finalizes)
	case settled:
		templates.WriteNoChanges()
	}
	templates.WriteExemptTables(result.ExemptTables)
}

// rendersNamespaceChanges reports whether the plan has DDL, a VSchema change,
// or a finalize to write under its namespaces.
func (w planWork) rendersNamespaceChanges() bool {
	return len(w.allChanges) > 0 || len(w.vschemaChanges) > 0 || len(w.finalizeOnly) > 0
}

// namespaces returns every namespace the plan writes changes under: its DDL,
// its VSchema changes, and its finalizes, sorted.
func (w planWork) namespaces() []string {
	set := make(map[string]bool)
	for ns := range w.namespaceMap {
		set[ns] = true
	}
	for _, vc := range w.vschemaChanges {
		set[vc.Keyspace] = true
	}
	for ns := range w.finalizeOnly {
		set[ns] = true
	}
	return slices.Sorted(maps.Keys(set))
}

// opensOnNamespaceHeader reports whether writeChangeDetail opens the plan on a
// namespace header, which writes its own blank line above itself.
func opensOnNamespaceHeader(result *apitypes.PlanResponse, w planWork) bool {
	if !w.rendersNamespaceChanges() {
		return false
	}
	isMySQL := !state.IsPlanetScaleEngine(result.Engine)
	return !templates.OmitsNamespaceHeader(w.namespaces(), isMySQL, result.Database)
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

// planWork is what one plan runs, gathered once so its DDL block and its
// summary are built from the same set.
type planWork struct {
	// renderedTables counts the plan's rendered table changes, internal
	// Spirit tables included, so a plan made only of those still has work.
	renderedTables int
	// namespaceMap holds the DDL changes shown, grouped by namespace.
	namespaceMap map[string][]templates.DDLChange
	// allChanges flattens namespaceMap for the summary.
	allChanges []templates.DDLChange
	// vschemaChanges holds the VSchema diffs, one per keyspace.
	vschemaChanges []templates.VSchemaChange
	// finalize holds the namespaces the engine asked to finalize, and
	// finalizeOnly those of them with no VSchema change to show: a finalize
	// is work the apply runs, so a plan made only of finalizes must not read
	// as "no changes".
	finalize     map[string]bool
	finalizeOnly map[string]bool
}

// collectPlanWork gathers what result runs. For a sharded plan the tables are
// every distinct per-shard statement, the same set the PR comment counts.
func collectPlanWork(result *apitypes.PlanResponse) planWork {
	w := planWork{
		namespaceMap: make(map[string][]templates.DDLChange),
		finalize:     map[string]bool{},
		finalizeOnly: map[string]bool{},
	}
	for _, sc := range result.Changes {
		if sc.NeedsFinalizer() {
			w.finalize[renderedNamespace(sc.Namespace, result.Database)] = true
		}
		if sc.ShowsVSchemaChange() {
			w.vschemaChanges = append(w.vschemaChanges, templates.VSchemaChange{
				Keyspace: sc.Namespace,
				Diff:     sc.Metadata[apitypes.VSchemaDiffMetadataKey],
			})
		} else if sc.NeedsFinalizer() {
			w.finalizeOnly[renderedNamespace(sc.Namespace, result.Database)] = true
		}
	}
	tables := result.RenderedTables()
	w.renderedTables = len(tables)
	// Collect DDL changes (filter out internal Spirit tables), grouped by namespace
	for _, tbl := range ddl.FilterInternalTablesTyped(tables) {
		ns := renderedNamespace(tbl.Namespace, result.Database)
		w.namespaceMap[ns] = append(w.namespaceMap[ns], templates.DDLChange{
			ChangeType: tbl.ChangeType,
			Namespace:  ns,
			TableName:  tbl.TableName,
			DDL:        tbl.DDL,
		})
	}
	for _, c := range w.namespaceMap {
		w.allChanges = append(w.allChanges, c...)
	}
	return w
}

// empty reports whether the plan runs nothing: no DDL, no VSchema change, and
// no finalize.
func (w planWork) empty() bool {
	return w.renderedTables == 0 && len(w.vschemaChanges) == 0 && len(w.finalizeOnly) == 0
}

// finalizes counts the namespaces whose only work is a finalize. A namespace
// with DDL finalizes as part of that work, so it is not counted.
func (w planWork) finalizes() int {
	n := 0
	for ns := range w.finalizeOnly {
		if len(w.namespaceMap[ns]) == 0 {
			n++
		}
	}
	return n
}

// planSummary is what a rollout's one plan summary counts across its groups.
type planSummary struct {
	changes        []templates.DDLChange
	vschemaChanges []templates.VSchemaChange
	// finalizes counts the namespaces whose only work, on some group, is a
	// finalize.
	finalizes int
}

// combinePlanWork merges the work of a rollout's groups into what the summary
// counts, as the PR comment summarizes target plans together: a statement run
// by several groups is counted once, as is a keyspace's VSchema change, and a
// namespace whose only work is a finalize on some members is counted as a
// finalize even where another group also runs DDL in it.
func combinePlanWork(groups []planWork) planSummary {
	var combined planSummary
	seenStatements := make(map[[2]string]bool)
	seenVSchema := make(map[string]bool)
	finalizeOnly := make(map[string]bool)
	for _, g := range groups {
		for _, c := range g.allChanges {
			key := [2]string{c.Namespace, c.DDL}
			if seenStatements[key] {
				continue
			}
			seenStatements[key] = true
			combined.changes = append(combined.changes, c)
		}
		for _, vc := range g.vschemaChanges {
			if seenVSchema[vc.Keyspace] {
				continue
			}
			seenVSchema[vc.Keyspace] = true
			combined.vschemaChanges = append(combined.vschemaChanges, vc)
		}
		for ns := range g.finalizeOnly {
			if len(g.namespaceMap[ns]) == 0 {
				finalizeOnly[ns] = true
			}
		}
	}
	combined.finalizes = len(finalizeOnly)
	return combined
}

// writeChangesBody writes one plan's changes, unsafe warnings, lint, and
// summary.
func writeChangesBody(result *apitypes.PlanResponse, isApply bool) {
	// The exempt-table disclosure renders on both branches: a clean result is
	// exactly where a reader needs to tell an exempted live table from one the
	// plan simply found declared.
	w := collectPlanWork(result)
	if w.empty() {
		templates.WriteNoChanges()
		templates.WriteExemptTables(result.ExemptTables)
		return
	}
	writeChangeDetail(result, w, isApply)

	// Show advisory (non-error) lint violations
	lintViolations := result.LintNonErrors()
	if len(lintViolations) > 0 {
		templates.WriteLintViolations(lintViolations)
	}

	// The summary counts what the DDL block above shows.
	finalizes := w.finalizes()
	switch {
	case len(w.vschemaChanges) > 0 || finalizes > 0:
		templates.WritePlanSummaryWithKeyspaceUpdates(w.allChanges, w.vschemaChanges, finalizes)
	default:
		templates.WritePlanSummary(w.allChanges)
	}
	templates.WriteExemptTables(result.ExemptTables)
}

// writeChangeDetail writes one plan's DDL and VSchema changes grouped by
// namespace, then its direct-execution and unsafe-change disclosures. It
// writes no summary, so a rollout can close every group's detail with one.
func writeChangeDetail(result *apitypes.PlanResponse, w planWork, isApply bool) {
	// Build VSchema diff map by keyspace for merging into namespace changes
	vsDiffByKS := make(map[string]string)
	for _, vc := range w.vschemaChanges {
		vsDiffByKS[vc.Keyspace] = vc.Diff
	}

	// Render DDL + VSchema changes grouped by namespace/keyspace
	isVitess := state.IsPlanetScaleEngine(result.Engine)
	if w.rendersNamespaceChanges() {
		var nsChanges []templates.NamespaceChange
		for _, ns := range w.namespaces() {
			nc := templates.NamespaceChange{
				Namespace: ns,
				Changes:   w.namespaceMap[ns],
				Finalize:  w.finalize[ns],
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
}

// directChangeNotices lists the plan's direct-execution changes one per
// namespace, table and reason, so a statement that runs the same way on every
// shard of a namespace is named once, and the same table run directly in two
// namespaces is named in each. When the notices span more than one namespace,
// each table is qualified with its namespace so the two entries read apart.
func directChangeNotices(result *apitypes.PlanResponse) []templates.UnsafeChange {
	type notice struct {
		namespace string
		change    *apitypes.TableChangeResponse
	}
	var found []notice
	seen := make(map[[3]string]bool)
	namespaces := make(map[string]bool)
	add := func(namespace string, tc *apitypes.TableChangeResponse) {
		if !tc.DirectExecution() {
			return
		}
		ns := renderedNamespace(cmp.Or(namespace, tc.Namespace), result.Database)
		key := [3]string{ns, tc.TableName, tc.ModeReason}
		if seen[key] {
			return
		}
		seen[key] = true
		namespaces[ns] = true
		found = append(found, notice{namespace: ns, change: tc})
	}
	for _, sc := range result.Changes {
		if sc == nil {
			continue
		}
		for _, tc := range sc.TableChanges {
			add(sc.Namespace, tc)
		}
	}
	for _, sp := range result.Shards {
		if sp == nil {
			continue
		}
		for _, tc := range sp.Changes {
			add(sp.Namespace, tc)
		}
	}
	notices := make([]templates.UnsafeChange, 0, len(found))
	for _, n := range found {
		table := n.change.TableName
		if len(namespaces) > 1 {
			table = n.namespace + "." + table
		}
		notices = append(notices, templates.UnsafeChange{Table: table, Reason: n.change.ModeReason, ChangeType: n.change.ChangeType})
	}
	return notices
}

// hasResultChanges returns true if the result has schema changes (DDL or
// VSchema) on any member of its rollout.
func hasResultChanges(result *apitypes.PlanResponse) bool {
	return result != nil && result.RolloutHasChanges()
}

// everyPlanMatches reports whether every environment's plan fingerprints the
// same as reference, which is what lets them render as one combined section.
// An environment with no plan is not a match: it renders as not configured.
// Nor is a rollout's plan, which names its members, so it never reads as
// another environment's plan.
func everyPlanMatches(results map[string]*apitypes.PlanResponse, reference *apitypes.PlanResponse) bool {
	want := planFingerprint(reference)
	for _, result := range results {
		if result == nil || result.WholeRollout() != nil || planFingerprint(result) != want {
			return false
		}
	}
	return true
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
// Two plans fingerprint the same when their sections would read the same: the
// same statements under the same namespaces, the same VSchema updates and
// finalizes, the same unsafe findings, the same advisory lint, and the same
// exempt-table disclosure. The unsafe and lint verdicts are part of it because
// they come from each environment's live pre-state, not from the statement: an
// index made invisible in staging but not yet in production gives both the
// same DROP INDEX and only production a finding, and folding production under
// staging's clean section would hide it. Whatever writePlanBody renders has to
// be in here, or an environment that differs only in that detail folds away.
func planFingerprint(result *apitypes.PlanResponse) string {
	// Check for errors first
	if len(result.Errors) > 0 {
		data, _ := json.Marshal(result.Errors)
		return "errors:" + string(data)
	}

	var ddls []string
	for _, tbl := range result.RenderedTables() {
		ddls = append(ddls, renderedNamespace(tbl.Namespace, result.Database)+":"+tbl.ChangeType+":"+tbl.TableName+":"+tbl.DDL)
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

	var unsafeFindings []string
	for _, change := range result.UnsafeChanges() {
		unsafeFindings = append(unsafeFindings, change.Table+":"+change.ChangeType+":"+change.Reason)
	}
	var lint []string
	for _, violation := range result.LintNonErrors() {
		lint = append(lint, violation.Table+":"+violation.Message)
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
	sort.Strings(unsafeFindings)
	sort.Strings(lint)
	sort.Strings(exempt)

	data, _ := json.Marshal(struct {
		DDLs      []string `json:"ddls"`
		VSchemas  []string `json:"vschemas"`
		Finalizes []string `json:"finalizes"`
		Unsafe    []string `json:"unsafe"`
		Lint      []string `json:"lint"`
		Exempt    []string `json:"exempt"`
	}{ddls, vschemas, finalizes, unsafeFindings, lint, exempt})
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
