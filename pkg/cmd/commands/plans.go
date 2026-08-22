package commands

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/ui"
)

// PlansCmd lists recently generated plans, or shows one stored plan's content.
type PlansCmd struct {
	PlanIDArg   string `arg:"" optional:"" help:"Plan ID to show full stored content for" name:"plan_id"`
	Database    string `short:"d" help:"Database filter"`
	Environment string `short:"e" help:"Environment filter"`
	Repository  string `help:"Repository filter (owner/name)"`
	PR          int    `help:"Pull request filter; requires --repository" name:"pr"`
	Last        string `help:"Only show plans created within this window, for example 6h or 2d; by default the list is bounded by --limit alone"`
	Limit       int    `short:"n" help:"Maximum recent plans to show (default 20, max 200)"`
	JSON        bool   `help:"Output as JSON"`
}

// Run executes the plans command.
func (cmd *PlansCmd) Run(g *Globals) error {
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}

	if cmd.PlanIDArg != "" {
		return showStoredPlan(ep, cmd.PlanIDArg, cmd.JSON)
	}

	if cmd.PR > 0 && cmd.Repository == "" {
		return fmt.Errorf("--pr requires --repository")
	}
	var last time.Duration
	if cmd.Last != "" {
		window, err := parseOperatorDuration(cmd.Last)
		if err != nil {
			return fmt.Errorf("parse --last: %w", err)
		}
		last = window
	}

	var result *apitypes.PlansResponse
	err = withLoading("Loading plans...", !cmd.JSON, func() error {
		var loadErr error
		result, loadErr = client.GetPlans(ep, client.PlansOptions{
			Limit:       cmd.Limit,
			Database:    cmd.Database,
			Environment: cmd.Environment,
			Repository:  cmd.Repository,
			PullRequest: cmd.PR,
			Last:        last,
		})
		return loadErr
	})
	if err != nil {
		return err
	}

	if cmd.JSON {
		return writeJSON(result)
	}

	rows := make([]templates.PlanSummaryData, 0, len(result.Plans))
	for _, p := range result.Plans {
		rows = append(rows, templates.PlanSummaryData{
			PlanID:      p.PlanID,
			Database:    p.Database,
			Environment: p.Environment,
			Source:      planSource(p),
			CreatedAt:   p.CreatedAt,
			Changes:     planChangeSummary(p),
		})
	}
	templates.WritePlansList(templates.PlansListData{
		Limit:    result.Limit,
		MaxLimit: result.MaxLimit,
		HasMore:  result.HasMore,
		Last:     cmd.Last,
		Plans:    rows,
	})
	return nil
}

// showStoredPlan shows one stored plan: its provenance header plus the stored
// plan content, rendered through the same body a fresh plan uses.
func showStoredPlan(endpoint, planID string, outputJSON bool) error {
	var result *apitypes.StoredPlanResponse
	err := withLoading("Loading plan...", !outputJSON, func() error {
		var loadErr error
		result, loadErr = client.GetStoredPlan(endpoint, planID)
		return loadErr
	})
	if err != nil {
		if client.IsNotFound(err) {
			fmt.Printf("No plan found for '%s'\n", planID)
			return nil
		}
		return err
	}

	if outputJSON {
		return writeJSON(result)
	}

	fmt.Println(result.PlanID)
	fmt.Printf("  Database:    %s (%s)\n", result.Database, result.DatabaseType)
	fmt.Printf("  Environment: %s\n", result.Environment)
	fmt.Printf("  Source:      %s\n", planSource(&result.PlanSummaryResponse))
	if result.HeadSHA != "" {
		fmt.Printf("  Head SHA:    %s\n", result.HeadSHA)
	}
	fmt.Printf("  Created:     %s\n", result.CreatedAt.Format(time.RFC3339))
	fmt.Println()
	writePlanBody(result.Plan, false)
	return nil
}

// planSource renders a plan's provenance the way the status list renders an
// apply's source: the short "owner/repo#pr" as an OSC 8 hyperlink to the PR on
// an interactive terminal (the full URL everywhere else), the repository alone
// when the plan names no PR, and "ad-hoc" for a plan created without either.
func planSource(p *apitypes.PlanSummaryResponse) string {
	if p.Repository != "" && p.PullRequest > 0 {
		url := fmt.Sprintf("https://github.com/%s/pull/%d", p.Repository, p.PullRequest)
		return ui.Link(fmt.Sprintf("%s#%d", p.Repository, p.PullRequest), url)
	}
	if p.Repository != "" {
		return p.Repository
	}
	return "ad-hoc"
}

// planChangeSummary renders a stored plan's change counts as one line, such as
// "3 changes: 1 create, 2 alter · 1 unsafe".
func planChangeSummary(p *apitypes.PlanSummaryResponse) string {
	total := 0
	for _, count := range p.ChangeCounts {
		total += count
	}
	if total == 0 && p.VSchemaChangeCount == 0 {
		return "no changes"
	}

	var parts []string
	if total > 0 {
		head := fmt.Sprintf("%d change", total)
		if total != 1 {
			head += "s"
		}
		var ops []string
		for _, op := range orderedChangeOperations(p.ChangeCounts) {
			ops = append(ops, fmt.Sprintf("%d %s", p.ChangeCounts[op], op))
		}
		parts = append(parts, head+": "+strings.Join(ops, ", "))
	}
	if p.VSchemaChangeCount > 0 {
		parts = append(parts, fmt.Sprintf("%d vschema", p.VSchemaChangeCount))
	}
	if p.UnsafeCount > 0 {
		parts = append(parts, fmt.Sprintf("⚠️ %d unsafe", p.UnsafeCount))
	}
	if p.BlockedCount > 0 {
		parts = append(parts, fmt.Sprintf("⛔ %d blocked", p.BlockedCount))
	}
	return strings.Join(parts, " · ")
}

// orderedChangeOperations returns the plan's change operations with the
// common table operations first, in lifecycle order, and any remaining
// operations sorted after them.
func orderedChangeOperations(counts map[string]int) []string {
	canonical := []string{"create", "alter", "drop"}
	seen := make(map[string]bool, len(canonical))
	var ops []string
	for _, op := range canonical {
		if counts[op] > 0 {
			ops = append(ops, op)
			seen[op] = true
		}
	}
	var rest []string
	for op := range counts {
		if !seen[op] {
			rest = append(rest, op)
		}
	}
	sort.Strings(rest)
	return append(ops, rest...)
}
