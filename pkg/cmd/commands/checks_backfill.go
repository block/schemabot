package commands

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	cmdclient "github.com/block/schemabot/pkg/cmd/client"
)

// ChecksCmd groups SchemaBot Check Run operator commands.
type ChecksCmd struct {
	Backfill ChecksBackfillCmd `cmd:"" help:"Find open PRs missing SchemaBot Check Runs and recreate them"`
}

// ChecksBackfillCmd finds open PRs missing SchemaBot Check Runs and recreates
// them, choosing the right remedy per PR: redeliver the PR's failed webhook
// delivery when GitHub still retains one, otherwise replay the auto-plan flow
// server-side. --dry-run prints the per-PR classification without acting.
type ChecksBackfillCmd struct {
	Repo             string `arg:"" help:"Repository to backfill, in owner/name form"`
	Environment      string `help:"Only backfill this environment's SchemaBot Check Run name"`
	CheckName        string `help:"Override the SchemaBot Check Run name to look for"`
	Limit            int    `help:"Maximum open PRs to consider; 0 considers all" default:"0"`
	DeliveryWindow   string `help:"How far back to search delivery history for redriveable deliveries, for example 6h or 14d" default:"14d"`
	MaxDeliveryPages int    `help:"Maximum delivery pages to search per App" default:"300"`
	DryRun           bool   `help:"Print the per-PR classification without redelivering or synthesizing"`
	JSON             bool   `help:"Output as JSON"`
}

// checksSynthesizeBatchSize matches the server's per-request PR cap.
const checksSynthesizeBatchSize = 10

// checksRedriveBatchSize bounds the delivery IDs redelivered per request, so
// the redrive phase stays chunked (like synthesize) and no single request can
// outlive an intermediary HTTP timeout under the server's per-delivery delay.
const checksRedriveBatchSize = 50

// checksBackfillAction is one PR's classification and (after acting) outcome.
type checksBackfillAction struct {
	PR           int      `json:"pr"`
	URL          string   `json:"url"`
	Title        string   `json:"title"`
	HeadSHA      string   `json:"head_sha"`
	MissingNames []string `json:"missing_names"`
	// UntrustedConflictNames are missing checks whose slot is already held by
	// an untrusted app's Check Run; backfill recreates the trusted check but
	// the operator likely also needs to resolve the conflicting one.
	UntrustedConflictNames []string `json:"untrusted_conflict_names,omitempty"`
	// Classification is "redrive" when GitHub still retains a failed
	// check-creating delivery for the PR, otherwise "synthesize".
	Classification string `json:"classification"`
	// RedriveByApp groups the PR's redriveable delivery IDs by the App that
	// owns them. A repo/PR can have retained failed deliveries in more than
	// one App (for example after an App migration), and a delivery can only be
	// redelivered with its own App's token, so the grouping must be preserved.
	RedriveByApp map[string][]int64 `json:"redrive_by_app,omitempty"`
	Outcome      string             `json:"outcome,omitempty"`
	Error        string             `json:"error,omitempty"`
}

type checksBackfillReport struct {
	Repo                   string                 `json:"repo"`
	CheckNames             []string               `json:"check_names"`
	Scanned                int                    `json:"scanned"`
	DeliverySearchComplete bool                   `json:"delivery_search_complete"`
	DryRun                 bool                   `json:"dry_run"`
	Actions                []checksBackfillAction `json:"actions"`
}

func (cmd *ChecksBackfillCmd) Run(ctx context.Context, g *Globals) error {
	if cmd.Limit < 0 {
		return fmt.Errorf("--limit must be non-negative")
	}
	if cmd.MaxDeliveryPages <= 0 {
		return fmt.Errorf("--max-delivery-pages must be positive")
	}
	deliveryWindow, err := parseWebhookRedriveLast(cmd.DeliveryWindow)
	if err != nil {
		return fmt.Errorf("parse --delivery-window: %w", err)
	}
	endpoint, err := g.Resolve()
	if err != nil {
		return err
	}

	updateProgress, stopProgress := startLiveProgress(!cmd.JSON)
	defer stopProgress()

	// Phase 1: one crawl of the retained delivery history builds the
	// redriveable index — proving "no delivery exists" for a PR any other way
	// would cost a crawl per PR. Partial coverage is tolerated: a PR whose
	// failed delivery sits beyond the budget is synthesized instead, which
	// converges to the same checks.
	windowEnd := webhookRedriveNow()
	windowStart := windowEnd.Add(-deliveryWindow)
	updateProgress(fmt.Sprintf("searching delivery history for failed deliveries in %s...", cmd.Repo))
	crawl, err := runChunkedWebhookRedrive(ctx, cmdclient.RedriveWebhooks, endpoint, apitypes.WebhookRedriveRequest{
		WindowStart: windowStart.Format(time.RFC3339),
		WindowEnd:   windowEnd.Format(time.RFC3339),
		Repo:        cmd.Repo,
		MaxPages:    cmd.MaxDeliveryPages,
		DryRun:      true,
	}, cmd.MaxDeliveryPages, false, func(r apitypes.WebhookRedriveResult) {
		updateProgress(fmt.Sprintf("app %s: %d delivery pages searched, %d failed deliveries found for %s", r.AppName, r.Pages, len(r.Selected), cmd.Repo))
	})
	if canceled := backfillCanceledError(err, stopProgress); canceled != nil {
		return canceled
	}
	if err != nil {
		return fmt.Errorf("search delivery history: %w", err)
	}
	redriveable, searchComplete := indexRedriveableDeliveries(crawl)

	// Phase 2: page through open PRs missing the configured Check Runs and
	// classify each against the delivery index.
	report := &checksBackfillReport{Repo: cmd.Repo, DeliverySearchComplete: searchComplete, DryRun: cmd.DryRun}
	page := 1
	for {
		chunk, err := cmdclient.ChecksScan(ctx, endpoint, apitypes.ChecksScanRequest{
			Repo:        cmd.Repo,
			Environment: cmd.Environment,
			CheckName:   cmd.CheckName,
			Page:        page,
		})
		if canceled := backfillCanceledError(err, stopProgress); canceled != nil {
			return canceled
		}
		if err != nil {
			return fmt.Errorf("scan open PRs for missing Check Runs: %w", err)
		}
		report.CheckNames = chunk.CheckNames
		report.Scanned += chunk.Scanned
		for _, missing := range chunk.Missing {
			action := checksBackfillAction{
				PR:                     missing.Number,
				URL:                    missing.URL,
				Title:                  missing.Title,
				HeadSHA:                missing.HeadSHA,
				MissingNames:           missing.MissingNames,
				UntrustedConflictNames: missing.UntrustedConflictNames,
				Classification:         "synthesize",
			}
			if byApp, ok := redriveable[missing.Number]; ok {
				action.Classification = "redrive"
				action.RedriveByApp = byApp
			}
			report.Actions = append(report.Actions, action)
		}
		updateProgress(fmt.Sprintf("scanned %d open PRs in %s, %d missing Check Runs so far", report.Scanned, cmd.Repo, len(report.Actions)))
		if chunk.NextPage == 0 || (cmd.Limit > 0 && report.Scanned >= cmd.Limit) {
			break
		}
		page = chunk.NextPage
	}

	if !cmd.DryRun {
		// Phase 3: act — redeliver the redriveable PRs' deliveries per App,
		// then synthesize the rest in server-bounded batches.
		err := cmd.act(ctx, endpoint, report, updateProgress)
		if canceled := backfillCanceledError(err, stopProgress); canceled != nil {
			// Print what completed before the interrupt, then exit non-zero.
			stopProgress()
			_ = cmd.write(report)
			return canceled
		}
		if err != nil {
			return err
		}
	}
	stopProgress()
	return cmd.write(report)
}

// backfillCanceledError maps an operator cancellation (Ctrl+C) to a clean
// stop: it clears the progress line, prints a notice, and returns ErrSilent so
// the CLI exits non-zero without a raw "context canceled" error line. It
// returns nil for any other error (including none) so the caller handles it
// normally.
func backfillCanceledError(err error, stopProgress func()) error {
	if !errors.Is(err, context.Canceled) {
		return nil
	}
	stopProgress()
	fmt.Fprintln(os.Stderr, "checks backfill canceled")
	return ErrSilent
}

// redriveableDeliveries is one PR's failed check-creating deliveries; all of
// a repo's deliveries come from the App that serves it, so one App per PR.

// indexRedriveableDeliveries maps PR number to its failed check-creating
// deliveries from the crawl. searchComplete is false when any App's crawl
// stopped before covering the window (page budget), meaning some redriveable
// PRs may classify as synthesize instead.
func indexRedriveableDeliveries(crawl *apitypes.WebhookRedriveResponse) (map[int]map[string][]int64, bool) {
	redriveable := make(map[int]map[string][]int64)
	searchComplete := true
	if crawl == nil {
		return redriveable, searchComplete
	}
	for _, result := range crawl.Results {
		if !result.ReachedWindowStart && !result.HistoryExhausted {
			searchComplete = false
		}
		for _, selected := range result.Selected {
			if selected.PR == 0 {
				continue
			}
			byApp := redriveable[selected.PR]
			if byApp == nil {
				byApp = make(map[string][]int64)
				redriveable[selected.PR] = byApp
			}
			// Preserve the owning App per delivery: a PR can have deliveries in
			// more than one App, and each can only be redelivered with its own
			// App's token.
			byApp[result.AppName] = append(byApp[result.AppName], selected.ID)
		}
	}
	return redriveable, searchComplete
}

func (cmd *ChecksBackfillCmd) act(ctx context.Context, endpoint string, report *checksBackfillReport, updateProgress func(string)) error {
	// Redeliveries first, grouped per App (a delivery only redelivers with its
	// own App's token) and chunked so no single request redelivers an
	// unbounded number of deliveries.
	idsByApp := make(map[string][]int64)
	for _, action := range report.Actions {
		for app, ids := range action.RedriveByApp {
			idsByApp[app] = append(idsByApp[app], ids...)
		}
	}
	redriveFailedByApp := make(map[string]bool)
	for _, app := range sortedKeys(idsByApp) {
		ids := idsByApp[app]
		for start := 0; start < len(ids); start += checksRedriveBatchSize {
			batch := ids[start:min(start+checksRedriveBatchSize, len(ids))]
			updateProgress(fmt.Sprintf("redelivering %d/%d failed deliveries via app %s...", min(start+len(batch), len(ids)), len(ids), app))
			resp, err := cmdclient.RedriveWebhooks(ctx, endpoint, apitypes.WebhookRedriveRequest{
				App:         app,
				DeliveryIDs: batch,
			})
			if err != nil {
				return fmt.Errorf("redeliver failed deliveries via app %q: %w", app, err)
			}
			for _, result := range resp.Results {
				if result.Failed > 0 {
					redriveFailedByApp[app] = true
				}
			}
		}
	}

	// Then synthesize, in server-bounded batches.
	var synthesizePRs []int
	actionByPR := make(map[int]*checksBackfillAction)
	for i := range report.Actions {
		action := &report.Actions[i]
		actionByPR[action.PR] = action
		switch action.Classification {
		case "redrive":
			anyFailed := false
			for app := range action.RedriveByApp {
				if redriveFailedByApp[app] {
					anyFailed = true
				}
			}
			if anyFailed {
				action.Outcome = "redelivered (some redeliveries failed; see server logs)"
			} else {
				action.Outcome = "redelivered"
			}
		case "synthesize":
			synthesizePRs = append(synthesizePRs, action.PR)
		}
	}
	for start := 0; start < len(synthesizePRs); start += checksSynthesizeBatchSize {
		batch := synthesizePRs[start:min(start+checksSynthesizeBatchSize, len(synthesizePRs))]
		updateProgress(fmt.Sprintf("synthesizing Check Runs for %d/%d PRs...", min(start+len(batch), len(synthesizePRs)), len(synthesizePRs)))
		resp, err := cmdclient.ChecksSynthesize(ctx, endpoint, apitypes.ChecksSynthesizeRequest{Repo: cmd.Repo, PRs: batch})
		if err != nil {
			return fmt.Errorf("synthesize Check Runs: %w", err)
		}
		for _, result := range resp.Results {
			if action, ok := actionByPR[result.PR]; ok {
				action.Outcome = result.Outcome
				action.Error = result.Error
			}
		}
	}
	return nil
}

func (cmd *ChecksBackfillCmd) write(report *checksBackfillReport) error {
	if cmd.JSON {
		return writeJSON(report)
	}
	return writeChecksBackfillReport(os.Stdout, report)
}

func writeChecksBackfillReport(w io.Writer, report *checksBackfillReport) error {
	if _, err := fmt.Fprintf(w, "Scanned %d open PRs in %s for %s.\n", report.Scanned, report.Repo, strings.Join(report.CheckNames, ", ")); err != nil {
		return err
	}
	if !report.DeliverySearchComplete {
		if _, err := fmt.Fprintln(w, "Note: the delivery-history search did not cover the full window; PRs with older failed deliveries are synthesized instead (same resulting Check Runs)."); err != nil {
			return err
		}
	}
	if len(report.Actions) == 0 {
		_, err := fmt.Fprintln(w, "No missing SchemaBot Check Runs found.")
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	header := "PR\tHEAD SHA\tMISSING CHECKS\tACTION\tTITLE"
	if !report.DryRun {
		header = "PR\tHEAD SHA\tMISSING CHECKS\tOUTCOME\tTITLE"
	}
	if _, err := fmt.Fprintln(tw, header); err != nil {
		return err
	}
	failed := 0
	for _, action := range report.Actions {
		status := describeBackfillPlan(action)
		if !report.DryRun {
			status = action.Outcome
			if action.Error != "" {
				status = "failed: " + action.Error
				failed++
			}
		}
		// The status (server outcome/error) and the GitHub-controlled title are
		// tab-separated cells: strip tabs/newlines so a value containing them
		// cannot break the table layout.
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", action.URL, shortSHA(action.HeadSHA), strings.Join(action.MissingNames, ", "), sanitizeCell(status), sanitizeCell(action.Title)); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	for _, action := range report.Actions {
		if len(action.UntrustedConflictNames) > 0 {
			if _, err := fmt.Fprintf(w, "Note: PR #%d has checks held by an untrusted app (%s); backfill recreates the trusted check, but resolve the conflicting one too.\n", action.PR, strings.Join(action.UntrustedConflictNames, ", ")); err != nil {
				return err
			}
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d Check Run backfills failed; see the outcomes above", failed)
	}
	return nil
}

// describeBackfillPlan renders a dry-run action, naming the remedy the
// backfill would use for the PR.
func describeBackfillPlan(action checksBackfillAction) string {
	if action.Classification == "redrive" {
		total := 0
		for _, ids := range action.RedriveByApp {
			total += len(ids)
		}
		return fmt.Sprintf("redrive %d failed deliveries (app %s)", total, strings.Join(sortedKeys(action.RedriveByApp), ", "))
	}
	return "synthesize via auto-plan (no retained delivery)"
}

// sortedKeys returns the map keys in deterministic order, so per-App output
// (dry-run plan, redrive progress) does not depend on map iteration order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sanitizeCell collapses tabs and newlines to spaces so a caller-influenced
// value (a PR title, a server error string) cannot break the tab-separated
// table layout.
func sanitizeCell(s string) string {
	return strings.NewReplacer("\t", " ", "\n", " ", "\r", " ").Replace(s)
}

func shortSHA(sha string) string {
	if len(sha) <= 12 {
		return sha
	}
	return sha[:12]
}
