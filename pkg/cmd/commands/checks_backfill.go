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
	Backfill ChecksBackfillCmd `cmd:"" help:"Find open PRs with missing or stuck SchemaBot Check Runs; recreate the missing ones"`
}

// ChecksBackfillCmd walks open PRs and asks the two questions that matter
// for the check gate: is the expected SchemaBot Check Run present, and if
// present, did it complete? Missing checks are recreated by replaying the
// auto-plan flow server-side; existing-but-uncompleted checks aged past
// --stuck-after are reported for investigation, never acted on, because an
// uncompleted check can belong to a genuinely in-flight apply.
//
// --all-repos sweeps every repository declared on the server, and --last
// bounds the sweep to PRs updated within a window — together they make an
// incident sweep cost O(incident) instead of O(every open PR at the org).
// Without --last the scan covers every open PR, which is the one-time
// backfill for a repository that predates check enablement. --dry-run
// reports without acting.
type ChecksBackfillCmd struct {
	Repo             string `arg:"" optional:"" help:"Repository to backfill, in owner/name form; omit with --all-repos"`
	AllRepos         bool   `help:"Scan every repository declared in the server's repos config"`
	Last             string `help:"Only scan PRs updated within this window, for example 1h or 1d; by default every open PR is scanned"`
	Environment      string `help:"Only backfill this environment's SchemaBot Check Run name"`
	CheckName        string `help:"Override the SchemaBot Check Run name to look for"`
	Limit            int    `help:"Maximum open PRs to consider across all scanned repositories; 0 considers all" default:"0"`
	StuckAfter       string `help:"Report an existing but uncompleted Check Run as stuck once it has been sitting this long, for example 1h or 2d" default:"1h"`
	RateLimitFloor   int    `help:"Pause whenever the GitHub budget drops below this percentage of its limit, resuming after it resets, so live webhook traffic keeps headroom; 0 disables pacing" default:"20"`
	Redrive          bool   `help:"Redeliver a missing-check PR's retained failed webhook delivery instead of synthesizing, when GitHub still has one (single repository only; slow: GitHub lists delivery history App-wide)"`
	DeliveryWindow   string `help:"With --redrive: how far back to search delivery history, for example 6h or 14d" default:"14d"`
	MaxDeliveryPages int    `help:"With --redrive: maximum delivery pages to search per App" default:"300"`
	DryRun           bool   `help:"Report missing and stuck Check Runs without redelivering or synthesizing"`
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
	Repo         string   `json:"repo"`
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
	// check-creating delivery for the PR (found only under --redrive),
	// otherwise "synthesize".
	Classification string `json:"classification"`
	// RedriveByApp groups the PR's redriveable delivery IDs by the App that
	// owns them. A repo/PR can have retained failed deliveries in more than
	// one App (for example after an App migration), and a delivery can only be
	// redelivered with its own App's token, so the grouping must be preserved.
	RedriveByApp map[string][]int64 `json:"redrive_by_app,omitempty"`
	Outcome      string             `json:"outcome,omitempty"`
	Error        string             `json:"error,omitempty"`
}

// checksStuckCheck is one existing-but-uncompleted Check Run on an open PR,
// aged past --stuck-after. Reported for investigation only; the backfill
// never acts on a check that already exists.
type checksStuckCheck struct {
	Repo       string `json:"repo"`
	PR         int    `json:"pr"`
	URL        string `json:"url"`
	Title      string `json:"title"`
	HeadSHA    string `json:"head_sha"`
	CheckName  string `json:"check_name"`
	CheckRunID int64  `json:"check_run_id"`
	Status     string `json:"status"`
	StartedAt  string `json:"started_at,omitempty"`
	// Age is how long the run has been sitting uncompleted at scan time;
	// "unknown" when GitHub did not report a start time.
	Age string `json:"age"`
}

type checksBackfillReport struct {
	Repos      []string `json:"repos"`
	CheckNames []string `json:"check_names"`
	Scanned    int      `json:"scanned"`
	// Last echoes the --last window bounding the sweep; empty means every
	// open PR was scanned.
	Last                   string                 `json:"last,omitempty"`
	Redrive                bool                   `json:"redrive"`
	DeliverySearchComplete bool                   `json:"delivery_search_complete"`
	DryRun                 bool                   `json:"dry_run"`
	StuckAfter             string                 `json:"stuck_after"`
	Actions                []checksBackfillAction `json:"actions"`
	Stuck                  []checksStuckCheck     `json:"stuck,omitempty"`
}

func (cmd *ChecksBackfillCmd) Run(ctx context.Context, g *Globals) error {
	if cmd.Repo == "" && !cmd.AllRepos {
		return fmt.Errorf("name a repository in owner/name form, or pass --all-repos to sweep every repository declared on the server")
	}
	if cmd.Repo != "" && cmd.AllRepos {
		return fmt.Errorf("use either a repository argument or --all-repos, not both")
	}
	if cmd.Redrive && cmd.AllRepos {
		return fmt.Errorf("--redrive searches one repository's delivery history; use it with a repository argument, not --all-repos")
	}
	if cmd.Limit < 0 {
		return fmt.Errorf("--limit must be non-negative")
	}
	if cmd.RateLimitFloor < 0 || cmd.RateLimitFloor > 99 {
		return fmt.Errorf("--rate-limit-floor must be between 0 and 99")
	}
	stuckAfter, err := parseOperatorDuration(cmd.StuckAfter)
	if err != nil {
		return fmt.Errorf("parse --stuck-after: %w", err)
	}
	var updatedSince string
	if cmd.Last != "" {
		window, err := parseOperatorDuration(cmd.Last)
		if err != nil {
			return fmt.Errorf("parse --last: %w", err)
		}
		updatedSince = webhookRedriveNow().Add(-window).Format(time.RFC3339)
	}
	endpoint, err := g.Resolve()
	if err != nil {
		return err
	}

	updateProgress, stopProgress := startLiveProgress(!cmd.JSON)
	defer stopProgress()

	repos := []string{cmd.Repo}
	if cmd.AllRepos {
		updateProgress("listing the repositories declared on the server...")
		declared, err := cmdclient.ChecksRepos(ctx, endpoint)
		if canceled := backfillCanceledError(err, stopProgress); canceled != nil {
			return canceled
		}
		if err != nil {
			return fmt.Errorf("list the server's declared repositories: %w", err)
		}
		if len(declared.Repos) == 0 {
			return fmt.Errorf("the server declared no repositories; name a repository explicitly")
		}
		repos = declared.Repos
	}

	// Optional redrive phase: one crawl of the retained delivery history
	// builds the redriveable index — proving "no delivery exists" for a PR any
	// other way would cost a crawl per PR. GitHub lists delivery history
	// App-wide (there is no repo filter), so the crawl walks every repo's
	// deliveries and is expensive; it is opt-in because a missing check
	// synthesizes to the same resulting Check Runs without it.
	redriveable := map[int]map[string][]int64{}
	searchComplete := true
	if cmd.Redrive {
		windowEnd := webhookRedriveNow()
		deliveryWindow, err := parseOperatorDuration(cmd.DeliveryWindow)
		if err != nil {
			return fmt.Errorf("parse --delivery-window: %w", err)
		}
		if cmd.MaxDeliveryPages <= 0 {
			return fmt.Errorf("--max-delivery-pages must be positive")
		}
		windowStart := windowEnd.Add(-deliveryWindow)
		updateProgress(fmt.Sprintf("searching delivery history for failed deliveries in %s...", cmd.Repo))
		crawl, err := runChunkedWebhookRedrive(ctx, cmdclient.RedriveWebhooks, endpoint, apitypes.WebhookRedriveRequest{
			WindowStart: windowStart.Format(time.RFC3339),
			WindowEnd:   windowEnd.Format(time.RFC3339),
			Repo:        cmd.Repo,
			MaxPages:    cmd.MaxDeliveryPages,
			DryRun:      true,
		}, cmd.MaxDeliveryPages, false, func(r apitypes.WebhookRedriveResult) {
			updateProgress(fmt.Sprintf("app %s: %d/%d delivery pages searched, %d failed deliveries found for %s", r.AppName, r.Pages, cmd.MaxDeliveryPages, len(r.Selected), cmd.Repo))
		})
		if canceled := backfillCanceledError(err, stopProgress); canceled != nil {
			return canceled
		}
		if err != nil {
			return fmt.Errorf("search delivery history: %w", err)
		}
		redriveable, searchComplete = indexRedriveableDeliveries(crawl)
	}

	// Scan phase: page through each repository's open PRs and ask the two
	// questions per PR — is the expected Check Run present, and did it
	// complete? The listing is newest-updated first, so a --last window stops
	// each repo's paging as soon as it crosses the window start.
	report := &checksBackfillReport{
		Repos:                  repos,
		Last:                   cmd.Last,
		Redrive:                cmd.Redrive,
		DeliverySearchComplete: searchComplete,
		DryRun:                 cmd.DryRun,
		StuckAfter:             cmd.StuckAfter,
	}
	checkNamesSeen := map[string]bool{}
scanning:
	for i, repo := range repos {
		page := 1
		repoScanned := 0
		for {
			chunk, err := cmdclient.ChecksScan(ctx, endpoint, apitypes.ChecksScanRequest{
				Repo:         repo,
				Environment:  cmd.Environment,
				CheckName:    cmd.CheckName,
				Page:         page,
				UpdatedSince: updatedSince,
			})
			if canceled := backfillCanceledError(err, stopProgress); canceled != nil {
				return canceled
			}
			if err != nil {
				return fmt.Errorf("scan %s open PRs for missing or stuck Check Runs: %w", repo, err)
			}
			for _, name := range chunk.CheckNames {
				if !checkNamesSeen[name] {
					checkNamesSeen[name] = true
					report.CheckNames = append(report.CheckNames, name)
				}
			}
			repoScanned += chunk.Scanned
			report.Scanned += chunk.Scanned
			for _, missing := range chunk.Missing {
				action := checksBackfillAction{
					Repo:                   repo,
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
			report.Stuck = append(report.Stuck, stuckChecksPastThreshold(repo, chunk.Stuck, stuckAfter, webhookRedriveNow())...)
			updateProgress(fmt.Sprintf("repo %d/%d %s: %d PRs scanned (%d total) — %d missing, %d stuck Check Runs so far", i+1, len(repos), repo, repoScanned, report.Scanned, len(report.Actions), len(report.Stuck)))
			if cmd.Limit > 0 && report.Scanned >= cmd.Limit {
				break scanning
			}
			if chunk.NextPage == 0 {
				break
			}
			if err := pauseForRateLimit(ctx, chunk.RateLimit, cmd.RateLimitFloor, updateProgress); err != nil {
				if canceled := backfillCanceledError(err, stopProgress); canceled != nil {
					return canceled
				}
				return err
			}
			page = chunk.NextPage
		}
	}
	sort.Strings(report.CheckNames)

	if !cmd.DryRun {
		// Act phase: redeliver the redriveable PRs' deliveries per App, then
		// synthesize the rest in server-bounded batches per repository.
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

// pauseForRateLimit sleeps until the GitHub budget resets when the remaining
// budget has fallen below floorPct percent of its limit. The backfill shares
// its installation budget with live webhook serving — check creation for PRs
// being pushed right now — so a large sweep must leave headroom rather than
// drain the budget and starve the live path. A nil snapshot (the server could
// not read the rate state) proceeds without pausing; GitHub's reactive
// secondary-rate-limit handling still applies underneath.
func pauseForRateLimit(ctx context.Context, rate *apitypes.GitHubRateLimit, floorPct int, updateProgress func(string)) error {
	wait, ok := rateLimitPauseDuration(rate, floorPct, webhookRedriveNow())
	if !ok {
		return nil
	}
	updateProgress(fmt.Sprintf("GitHub budget below %d%% (%d/%d requests left); pausing %s until it resets so live webhook traffic keeps headroom", floorPct, rate.Remaining, rate.Limit, wait.Truncate(time.Second)))
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// rateLimitPauseDuration decides whether the budget snapshot is below the
// floor and how long to wait for the reset. It returns false — no pause —
// when pacing is disabled, the snapshot is missing, the budget is above the
// floor, or the reset time is unparseable or already past (the next request
// sees a replenished budget, so waiting would only slow the sweep).
func rateLimitPauseDuration(rate *apitypes.GitHubRateLimit, floorPct int, now time.Time) (time.Duration, bool) {
	if rate == nil || floorPct <= 0 || rate.Limit <= 0 {
		return 0, false
	}
	if rate.Remaining*100 >= rate.Limit*floorPct {
		return 0, false
	}
	resetAt, err := time.Parse(time.RFC3339, rate.ResetAt)
	if err != nil || !resetAt.After(now) {
		return 0, false
	}
	// A minute of slack lets GitHub's reset land before the sweep resumes.
	return resetAt.Sub(now) + time.Minute, true
}

// stuckChecksPastThreshold flattens the scan's uncompleted Check Runs to one
// row per (PR, check), keeping only runs that have been sitting longer than
// stuckAfter. A run whose start time is missing, unparseable, or in the
// future (clock skew) is always kept with an "unknown" age — a start time
// that cannot prove the run is young must not hide it.
func stuckChecksPastThreshold(repo string, prs []apitypes.StuckCheckPR, stuckAfter time.Duration, now time.Time) []checksStuckCheck {
	var out []checksStuckCheck
	for _, pr := range prs {
		for _, check := range pr.Checks {
			age := "unknown"
			if check.StartedAt != "" {
				startedAt, err := time.Parse(time.RFC3339, check.StartedAt)
				if err == nil && !startedAt.After(now) {
					sitting := now.Sub(startedAt)
					if sitting < stuckAfter {
						continue
					}
					age = sitting.Truncate(time.Minute).String()
				}
			}
			out = append(out, checksStuckCheck{
				Repo:       repo,
				PR:         pr.Number,
				URL:        pr.URL,
				Title:      pr.Title,
				HeadSHA:    pr.HeadSHA,
				CheckName:  check.Name,
				CheckRunID: check.CheckRunID,
				Status:     check.Status,
				StartedAt:  check.StartedAt,
				Age:        age,
			})
		}
	}
	return out
}

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

// repoPR keys a PR within a multi-repo report.
type repoPR struct {
	repo string
	pr   int
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

	// Then synthesize, per repository, in server-bounded batches.
	synthesizeByRepo := make(map[string][]int)
	actionByPR := make(map[repoPR]*checksBackfillAction)
	for i := range report.Actions {
		action := &report.Actions[i]
		actionByPR[repoPR{repo: action.Repo, pr: action.PR}] = action
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
			synthesizeByRepo[action.Repo] = append(synthesizeByRepo[action.Repo], action.PR)
		}
	}
	for _, repo := range sortedKeys(synthesizeByRepo) {
		prs := synthesizeByRepo[repo]
		for start := 0; start < len(prs); start += checksSynthesizeBatchSize {
			batch := prs[start:min(start+checksSynthesizeBatchSize, len(prs))]
			updateProgress(fmt.Sprintf("synthesizing Check Runs for %d/%d PRs in %s...", min(start+len(batch), len(prs)), len(prs), repo))
			resp, err := cmdclient.ChecksSynthesize(ctx, endpoint, apitypes.ChecksSynthesizeRequest{Repo: repo, PRs: batch})
			if err != nil {
				return fmt.Errorf("synthesize Check Runs for %s: %w", repo, err)
			}
			for _, result := range resp.Results {
				if action, ok := actionByPR[repoPR{repo: repo, pr: result.PR}]; ok {
					action.Outcome = result.Outcome
					action.Error = result.Error
				}
			}
			if err := pauseForRateLimit(ctx, resp.RateLimit, cmd.RateLimitFloor, updateProgress); err != nil {
				return err
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
	repoScope := strings.Join(report.Repos, ", ")
	if len(report.Repos) > 3 {
		repoScope = fmt.Sprintf("%d repositories", len(report.Repos))
	}
	window := ""
	if report.Last != "" {
		window = fmt.Sprintf(" updated in the last %s", report.Last)
	}
	if _, err := fmt.Fprintf(w, "Scanned %d open PRs%s in %s for %s.\n", report.Scanned, window, repoScope, strings.Join(report.CheckNames, ", ")); err != nil {
		return err
	}
	if report.Redrive && !report.DeliverySearchComplete {
		if _, err := fmt.Fprintln(w, "Note: the delivery-history search did not cover the full window; PRs with older failed deliveries are synthesized instead (same resulting Check Runs)."); err != nil {
			return err
		}
	}
	if err := writeChecksStuckSection(w, report); err != nil {
		return err
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
		status := describeBackfillPlan(action, report.Redrive)
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
			if _, err := fmt.Fprintf(w, "Note: %s#%d has checks held by an untrusted app (%s); backfill recreates the trusted check, but resolve the conflicting one too.\n", action.Repo, action.PR, strings.Join(action.UntrustedConflictNames, ", ")); err != nil {
				return err
			}
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d Check Run backfills failed; see the outcomes above", failed)
	}
	return nil
}

// writeChecksStuckSection renders the stuck Check Runs the scan found: runs
// that exist on an open PR's head but have sat uncompleted past --stuck-after.
// Backfill never acts on these — an existing run may belong to an in-flight
// apply, and overwriting it could convert real uncertainty into a passing
// check — so the section tells the operator to investigate instead.
func writeChecksStuckSection(w io.Writer, report *checksBackfillReport) error {
	if len(report.Stuck) == 0 {
		return nil
	}
	if _, err := fmt.Fprintf(w, "\nStuck Check Runs — uncompleted for over %s (backfill does not act on existing Check Runs; investigate the apply or plan that owns each):\n", report.StuckAfter); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "PR\tHEAD SHA\tCHECK\tSTATUS\tAGE\tTITLE"); err != nil {
		return err
	}
	for _, stuck := range report.Stuck {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", stuck.URL, shortSHA(stuck.HeadSHA), stuck.CheckName, stuck.Status, stuck.Age, sanitizeCell(stuck.Title)); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintln(w)
	return err
}

// describeBackfillPlan renders a dry-run action, naming the remedy the
// backfill would use for the PR. "no retained delivery" is only a finding
// when the delivery history was actually searched.
func describeBackfillPlan(action checksBackfillAction, searchedDeliveries bool) string {
	if action.Classification == "redrive" {
		total := 0
		for _, ids := range action.RedriveByApp {
			total += len(ids)
		}
		return fmt.Sprintf("redrive %d failed deliveries (app %s)", total, strings.Join(sortedKeys(action.RedriveByApp), ", "))
	}
	if searchedDeliveries {
		return "synthesize via auto-plan (no retained delivery)"
	}
	return "synthesize via auto-plan"
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
