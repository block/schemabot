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
// uncompleted check can belong to a genuinely in-flight apply. For the same
// reason, a missing-check PR whose head also carries an uncompleted check is
// held out of the backfill (and the server independently refuses any PR with
// a non-terminal apply).
//
// --all-repos sweeps every repository declared on the server, and --last
// bounds the sweep to PRs updated within a window — together they make an
// incident sweep cost O(incident) instead of O(every open PR at the org).
// Without --last the scan covers every open PR, which is the one-time
// backfill for a repository that predates check enablement. --dry-run
// reports without acting.
type ChecksBackfillCmd struct {
	Repo           string `arg:"" optional:"" help:"Repository to backfill, in owner/name form; omit with --all-repos"`
	AllRepos       bool   `help:"Scan every repository declared in the server's repos config"`
	Last           string `help:"Only scan PRs updated within this window, for example 1h or 1d; by default every open PR is scanned"`
	Environment    string `short:"e" help:"Only backfill this environment's SchemaBot Check Run name"`
	CheckName      string `help:"Override the SchemaBot Check Run name to look for"`
	Limit          int    `help:"Maximum open PRs to consider across all scanned repositories; 0 considers all" default:"0"`
	StuckAfter     string `help:"Report an existing but uncompleted Check Run as stuck once it has been sitting this long, for example 1h or 2d" default:"1h"`
	RateLimitFloor int    `help:"Pause whenever the GitHub budget drops below this percentage of its limit, resuming after it resets, so live webhook traffic keeps headroom; 0 disables pacing" default:"20"`
	DryRun         bool   `help:"Report missing and stuck Check Runs without synthesizing"`
	JSON           bool   `help:"Output as JSON"`
}

// checksSynthesizeBatchSize matches the server's per-request PR cap.
const checksSynthesizeBatchSize = 10

// checksBackfillHeldStatus explains a held PR in the action table. The server
// refuses these too (a non-terminal apply refuses its PR's backfill), but the
// hold works from the scan's own signal — an uncompleted Check Run on the same
// head — so the PR never even reaches the server, and the operator sees why.
const checksBackfillHeldStatus = "held: an uncompleted Check Run sits on this head (a started apply may own it); investigate before backfilling"

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
	// Held marks a missing-check PR the backfill refuses to act on: the same
	// head also carries an uncompleted Check Run, which a genuinely in-flight
	// apply may own, and re-planning the PR could replace an apply-owned merge
	// block with a fresh passing plan. Held PRs are reported, never sent to
	// the server.
	Held    bool   `json:"held,omitempty"`
	Outcome string `json:"outcome,omitempty"`
	Error   string `json:"error,omitempty"`
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
	Last       string                 `json:"last,omitempty"`
	DryRun     bool                   `json:"dry_run"`
	StuckAfter string                 `json:"stuck_after"`
	Actions    []checksBackfillAction `json:"actions"`
	Stuck      []checksStuckCheck     `json:"stuck,omitempty"`
}

func (cmd *ChecksBackfillCmd) Run(ctx context.Context, g *Globals) error {
	if cmd.Repo == "" && !cmd.AllRepos {
		return fmt.Errorf("name a repository in owner/name form, or pass --all-repos to sweep every repository declared on the server")
	}
	if cmd.Repo != "" && cmd.AllRepos {
		return fmt.Errorf("use either a repository argument or --all-repos, not both")
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

	// Scan phase: page through each repository's open PRs and ask the two
	// questions per PR — is the expected Check Run present, and did it
	// complete? The listing is newest-updated first, so a --last window stops
	// each repo's paging as soon as it crosses the window start.
	report := &checksBackfillReport{
		Repos:      repos,
		Last:       cmd.Last,
		DryRun:     cmd.DryRun,
		StuckAfter: cmd.StuckAfter,
	}
	checkNamesSeen := map[string]bool{}
	held := 0
scanning:
	for i, repo := range repos {
		page := 1
		repoScanned := 0
		repoEstimate := 0
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
			if chunk.EstimatedOpenPRs > 0 {
				repoEstimate = chunk.EstimatedOpenPRs
			}
			// Every uncompleted run holds its PR, regardless of how long it has
			// been sitting — --stuck-after shapes the stuck *report*, but an
			// uncompleted run of any age could belong to an in-flight apply.
			uncompleted := make(map[int]bool, len(chunk.Stuck))
			for _, stuckPR := range chunk.Stuck {
				uncompleted[stuckPR.Number] = true
			}
			for _, missing := range chunk.Missing {
				action := checksBackfillAction{
					Repo:                   repo,
					PR:                     missing.Number,
					URL:                    missing.URL,
					Title:                  missing.Title,
					HeadSHA:                missing.HeadSHA,
					MissingNames:           missing.MissingNames,
					UntrustedConflictNames: missing.UntrustedConflictNames,
					Held:                   uncompleted[missing.Number],
				}
				if action.Held {
					held++
				}
				report.Actions = append(report.Actions, action)
			}
			report.Stuck = append(report.Stuck, stuckChecksPastThreshold(repo, chunk.Stuck, stuckAfter, webhookRedriveNow())...)
			updateProgress(checksScanProgressLine(i+1, len(repos), repo, repoScanned, repoEstimate, report.Scanned, len(report.Actions), held, len(report.Stuck)))
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
		// Act phase: synthesize the missing checks in server-bounded batches
		// per repository, skipping held PRs.
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

// checksScanProgressLine renders one scan progress update. The denominator is
// GitHub's own open-PR count for the repository, recomputed every page, so a
// long sweep always reads as progress toward a bound instead of an endless
// loop; the counts after the dash say what the sweep has found so far. The
// denominator is an upper bound until the repo's final page (a --last window
// legitimately stops short of it).
func checksScanProgressLine(repoIndex, repoCount int, repo string, repoScanned, repoEstimate, totalScanned, missing, held, stuck int) string {
	var b strings.Builder
	if repoCount > 1 {
		fmt.Fprintf(&b, "repo %d/%d ", repoIndex, repoCount)
	}
	fmt.Fprintf(&b, "%s: %d", repo, repoScanned)
	if repoEstimate > repoScanned {
		fmt.Fprintf(&b, "/~%d", repoEstimate)
	}
	b.WriteString(" PRs scanned")
	if repoCount > 1 {
		fmt.Fprintf(&b, " (%d across all repos)", totalScanned)
	}
	fmt.Fprintf(&b, " — %d missing", missing)
	if held > 0 {
		fmt.Fprintf(&b, " (%d held)", held)
	}
	fmt.Fprintf(&b, ", %d stuck Check Runs", stuck)
	return b.String()
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

// repoPR keys a PR within a multi-repo report.
type repoPR struct {
	repo string
	pr   int
}

func (cmd *ChecksBackfillCmd) act(ctx context.Context, endpoint string, report *checksBackfillReport, updateProgress func(string)) error {
	// Synthesize per repository, in server-bounded batches. Held PRs never
	// reach the server; their outcome records why.
	synthesizeByRepo := make(map[string][]int)
	actionByPR := make(map[repoPR]*checksBackfillAction)
	for i := range report.Actions {
		action := &report.Actions[i]
		actionByPR[repoPR{repo: action.Repo, pr: action.PR}] = action
		if action.Held {
			action.Outcome = checksBackfillHeldStatus
			continue
		}
		synthesizeByRepo[action.Repo] = append(synthesizeByRepo[action.Repo], action.PR)
	}
	failed := 0
	for _, repo := range sortedKeys(synthesizeByRepo) {
		prs := synthesizeByRepo[repo]
		for start := 0; start < len(prs); start += checksSynthesizeBatchSize {
			batch := prs[start:min(start+checksSynthesizeBatchSize, len(prs))]
			progressLine := fmt.Sprintf("synthesizing Check Runs for %d/%d PRs in %s...", min(start+len(batch), len(prs)), len(prs), repo)
			if failed > 0 {
				// Surface failures as they happen so the operator can interrupt a
				// failing sweep instead of discovering it in the final table.
				progressLine += fmt.Sprintf(" (%d failed so far)", failed)
			}
			updateProgress(progressLine)
			resp, err := cmdclient.ChecksSynthesize(ctx, endpoint, apitypes.ChecksSynthesizeRequest{Repo: repo, PRs: batch})
			if err != nil {
				return fmt.Errorf("synthesize Check Runs for %s: %w", repo, err)
			}
			for _, result := range resp.Results {
				if result.Error != "" {
					failed++
				}
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
	if err := writeChecksStuckSection(w, report); err != nil {
		return err
	}
	if len(report.Actions) == 0 {
		_, err := fmt.Fprintln(w, "No missing SchemaBot Check Runs found.")
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	header := "PR\tMISSING CHECKS\tACTION"
	if !report.DryRun {
		header = "PR\tMISSING CHECKS\tOUTCOME"
	}
	if _, err := fmt.Fprintln(tw, header); err != nil {
		return err
	}
	failed := 0
	for _, action := range report.Actions {
		status := "synthesize via auto-plan"
		if action.Held {
			status = checksBackfillHeldStatus
		}
		if !report.DryRun {
			status = action.Outcome
			if action.Error != "" {
				status = "failed: " + action.Error
				failed++
			}
		}
		// The status (server outcome/error) is a tab-separated cell: strip
		// tabs/newlines so a value containing them cannot break the layout.
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\n", action.URL, strings.Join(action.MissingNames, ", "), sanitizeCell(status)); err != nil {
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
	if _, err := fmt.Fprintln(tw, "PR\tCHECK\tSTATUS\tAGE"); err != nil {
		return err
	}
	for _, stuck := range report.Stuck {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", stuck.URL, stuck.CheckName, stuck.Status, stuck.Age); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintln(w)
	return err
}

// sortedKeys returns the map keys in deterministic order, so per-repo output
// (synthesize progress, batch order) does not depend on map iteration order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sanitizeCell collapses tabs and newlines to spaces so a caller-influenced
// value (a server outcome or error string) cannot break the tab-separated
// table layout.
func sanitizeCell(s string) string {
	return strings.NewReplacer("\t", " ", "\n", " ", "\r", " ").Replace(s)
}
