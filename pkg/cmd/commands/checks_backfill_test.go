package commands

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/checkstate"
)

// The scan progress line always reads as progress toward a bound: PRs scanned
// against GitHub's own open-PR count for the repository (while the count still
// exceeds what was scanned), repo position within a fleet sweep, and the
// findings so far with held PRs called out inside the missing count.
func TestChecksScanProgressLine(t *testing.T) {
	assert.Equal(t,
		"octo/repo: 90/~1500 PRs scanned — 4 missing, 2 stuck Check Runs",
		checksScanProgressLine(1, 1, "octo/repo", 90, 1500, 90, 4, 0, 2))

	assert.Equal(t,
		"repo 2/6 octo/repo: 90/~1500 PRs scanned (312 across all repos) — 4 missing (1 held), 2 stuck Check Runs",
		checksScanProgressLine(2, 6, "octo/repo", 90, 1500, 312, 4, 1, 2))

	assert.Equal(t,
		"octo/repo: 12 PRs scanned — 0 missing, 0 stuck Check Runs",
		checksScanProgressLine(1, 1, "octo/repo", 12, 12, 12, 0, 0, 0),
		"once the scan reaches the repo's count, the denominator adds nothing")
}

// A server outcome or error containing tabs/newlines is neutralized so it
// cannot break the tab-separated report layout.
func TestSanitizeCell(t *testing.T) {
	assert.Equal(t, "a b c", sanitizeCell("a\tb\nc"))
	assert.Equal(t, "plain", sanitizeCell("plain"))
}

// The stuck filter keeps only uncompleted Check Runs that have sat past the
// threshold: young runs are legitimately in flight and stay out of the
// report, aged runs are flattened to one row per (PR, check) with a
// human-readable age, and a run whose start time is missing or in the future
// (clock skew) is always kept — its age cannot prove it is young.
func TestStuckChecksPastThreshold(t *testing.T) {
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	stuck := stuckChecksPastThreshold("octo/repo", []apitypes.StuckCheckPR{
		{
			Number: 5, URL: "https://github.com/octo/repo/pull/5", Title: "aged and young", HeadSHA: "sha5",
			Checks: []apitypes.IncompleteCheckRun{
				{Name: "SchemaBot (production)", CheckRunID: 50, Status: "in_progress", StartedAt: "2026-07-12T08:30:00Z"},
				{Name: "SchemaBot (staging)", CheckRunID: 51, Status: "in_progress", StartedAt: "2026-07-12T11:50:00Z"},
			},
		},
		{
			Number: 6, URL: "https://github.com/octo/repo/pull/6", Title: "no start time", HeadSHA: "sha6",
			Checks: []apitypes.IncompleteCheckRun{
				{Name: "SchemaBot (production)", CheckRunID: 60, Status: "queued"},
			},
		},
		{
			Number: 7, URL: "https://github.com/octo/repo/pull/7", Title: "future start time", HeadSHA: "sha7",
			Checks: []apitypes.IncompleteCheckRun{
				{Name: "SchemaBot (production)", CheckRunID: 70, Status: "in_progress", StartedAt: "2026-07-12T13:00:00Z"},
			},
		},
	}, time.Hour, now)

	require.Len(t, stuck, 3)
	assert.Equal(t, "octo/repo", stuck[0].Repo)
	assert.Equal(t, 5, stuck[0].PR)
	assert.Equal(t, "SchemaBot (production)", stuck[0].CheckName)
	assert.Equal(t, "3h30m0s", stuck[0].Age)
	assert.Equal(t, 6, stuck[1].PR)
	assert.Equal(t, "unknown", stuck[1].Age)
	assert.Equal(t, 7, stuck[2].PR)
	assert.Equal(t, "unknown", stuck[2].Age, "a start time ahead of the scan clock cannot prove the run is young")
}

// The threshold is applied twice on one run: the server decides whether to
// read the stored rows explaining it, and this command decides whether to
// render it. The two must reach the same verdict, so the runs are aged against
// the clock the server reported rather than the caller's, which is a round
// trip ahead of it. Aged locally, a run the server judged just short of the
// threshold would be rendered with an empty WAITING ON and REASON, and those
// columns mean "no stored row was blocking" — a finding the scan never made.
func TestScanObservedAtPrefersTheServersClock(t *testing.T) {
	t.Parallel()

	observed := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, observed,
		scanObservedAt(&apitypes.ChecksScanResponse{ObservedAt: "2026-07-12T12:00:00Z"}).UTC())

	// A server reporting no clock is one that does not annotate either, so the
	// caller's own clock is all there is and the threshold still applies.
	before := operatorNow()
	fallback := scanObservedAt(&apitypes.ChecksScanResponse{})
	assert.False(t, fallback.Before(before), "an unreported clock falls back to the caller's own")

	// Trusting an unparseable one would age every run from the zero time and
	// sweep the whole fleet into the report.
	garbled := scanObservedAt(&apitypes.ChecksScanResponse{ObservedAt: "yesterday"})
	assert.False(t, garbled.Before(before), "an unparseable clock is not trusted")
}

// The report renders stuck Check Runs in their own section, telling the
// operator backfill does not act on them, and still prints it when no checks
// are missing at all.
func TestWriteChecksBackfillReportRendersStuckSection(t *testing.T) {
	report := &checksBackfillReport{
		Repos:      []string{"octo/repo"},
		CheckNames: []string{"SchemaBot (production)"},
		Scanned:    12,
		Last:       "1d",
		DryRun:     true,
		StuckAfter: "1h",
		Stuck: []checksStuckCheck{
			{
				Repo: "octo/repo", PR: 5, URL: "https://github.com/octo/repo/pull/5", Title: "wedged", HeadSHA: "sha5555555555555",
				CheckName: "SchemaBot (production)", CheckRunID: 50, Status: "in_progress", Age: "3h30m0s",
			},
		},
	}

	var out strings.Builder
	require.NoError(t, writeChecksBackfillReport(&out, report))

	rendered := out.String()
	assert.Contains(t, rendered, "Scanned 12 open PRs updated in the last 1d in octo/repo")
	assert.Contains(t, rendered, "Stuck Check Runs — uncompleted for over 1h")
	assert.Contains(t, rendered, "backfill does not act on existing Check Runs")
	assert.Contains(t, rendered, "https://github.com/octo/repo/pull/5")
	assert.Contains(t, rendered, "in_progress")
	assert.Contains(t, rendered, "3h30m0s")
	assert.Contains(t, rendered, "No missing SchemaBot Check Runs found.")
}

// A fleet-wide report summarizes the repo count instead of naming every
// repository, a plain missing-check PR plans a synthesize, and a held PR —
// one whose head also carries an uncompleted Check Run a started apply may
// own — is explicitly marked as not acted on.
func TestWriteChecksBackfillReportFleetHeadlineAndHeldPRs(t *testing.T) {
	report := &checksBackfillReport{
		Repos:      []string{"octo/a", "octo/b", "octo/c", "octo/d"},
		CheckNames: []string{"SchemaBot (production)"},
		Scanned:    40,
		DryRun:     true,
		StuckAfter: "1h",
		Actions: []checksBackfillAction{
			{Repo: "octo/b", PR: 7, URL: "https://github.com/octo/b/pull/7", Title: "missing", HeadSHA: "sha7", MissingNames: []string{"SchemaBot (production)"}},
			{Repo: "octo/c", PR: 9, URL: "https://github.com/octo/c/pull/9", Title: "missing but uncompleted sibling", HeadSHA: "sha9", MissingNames: []string{"SchemaBot (production)"}, Held: true},
		},
	}

	var out strings.Builder
	require.NoError(t, writeChecksBackfillReport(&out, report))

	rendered := out.String()
	assert.Contains(t, rendered, "Scanned 40 open PRs in 4 repositories")
	assert.Contains(t, rendered, checksBackfillDryRunStatus)
	assert.Contains(t, rendered, "held: an uncompleted Check Run sits on this head")
}

// Repositories whose Check Run publishing is turned off are skipped, not
// scanned, and the report names them so the operator knows the sweep left
// them out deliberately.
func TestWriteChecksBackfillReportNamesSkippedDisabledRepos(t *testing.T) {
	report := &checksBackfillReport{
		Repos:           []string{"octo/a", "octo/b"},
		SkippedDisabled: []string{"octo/off", "octo/quiet"},
		CheckNames:      []string{"SchemaBot (production)"},
		Scanned:         20,
		DryRun:          true,
		StuckAfter:      "1h",
	}

	var out strings.Builder
	require.NoError(t, writeChecksBackfillReport(&out, report))

	rendered := out.String()
	assert.Contains(t, rendered, "Scanned 20 open PRs in octo/a, octo/b")
	assert.Contains(t, rendered, "Skipped repositories with Check Runs disabled (enable_checks: false): octo/off, octo/quiet.")
	assert.Contains(t, rendered, "No missing SchemaBot Check Runs found.")
}

// When every declared repository has Check Run publishing turned off, a fleet
// sweep has nothing to scan; the report says so plainly instead of rendering
// an empty scan headline.
func TestWriteChecksBackfillReportAllReposDisabled(t *testing.T) {
	report := &checksBackfillReport{
		SkippedDisabled: []string{"octo/off", "octo/quiet"},
		DryRun:          true,
		StuckAfter:      "1h",
	}

	var out strings.Builder
	require.NoError(t, writeChecksBackfillReport(&out, report))

	rendered := out.String()
	assert.Contains(t, rendered, "All 2 repositories have Check Runs disabled (enable_checks: false); nothing to scan: octo/off, octo/quiet.")
	assert.NotContains(t, rendered, "Scanned")
}

// A single repository with Check Run publishing turned off — the shape a
// named-repo invocation produces — reads as a sentence about that repository,
// not a fleet summary.
func TestWriteChecksBackfillReportSingleRepoDisabled(t *testing.T) {
	report := &checksBackfillReport{
		SkippedDisabled: []string{"octo/off"},
		DryRun:          true,
		StuckAfter:      "1h",
	}

	var out strings.Builder
	require.NoError(t, writeChecksBackfillReport(&out, report))

	rendered := out.String()
	assert.Contains(t, rendered, "Repository octo/off has Check Runs disabled (enable_checks: false); nothing to scan.")
	assert.NotContains(t, rendered, "Scanned")
}

// Naming a repository whose Check Run publishing is turned off scans nothing:
// the server reports the repo as disabled and the CLI reports the skip with
// the reason. The repo appears only in the skip list — not in the scanned-repo
// scope — so the report never implies a scan that did not happen.
func TestChecksBackfillRunSkipsNamedDisabledRepo(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/checks/scan", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(currentServerScan(apitypes.ChecksScanResponse{Repo: "octo/off", ChecksDisabled: true})))
	}))
	t.Cleanup(server.Close)

	cmd := &ChecksBackfillCmd{Repo: "octo/off", DryRun: true, StuckAfter: "1h", JSON: true}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})
	require.NoError(t, runErr)

	var report checksBackfillReport
	require.NoError(t, json.Unmarshal([]byte(output), &report))
	assert.Empty(t, report.Repos, "a repo the server reported as disabled was not scanned and must not appear in the scanned-repo list")
	assert.Equal(t, []string{"octo/off"}, report.SkippedDisabled)
	assert.Zero(t, report.Scanned)
	assert.Empty(t, report.Actions)
}

// --stuck-after accepts spellings this CLI understands and the server does
// not, so the value goes out normalized. Forwarding "2d" as typed fails the
// server's own parse and takes the whole sweep down on its first page, while
// the report still shows the operator what they asked for.
func TestChecksBackfillNormalizesStuckAfterOnTheWire(t *testing.T) {
	var sent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req apitypes.ChecksScanRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		sent = req.StuckAfter
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(currentServerScan(apitypes.ChecksScanResponse{Repo: "octo/repo", Scanned: 1})))
	}))
	t.Cleanup(server.Close)

	cmd := &ChecksBackfillCmd{Repo: "octo/repo", DryRun: true, StuckAfter: "2d", JSON: true}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})
	require.NoError(t, runErr)

	_, err := time.ParseDuration(sent)
	require.NoError(t, err, "the server parses this with time.ParseDuration, which does not know %q", "2d")
	assert.Equal(t, (48 * time.Hour).String(), sent)

	var report checksBackfillReport
	require.NoError(t, json.Unmarshal([]byte(output), &report))
	assert.Equal(t, "2d", report.StuckAfter, "the report keeps the operator's own spelling")
}

// checksBackfillScanServer serves a single-page checks scan returning the
// given missing and stuck PRs, the shape a dry-run sweep consumes.
func checksBackfillScanServer(t *testing.T, missing []apitypes.MissingCheckPR, stuck []apitypes.StuckCheckPR) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/checks/scan", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(currentServerScan(apitypes.ChecksScanResponse{
			Repo:       "octo/repo",
			CheckNames: []string{"SchemaBot (production)"},
			Scanned:    5,
			Missing:    missing,
			Stuck:      stuck,
		})))
	}))
	t.Cleanup(server.Close)
	return server
}

// A dry-run scan that surfaces PRs needing attention — missing Check Runs
// and stuck Check Runs — exits nonzero with a one-line error stating the
// counts, after the full report has already been printed, so a scheduled
// sweep can alert on $? without parsing the output.
func TestChecksBackfillDryRunFindingsExitNonzero(t *testing.T) {
	server := checksBackfillScanServer(t,
		[]apitypes.MissingCheckPR{
			{Number: 7, URL: "https://github.com/octo/repo/pull/7", Title: "missing", HeadSHA: "sha7", MissingNames: []string{"SchemaBot (production)"}},
		},
		[]apitypes.StuckCheckPR{
			{
				Number: 9, URL: "https://github.com/octo/repo/pull/9", Title: "stuck", HeadSHA: "sha9",
				Checks: []apitypes.IncompleteCheckRun{{Name: "SchemaBot (production)", CheckRunID: 90, Status: "queued"}},
			},
		})

	cmd := &ChecksBackfillCmd{Repo: "octo/repo", DryRun: true, StuckAfter: "1h", JSON: true}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	require.Error(t, runErr)
	assert.EqualError(t, runErr, "checks backfill left 1 PR with missing Check Runs and 1 stuck Check Run needing attention")
	assert.ErrorIs(t, runErr, ErrSilent, "the report on stdout is the whole response, so main adds no JSON error after it")
	assert.Equal(t, 1, ExitCodeFor(runErr))

	var report checksBackfillReport
	require.NoError(t, json.Unmarshal([]byte(output), &report), "stdout is the report alone, still printed before the non-zero exit")
	require.Len(t, report.Actions, 1)
	assert.Equal(t, 7, report.Actions[0].PR)
	require.Len(t, report.Stuck, 1)
	assert.Equal(t, 9, report.Stuck[0].PR)
}

// A clean scan — nothing missing, nothing stuck — exits zero, so a scheduled
// sweep alerts only when an operator has something to do.
func TestChecksBackfillCleanScanExitsZero(t *testing.T) {
	server := checksBackfillScanServer(t, nil, nil)

	cmd := &ChecksBackfillCmd{Repo: "octo/repo", DryRun: true, StuckAfter: "1h", JSON: true}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	require.NoError(t, runErr)
	var report checksBackfillReport
	require.NoError(t, json.Unmarshal([]byte(output), &report))
	assert.Empty(t, report.Actions)
	assert.Empty(t, report.Stuck)
}

// The exit code means "nothing needs an operator after this run". In a
// dry-run every finding remains. After an act phase, a successfully
// recreated check is resolved and does not fail the run, while held PRs,
// failed recreations, and stuck Check Runs — which the backfill never acts
// on — still do.
func TestChecksBackfillAttentionExitError(t *testing.T) {
	assert.NoError(t, attentionExitError(&checksBackfillReport{DryRun: true}))

	err := attentionExitError(&checksBackfillReport{
		DryRun:  true,
		Actions: []checksBackfillAction{{PR: 1}, {PR: 2, Held: true}},
	})
	assert.EqualError(t, err, "checks backfill left 2 PRs with missing Check Runs (1 held) needing attention")

	err = attentionExitError(&checksBackfillReport{
		DryRun: true,
		Stuck:  []checksStuckCheck{{PR: 3}, {PR: 4}},
	})
	assert.EqualError(t, err, "checks backfill left 2 stuck Check Runs needing attention")

	assert.NoError(t, attentionExitError(&checksBackfillReport{
		Actions: []checksBackfillAction{{PR: 1, Outcome: "created 1 Check Run"}},
	}), "a recreated check is resolved, not left needing attention")

	err = attentionExitError(&checksBackfillReport{
		Actions: []checksBackfillAction{
			{PR: 1, Outcome: "created 1 Check Run"},
			{PR: 2, Held: true},
			{PR: 3, Error: "boom"},
		},
		Stuck: []checksStuckCheck{{PR: 4}},
	})
	assert.EqualError(t, err, "checks backfill left 1 held PR and 1 failed Check Run recreation and 1 stuck Check Run needing attention")
}

// The pacing decision: pause only when the budget snapshot is below the floor
// and a future reset exists to wait for. Missing snapshots, disabled pacing,
// healthy budgets, and already-past resets all proceed without waiting.
func TestRateLimitPauseDuration(t *testing.T) {
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	reset := now.Add(10 * time.Minute).Format(time.RFC3339)

	wait, pause := rateLimitPauseDuration(&apitypes.GitHubRateLimit{Remaining: 500, Limit: 5000, ResetAt: reset}, 20, now)
	require.True(t, pause, "10% remaining is below a 20% floor")
	assert.Equal(t, 11*time.Minute, wait, "waits out the reset plus a minute of slack")

	_, pause = rateLimitPauseDuration(&apitypes.GitHubRateLimit{Remaining: 2500, Limit: 5000, ResetAt: reset}, 20, now)
	assert.False(t, pause, "half the budget left is comfortably above the floor")

	_, pause = rateLimitPauseDuration(nil, 20, now)
	assert.False(t, pause, "no snapshot means nothing to pace against")

	_, pause = rateLimitPauseDuration(&apitypes.GitHubRateLimit{Remaining: 0, Limit: 5000, ResetAt: reset}, 0, now)
	assert.False(t, pause, "a zero floor disables pacing")

	_, pause = rateLimitPauseDuration(&apitypes.GitHubRateLimit{Remaining: 0, Limit: 5000, ResetAt: now.Add(-time.Minute).Format(time.RFC3339)}, 20, now)
	assert.False(t, pause, "a past reset means the next request sees a fresh budget")
}

// The stuck section carries what the server concluded about each run, so an
// operator sweeping a fleet can act on the report itself rather than opening
// every pull request in it. Rows that already resolved are left out of the
// reason cell, and so is the rollup: neither explains why the run is still
// sitting, and listing them alongside the real cause hides it.
func TestStuckChecksPastThresholdCarriesTheServerDisposition(t *testing.T) {
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	stuck := stuckChecksPastThreshold("octo/repo", []apitypes.StuckCheckPR{
		{
			Number: 5, URL: "https://github.com/octo/repo/pull/5", HeadSHA: "sha5",
			Checks: []apitypes.IncompleteCheckRun{
				{
					Name: "SchemaBot (production)", CheckRunID: 50, Status: "in_progress", StartedAt: "2026-07-12T08:30:00Z",
					WaitingOn: apitypes.WaitingOnOperator,
					StoredRows: []apitypes.InspectedCheck{
						{Database: checkstate.AggregateSentinel, Aggregate: true, Reason: checkstate.ReasonAggregateRollup, Blocking: true, SelfConverging: true},
						{Database: "resolved", Reason: checkstate.ReasonResolved, SelfConverging: true},
						{Database: "owed", Reason: checkstate.ReasonReconciliationOwed, Blocking: true},
						{Database: "also-owed", Reason: checkstate.ReasonReconciliationOwed, Blocking: true},
						{Database: "waiting", Reason: checkstate.ReasonApplyRunning, Blocking: true, SelfConverging: true},
					},
				},
			},
		},
	}, time.Hour, now)

	require.Len(t, stuck, 1)
	assert.Equal(t, apitypes.WaitingOnOperator, stuck[0].WaitingOn)
	assert.Equal(t, []string{checkstate.ReasonReconciliationOwed, checkstate.ReasonApplyRunning}, stuck[0].Reasons,
		"blocking reasons only, deduplicated, in the order the server reported them")
}

// An entry the server could not explain renders as unknown rather than as
// self-converging, because the absence of stored rows is not evidence that
// nothing needs a person.
func TestWriteChecksBackfillReportRendersStuckDisposition(t *testing.T) {
	report := &checksBackfillReport{
		Repos:      []string{"octo/repo"},
		CheckNames: []string{"SchemaBot (production)"},
		Scanned:    12,
		DryRun:     true,
		StuckAfter: "1h",
		Stuck: []checksStuckCheck{
			{
				Repo: "octo/repo", PR: 5, URL: "https://github.com/octo/repo/pull/5",
				CheckName: "SchemaBot (production)", Status: "in_progress", Age: "3h30m0s",
				WaitingOn: apitypes.WaitingOnOperator,
				Reasons:   []string{checkstate.ReasonReconciliationOwed},
			},
			{
				Repo: "octo/repo", PR: 6, URL: "https://github.com/octo/repo/pull/6",
				CheckName: "SchemaBot (production)", Status: "in_progress", Age: "2h0m0s",
			},
		},
	}

	var out strings.Builder
	require.NoError(t, writeChecksBackfillReport(&out, report))

	rendered := out.String()
	assert.Contains(t, rendered, "WAITING ON")
	assert.Contains(t, rendered, "REASON")
	assert.Contains(t, rendered, apitypes.WaitingOnOperator)
	assert.Contains(t, rendered, checkstate.ReasonReconciliationOwed)
	assert.Contains(t, rendered, "sq schemabot checks show <owner/repo> <pr>")

	// A bare substring check would pass on the "in_progress" and "2h0m0s"
	// cells that already carry a hyphen, so the two cells that matter are
	// read out of the row and compared whole.
	cells := renderedCells(lineContaining(t, rendered, "https://github.com/octo/repo/pull/6"))
	require.Len(t, cells, 6, "PR, check, status, age, waiting-on, reason")
	assert.Equal(t, "-", cells[4], "an unexplained entry renders as unknown, never as self-converging")
	assert.Equal(t, "-", cells[5], "no stored row means no reason to name")
}

// tabwriterCellGap matches the run of padding between two rendered cells. A
// cell's own text can hold single spaces, so only a longer run separates one
// cell from the next.
var tabwriterCellGap = regexp.MustCompile(`\s{2,}`)

// renderedCells splits one tabwriter row back into its cells, so a test can
// assert on the value of a column rather than on a substring that any column
// could satisfy.
func renderedCells(line string) []string {
	return tabwriterCellGap.Split(strings.TrimSpace(line), -1)
}

// lineContaining returns the single rendered line carrying needle, so a
// per-row assertion cannot accidentally be satisfied by a different row.
func lineContaining(t *testing.T, rendered, needle string) string {
	t.Helper()
	for line := range strings.SplitSeq(rendered, "\n") {
		if strings.Contains(line, needle) {
			return line
		}
	}
	require.Fail(t, "no rendered line contains "+needle)
	return ""
}

// checksBackfillFleetServer serves a fleet sweep: the declared repositories,
// then each repository's scan pages from scans, and a synthesize that recreates
// every requested PR's Check Run. A repository listed in scanErrors answers
// its scan with that server error instead. It records the repositories
// scanned, in order, and the PRs sent for synthesis.
type checksBackfillFleetServer struct {
	*httptest.Server

	mu          sync.Mutex
	scanned     []string
	synthesized map[string][]int
	// optedIn is false once any scan request omitted report_app_not_installed.
	optedIn bool
}

func (s *checksBackfillFleetServer) scannedRepos() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.scanned)
}

func (s *checksBackfillFleetServer) synthesizedPRs() map[string][]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]int, len(s.synthesized))
	for repo, prs := range s.synthesized {
		out[repo] = slices.Clone(prs)
	}
	return out
}

func (s *checksBackfillFleetServer) everyScanOptedIn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.optedIn
}

// singlePage serves each repository's scan as one page.
func singlePage(scans map[string]apitypes.ChecksScanResponse) map[string][]apitypes.ChecksScanResponse {
	pages := make(map[string][]apitypes.ChecksScanResponse, len(scans))
	for repo, scan := range scans {
		pages[repo] = []apitypes.ChecksScanResponse{scan}
	}
	return pages
}

func newChecksBackfillFleetServer(t *testing.T, declared []string, scans map[string][]apitypes.ChecksScanResponse, scanErrors map[string]string) *checksBackfillFleetServer {
	fleet := &checksBackfillFleetServer{synthesized: map[string][]int{}, optedIn: true}
	fleet.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/checks/repos":
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.ChecksReposResponse{Repos: declared}))
		case "/api/checks/scan":
			var req apitypes.ChecksScanRequest
			if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&req)) {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			fleet.mu.Lock()
			fleet.scanned = append(fleet.scanned, req.Repo)
			fleet.optedIn = fleet.optedIn && req.ReportAppNotInstalled
			fleet.mu.Unlock()
			if message, ok := scanErrors[req.Repo]; ok {
				w.WriteHeader(http.StatusInternalServerError)
				assert.NoError(t, json.NewEncoder(w).Encode(apitypes.ErrorResponse{Error: message}))
				return
			}
			page := max(req.Page, 1)
			pages := scans[req.Repo]
			if !assert.LessOrEqual(t, page, len(pages), "unexpected scan of %s page %d", req.Repo, page) {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			assert.NoError(t, json.NewEncoder(w).Encode(currentServerScan(pages[page-1])))
		case "/api/checks/synthesize":
			var req apitypes.ChecksSynthesizeRequest
			if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&req)) {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			fleet.mu.Lock()
			fleet.synthesized[req.Repo] = append(fleet.synthesized[req.Repo], req.PRs...)
			fleet.mu.Unlock()
			response := apitypes.ChecksSynthesizeResponse{Repo: req.Repo}
			for _, pr := range req.PRs {
				response.Results = append(response.Results, apitypes.ChecksSynthesizeResult{PR: pr, Outcome: "created 1 Check Run"})
			}
			assert.NoError(t, json.NewEncoder(w).Encode(response))
		default:
			t.Errorf("unexpected request path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(fleet.Close)
	return fleet
}

// fleetScansWithOneUninstalledRepo is a three-repository fleet whose middle
// repository is declared on the server but does not have the server's GitHub
// App installed yet, the state a repository is in while its onboarding PR is
// still open.
func fleetScansWithOneUninstalledRepo() ([]string, map[string]apitypes.ChecksScanResponse) {
	declared := []string{"octo/alpha", "octo/onboarding", "octo/zeta"}
	scans := map[string]apitypes.ChecksScanResponse{
		"octo/alpha": {
			Repo:       "octo/alpha",
			CheckNames: []string{"SchemaBot (staging)"},
			Scanned:    2,
			Missing: []apitypes.MissingCheckPR{
				{Number: 3, URL: "https://github.com/octo/alpha/pull/3", Title: "add index", HeadSHA: "sha3", MissingNames: []string{"SchemaBot (staging)"}},
			},
		},
		"octo/onboarding": {Repo: "octo/onboarding", AppNotInstalled: true, GitHubApp: "staging"},
		"octo/zeta": {
			Repo:       "octo/zeta",
			CheckNames: []string{"SchemaBot (staging)"},
			Scanned:    4,
		},
	}
	return declared, scans
}

// An --all-repos sweep during a GitHub outage reaches a repository the
// server's GitHub App is not installed on. The sweep skips it, scans every
// repository after it, and names the skip in the report. Because that
// repository was never scanned, the run still exits nonzero, after printing
// the full report.
func TestChecksBackfillAllReposSkipsRepoWithoutTheAppInstalled(t *testing.T) {
	declared, scans := fleetScansWithOneUninstalledRepo()
	server := newChecksBackfillFleetServer(t, declared, singlePage(scans), nil)

	cmd := &ChecksBackfillCmd{AllRepos: true, Environment: "staging", Last: "2h", DryRun: true, StuckAfter: "1h", JSON: true}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	assert.Equal(t, declared, server.scannedRepos(), "the repository after the skipped one is still scanned")
	assert.True(t, server.everyScanOptedIn(), "every scan tells the server this command handles the not-installed skip")
	require.Error(t, runErr)
	assert.EqualError(t, runErr, "checks backfill left 1 PR with missing Check Runs and 1 repository without the GitHub App installed needing attention")

	var report checksBackfillReport
	require.NoError(t, json.Unmarshal([]byte(output), &report), "the full report is printed before the exit-code error")
	assert.Equal(t, []string{"octo/alpha", "octo/zeta"}, report.Repos, "a skipped repository was not scanned and must not appear in the scanned-repo list")
	assert.Equal(t, []string{"octo/onboarding"}, report.SkippedAppNotInstalled)
	assert.Empty(t, report.SkippedDisabled)
	assert.Equal(t, 6, report.Scanned)
	require.Len(t, report.Actions, 1)
	assert.Equal(t, "octo/alpha", report.Actions[0].Repo)
	assert.Equal(t, 3, report.Actions[0].PR)
}

// The human report names the skipped repository and what to do about it on
// its own line, beside the scan headline for the repositories that were
// scanned.
func TestChecksBackfillAllReposReportsTheUninstalledRepoSkip(t *testing.T) {
	declared, scans := fleetScansWithOneUninstalledRepo()
	server := newChecksBackfillFleetServer(t, declared, singlePage(scans), nil)

	cmd := &ChecksBackfillCmd{AllRepos: true, Environment: "staging", DryRun: true, StuckAfter: "1h"}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	require.Error(t, runErr)
	assert.Contains(t, output, "Scanned 6 open PRs in octo/alpha, octo/zeta for SchemaBot (staging).")
	assert.Contains(t, output, "Skipped repositories the server's GitHub App is not installed on: octo/onboarding. Install the App on each, or remove it from the server's repos config.")
	assert.Contains(t, output, "https://github.com/octo/alpha/pull/3")
}

// A scan failure that is not GitHub confirming the App is not installed (here,
// a 502 from the installation lookup during a GitHub outage) leaves the
// repository unread rather than known to be empty. The sweep stops at it with
// the cause, never scans past it, and prints no report that could read as a
// clean sweep.
func TestChecksBackfillAllReposStopsWhenAScanFails(t *testing.T) {
	declared := []string{"octo/alpha", "octo/flaky", "octo/zeta"}
	scans := map[string]apitypes.ChecksScanResponse{
		"octo/alpha": {Repo: "octo/alpha", CheckNames: []string{"SchemaBot (staging)"}, Scanned: 2},
		"octo/zeta":  {Repo: "octo/zeta", CheckNames: []string{"SchemaBot (staging)"}, Scanned: 4},
	}
	scanErrors := map[string]string{
		"octo/flaky": `resolve GitHub App "staging" installation for octo/flaky: find installation for octo/flaky: GET https://api.github.com/repos/octo/flaky/installation: 502 Bad Gateway []`,
	}
	server := newChecksBackfillFleetServer(t, declared, singlePage(scans), scanErrors)

	cmd := &ChecksBackfillCmd{AllRepos: true, Environment: "staging", DryRun: true, StuckAfter: "1h", JSON: true}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	require.Error(t, runErr)
	assert.Contains(t, runErr.Error(), "scan octo/flaky open PRs for missing or stuck Check Runs")
	assert.Contains(t, runErr.Error(), "502 Bad Gateway")
	assert.Equal(t, []string{"octo/alpha", "octo/flaky"}, server.scannedRepos(), "the sweep stops at the failed repository")
	assert.Empty(t, output)
	assert.Equal(t, 1, ExitCodeFor(runErr), "a failed scan is a failure, not an interrupt")
}

// An operator presses Ctrl-C while a sweep is waiting on a scan. The command
// says the backfill was canceled and exits with the interrupt status, so a
// script can tell an interrupted sweep from one that failed or left findings.
func TestChecksBackfillCancelExitsInterrupted(t *testing.T) {
	hold := newHoldingHandler(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/checks/scan", r.URL.Path)
		// The server notices the client hanging up only once the body is read.
		_, err := io.Copy(io.Discard, r.Body)
		assert.NoError(t, err)
		hold.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	cmd := &ChecksBackfillCmd{Repo: "octo/repo", DryRun: true, StuckAfter: "1h", JSON: true}

	output, stderr, runErr := cancelBackfillOnceHeld(t, cmd, server, hold.inFlight)

	require.ErrorIs(t, runErr, ErrSilent)
	require.ErrorIs(t, runErr, context.Canceled)
	assert.Equal(t, ExitInterrupted, ExitCodeFor(runErr))
	assert.Equal(t, "checks backfill canceled\n", stderr)
	assert.Empty(t, output)
}

// cancelBackfillOnceHeld runs cmd against server, presses the operator's
// Ctrl-C once held reports a request waiting on the server, and returns what
// the command printed and the error it ended with.
func cancelBackfillOnceHeld(t *testing.T, cmd *ChecksBackfillCmd, server *httptest.Server, held <-chan struct{}) (stdout, stderr string, runErr error) {
	t.Helper()
	stderr = captureStderr(t, func() {
		stdout = captureStdout(func() {
			runErr = runBackfillUntilCanceled(t, cmd, server, held)
		})
	})
	return stdout, stderr, runErr
}

// runBackfillUntilCanceled runs cmd against server, presses the operator's
// Ctrl-C once held reports a request waiting on the server, and returns the
// error the command ended with.
func runBackfillUntilCanceled(t *testing.T, cmd *ChecksBackfillCmd, server *httptest.Server, held <-chan struct{}) error {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- cmd.Run(ctx, &Globals{Endpoint: server.URL}) }()
	awaitSignal(t, held, "a backfill request to reach the server")
	cancel()
	return awaitSignal(t, done, "the canceled backfill to end")
}

// cancelBackfillWithUnwritableStdout is cancelBackfillOnceHeld with a stdout
// that rejects every write. It returns what the command printed on stderr, the
// error it ended with, and the error stdout gives a writer.
func cancelBackfillWithUnwritableStdout(t *testing.T, cmd *ChecksBackfillCmd, server *httptest.Server, held <-chan struct{}) (stderr string, runErr, writeErr error) {
	t.Helper()
	// A file opened for reading only refuses writes.
	unwritable, err := os.Open(os.DevNull)
	require.NoError(t, err)
	defer utils.CloseAndLog(unwritable)
	_, writeErr = unwritable.WriteString("report")
	require.Error(t, writeErr, "stdout must reject writes for this test to mean anything")

	old := os.Stdout
	defer func() { os.Stdout = old }()
	os.Stdout = unwritable
	stderr = captureStderr(t, func() {
		runErr = runBackfillUntilCanceled(t, cmd, server, held)
	})
	return stderr, runErr, writeErr
}

// An operator presses Ctrl-C on a backfill whose stdout cannot take the
// report, such as a redirect to a full disk. The command still exits with the
// interrupt status, and stderr says, after the cancel line, that the report
// was not written, whether any Check Run was recreated, and what to run next.
func TestChecksBackfillCancelWithUnwritableStdoutSaysTheReportWasNotWritten(t *testing.T) {
	tests := []struct {
		name     string
		server   func(t *testing.T, hold *holdingHandler) *httptest.Server
		cmd      *ChecksBackfillCmd
		wantNote string
	}{
		{
			name:     "interrupted during the scan",
			server:   checksBackfillTwoPageScanServer,
			cmd:      &ChecksBackfillCmd{Repo: "octo/repo", StuckAfter: "1h", JSON: true},
			wantNote: "No Check Run was recreated. Run the command again for a full sweep.",
		},
		{
			name: "interrupted while recreating Check Runs",
			server: func(t *testing.T, hold *holdingHandler) *httptest.Server {
				server, _ := checksBackfillRecreateServer(t, checksSynthesizeBatchSize+1, hold, func(prs []int) bool { return prs[0] != 1 })
				return server
			},
			cmd:      &ChecksBackfillCmd{Repo: "octo/repo", StuckAfter: "1h", JSON: true},
			wantNote: "Some Check Runs may already be recreated. Run the command again to finish; it recreates only the Check Runs still missing.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hold := newHoldingHandler(t)

			stderr, runErr, writeErr := cancelBackfillWithUnwritableStdout(t, tt.cmd, tt.server(t, hold), hold.inFlight)

			require.ErrorIs(t, runErr, ErrSilent)
			require.ErrorIs(t, runErr, context.Canceled)
			assert.Equal(t, ExitInterrupted, ExitCodeFor(runErr))
			assert.Equal(t,
				"checks backfill canceled\n"+
					"The report could not be written to stdout, so it is missing or incomplete: "+writeErr.Error()+". "+tt.wantNote+"\n",
				stderr)
		})
	}
}

// An operator presses Ctrl-C while an --all-repos sweep is still asking the
// server which repositories it declares. Nothing has been scanned, so the
// command says the backfill was canceled, prints no report, and exits with
// the interrupt status.
func TestChecksBackfillCancelWhileListingReposExitsInterrupted(t *testing.T) {
	hold := newHoldingHandler(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/checks/repos", r.URL.Path)
		// The server notices the client hanging up only once the body is read.
		_, err := io.Copy(io.Discard, r.Body)
		assert.NoError(t, err)
		hold.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	cmd := &ChecksBackfillCmd{AllRepos: true, DryRun: true, StuckAfter: "1h", JSON: true}

	output, stderr, runErr := cancelBackfillOnceHeld(t, cmd, server, hold.inFlight)

	require.ErrorIs(t, runErr, ErrSilent)
	require.ErrorIs(t, runErr, context.Canceled)
	assert.Equal(t, ExitInterrupted, ExitCodeFor(runErr))
	assert.Equal(t, "checks backfill canceled\n", stderr)
	assert.Empty(t, output)
}

// checksBackfillRecreateServer scans prCount missing-check PRs in one page and
// answers every synthesize batch with "created 1 Check Run", except that a
// batch for which holdBatch returns true is held open on hold, and a PR in
// failingPRs is answered with the error "boom". It records the first PR of
// every synthesize batch it received.
func checksBackfillRecreateServer(t *testing.T, prCount int, hold *holdingHandler, holdBatch func(prs []int) bool, failingPRs ...int) (*httptest.Server, func() []int) {
	t.Helper()
	missing := make([]apitypes.MissingCheckPR, 0, prCount)
	for pr := 1; pr <= prCount; pr++ {
		missing = append(missing, apitypes.MissingCheckPR{
			Number:       pr,
			URL:          "https://github.com/octo/repo/pull/" + strconv.Itoa(pr),
			MissingNames: []string{"SchemaBot (production)"},
		})
	}
	var mu sync.Mutex
	var batchStarts []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The server notices the client hanging up only once the body is read.
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/checks/scan":
			assert.NoError(t, json.NewEncoder(w).Encode(currentServerScan(apitypes.ChecksScanResponse{
				Repo:       "octo/repo",
				CheckNames: []string{"SchemaBot (production)"},
				Scanned:    prCount,
				Missing:    missing,
			})))
		case "/api/checks/synthesize":
			var req apitypes.ChecksSynthesizeRequest
			assert.NoError(t, json.Unmarshal(body, &req))
			mu.Lock()
			batchStarts = append(batchStarts, req.PRs[0])
			mu.Unlock()
			if holdBatch(req.PRs) {
				hold.ServeHTTP(w, r)
				return
			}
			response := apitypes.ChecksSynthesizeResponse{Repo: req.Repo}
			for _, pr := range req.PRs {
				if slices.Contains(failingPRs, pr) {
					response.Results = append(response.Results, apitypes.ChecksSynthesizeResult{PR: pr, Error: "boom"})
					continue
				}
				response.Results = append(response.Results, apitypes.ChecksSynthesizeResult{PR: pr, Outcome: "created 1 Check Run"})
			}
			assert.NoError(t, json.NewEncoder(w).Encode(response))
		default:
			t.Errorf("unexpected request path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server, func() []int {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(batchStarts)
	}
}

// An operator presses Ctrl-C while a backfill is recreating Check Runs: the
// first batch of PRs has its outcomes, the second batch is waiting on the
// server, and the third was never sent. The command says the backfill was
// canceled and still prints the report, marked as interrupted. Every PR it
// heard back about keeps its outcome. The PRs in the batch the interrupt cut
// off read "not confirmed", since the server may have acted on that batch.
// The PR in the batch that was never sent reads "not attempted", so the
// operator can tell it apart from the ones that may already have a Check Run.
// The command then exits with the interrupt status.
func TestChecksBackfillCancelWhileRecreatingChecksPrintsPartialReport(t *testing.T) {
	hold := newHoldingHandler(t)
	const prCount = 2*checksSynthesizeBatchSize + 1
	server, batchStarts := checksBackfillRecreateServer(t, prCount, hold, func(prs []int) bool { return prs[0] != 1 })
	cmd := &ChecksBackfillCmd{Repo: "octo/repo", StuckAfter: "1h", JSON: true}

	output, stderr, runErr := cancelBackfillOnceHeld(t, cmd, server, hold.inFlight)

	require.ErrorIs(t, runErr, ErrSilent)
	require.ErrorIs(t, runErr, context.Canceled)
	assert.Equal(t, ExitInterrupted, ExitCodeFor(runErr))
	assert.Equal(t, "checks backfill canceled\n", stderr)
	assert.Equal(t, []int{1, checksSynthesizeBatchSize + 1}, batchStarts(), "the third batch is never sent")
	var report checksBackfillReport
	require.NoError(t, json.Unmarshal([]byte(output), &report))
	assert.True(t, report.Interrupted)
	require.Len(t, report.Actions, prCount)
	for _, action := range report.Actions[:checksSynthesizeBatchSize] {
		assert.Equal(t, "created 1 Check Run", action.Outcome, "PR %d was recreated before the interrupt", action.PR)
	}
	for _, action := range report.Actions[checksSynthesizeBatchSize : 2*checksSynthesizeBatchSize] {
		assert.Equal(t, "not confirmed: interrupted before the server answered", action.Outcome, "PR %d was sent but never answered", action.PR)
		assert.Empty(t, action.Error)
	}
	neverSent := report.Actions[2*checksSynthesizeBatchSize]
	assert.Equal(t, "not attempted: interrupted before this PR was sent", neverSent.Outcome, "PR %d was never sent", neverSent.PR)
	assert.Empty(t, neverSent.Error)
}

// The note under an interrupted report's headline explains only the
// interrupted outcomes the report shows: "not confirmed" for a PR the server
// may have acted on, "not attempted" for a PR that was never sent.
func TestChecksBackfillInterruptedRecreateNoteExplainsTheOutcomesShown(t *testing.T) {
	const finish = " Run the command again to finish; it recreates only the Check Runs still missing."
	tests := []struct {
		name     string
		outcomes []string
		wantNote string
	}{
		{
			name:     "a batch was in flight and later PRs were never sent",
			outcomes: []string{"created 1 Check Run", checksBackfillUnconfirmedStatus, checksBackfillNotAttemptedStatus},
			wantNote: `Interrupted while recreating Check Runs: a PR whose outcome is "not confirmed" may or may not have its Check Run yet, and a PR whose outcome is "not attempted" was never sent to the server.` + finish,
		},
		{
			name:     "the last batch was in flight",
			outcomes: []string{"created 1 Check Run", checksBackfillUnconfirmedStatus},
			wantNote: `Interrupted while recreating Check Runs: a PR whose outcome is "not confirmed" may or may not have its Check Run yet.` + finish,
		},
		{
			name:     "interrupted between batches",
			outcomes: []string{"created 1 Check Run", checksBackfillNotAttemptedStatus},
			wantNote: `Interrupted while recreating Check Runs: a PR whose outcome is "not attempted" was never sent to the server.` + finish,
		},
		{
			name:     "every PR was answered",
			outcomes: []string{"created 1 Check Run"},
			wantNote: "Interrupted while recreating Check Runs." + finish,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := &checksBackfillReport{Repos: []string{"octo/repo"}, Scanned: len(tt.outcomes), Interrupted: true}
			for i, outcome := range tt.outcomes {
				report.Actions = append(report.Actions, checksBackfillAction{
					Repo: "octo/repo", PR: i + 1, URL: "https://github.com/octo/repo/pull/" + strconv.Itoa(i+1),
					MissingNames: []string{"SchemaBot (production)"}, Outcome: outcome,
				})
			}

			var out strings.Builder
			require.NoError(t, writeChecksBackfillReport(&out, report))

			lines := strings.Split(out.String(), "\n")
			require.GreaterOrEqual(t, len(lines), 2)
			assert.Equal(t, tt.wantNote, lines[1])
		})
	}
}

// The same interrupt in the text report: the PRs the server answered show
// their outcome, the PR in the batch in flight reads "not confirmed", and a
// line under the headline says the run was interrupted and how to finish it.
func TestChecksBackfillCancelWhileRecreatingChecksMarksTheTextReport(t *testing.T) {
	hold := newHoldingHandler(t)
	const prCount = checksSynthesizeBatchSize + 1
	server, _ := checksBackfillRecreateServer(t, prCount, hold, func(prs []int) bool { return prs[0] != 1 })
	cmd := &ChecksBackfillCmd{Repo: "octo/repo", StuckAfter: "1h"}

	output, stderr, runErr := cancelBackfillOnceHeld(t, cmd, server, hold.inFlight)

	assert.Equal(t, ExitInterrupted, ExitCodeFor(runErr))
	assert.True(t, strings.HasSuffix(stderr, "checks backfill canceled\n"), stderr)
	lines := strings.Split(output, "\n")
	require.GreaterOrEqual(t, len(lines), 2)
	assert.Equal(t, "Scanned 11 open PRs in octo/repo for SchemaBot (production).", lines[0])
	assert.Equal(t, `Interrupted while recreating Check Runs: a PR whose outcome is "not confirmed" may or may not have its Check Run yet. Run the command again to finish; it recreates only the Check Runs still missing.`, lines[1])
	assert.Equal(t, []string{"https://github.com/octo/repo/pull/1", "SchemaBot (production)", "created 1 Check Run"},
		renderedCells(lineContaining(t, output, "/pull/1 ")))
	assert.Equal(t, []string{"https://github.com/octo/repo/pull/11", "SchemaBot (production)", checksBackfillUnconfirmedStatus},
		renderedCells(lineContaining(t, output, "/pull/11 ")))
}

// An operator presses Ctrl-C after the server has answered one batch in which
// a recreation failed. Stdout takes the whole text report, with the failed PR
// showing its error, so stderr carries the cancel line alone and does not say
// the report is missing. The command exits with the interrupt status.
func TestChecksBackfillCancelAfterAFailedRecreationPrintsTheTextReport(t *testing.T) {
	hold := newHoldingHandler(t)
	const prCount = checksSynthesizeBatchSize + 1
	server, _ := checksBackfillRecreateServer(t, prCount, hold, func(prs []int) bool { return prs[0] != 1 }, 2)
	cmd := &ChecksBackfillCmd{Repo: "octo/repo", StuckAfter: "1h"}

	output, stderr, runErr := cancelBackfillOnceHeld(t, cmd, server, hold.inFlight)

	require.ErrorIs(t, runErr, ErrSilent)
	assert.Equal(t, ExitInterrupted, ExitCodeFor(runErr))
	assert.True(t, strings.HasSuffix(stderr, "checks backfill canceled\n"), stderr)
	assert.NotContains(t, stderr, "could not be written")
	assert.Equal(t, `Interrupted while recreating Check Runs: a PR whose outcome is "not confirmed" may or may not have its Check Run yet. Run the command again to finish; it recreates only the Check Runs still missing.`, strings.Split(output, "\n")[1])
	assert.Equal(t, []string{"https://github.com/octo/repo/pull/1", "SchemaBot (production)", "created 1 Check Run"},
		renderedCells(lineContaining(t, output, "/pull/1 ")))
	assert.Equal(t, []string{"https://github.com/octo/repo/pull/2", "SchemaBot (production)", "failed: boom"},
		renderedCells(lineContaining(t, output, "/pull/2 ")))
	assert.Equal(t, []string{"https://github.com/octo/repo/pull/11", "SchemaBot (production)", checksBackfillUnconfirmedStatus},
		renderedCells(lineContaining(t, output, "/pull/11 ")))
}

// A backfill nobody interrupts has one recreation fail. The text report and
// the JSON report both show the failure, and both runs exit with the same
// error naming the failed recreation.
func TestChecksBackfillFailedRecreationExitsWithTheSameErrorInTextAndJSON(t *testing.T) {
	for name, jsonOutput := range map[string]bool{"text report": false, "JSON report": true} {
		t.Run(name, func(t *testing.T) {
			server, _ := checksBackfillRecreateServer(t, 2, newHoldingHandler(t), func([]int) bool { return false }, 2)
			cmd := &ChecksBackfillCmd{Repo: "octo/repo", StuckAfter: "1h", JSON: jsonOutput}

			var runErr error
			output := captureStdout(func() {
				runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
			})

			require.EqualError(t, runErr, "checks backfill left 1 failed Check Run recreation needing attention")
			assert.Equal(t, 1, ExitCodeFor(runErr))
			if jsonOutput {
				var report checksBackfillReport
				require.NoError(t, json.Unmarshal([]byte(output), &report))
				require.Len(t, report.Actions, 2)
				assert.Equal(t, "boom", report.Actions[1].Error)
				return
			}
			assert.Equal(t, []string{"https://github.com/octo/repo/pull/2", "SchemaBot (production)", "failed: boom"},
				renderedCells(lineContaining(t, output, "/pull/2 ")))
		})
	}
}

// A run nobody interrupts reports interrupted as false, always present so a
// consumer can rely on the key, and every PR keeps the outcome the server sent.
func TestChecksBackfillUninterruptedRunReportsNotInterrupted(t *testing.T) {
	const prCount = checksSynthesizeBatchSize + 1
	server, _ := checksBackfillRecreateServer(t, prCount, newHoldingHandler(t), func([]int) bool { return false })
	cmd := &ChecksBackfillCmd{Repo: "octo/repo", StuckAfter: "1h", JSON: true}

	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	require.NoError(t, runErr)
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(output), &raw))
	assert.JSONEq(t, "false", string(raw["interrupted"]))
	var report checksBackfillReport
	require.NoError(t, json.Unmarshal([]byte(output), &report))
	require.Len(t, report.Actions, prCount)
	for _, action := range report.Actions {
		assert.Equal(t, "created 1 Check Run", action.Outcome, "PR %d", action.PR)
	}
}

// checksBackfillTwoPageScanServer answers the first scan page with one
// missing-check PR and one stuck Check Run, and holds the second page open on
// hold. A synthesize request fails the test: an interrupted scan sends none.
func checksBackfillTwoPageScanServer(t *testing.T, hold *holdingHandler) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The server notices the client hanging up only once the body is read.
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		if r.URL.Path != "/api/checks/scan" {
			t.Errorf("unexpected request path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var req apitypes.ChecksScanRequest
		assert.NoError(t, json.Unmarshal(body, &req))
		if req.Page > 1 {
			hold.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(currentServerScan(apitypes.ChecksScanResponse{
			Repo:       "octo/repo",
			CheckNames: []string{"SchemaBot (production)"},
			Scanned:    100,
			NextPage:   2,
			Missing: []apitypes.MissingCheckPR{
				{Number: 7, URL: "https://github.com/octo/repo/pull/7", MissingNames: []string{"SchemaBot (production)"}},
			},
			Stuck: []apitypes.StuckCheckPR{{
				Number: 9, URL: "https://github.com/octo/repo/pull/9",
				Checks: []apitypes.IncompleteCheckRun{{Name: "SchemaBot (production)", CheckRunID: 90, Status: "queued"}},
			}},
		})))
	}))
	t.Cleanup(server.Close)
	return server
}

// An operator presses Ctrl-C while the scan waits on its second page. The
// first page's findings are kept: the command prints them, marked as
// interrupted, and recreates nothing, since the scan never finished. It then
// exits with the interrupt status.
func TestChecksBackfillCancelDuringScanPrintsPartialFindings(t *testing.T) {
	hold := newHoldingHandler(t)
	server := checksBackfillTwoPageScanServer(t, hold)
	cmd := &ChecksBackfillCmd{Repo: "octo/repo", StuckAfter: "1h", JSON: true}

	output, stderr, runErr := cancelBackfillOnceHeld(t, cmd, server, hold.inFlight)

	require.ErrorIs(t, runErr, ErrSilent)
	require.ErrorIs(t, runErr, context.Canceled)
	assert.Equal(t, ExitInterrupted, ExitCodeFor(runErr))
	assert.Equal(t, "checks backfill canceled\n", stderr)
	var report checksBackfillReport
	require.NoError(t, json.Unmarshal([]byte(output), &report), "the partial findings are printed")
	assert.True(t, report.Interrupted)
	assert.Equal(t, []string{"octo/repo"}, report.Repos)
	assert.Equal(t, 100, report.Scanned)
	require.Len(t, report.Actions, 1)
	assert.Equal(t, 7, report.Actions[0].PR)
	assert.Empty(t, report.Actions[0].Outcome, "nothing was sent for recreation")
	require.Len(t, report.Stuck, 1)
	assert.Equal(t, 9, report.Stuck[0].PR)
}

// The same interrupted scan in the text report: a line under the headline says
// the findings are partial and nothing was recreated, and the table lists what
// the backfill would do rather than an outcome.
func TestChecksBackfillCancelDuringScanMarksTheTextReport(t *testing.T) {
	hold := newHoldingHandler(t)
	server := checksBackfillTwoPageScanServer(t, hold)
	cmd := &ChecksBackfillCmd{Repo: "octo/repo", StuckAfter: "1h"}

	output, stderr, runErr := cancelBackfillOnceHeld(t, cmd, server, hold.inFlight)

	assert.Equal(t, ExitInterrupted, ExitCodeFor(runErr))
	assert.True(t, strings.HasSuffix(stderr, "checks backfill canceled\n"), stderr)
	lines := strings.Split(output, "\n")
	require.GreaterOrEqual(t, len(lines), 2)
	assert.Equal(t, "Scanned 100 open PRs in octo/repo for SchemaBot (production).", lines[0])
	assert.Equal(t, checksBackfillInterruptedScanNote, lines[1])
	assert.Contains(t, output, "https://github.com/octo/repo/pull/9")
	assert.Equal(t, []string{"PR", "MISSING CHECKS", "ACTION"}, renderedCells(lineContaining(t, output, "MISSING CHECKS")))
	assert.Equal(t, []string{"https://github.com/octo/repo/pull/7", "SchemaBot (production)", checksBackfillDryRunStatus},
		renderedCells(lineContaining(t, output, "/pull/7 ")))
}

// An operator presses Ctrl-C while an --all-repos sweep waits on the scan of
// its last repository. The report keeps the first repository's findings and
// the skip of the second, and names only the repository it read as scanned:
// the one the interrupt cut off was never read.
func TestChecksBackfillCancelDuringFleetScanReportsOnlyTheReposRead(t *testing.T) {
	hold := newHoldingHandler(t)
	declared, scans := fleetScansWithOneUninstalledRepo()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The server notices the client hanging up only once the body is read.
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/checks/repos":
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.ChecksReposResponse{Repos: declared}))
		case "/api/checks/scan":
			var req apitypes.ChecksScanRequest
			assert.NoError(t, json.Unmarshal(body, &req))
			if req.Repo == "octo/zeta" {
				hold.ServeHTTP(w, r)
				return
			}
			assert.NoError(t, json.NewEncoder(w).Encode(currentServerScan(scans[req.Repo])))
		default:
			t.Errorf("unexpected request path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	cmd := &ChecksBackfillCmd{AllRepos: true, StuckAfter: "1h", JSON: true}

	output, _, runErr := cancelBackfillOnceHeld(t, cmd, server, hold.inFlight)

	assert.Equal(t, ExitInterrupted, ExitCodeFor(runErr))
	var report checksBackfillReport
	require.NoError(t, json.Unmarshal([]byte(output), &report))
	assert.True(t, report.Interrupted)
	assert.Equal(t, []string{"octo/alpha"}, report.Repos)
	assert.Equal(t, []string{"octo/onboarding"}, report.SkippedAppNotInstalled)
	require.Len(t, report.Actions, 1)
	assert.Equal(t, 3, report.Actions[0].PR)
}

// Naming one repository the server's GitHub App is not installed on is the
// operator asking about that repository, so the command fails with what is
// wrong and how to fix it instead of reporting a skip.
func TestChecksBackfillNamedRepoWithoutTheAppInstalledFails(t *testing.T) {
	declared, scans := fleetScansWithOneUninstalledRepo()
	server := newChecksBackfillFleetServer(t, declared, singlePage(scans), nil)

	cmd := &ChecksBackfillCmd{Repo: "octo/onboarding", DryRun: true, StuckAfter: "1h", JSON: true}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	require.Error(t, runErr)
	assert.EqualError(t, runErr, `GitHub App "staging" is not installed on octo/onboarding, so SchemaBot cannot read or create its Check Runs; install the App on octo/onboarding, or remove octo/onboarding from the server's repos config`)
	assert.Equal(t, []string{"octo/onboarding"}, server.scannedRepos())
	assert.Empty(t, output)
}

// A sweep in which no declared repository has the App installed scanned
// nothing, and says so instead of rendering an empty scan headline.
func TestWriteChecksBackfillReportNoRepoHasTheAppInstalled(t *testing.T) {
	report := &checksBackfillReport{
		SkippedDisabled:        []string{"octo/off"},
		SkippedAppNotInstalled: []string{"octo/new", "octo/newer"},
		DryRun:                 true,
		StuckAfter:             "1h",
	}

	var out strings.Builder
	require.NoError(t, writeChecksBackfillReport(&out, report))

	assert.Equal(t, "No repositories scanned.\n"+
		"Skipped repositories with Check Runs disabled (enable_checks: false): octo/off.\n"+
		"Skipped repositories the server's GitHub App is not installed on: octo/new, octo/newer. Install the App on each, or remove it from the server's repos config.\n",
		out.String())
	assert.EqualError(t, attentionExitError(report), "checks backfill left 2 repositories without the GitHub App installed needing attention")
}

// A repository can stop being scannable while a sweep is paging through it:
// the App is uninstalled, or a config reload turns its Check Runs off. The
// skip then arrives after earlier pages already reported findings. Those
// findings belong to a repository the sweep no longer covers: they are
// dropped with the rest of the repository, never sent for synthesis, and the
// healthy repository after it is still repaired and still named in the
// report's check scope.
func TestChecksBackfillDropsTheFindingsOfARepoSkippedMidScan(t *testing.T) {
	cases := []struct {
		name string
		// skip is the page-2 answer for octo/alpha.
		skip                       apitypes.ChecksScanResponse
		wantErr                    string
		wantSkippedAppNotInstalled []string
		wantSkippedDisabled        []string
	}{
		{
			name:                       "GitHub App uninstalled",
			skip:                       apitypes.ChecksScanResponse{Repo: "octo/alpha", AppNotInstalled: true, GitHubApp: "staging"},
			wantErr:                    "checks backfill left 1 repository without the GitHub App installed needing attention",
			wantSkippedAppNotInstalled: []string{"octo/alpha"},
		},
		{
			name:                "Check Runs disabled",
			skip:                apitypes.ChecksScanResponse{Repo: "octo/alpha", ChecksDisabled: true},
			wantSkippedDisabled: []string{"octo/alpha"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			declared := []string{"octo/alpha", "octo/zeta"}
			scans := map[string][]apitypes.ChecksScanResponse{
				"octo/alpha": {
					{
						Repo:       "octo/alpha",
						CheckNames: []string{"SchemaBot (staging)"},
						Scanned:    2,
						NextPage:   2,
						Missing: []apitypes.MissingCheckPR{
							{Number: 3, URL: "https://github.com/octo/alpha/pull/3", Title: "add index", HeadSHA: "sha3", MissingNames: []string{"SchemaBot (staging)"}},
							{Number: 4, URL: "https://github.com/octo/alpha/pull/4", Title: "held", HeadSHA: "sha4", MissingNames: []string{"SchemaBot (staging)"}},
						},
						Stuck: []apitypes.StuckCheckPR{
							{
								Number: 4, URL: "https://github.com/octo/alpha/pull/4", Title: "held", HeadSHA: "sha4",
								Checks: []apitypes.IncompleteCheckRun{{Name: "SchemaBot (production)", CheckRunID: 40, Status: "queued"}},
							},
						},
					},
					tc.skip,
				},
				"octo/zeta": {
					{
						Repo:       "octo/zeta",
						CheckNames: []string{"SchemaBot (staging)"},
						Scanned:    4,
						Missing: []apitypes.MissingCheckPR{
							{Number: 8, URL: "https://github.com/octo/zeta/pull/8", Title: "add column", HeadSHA: "sha8", MissingNames: []string{"SchemaBot (staging)"}},
						},
					},
				},
			}
			server := newChecksBackfillFleetServer(t, declared, scans, nil)

			cmd := &ChecksBackfillCmd{AllRepos: true, Environment: "staging", StuckAfter: "1h", JSON: true}
			var runErr error
			output := captureStdout(func() {
				runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
			})

			assert.Equal(t, map[string][]int{"octo/zeta": {8}}, server.synthesizedPRs(), "only the healthy repository's PRs are sent for synthesis")
			if tc.wantErr == "" {
				require.NoError(t, runErr)
			} else {
				require.EqualError(t, runErr, tc.wantErr)
			}

			var report checksBackfillReport
			require.NoError(t, json.Unmarshal([]byte(output), &report))
			assert.Equal(t, []string{"octo/zeta"}, report.Repos)
			assert.Equal(t, tc.wantSkippedAppNotInstalled, report.SkippedAppNotInstalled)
			assert.Equal(t, tc.wantSkippedDisabled, report.SkippedDisabled)
			assert.Equal(t, []string{"SchemaBot (staging)"}, report.CheckNames, "a check name the skipped repository reported first is still recorded for the repository after it")
			assert.Equal(t, 4, report.Scanned, "the skipped repository's pages do not count as scanned")
			assert.Empty(t, report.Stuck, "the skipped repository's stuck runs are dropped with it")
			require.Len(t, report.Actions, 1)
			assert.Equal(t, "octo/zeta", report.Actions[0].Repo)
			assert.Equal(t, 8, report.Actions[0].PR)
			assert.Equal(t, "created 1 Check Run", report.Actions[0].Outcome)
		})
	}
}

// Flag combinations that cannot mean what the operator typed are refused
// before any request: --skip-repo outside a fleet sweep, --pr without one
// repository or beside --last, a nonpositive PR number, and --plan without
// naming the PRs to plan.
func TestChecksBackfillScopeValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		cmd  ChecksBackfillCmd
		want string
	}{
		{"skip-repo without all-repos", ChecksBackfillCmd{Repo: "octo/repo", SkipRepo: []string{"octo/big"}}, "--skip-repo only applies with --all-repos"},
		{"pr with all-repos", ChecksBackfillCmd{AllRepos: true, PR: []int{7}}, "--pr needs a repository argument; PR numbers belong to one repository"},
		{"pr with last", ChecksBackfillCmd{Repo: "octo/repo", PR: []int{7}, Last: "2h"}, "--pr and --last cannot be used together; --pr already names the PRs to scan"},
		{"nonpositive pr", ChecksBackfillCmd{Repo: "octo/repo", PR: []int{7, 0}}, "--pr numbers must be positive; got 0"},
		{"plan without pr", ChecksBackfillCmd{Repo: "octo/repo", Plan: true}, "--plan requires --pr: each plan reads every changed database's schema once per environment, so name the PRs to plan"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.EqualError(t, tc.cmd.validateScope(), tc.want)
		})
	}
	valid := ChecksBackfillCmd{Repo: "octo/repo", PR: []int{7, 9}, Plan: true}
	assert.NoError(t, valid.validateScope())
}

// An operator sweeps the fleet after an outage but has already backfilled
// the largest repository by hand, so they leave it out with --skip-repo. The
// sweep never scans it, matches the name without regard to case, and the
// report says it was left out on purpose.
func TestChecksBackfillAllReposLeavesOutSkippedRepos(t *testing.T) {
	declared := []string{"octo/alpha", "octo/big", "octo/zeta"}
	scans := map[string]apitypes.ChecksScanResponse{
		"octo/alpha": {Repo: "octo/alpha", CheckNames: []string{"SchemaBot (staging)"}, Scanned: 1},
		"octo/zeta":  {Repo: "octo/zeta", CheckNames: []string{"SchemaBot (staging)"}, Scanned: 2},
	}
	server := newChecksBackfillFleetServer(t, declared, singlePage(scans), nil)

	cmd := &ChecksBackfillCmd{AllRepos: true, SkipRepo: []string{"Octo/Big"}, Environment: "staging", DryRun: true, StuckAfter: "1h"}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	require.NoError(t, runErr)
	assert.Equal(t, []string{"octo/alpha", "octo/zeta"}, server.scannedRepos())
	assert.Contains(t, output, "Scanned 3 open PRs in octo/alpha, octo/zeta for SchemaBot (staging).")
	assert.Contains(t, output, "Skipped repositories named by --skip-repo: Octo/Big.")
}

// A --skip-repo name the server does not declare is most likely a typo for
// the repository the operator meant to leave out, so the sweep stops before
// scanning anything rather than sweep that repository anyway.
func TestChecksBackfillSkipRepoTypoStopsTheSweep(t *testing.T) {
	declared := []string{"octo/alpha", "octo/big"}
	server := newChecksBackfillFleetServer(t, declared, nil, nil)

	cmd := &ChecksBackfillCmd{AllRepos: true, SkipRepo: []string{"octo/bgi"}, DryRun: true, StuckAfter: "1h"}
	var runErr error
	captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	require.Error(t, runErr)
	assert.Contains(t, runErr.Error(), "--skip-repo names octo/bgi, which the server does not declare")
	assert.Empty(t, server.scannedRepos())
}

// backfillServer is a single-repository checks server that records every
// scan and synthesize request and answers each synthesize from synthesize.
type backfillServer struct {
	*httptest.Server

	mu          sync.Mutex
	scans       []apitypes.ChecksScanRequest
	synthesizes []apitypes.ChecksSynthesizeRequest
	// supports, when set, replaces the behaviors the scan reports, to stand
	// in for an older server.
	supports *apitypes.ChecksBackfillSupport
	// queue is the repository's merge groups missing a check, and
	// queueScanned how many groups the merge queue holds in all. The scan
	// reports them when the request asks for the merge queue.
	queue        []apitypes.MissingMergeQueueCheck
	queueScanned int
	// queueError, when set, is the error the scan reports for a merge queue
	// it failed to read.
	queueError string
	// failMergeQueueSynthesize, when set, fails every synthesize request
	// that posts merge queue checks with a 502.
	failMergeQueueSynthesize bool
}

func (s *backfillServer) scanRequests() []apitypes.ChecksScanRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.scans)
}

func (s *backfillServer) synthesizeRequests() []apitypes.ChecksSynthesizeRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.synthesizes)
}

func newBackfillServer(t *testing.T, missing []int, synthesize func(call int, req apitypes.ChecksSynthesizeRequest) apitypes.ChecksSynthesizeResponse) *backfillServer {
	t.Helper()
	server := &backfillServer{}
	server.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/checks/scan":
			var req apitypes.ChecksScanRequest
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			server.mu.Lock()
			server.scans = append(server.scans, req)
			supports := server.supports
			queue, queueScanned, queueError := server.queue, server.queueScanned, server.queueError
			server.mu.Unlock()
			response := apitypes.ChecksScanResponse{Repo: "octo/repo", CheckNames: []string{"SchemaBot (production)"}, Scanned: len(missing)}
			if req.MergeQueue && (supports == nil || supports.MergeQueue) {
				response.MergeQueueScanned = queueScanned
				response.MergeQueue = queue
				response.MergeQueueError = queueError
			}
			for _, pr := range missing {
				response.Missing = append(response.Missing, apitypes.MissingCheckPR{
					Number:       pr,
					URL:          "https://github.com/octo/repo/pull/" + strconv.Itoa(pr),
					HeadSHA:      "sha" + strconv.Itoa(pr),
					MissingNames: []string{"SchemaBot (production)"},
				})
			}
			response = currentServerScan(response)
			if supports != nil {
				response.Supports = *supports
			}
			assert.NoError(t, json.NewEncoder(w).Encode(response))
		case "/api/checks/synthesize":
			var req apitypes.ChecksSynthesizeRequest
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			server.mu.Lock()
			server.synthesizes = append(server.synthesizes, req)
			call := len(server.synthesizes)
			failMergeQueue := server.failMergeQueueSynthesize
			server.mu.Unlock()
			if failMergeQueue && len(req.MergeQueueHeads) > 0 {
				w.WriteHeader(http.StatusBadGateway)
				assert.NoError(t, json.NewEncoder(w).Encode(apitypes.ErrorResponse{Error: "upstream unavailable"}))
				return
			}
			assert.NoError(t, json.NewEncoder(w).Encode(synthesize(call, req)))
		default:
			t.Errorf("unexpected request path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

// createdForEach answers a synthesize by recreating every requested PR's
// Check Run.
func createdForEach(_ int, req apitypes.ChecksSynthesizeRequest) apitypes.ChecksSynthesizeResponse {
	response := apitypes.ChecksSynthesizeResponse{Repo: req.Repo}
	for _, pr := range req.PRs {
		response.Results = append(response.Results, apitypes.ChecksSynthesizeResult{PR: pr, Outcome: "created 1 Check Run"})
	}
	return response
}

// An operator plans two schema PRs a sweep deferred. The scan asks the server
// for exactly those PRs instead of paging the repository, the report keeps
// only them even if the server answers with more, and the synthesize asks
// for the plans.
func TestChecksBackfillNamedPRsScanAndPlanOnlyThosePRs(t *testing.T) {
	server := newBackfillServer(t, []int{7, 8, 9}, createdForEach)

	cmd := &ChecksBackfillCmd{Repo: "octo/repo", PR: []int{7, 9}, Plan: true, StuckAfter: "1h", JSON: true}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})
	require.NoError(t, runErr)

	scans := server.scanRequests()
	require.Len(t, scans, 1)
	assert.Equal(t, []int{7, 9}, scans[0].PRs)
	assert.Zero(t, scans[0].Page, "a named-PR scan does not page the repository")

	synthesizes := server.synthesizeRequests()
	require.Len(t, synthesizes, 1)
	assert.Equal(t, []int{7, 9}, synthesizes[0].PRs)
	assert.True(t, synthesizes[0].Plan)

	var report checksBackfillReport
	require.NoError(t, json.Unmarshal([]byte(output), &report))
	assert.True(t, report.Plan)
	require.Len(t, report.Actions, 2)
	assert.Equal(t, 7, report.Actions[0].PR)
	assert.Equal(t, 9, report.Actions[1].PR)
}

// More named PRs than one scan request takes go out in batches the server
// accepts, in the order the operator gave them.
func TestChecksBackfillNamedPRsScanInBatches(t *testing.T) {
	server := newBackfillServer(t, nil, createdForEach)
	prs := make([]int, 0, checksScanNamedBatchSize+2)
	for pr := 1; pr <= checksScanNamedBatchSize+2; pr++ {
		prs = append(prs, pr)
	}

	cmd := &ChecksBackfillCmd{Repo: "octo/repo", PR: prs, DryRun: true, StuckAfter: "1h", JSON: true}
	var runErr error
	captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})
	require.NoError(t, runErr)

	scans := server.scanRequests()
	require.Len(t, scans, 2)
	assert.Equal(t, prs[:checksScanNamedBatchSize], scans[0].PRs)
	assert.Equal(t, prs[checksScanNamedBatchSize:], scans[1].PRs)
}

// A sweep without --plan finds two PRs that change managed schema. The
// server defers both, so their checks are still missing and their merges
// stay blocked. The report says what each would plan, prints the command
// that plans them, and the run exits nonzero.
func TestChecksBackfillReportsDeferredSchemaPRs(t *testing.T) {
	server := newBackfillServer(t, []int{7, 9, 11}, func(_ int, req apitypes.ChecksSynthesizeRequest) apitypes.ChecksSynthesizeResponse {
		assert.False(t, req.Plan)
		return apitypes.ChecksSynthesizeResponse{Repo: req.Repo, Results: []apitypes.ChecksSynthesizeResult{
			{PR: 7, Outcome: "deferred: PR changes managed schema", Deferred: true, Databases: []string{"orders"}, ChangedFiles: 1},
			{PR: 9, Outcome: "deferred: PR changes managed schema", Deferred: true, Databases: []string{"a", "b", "c", "d", "e"}, ChangedFiles: 409},
			{PR: 11, Outcome: "created 1 Check Run"},
		}}
	})

	cmd := &ChecksBackfillCmd{Repo: "octo/repo", StuckAfter: "1h"}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	assert.EqualError(t, runErr, "checks backfill left 2 deferred schema PRs needing attention")
	assert.Equal(t, []string{"https://github.com/octo/repo/pull/7", "SchemaBot (production)", "deferred: changes schema in 1 database (orders); 1 changed file"},
		renderedCells(lineContaining(t, output, "/pull/7 ")))
	assert.Equal(t, []string{"https://github.com/octo/repo/pull/9", "SchemaBot (production)", "deferred: changes schema in 5 databases (a, b, c and 2 more); 409 changed files"},
		renderedCells(lineContaining(t, output, "/pull/9 ")))
	assert.Contains(t, output, "2 PRs change managed schema and were not planned, so their Check Runs are still missing and the merge stays blocked.")
	assert.Contains(t, output, "  schemabot checks backfill octo/repo --plan --pr 7,9\n")
}

// The server stops a batch when the GitHub budget falls below the floor and
// returns the PRs it did not start. The command waits for the budget reset
// and sends those PRs again, so every check is recreated in one run.
func TestChecksBackfillResendsPRsTheServerDidNotStart(t *testing.T) {
	low := &apitypes.GitHubRateLimit{Remaining: 100, Limit: 5000, ResetAt: time.Now().Add(10 * time.Minute).UTC().Format(time.RFC3339)}
	server := newBackfillServer(t, []int{1, 2, 3}, func(call int, req apitypes.ChecksSynthesizeRequest) apitypes.ChecksSynthesizeResponse {
		if call > 1 {
			return createdForEach(call, req)
		}
		return apitypes.ChecksSynthesizeResponse{Repo: req.Repo, RateLimit: low, Results: []apitypes.ChecksSynthesizeResult{
			{PR: 1, Outcome: "created 1 Check Run"},
			{PR: 2, Outcome: "not started: GitHub budget is below 20% (100 of 5000 requests left)", RateLimited: true},
			{PR: 3, Outcome: "not started: GitHub budget is below 20% (100 of 5000 requests left)", RateLimited: true},
		}}
	})

	var waited []*apitypes.GitHubRateLimit
	cmd := &ChecksBackfillCmd{
		Repo: "octo/repo", StuckAfter: "1h", RateLimitFloor: 20, JSON: true,
		waitForBudget: func(_ context.Context, rate *apitypes.GitHubRateLimit, floorPct int, _ func(string)) error {
			assert.Equal(t, 20, floorPct)
			waited = append(waited, rate)
			return nil
		},
	}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})
	require.NoError(t, runErr)

	synthesizes := server.synthesizeRequests()
	require.Len(t, synthesizes, 2)
	assert.Equal(t, []int{1, 2, 3}, synthesizes[0].PRs)
	assert.Equal(t, 20, synthesizes[0].RateLimitFloorPercent)
	assert.Equal(t, []int{2, 3}, synthesizes[1].PRs)
	require.NotEmpty(t, waited)
	assert.Equal(t, low, waited[0], "the command waits on the snapshot that stopped the batch")

	var report checksBackfillReport
	require.NoError(t, json.Unmarshal([]byte(output), &report))
	require.Len(t, report.Actions, 3)
	for _, action := range report.Actions {
		assert.Equal(t, "created 1 Check Run", action.Outcome, "PR %d", action.PR)
		assert.Empty(t, action.Error, "PR %d", action.PR)
	}
}

// When pacing is off the command has no reset to wait for, so a PR the
// server did not start is reported as a failure rather than resent forever.
func TestApplySynthesizeResultCountsANotStartedPRAsFailed(t *testing.T) {
	var action checksBackfillAction
	applySynthesizeResult(&action, apitypes.ChecksSynthesizeResult{PR: 2, Outcome: "not started: GitHub budget is below 20% (100 of 5000 requests left)", RateLimited: true})
	assert.Equal(t, "not started: GitHub budget is below 20% (100 of 5000 requests left)", action.Error)
}

// currentServerScan stamps a scan response with every backfill behavior this
// CLI relies on, as a server at the same version reports.
func currentServerScan(response apitypes.ChecksScanResponse) apitypes.ChecksScanResponse {
	response.Supports = apitypes.ChecksBackfillSupport{NamedPRs: true, DeferSchemaPRs: true, MergeQueue: true}
	return response
}

// A server that predates deferral would plan every schema PR and skip the
// budget floor between PRs, and one that predates named scans would page the
// repository instead of reading the named PRs. The command refuses both runs
// before it asks the server to write anything, and says what to do instead.
// A dry run writes nothing, so it still runs against such a server.
func TestChecksBackfillRefusesBehaviorsTheServerDoesNotConfirm(t *testing.T) {
	for name, tc := range map[string]struct {
		cmd      ChecksBackfillCmd
		supports apitypes.ChecksBackfillSupport
		want     string
	}{
		"recreate without deferral": {
			cmd:      ChecksBackfillCmd{Repo: "octo/repo", StuckAfter: "1h"},
			supports: apitypes.ChecksBackfillSupport{NamedPRs: true},
			want:     "the server does not defer PRs that change managed schema: it would plan every one and skip the --rate-limit-floor check between PRs, which can drain the GitHub budget live webhooks share. Upgrade the server, or run with --dry-run to report missing Check Runs without recreating them",
		},
		"named PRs": {
			cmd:      ChecksBackfillCmd{Repo: "octo/repo", PR: []int{7}, DryRun: true, StuckAfter: "1h"},
			supports: apitypes.ChecksBackfillSupport{DeferSchemaPRs: true},
			want:     "the server does not support --pr: it would scan the repository's open PRs page by page instead of the PRs you named. Upgrade the server, or run without --pr",
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := newBackfillServer(t, []int{7}, createdForEach)
			server.supports = &tc.supports

			var runErr error
			captureStdout(func() {
				runErr = tc.cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
			})
			require.EqualError(t, runErr, tc.want)
			assert.Empty(t, server.synthesizeRequests())
		})
	}

	server := newBackfillServer(t, []int{7}, createdForEach)
	server.supports = &apitypes.ChecksBackfillSupport{}
	cmd := &ChecksBackfillCmd{Repo: "octo/repo", DryRun: true, StuckAfter: "1h"}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})
	assert.EqualError(t, runErr, "checks backfill left 1 PR with missing Check Runs and 1 repository with an unchecked merge queue needing attention")
	assert.Contains(t, output, "https://github.com/octo/repo/pull/7")
}

// The printed command that plans the deferred PRs repeats the run's -e and
// --check-name filters, so the follow-up acts on the same missing checks. A
// check name with spaces is quoted so the command pastes as typed.
func TestChecksBackfillDeferredHintKeepsTheRunsFilters(t *testing.T) {
	server := newBackfillServer(t, []int{7}, func(_ int, req apitypes.ChecksSynthesizeRequest) apitypes.ChecksSynthesizeResponse {
		return apitypes.ChecksSynthesizeResponse{Repo: req.Repo, Results: []apitypes.ChecksSynthesizeResult{
			{PR: 7, Outcome: "deferred: PR changes managed schema", Deferred: true, Databases: []string{"orders"}, ChangedFiles: 1},
		}}
	})

	cmd := &ChecksBackfillCmd{Repo: "octo/repo", Environment: "production", CheckName: "SchemaBot (production)", StuckAfter: "1h"}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	assert.EqualError(t, runErr, "checks backfill left 1 deferred schema PR needing attention")
	assert.Contains(t, output, "  schemabot checks backfill octo/repo -e production --check-name 'SchemaBot (production)' --plan --pr 7\n")
}

func TestShellQuoteArg(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "production", shellQuoteArg("production"))
	assert.Equal(t, "'SchemaBot (production)'", shellQuoteArg("SchemaBot (production)"))
	assert.Equal(t, `'it'\''s'`, shellQuoteArg("it's"))
	assert.Equal(t, "''", shellQuoteArg(""))
}

// testMergeGroup is a merge group the queue built for pr, missing the
// production check on its head commit.
func testMergeGroup(pr int, head string) apitypes.MissingMergeQueueCheck {
	return apitypes.MissingMergeQueueCheck{
		Ref:          "gh-readonly-queue/main/pr-" + strconv.Itoa(pr) + "-" + head,
		BaseRef:      "main",
		PR:           pr,
		URL:          "https://github.com/octo/repo/pull/" + strconv.Itoa(pr),
		HeadSHA:      head,
		MissingNames: []string{"SchemaBot (production)"},
	}
}

// mergeQueueAnswers answers a synthesize that carries merge group heads with
// outcomes[head], and recreates every PR in one that does not.
func mergeQueueAnswers(outcomes map[string]apitypes.ChecksMergeQueueResult) func(int, apitypes.ChecksSynthesizeRequest) apitypes.ChecksSynthesizeResponse {
	return func(call int, req apitypes.ChecksSynthesizeRequest) apitypes.ChecksSynthesizeResponse {
		if len(req.MergeQueueHeads) == 0 {
			return createdForEach(call, req)
		}
		response := apitypes.ChecksSynthesizeResponse{Repo: req.Repo}
		for _, head := range req.MergeQueueHeads {
			result := outcomes[head]
			result.HeadSHA = head
			response.MergeQueue = append(response.MergeQueue, result)
		}
		return response
	}
}

// During an outage the merge queue stalls: a merge group missing its
// SchemaBot check blocks every PR queued behind it. A dry run of a repository
// sweep reads the queue along with the open PRs, lists the blocked groups
// above the PRs, and exits nonzero for them.
func TestChecksBackfillDryRunListsMergeGroupsMissingChecks(t *testing.T) {
	server := newBackfillServer(t, []int{7}, createdForEach)
	server.queue = []apitypes.MissingMergeQueueCheck{testMergeGroup(412, "mq412")}
	server.queueScanned = 2

	cmd := &ChecksBackfillCmd{Repo: "octo/repo", DryRun: true, StuckAfter: "1h"}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	assert.EqualError(t, runErr, "checks backfill left 1 merge group missing Check Runs and 1 PR with missing Check Runs needing attention")
	scans := server.scanRequests()
	require.Len(t, scans, 1)
	assert.True(t, scans[0].MergeQueue, "a repository sweep reads the merge queue")
	assert.Empty(t, server.synthesizeRequests())
	assert.Contains(t, output, "Scanned 1 open PRs and 2 queued merge groups in octo/repo for SchemaBot (production).\n")
	assert.Contains(t, output, "1 merge group is missing SchemaBot Check Runs, blocking the PRs queued behind it:\n")
	assert.Equal(t, []string{"https://github.com/octo/repo/pull/412", "main", "SchemaBot (production)", checksBackfillMergeQueueDryRunStatus},
		renderedCells(lineContaining(t, output, "/pull/412 ")))
	assert.Less(t, strings.Index(output, "/pull/412 "), strings.Index(output, "/pull/7 "), "the blocked merge queue is listed before the PRs")
}

// A merge queue request that fails does not stop the sweep. The PRs' own
// checks are still recreated, the merge groups in the failed request are
// reported as not confirmed, since the server may have posted some of them,
// and the run exits nonzero for them.
func TestChecksBackfillRecreatesPRChecksWhenAMergeQueueRequestFails(t *testing.T) {
	server := newBackfillServer(t, []int{7}, createdForEach)
	server.queue = []apitypes.MissingMergeQueueCheck{testMergeGroup(412, "mq412")}
	server.queueScanned = 1
	server.failMergeQueueSynthesize = true

	cmd := &ChecksBackfillCmd{Repo: "octo/repo", StuckAfter: "1h"}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	require.EqualError(t, runErr, "checks backfill left 1 merge group still missing Check Runs needing attention")
	synthesizes := server.synthesizeRequests()
	require.Len(t, synthesizes, 2)
	assert.Equal(t, []string{"mq412"}, synthesizes[0].MergeQueueHeads)
	assert.Equal(t, []int{7}, synthesizes[1].PRs, "the PR pass runs after the failed merge queue request")
	groupCells := renderedCells(lineContaining(t, output, "/pull/412 "))
	require.Len(t, groupCells, 4)
	assert.Contains(t, groupCells[3], "failed: request failed before the server answered, so the check may or may not have been posted")
	assert.Equal(t, []string{"https://github.com/octo/repo/pull/7", "SchemaBot (production)", "created 1 Check Run"},
		renderedCells(lineContaining(t, output, "/pull/7 ")))
}

// The backfill posts the merge queue checks before it recreates any PR's
// check, since each blocked group holds every PR behind it. A group the queue
// no longer holds needs nothing, so only a failed post leaves the run
// needing attention.
func TestChecksBackfillPostsMergeQueueChecksBeforePRs(t *testing.T) {
	server := newBackfillServer(t, []int{7}, mergeQueueAnswers(map[string]apitypes.ChecksMergeQueueResult{
		"mq412": {Outcome: "posted passing merge queue check"},
		"mq413": {Outcome: "skipped: no longer in the merge queue"},
		"mq414": {Outcome: "failed", Error: "create check run: 502 Bad Gateway"},
	}))
	server.queue = []apitypes.MissingMergeQueueCheck{testMergeGroup(412, "mq412"), testMergeGroup(413, "mq413"), testMergeGroup(414, "mq414")}
	server.queueScanned = 3

	cmd := &ChecksBackfillCmd{Repo: "octo/repo", Environment: "production", CheckName: "SchemaBot (production)", StuckAfter: "1h", RateLimitFloor: 20}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	assert.EqualError(t, runErr, "checks backfill left 1 merge group still missing Check Runs needing attention")
	synthesizes := server.synthesizeRequests()
	require.Len(t, synthesizes, 2)
	assert.Equal(t, []string{"mq412", "mq413", "mq414"}, synthesizes[0].MergeQueueHeads)
	assert.Empty(t, synthesizes[0].PRs)
	assert.Equal(t, 20, synthesizes[0].RateLimitFloorPercent)
	assert.Equal(t, "production", synthesizes[0].Environment, "merge queue posts stay within the run's environment")
	assert.Equal(t, "SchemaBot (production)", synthesizes[0].CheckName, "merge queue posts stay within the run's check name")
	assert.Equal(t, []int{7}, synthesizes[1].PRs)
	assert.Empty(t, synthesizes[1].MergeQueueHeads)

	assert.Equal(t, []string{"https://github.com/octo/repo/pull/412", "main", "SchemaBot (production)", "posted passing merge queue check"},
		renderedCells(lineContaining(t, output, "/pull/412 ")))
	assert.Equal(t, []string{"https://github.com/octo/repo/pull/413", "main", "SchemaBot (production)", "skipped: no longer in the merge queue"},
		renderedCells(lineContaining(t, output, "/pull/413 ")))
	assert.Equal(t, []string{"https://github.com/octo/repo/pull/414", "main", "SchemaBot (production)", "failed: create check run: 502 Bad Gateway"},
		renderedCells(lineContaining(t, output, "/pull/414 ")))
	assert.Equal(t, []string{"https://github.com/octo/repo/pull/7", "SchemaBot (production)", "created 1 Check Run"},
		renderedCells(lineContaining(t, output, "/pull/7 ")))
}

// The server stops posting merge queue checks when the GitHub budget falls
// below the floor. The command waits for the reset and sends the heads it
// did not start, so every blocked group is unblocked in one run.
func TestChecksBackfillResendsMergeGroupsTheServerDidNotStart(t *testing.T) {
	low := &apitypes.GitHubRateLimit{Remaining: 100, Limit: 5000, ResetAt: time.Now().Add(10 * time.Minute).UTC().Format(time.RFC3339)}
	server := newBackfillServer(t, nil, func(call int, req apitypes.ChecksSynthesizeRequest) apitypes.ChecksSynthesizeResponse {
		response := apitypes.ChecksSynthesizeResponse{Repo: req.Repo}
		for _, head := range req.MergeQueueHeads {
			result := apitypes.ChecksMergeQueueResult{HeadSHA: head, Outcome: "posted passing merge queue check"}
			if call == 1 && head == "mq413" {
				result = apitypes.ChecksMergeQueueResult{HeadSHA: head, Outcome: "not started: GitHub budget is below 20% (100 of 5000 requests left)", RateLimited: true}
				response.RateLimit = low
			}
			response.MergeQueue = append(response.MergeQueue, result)
		}
		return response
	})
	server.queue = []apitypes.MissingMergeQueueCheck{testMergeGroup(412, "mq412"), testMergeGroup(413, "mq413")}
	server.queueScanned = 2

	var waited []*apitypes.GitHubRateLimit
	cmd := &ChecksBackfillCmd{
		Repo: "octo/repo", StuckAfter: "1h", RateLimitFloor: 20, JSON: true,
		waitForBudget: func(_ context.Context, rate *apitypes.GitHubRateLimit, _ int, _ func(string)) error {
			waited = append(waited, rate)
			return nil
		},
	}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})
	require.NoError(t, runErr)

	synthesizes := server.synthesizeRequests()
	require.Len(t, synthesizes, 2)
	assert.Equal(t, []string{"mq412", "mq413"}, synthesizes[0].MergeQueueHeads)
	assert.Equal(t, []string{"mq413"}, synthesizes[1].MergeQueueHeads)
	require.NotEmpty(t, waited)
	assert.Equal(t, low, waited[0], "the command waits on the snapshot that stopped the batch")

	var report checksBackfillReport
	require.NoError(t, json.Unmarshal([]byte(output), &report))
	require.Len(t, report.MergeQueue, 2)
	for _, group := range report.MergeQueue {
		assert.Equal(t, "posted passing merge queue check", group.Outcome, "merge group %s", group.Ref)
		assert.Empty(t, group.Error, "merge group %s", group.Ref)
	}
}

// A scan of named PRs is the operator asking about those PRs only, so it
// leaves the merge queue out.
func TestChecksBackfillOfNamedPRsLeavesOutTheMergeQueue(t *testing.T) {
	server := newBackfillServer(t, []int{7}, createdForEach)
	server.queue = []apitypes.MissingMergeQueueCheck{testMergeGroup(412, "mq412")}

	cmd := &ChecksBackfillCmd{Repo: "octo/repo", PR: []int{7}, DryRun: true, StuckAfter: "1h"}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	assert.EqualError(t, runErr, "checks backfill left 1 PR with missing Check Runs needing attention")
	for _, scan := range server.scanRequests() {
		assert.False(t, scan.MergeQueue)
	}
	assert.NotContains(t, output, "merge group")
}

// A server that cannot read the merge queue still recreates the PRs' checks.
// Reading the queue only adds findings, so the sweep goes on, and the report
// says which queues went unread and what to do about it. The run exits
// nonzero, because an unread queue may be blocked.
func TestChecksBackfillSaysWhenTheServerCannotReadTheMergeQueue(t *testing.T) {
	server := newBackfillServer(t, []int{7}, createdForEach)
	server.supports = &apitypes.ChecksBackfillSupport{NamedPRs: true, DeferSchemaPRs: true}
	server.queue = []apitypes.MissingMergeQueueCheck{testMergeGroup(412, "mq412")}

	cmd := &ChecksBackfillCmd{Repo: "octo/repo", StuckAfter: "1h"}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	require.EqualError(t, runErr, "checks backfill left 1 repository with an unchecked merge queue needing attention")
	assert.Contains(t, output, "The server cannot read the merge queue, so queued merge groups in octo/repo were not checked. Upgrade the server to check them.\n")
	assert.Equal(t, []string{"https://github.com/octo/repo/pull/7", "SchemaBot (production)", "created 1 Check Run"},
		renderedCells(lineContaining(t, output, "/pull/7 ")))
}

// A merge queue the server failed to read does not stop the sweep: the PRs'
// checks are still recreated, the report names the repository and the error,
// and the run exits nonzero because the unread queue may be blocked.
func TestChecksBackfillReportsAMergeQueueTheServerFailedToRead(t *testing.T) {
	server := newBackfillServer(t, []int{7}, createdForEach)
	server.queueError = "scan merge queue of octo/repo for missing checks: 502 Bad Gateway"

	cmd := &ChecksBackfillCmd{Repo: "octo/repo", StuckAfter: "1h"}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	require.EqualError(t, runErr, "checks backfill left 1 repository with an unchecked merge queue needing attention")
	assert.Contains(t, output, "Could not read the merge queue of octo/repo, so its queued merge groups were not checked: scan merge queue of octo/repo for missing checks: 502 Bad Gateway. Run the backfill for octo/repo again to check them.\n")
	assert.Equal(t, []string{"https://github.com/octo/repo/pull/7", "SchemaBot (production)", "created 1 Check Run"},
		renderedCells(lineContaining(t, output, "/pull/7 ")))
}
