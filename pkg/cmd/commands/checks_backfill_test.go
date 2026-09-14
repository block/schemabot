package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

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
	before := webhookRedriveNow()
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
	assert.Contains(t, rendered, "synthesize via auto-plan")
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
		require.NoError(t, json.NewEncoder(w).Encode(apitypes.ChecksScanResponse{Repo: "octo/off", ChecksDisabled: true}))
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
		require.NoError(t, json.NewEncoder(w).Encode(apitypes.ChecksScanResponse{Repo: "octo/repo", Scanned: 1}))
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
		require.NoError(t, json.NewEncoder(w).Encode(apitypes.ChecksScanResponse{
			Repo:       "octo/repo",
			CheckNames: []string{"SchemaBot (production)"},
			Scanned:    5,
			Missing:    missing,
			Stuck:      stuck,
		}))
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

	var report checksBackfillReport
	require.NoError(t, json.Unmarshal([]byte(output), &report), "the full report is still printed before the exit-code error")
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
