package commands

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
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
