package commands

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// The classification core: a PR with a failed check-creating delivery in the
// crawl is redriveable (grouped under its App); everything else synthesizes.
// A crawl that stopped early marks the search incomplete so the report can
// say why some PRs synthesize instead of redriving.
func TestIndexRedriveableDeliveries(t *testing.T) {
	redriveable, complete := indexRedriveableDeliveries(&apitypes.WebhookRedriveResponse{
		Results: []apitypes.WebhookRedriveResult{
			{
				AppName:            "production",
				ReachedWindowStart: true,
				Selected: []apitypes.WebhookRedriveSelection{
					{ID: 11, PR: 5},
					{ID: 12, PR: 5},
					{ID: 13, PR: 9},
					{ID: 14, PR: 0},
				},
			},
		},
	})

	require.True(t, complete)
	require.Len(t, redriveable, 2)
	assert.Equal(t, map[string][]int64{"production": {11, 12}}, redriveable[5])
	assert.Equal(t, map[string][]int64{"production": {13}}, redriveable[9])

	_, complete = indexRedriveableDeliveries(&apitypes.WebhookRedriveResponse{
		Results: []apitypes.WebhookRedriveResult{
			{AppName: "production", NextCursor: "c1"},
		},
	})
	assert.False(t, complete, "a crawl stopped mid-window cannot prove which PRs are redriveable")

	_, complete = indexRedriveableDeliveries(&apitypes.WebhookRedriveResponse{
		Results: []apitypes.WebhookRedriveResult{
			{AppName: "production", HistoryExhausted: true},
		},
	})
	assert.True(t, complete, "exhausted history covered everything GitHub retains")
}

// A PR with retained failed deliveries in more than one App (for example
// after an App migration) keeps each App's delivery IDs under that App, so
// each is later redelivered with its own token rather than mixed.
func TestIndexRedriveableDeliveriesGroupsMultipleAppsPerPR(t *testing.T) {
	redriveable, _ := indexRedriveableDeliveries(&apitypes.WebhookRedriveResponse{
		Results: []apitypes.WebhookRedriveResult{
			{AppName: "old-app", ReachedWindowStart: true, Selected: []apitypes.WebhookRedriveSelection{{ID: 1, PR: 7}}},
			{AppName: "new-app", ReachedWindowStart: true, Selected: []apitypes.WebhookRedriveSelection{{ID: 2, PR: 7}}},
		},
	})

	assert.Equal(t, map[string][]int64{"old-app": {1}, "new-app": {2}}, redriveable[7])
}

// A PR title or server error containing tabs/newlines is neutralized so it
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
	stuck := stuckChecksPastThreshold([]apitypes.StuckCheckPR{
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
		Repo:                   "octo/repo",
		CheckNames:             []string{"SchemaBot (production)"},
		Scanned:                12,
		DeliverySearchComplete: true,
		DryRun:                 true,
		StuckAfter:             "1h",
		Stuck: []checksStuckCheck{
			{
				PR: 5, URL: "https://github.com/octo/repo/pull/5", Title: "wedged", HeadSHA: "sha5555555555555",
				CheckName: "SchemaBot (production)", CheckRunID: 50, Status: "in_progress", Age: "3h30m0s",
			},
		},
	}

	var out strings.Builder
	require.NoError(t, writeChecksBackfillReport(&out, report))

	rendered := out.String()
	assert.Contains(t, rendered, "Stuck Check Runs — uncompleted for over 1h")
	assert.Contains(t, rendered, "backfill does not act on existing Check Runs")
	assert.Contains(t, rendered, "https://github.com/octo/repo/pull/5")
	assert.Contains(t, rendered, "in_progress")
	assert.Contains(t, rendered, "3h30m0s")
	assert.Contains(t, rendered, "No missing SchemaBot Check Runs found.")
}
