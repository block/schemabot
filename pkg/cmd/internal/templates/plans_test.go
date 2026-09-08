package templates

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ui"
)

// The plans list mirrors the status list's column order: the plan identifier
// leads, the change summary sits between the target and its age, and the
// linkable source renders last so an operator scanning either list finds the
// same shape.
func TestWritePlansListColumnOrder(t *testing.T) {
	output := captureStdout(t, func() {
		WritePlansList(PlansListData{
			Limit:    20,
			MaxLimit: 200,
			Plans: []PlanSummaryData{
				{
					PlanID:      "plan-1700000000000000002",
					Database:    "orders-db",
					Environment: "staging",
					Source:      "https://github.com/acme/shop/pull/412",
					CreatedAt:   time.Now().Add(-10 * time.Minute),
					Changes:     "1 create, 2 alter · ⚠️",
					UnsafeCount: 1,
				},
				{
					PlanID:      "plan-1700000000000000001",
					Database:    "users-db",
					Environment: "production",
					Source:      "ad-hoc",
					CreatedAt:   time.Now().Add(-2 * time.Hour),
					Changes:     "1 alter",
				},
			},
		})
	})

	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")
	require.GreaterOrEqual(t, len(lines), 4)
	header := lines[2]

	last := -1
	for _, col := range []string{"PLAN ID", "DATABASE", "ENV", "CHANGES", "CREATED", "SOURCE"} {
		idx := strings.Index(header, col)
		require.NotEqual(t, -1, idx, "header missing column %q: %q", col, header)
		assert.Greater(t, idx, last, "column %q out of order in header %q", col, header)
		last = idx
	}

	assert.True(t, strings.HasPrefix(strings.TrimLeft(lines[3], " "), "plan-1700000000000000002"),
		"row should lead with the plan identifier: %q", lines[3])
	assert.Contains(t, lines[3], "https://github.com/acme/shop/pull/412")
}

// setNow pins the clock FormatTimeAgo reads, so a rendered age and a later
// recomputation of it cannot straddle a minute boundary.
func setNow(t *testing.T, now time.Time) {
	t.Helper()
	prev := ui.NowFunc
	ui.NowFunc = func() time.Time { return now }
	t.Cleanup(func() { ui.NowFunc = prev })
}

// A safety marker in a change summary is explained by a legend under the
// table naming exactly the markers on display; a listing without markers
// prints no legend at all.
func TestWritePlansListLegendNamesOnlyPresentMarkers(t *testing.T) {
	now := time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC)
	setNow(t, now)
	plan := func(changes string, unsafe, blocked int) PlanSummaryData {
		return PlanSummaryData{
			PlanID:       "plan-1700000000000000001",
			Database:     "orders-db",
			Environment:  "staging",
			Source:       "ad-hoc",
			CreatedAt:    now.Add(-10 * time.Minute),
			Changes:      changes,
			UnsafeCount:  unsafe,
			BlockedCount: blocked,
		}
	}

	unsafeOnly := captureStdout(t, func() {
		WritePlansList(PlansListData{Limit: 20, MaxLimit: 200, Plans: []PlanSummaryData{plan("1 drop · ⚠️", 1, 0)}})
	})
	assert.Contains(t, unsafeOnly, "⚠️ unsafe change")
	assert.NotContains(t, unsafeOnly, "⛔ blocked change")

	both := captureStdout(t, func() {
		WritePlansList(PlansListData{Limit: 20, MaxLimit: 200, Plans: []PlanSummaryData{plan("1 drop · ⚠️ · ⛔ 2", 1, 2)}})
	})
	assert.Contains(t, both, "⚠️ unsafe change · ⛔ blocked change")

	clean := captureStdout(t, func() {
		WritePlansList(PlansListData{Limit: 20, MaxLimit: 200, Plans: []PlanSummaryData{plan("1 alter", 0, 0)}})
	})
	assert.NotContains(t, clean, "unsafe change")
	assert.NotContains(t, clean, "blocked change")
}

// The change summary can carry multi-byte unsafe/blocked markers, so the
// columns after it must align by visible width across rows with and without
// them.
func TestWritePlansListAlignsChangeSummaries(t *testing.T) {
	now := time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC)
	setNow(t, now)
	created := now.Add(-10 * time.Minute)
	output := captureStdout(t, func() {
		WritePlansList(PlansListData{
			Limit:    20,
			MaxLimit: 200,
			Plans: []PlanSummaryData{
				// The widest summary is plain ASCII, so the marker-bearing
				// rows below it have to be padded to reach the column width.
				// A marker row that is itself the widest needs no padding at
				// all and would align under any padding rule.
				{
					PlanID:      "plan-1700000000000000003",
					Database:    "orders-db",
					Environment: "staging",
					Source:      "ad-hoc",
					CreatedAt:   created,
					Changes:     "12 create, 34 alter, 5 drop",
				},
				{
					PlanID:       "plan-1700000000000000002",
					Database:     "orders-db",
					Environment:  "staging",
					Source:       "https://github.com/acme/shop/pull/412",
					CreatedAt:    created,
					Changes:      "3 alter · ⚠️ · ⛔ 2",
					UnsafeCount:  1,
					BlockedCount: 2,
				},
				{
					PlanID:      "plan-1700000000000000001",
					Database:    "orders-db",
					Environment: "staging",
					Source:      "ad-hoc",
					CreatedAt:   created,
					Changes:     "1 alter",
				},
			},
		})
	})

	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")
	require.GreaterOrEqual(t, len(lines), 6)
	createdText := ui.FormatTimeAgo(created)
	rows := lines[3:6]
	offsets := make([]int, 0, len(rows))
	for _, row := range rows {
		idx := strings.Index(row, createdText)
		require.NotEqual(t, -1, idx, "row missing CREATED value: %q", row)
		offsets = append(offsets, ui.VisibleWidth(row[:idx]))
	}
	for _, offset := range offsets[1:] {
		assert.Equal(t, offsets[0], offset,
			"CREATED column misaligned:\n%q", strings.Join(rows, "\n"))
	}
}
