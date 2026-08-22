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
					Changes:     "3 changes: 1 create, 2 alter · ⚠️ 1 unsafe",
				},
				{
					PlanID:      "plan-1700000000000000001",
					Database:    "users-db",
					Environment: "production",
					Source:      "ad-hoc",
					CreatedAt:   time.Now().Add(-2 * time.Hour),
					Changes:     "1 change: 1 alter",
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
				{
					PlanID:      "plan-1700000000000000002",
					Database:    "orders-db",
					Environment: "staging",
					Source:      "https://github.com/acme/shop/pull/412",
					CreatedAt:   created,
					Changes:     "3 changes: 3 alter · ⚠️ 1 unsafe",
				},
				{
					PlanID:      "plan-1700000000000000001",
					Database:    "orders-db",
					Environment: "staging",
					Source:      "ad-hoc",
					CreatedAt:   created,
					Changes:     "1 change: 1 alter",
				},
			},
		})
	})

	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")
	require.GreaterOrEqual(t, len(lines), 5)
	createdText := ui.FormatTimeAgo(created)
	withMarker := lines[3]
	plain := lines[4]
	withMarkerIdx := strings.Index(withMarker, createdText)
	plainIdx := strings.Index(plain, createdText)
	require.NotEqual(t, -1, withMarkerIdx, "row missing CREATED value: %q", withMarker)
	require.NotEqual(t, -1, plainIdx, "row missing CREATED value: %q", plain)
	assert.Equal(t,
		ui.VisibleWidth(withMarker[:withMarkerIdx]),
		ui.VisibleWidth(plain[:plainIdx]),
		"CREATED column misaligned:\n%q\n%q", withMarker, plain)
}
