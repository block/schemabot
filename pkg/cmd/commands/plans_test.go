package commands

import (
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ui"
)

// The list-plans command lists stored plans by default and shows one plan's
// full content when a plan ID is given, mirroring how status takes an
// optional apply ID.
func TestPlansCmdParsesListFiltersAndPlanID(t *testing.T) {
	var cli struct {
		Plans PlansCmd `cmd:"" name:"list-plans"`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)

	_, err = parser.Parse([]string{"list-plans", "-e", "staging", "-d", "orders", "--repository", "org/repo", "--pr", "42", "--last", "24h", "-n", "50"})
	require.NoError(t, err)
	assert.Empty(t, cli.Plans.PlanIDArg)
	assert.Equal(t, "staging", cli.Plans.Environment)
	assert.Equal(t, "orders", cli.Plans.Database)
	assert.Equal(t, "org/repo", cli.Plans.Repository)
	assert.Equal(t, 42, cli.Plans.PR)
	assert.Equal(t, "24h", cli.Plans.Last)
	assert.Equal(t, 50, cli.Plans.Limit)

	_, err = parser.Parse([]string{"list-plans", "plan-1784327902264169990"})
	require.NoError(t, err)
	assert.Equal(t, "plan-1784327902264169990", cli.Plans.PlanIDArg)
}

// setHyperlinks pins the terminal hyperlink detection for the test, so
// assertions on linked or plain sources don't depend on where the test runs.
func setHyperlinks(t *testing.T, on bool) {
	t.Helper()
	prev := ui.Hyperlinks
	ui.Hyperlinks = on
	t.Cleanup(func() { ui.Hyperlinks = prev })
}

// A plan's source renders like the status list's: the full PR URL off a
// terminal, the repository alone without a PR, and "ad-hoc" without either.
func TestPlanSource(t *testing.T) {
	setHyperlinks(t, false)
	assert.Equal(t, "https://github.com/org/repo/pull/42", planSource(&apitypes.PlanSummaryResponse{Repository: "org/repo", PullRequest: 42}))
	assert.Equal(t, "org/repo", planSource(&apitypes.PlanSummaryResponse{Repository: "org/repo"}))
	assert.Equal(t, "ad-hoc", planSource(&apitypes.PlanSummaryResponse{}))
}

// On an interactive terminal the plan's PR provenance renders as the short
// "owner/repo#pr" hyperlinked to the PR, matching the status list's source
// column; sources without a PR are unaffected.
func TestPlanSourceHyperlinked(t *testing.T) {
	setHyperlinks(t, true)

	assert.Equal(t,
		"\x1b]8;;https://github.com/org/repo/pull/42\x1b\\org/repo#42\x1b]8;;\x1b\\",
		planSource(&apitypes.PlanSummaryResponse{Repository: "org/repo", PullRequest: 42}))
	assert.Equal(t, "org/repo", planSource(&apitypes.PlanSummaryResponse{Repository: "org/repo"}))
	assert.Equal(t, "ad-hoc", planSource(&apitypes.PlanSummaryResponse{}))
}

func TestPlanChangeSummary(t *testing.T) {
	tests := []struct {
		name    string
		summary *apitypes.PlanSummaryResponse
		want    string
	}{
		{
			name:    "no changes",
			summary: &apitypes.PlanSummaryResponse{},
			want:    "no changes",
		},
		{
			name:    "single change",
			summary: &apitypes.PlanSummaryResponse{ChangeCounts: map[string]int{"alter": 1}},
			want:    "1 change: 1 alter",
		},
		{
			name: "mixed operations in lifecycle order",
			summary: &apitypes.PlanSummaryResponse{
				ChangeCounts: map[string]int{"drop": 1, "create": 1, "alter": 2, "create_index": 1},
			},
			want: "5 changes: 1 create, 2 alter, 1 drop, 1 create_index",
		},
		{
			name: "unsafe and blocked flagged",
			summary: &apitypes.PlanSummaryResponse{
				ChangeCounts: map[string]int{"alter": 3},
				UnsafeCount:  1,
				BlockedCount: 2,
			},
			want: "3 changes: 3 alter · ⚠️ 1 unsafe · ⛔ 2 blocked",
		},
		{
			name:    "vschema-only plan is not a no-change plan",
			summary: &apitypes.PlanSummaryResponse{VSchemaChangeCount: 1},
			want:    "1 vschema",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, planChangeSummary(tc.summary))
		})
	}
}
