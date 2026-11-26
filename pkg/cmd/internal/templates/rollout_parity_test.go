package templates

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/ui"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

// parityTarget is one target of a table rolled up across targets, described
// once so the CLI and the PR comment can each render it.
type parityTarget struct {
	opState       string
	status        string
	copied, total int64
	eta           int64
	estimatedRows *int64
	throttled     bool
}

// The CLI and the PR comment read a table rolled up across targets the same
// way: for one rollout, the percent beside the table's bar and its rows line
// are the same text on both, whether targets have completed, are copying, are
// queued with or without the plan's estimate, or are queued with rows already
// reported, and both mark the line when the throttler holds a target back
// (UX-7).
func TestRolledUpTableReadsTheSameInTheCLIAndThePRComment(t *testing.T) {
	completed := func(copied int64) parityTarget {
		return parityTarget{opState: state.ApplyOperation.Completed, status: state.Task.Completed, copied: copied, total: 1000}
	}
	copying := func(copied int64) parityTarget {
		return parityTarget{opState: state.ApplyOperation.Running, status: state.Task.Running, copied: copied, total: 1000, eta: 120}
	}
	queued := func(total int64, estimate *int64) parityTarget {
		return parityTarget{opState: state.ApplyOperation.Pending, status: state.Task.Pending, total: total, estimatedRows: estimate}
	}
	cases := []struct {
		name    string
		targets []parityTarget
		percent string
		rows    string
	}{
		{
			name:    "copying beside targets that completed short of their total",
			targets: []parityTarget{copying(420), completed(990), completed(990), completed(990)},
			percent: "85.50%",
			rows:    "Rows: 3,420 / 4,000 · ETA: 2m 0s",
		},
		{
			name:    "a queued target with no estimate",
			targets: []parityTarget{completed(990), copying(420), queued(0, nil)},
			percent: "47%",
			rows:    "Rows: 1,420 / 2,000 across 2 of 3 targets · ETA: ≥ 2m 0s",
		},
		{
			name:    "a queued target that already reported its rows",
			targets: []parityTarget{completed(1000), copying(500), queued(9000, nil)},
			percent: "50%",
			rows:    "Rows: 1,500 / 2,000 across 2 of 3 targets · ETA: ≥ 2m 0s",
		},
		{
			name:    "a queued target counted by the plan's estimate",
			targets: []parityTarget{copying(500), queued(0, new(int64(1000)))},
			percent: "25%",
			rows:    "Rows: 500 / ~2k · ETA: ≥ 2m 0s",
		},
		{
			name:    "a throttled target",
			targets: []parityTarget{{opState: state.ApplyOperation.Running, status: state.Task.Running, copied: 420, total: 1000, eta: 120, throttled: true}, completed(990)},
			percent: "71.00%" + ui.ThrottledSuffix(true),
			rows:    "Rows: 1,420 / 2,000 · ETA: 2m 0s",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cliPercent, cliRows := cliTableReading(t, tc.targets)
			prPercent, prRows := prTableReading(t, tc.targets)
			assert.Equal(t, tc.percent, cliPercent, "CLI percent")
			assert.Equal(t, tc.rows, cliRows, "CLI rows")
			assert.Equal(t, cliPercent, prPercent, "PR comment percent")
			assert.Equal(t, cliRows, prRows, "PR comment rows")
		})
	}
}

// The CLI and the PR comment roll a table's per-target statuses up the same
// way, failure first: `orders` failed on one target reads as failed on both
// surfaces however far another target has got, until a target is copying rows
// whose progress the table's bar shows instead. A table with every other target
// copying, queued or complete reads as its bar on both (UX-7). A table still
// reverting on one target reads as reverting on both, whichever target is
// listed first.
func TestRolledUpTableStatusReadsTheSameInTheCLIAndThePRComment(t *testing.T) {
	const bar = "🟦🟦🟦🟦⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜ 20%"
	cases := []struct {
		name     string
		targets  []parityTarget
		headline string
		// prHeadline is the PR comment's reading where its wording for the
		// same rolled-up state is not yet the CLI's; empty means it is.
		prHeadline string
	}{
		{
			name: "running on one target and failed on another, before any rows are known",
			targets: []parityTarget{
				{opState: state.ApplyOperation.Running, status: state.Task.Running},
				{opState: state.ApplyOperation.Failed, status: state.Task.Failed},
			},
			headline: "❌ Failed",
		},
		{
			name: "reverting on one target and failed on another",
			targets: []parityTarget{
				{opState: state.ApplyOperation.Reverting, status: state.Task.Reverting, copied: 1000, total: 1000},
				{opState: state.ApplyOperation.Failed, status: state.Task.Failed, copied: 100, total: 1000},
			},
			headline: "❌ Failed",
		},
		{
			name: "reverted on one target and still reverting on another",
			targets: []parityTarget{
				{opState: state.ApplyOperation.Reverted, status: state.Task.Reverted},
				{opState: state.ApplyOperation.Reverting, status: state.Task.Reverting},
			},
			headline: "🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨 ↩️ Reverting", prHeadline: "↩️ Reverting",
		},
		{
			name: "still reverting on one target and reverted on another",
			targets: []parityTarget{
				{opState: state.ApplyOperation.Reverting, status: state.Task.Reverting},
				{opState: state.ApplyOperation.Reverted, status: state.Task.Reverted},
			},
			headline: "🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨 ↩️ Reverting", prHeadline: "↩️ Reverting",
		},
		{
			name: "running on one target and stopped on another",
			targets: []parityTarget{
				{opState: state.ApplyOperation.Running, status: state.Task.Running},
				{opState: state.ApplyOperation.Stopped, status: state.Task.Stopped},
			},
			headline: "⏹️ Stopped (not started)", prHeadline: "⏹️ Stopped",
		},
		{
			name: "running on one target and cancelled on another",
			targets: []parityTarget{
				{opState: state.ApplyOperation.Running, status: state.Task.Running},
				{opState: state.ApplyOperation.Cancelled, status: state.Task.Cancelled},
			},
			headline: "🚫 Cancelled (not started)", prHeadline: "⊘ Cancelled (not started)",
		},
		{
			name: "running on one target and retrying on another",
			targets: []parityTarget{
				{opState: state.ApplyOperation.Running, status: state.Task.Running},
				{opState: state.ApplyOperation.Running, status: state.Task.FailedRetryable},
			},
			headline: "Retrying", prHeadline: "🔄 Interrupted — retrying automatically",
		},
		{
			name: "copying on one target and failed on another",
			targets: []parityTarget{
				{opState: state.ApplyOperation.Running, status: state.Task.Running, copied: 400, total: 1000},
				{opState: state.ApplyOperation.Failed, status: state.Task.Failed, copied: 100, total: 1000},
			},
			headline: bar,
		},
		{
			name: "copying on one target and complete on another",
			targets: []parityTarget{
				{opState: state.ApplyOperation.Running, status: state.Task.Running, copied: 400, total: 1000},
				{opState: state.ApplyOperation.Completed, status: state.Task.Completed, copied: 1000, total: 1000},
			},
			headline: "🟦🟦🟦🟦🟦🟦🟦🟦🟦🟦🟦🟦🟦🟦⬜⬜⬜⬜⬜⬜ 70.00%",
		},
		{
			name: "queued on one target and complete on another",
			targets: []parityTarget{
				{opState: state.ApplyOperation.Pending, status: state.Task.Pending},
				{opState: state.ApplyOperation.Completed, status: state.Task.Completed, copied: 1000, total: 1000},
			},
			headline: "🟦🟦🟦🟦🟦🟦🟦🟦🟦🟦⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜ 50%",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rollout := make([]rolloutTarget, len(tc.targets))
			for i, target := range tc.targets {
				rollout[i] = rolloutTarget{
					opState: target.opState, status: target.status, rowsCopied: target.copied, rowsTotal: target.total,
				}
			}
			cli := tableHeadline(t, renderRollout(t, targetRolloutData(rollout)), cliTableHeadline)
			pr := tableHeadline(t, renderPRRollout(tc.targets), prTableHeadline)
			wantPR := tc.headline
			if tc.prHeadline != "" {
				wantPR = tc.prHeadline
			}
			assert.Equal(t, tc.headline, cli, "CLI table line")
			assert.Equal(t, wantPR, pr, "PR comment table line")
		})
	}
}

var (
	cliTableHeadline = regexp.MustCompile(`(?m)^\s*~ orders: (.+)$`)
	prTableHeadline  = regexp.MustCompile("(?m)^\\*\\*`orders`\\*\\*: (.+)$")
)

// tableHeadline is what follows the table's name on its line: its bar and
// percent, or its state.
func tableHeadline(t *testing.T, out string, tableLine *regexp.Regexp) string {
	t.Helper()
	line := tableLine.FindStringSubmatch(out)
	require.NotNil(t, line, "no table line:\n%s", out)
	return strings.TrimSpace(line[1])
}

var (
	cliTableLine  = regexp.MustCompile(`(?m)^\s*~ orders: \S+ (\S+(?: \(throttled\))?)$`)
	prTableLine   = regexp.MustCompile("(?m)^\\*\\*`orders`\\*\\*: \\S+ (\\S+(?: \\(throttled\\))?)$")
	rolledRowLine = regexp.MustCompile(`(?m)^\s*[•-] (Rows: .*)$`)
)

// cliTableReading renders the targets through the CLI and returns the percent
// beside the table's bar and its rows line.
func cliTableReading(t *testing.T, targets []parityTarget) (percent, rows string) {
	t.Helper()
	rollout := make([]rolloutTarget, len(targets))
	for i, target := range targets {
		rollout[i] = rolloutTarget{
			opState: target.opState, status: target.status, rowsCopied: target.copied, rowsTotal: target.total,
			eta: target.eta, estimatedRows: target.estimatedRows, throttled: target.throttled,
		}
	}
	return tableReading(t, renderRollout(t, targetRolloutData(rollout)), cliTableLine)
}

// prTableReading renders the targets through the PR comment and returns the
// percent beside the table's bar and its rows line.
func prTableReading(t *testing.T, targets []parityTarget) (percent, rows string) {
	t.Helper()
	return tableReading(t, renderPRRollout(targets), prTableLine)
}

// renderPRRollout renders the targets as the PR comment of one rollout.
func renderPRRollout(targets []parityTarget) string {
	ops := make([]presentation.Operation, len(targets))
	details := make([]*webhooktemplates.ApplyStatusCommentData, len(targets))
	for i, target := range targets {
		name := fmt.Sprintf("payments-%03d", i+1)
		ops[i] = presentation.Operation{Deployment: "prod", Target: name, State: target.opState, Parallel: true, ContinueOnFailure: true}
		applyState := state.Apply.Running
		if target.status == state.Task.Pending {
			applyState = state.Apply.Pending
		}
		details[i] = &webhooktemplates.ApplyStatusCommentData{
			Database: strings.ReplaceAll(name, "-", "_"), State: applyState, ApplyID: "apply-7f3c", Environment: "production",
			Engine: storage.EngineSpirit,
			Tables: []webhooktemplates.TableProgressData{{
				TableName: "orders", DDL: addRegion, Status: target.status,
				RowsCopied: target.copied, RowsTotal: target.total, ETASeconds: target.eta, EstimatedRows: target.estimatedRows,
				Throttled: target.throttled,
			}},
		}
	}
	return webhooktemplates.RenderMultiDeploymentApplyComment(webhooktemplates.MultiDeploymentApplyData{
		Model: presentation.Derive(ops), ApplyID: "apply-7f3c", Environment: "production", Details: details,
	})
}

// The CLI and the PR comment draw the same checksum bar for one table: the bar
// fills only once every row is verified, and shows a first segment as soon as
// verification has begun, on both surfaces (UX-5). Before Spirit reports a
// total there is nothing to measure against, so both draw a full activity bar
// with an open-ended label.
func TestChecksumBarReadsTheSameInTheCLIAndThePRComment(t *testing.T) {
	cases := []struct {
		name           string
		checked, total int64
		filled         int
		// label follows "Checksumming to verify data" on both surfaces.
		label string
	}{
		{name: "in its final stretch", checked: 997_500, total: 1_000_000, filled: 19, label: " (99.75%)"},
		{name: "just begun", checked: 4, total: 1_000, filled: 1, label: " (0.40%)"},
		{name: "every row verified", checked: 1_000_000, total: 1_000_000, filled: 20, label: " (100.00%)"},
		// Rounding draws two segments here and truncation one.
		{name: "9.6 percent", checked: 96, total: 1_000, filled: 1, label: " (9.60%)"},
		{name: "past the estimated total", checked: 1_200_000, total: 1_000_000, filled: 20, label: " (100.00%)"},
		{name: "total not yet reported", checked: 0, total: 0, filled: 20, label: "..."},
		{name: "rows reported without a total", checked: 500, total: 0, filled: 20, label: "..."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cli := FormatTableProgress(TableProgress{
				TableName: "orders", ChangeType: "alter", Status: state.Task.Checksumming,
				ChecksumRowsChecked: tc.checked, ChecksumRowsTotal: tc.total,
			})
			pr := webhooktemplates.RenderApplyStatusComment(webhooktemplates.ApplyStatusCommentData{
				Database: "payments", State: state.Apply.Running, ApplyID: "apply-7f3c", Environment: "production",
				Engine: storage.EngineSpirit,
				Tables: []webhooktemplates.TableProgressData{{
					TableName: "orders", DDL: addRegion, Status: state.Task.Checksumming,
					ChecksumRowsChecked: tc.checked, ChecksumRowsTotal: tc.total,
				}},
			})
			want := strings.Repeat(ui.ColorBlue, tc.filled) + strings.Repeat(ui.ColorEmpty, 20-tc.filled) +
				" 🔍 Checksumming to verify data" + tc.label
			assert.Contains(t, cli, want, "CLI checksum bar")
			assert.Contains(t, pr, want, "PR comment checksum bar")
		})
	}
}

func tableReading(t *testing.T, out string, tableLine *regexp.Regexp) (percent, rows string) {
	t.Helper()
	line := tableLine.FindStringSubmatch(out)
	require.NotNil(t, line, "no table line with a bar:\n%s", out)
	row := rolledRowLine.FindStringSubmatch(out)
	require.NotNil(t, row, "no rows line:\n%s", out)
	return line[1], strings.TrimSpace(row[1])
}
