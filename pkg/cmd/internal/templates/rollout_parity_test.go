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
}

// The CLI and the PR comment read a table rolled up across targets the same
// way: for one rollout, the percent beside the table's bar and its rows line
// are the same text on both, whether targets have completed, are copying, are
// queued with or without the plan's estimate, or are queued with rows already
// reported (UX-7).
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

var (
	cliTableLine  = regexp.MustCompile(`(?m)^\s*~ orders: \S+ (\S+)$`)
	prTableLine   = regexp.MustCompile("(?m)^\\*\\*`orders`\\*\\*: \\S+ (\\S+)$")
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
			eta: target.eta, estimatedRows: target.estimatedRows,
		}
	}
	return tableReading(t, renderRollout(t, targetRolloutData(rollout)), cliTableLine)
}

// prTableReading renders the targets through the PR comment and returns the
// percent beside the table's bar and its rows line.
func prTableReading(t *testing.T, targets []parityTarget) (percent, rows string) {
	t.Helper()
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
			}},
		}
	}
	out := webhooktemplates.RenderMultiDeploymentApplyComment(webhooktemplates.MultiDeploymentApplyData{
		Model: presentation.Derive(ops), ApplyID: "apply-7f3c", Environment: "production", Details: details,
	})
	return tableReading(t, out, prTableLine)
}

// The CLI and the PR comment draw the same checksum bar for one table: the bar
// fills only once every row is verified, and shows a first segment as soon as
// verification has begun, on both surfaces (UX-5).
func TestChecksumBarReadsTheSameInTheCLIAndThePRComment(t *testing.T) {
	cases := []struct {
		name           string
		checked, total int64
		filled         int
		percent        string
	}{
		{name: "in its final stretch", checked: 997_500, total: 1_000_000, filled: 19, percent: "99.75%"},
		{name: "just begun", checked: 4, total: 1_000, filled: 1, percent: "0.40%"},
		{name: "every row verified", checked: 1_000_000, total: 1_000_000, filled: 20, percent: "100.00%"},
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
				" 🔍 Checksumming to verify data (" + tc.percent + ")"
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
