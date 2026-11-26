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
			rows:    "Rows: 3,420 / 4,000 · ETA: 2m",
		},
		{
			name:    "a queued target with no estimate",
			targets: []parityTarget{completed(990), copying(420), queued(0, nil)},
			percent: "47%",
			rows:    "Rows: 1,420 / 2,000 across 2 of 3 targets · ETA: ≥ 2m",
		},
		{
			name:    "a queued target that already reported its rows",
			targets: []parityTarget{completed(1000), copying(500), queued(9000, nil)},
			percent: "50%",
			rows:    "Rows: 1,500 / 2,000 across 2 of 3 targets · ETA: ≥ 2m",
		},
		{
			name:    "a queued target counted by the plan's estimate",
			targets: []parityTarget{copying(500), queued(0, new(int64(1000)))},
			percent: "25%",
			rows:    "Rows: 500 / ~2k · ETA: ≥ 2m",
		},
		{
			name:    "a throttled target",
			targets: []parityTarget{{opState: state.ApplyOperation.Running, status: state.Task.Running, copied: 420, total: 1000, eta: 120, throttled: true}, completed(990)},
			percent: "71.00%" + ui.ThrottledSuffix(true),
			rows:    "Rows: 1,420 / 2,000 · ETA: 2m",
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
	out := webhooktemplates.RenderMultiDeploymentApplyComment(webhooktemplates.MultiDeploymentApplyData{
		Model: presentation.Derive(ops), ApplyID: "apply-7f3c", Environment: "production", Details: details,
	})
	return tableReading(t, out, prTableLine)
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

// A rollout across six targets runs under a limit of two operations at once:
// two targets copy and four wait their turn. The CLI and the PR comment both
// state the limit beside the counts, so the queue reads as waiting on the
// limit rather than stalled.
func TestOperationsAtOnceReadsTheSameInTheCLIAndThePRComment(t *testing.T) {
	rollout := []rolloutTarget{copyingTarget(400), copyingTarget(300), queuedTarget(), queuedTarget(), queuedTarget(), queuedTarget()}

	data := targetRolloutData(rollout)
	data.MaxOperationsAtOnce = 2
	cli := renderRollout(t, data)

	ops := make([]presentation.Operation, len(data.Operations))
	details := make([]*webhooktemplates.ApplyStatusCommentData, len(data.Operations))
	for i, op := range data.Operations {
		ops[i] = presentation.Operation{Deployment: op.Deployment, Target: op.Target, State: op.State, Parallel: true, ContinueOnFailure: true}
		details[i] = &webhooktemplates.ApplyStatusCommentData{
			Database: strings.ReplaceAll(op.Target, "-", "_"), State: state.Apply.Running, ApplyID: "apply-7f3c", Environment: "production",
			Engine: storage.EngineSpirit,
		}
	}
	model := presentation.Derive(ops)
	model.OperationsAtOnce = 2
	pr := webhooktemplates.RenderMultiDeploymentApplyComment(webhooktemplates.MultiDeploymentApplyData{
		Model: model, ApplyID: "apply-7f3c", Environment: "production", Details: details,
	})

	assert.Contains(t, cli, "Targets:      2 running · 4 queued (2 at a time)")
	assert.Contains(t, pr, "Rolling out: 0 of 6 targets done, 2 running, 4 queued (2 at a time)")
}

// A rollout across six targets has not started any of them under a limit of
// two operations at once, so every target reads queued. The watch view says
// "Queued on all 6 targets" rather than counting one state twice, and still
// states the limit, so the queue reads as waiting on it rather than stalled.
func TestRolloutCoverageStatesHowManyRunAtOnce(t *testing.T) {
	ops := make([]presentation.Operation, 6)
	for i := range ops {
		ops[i] = presentation.Operation{
			Deployment: "production", Target: fmt.Sprintf("payments-%03d", i+1),
			State: state.ApplyOperation.Pending, Parallel: true, ContinueOnFailure: true,
		}
	}
	model := presentation.Derive(ops)
	model.OperationsAtOnce = 2

	assert.Equal(t, "all 6 targets (2 at a time)", RolloutCoverage(model, model.Groups()))

	model.OperationsAtOnce = 0
	assert.Equal(t, "all 6 targets", RolloutCoverage(model, model.Groups()), "a server that reports no limit leaves the coverage as it is")
}
