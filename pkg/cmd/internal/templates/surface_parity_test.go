package templates

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
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

func tableReading(t *testing.T, out string, tableLine *regexp.Regexp) (percent, rows string) {
	t.Helper()
	line := tableLine.FindStringSubmatch(out)
	require.NotNil(t, line, "no table line with a bar:\n%s", out)
	row := rolledRowLine.FindStringSubmatch(out)
	require.NotNil(t, row, "no rows line:\n%s", out)
	return line[1], strings.TrimSpace(row[1])
}

// applyStateOfTask is the apply state a table in each task state is read
// under. Every task state must have one, so a new state is rendered through
// both surfaces as soon as it exists. While a whole apply waits for its
// deploy, the CLI shows a deploy prompt in place of the table list, so a table
// waiting for its deploy is read under an apply that lists its tables.
var applyStateOfTask = map[string]string{
	state.Task.Pending:           state.Apply.Running,
	state.Task.Running:           state.Apply.Running,
	state.Task.CatchingUp:        state.Apply.Running,
	state.Task.Checksumming:      state.Apply.Running,
	state.Task.PostChecksum:      state.Apply.Running,
	state.Task.WaitingForDeploy:  state.Apply.Running,
	state.Task.WaitingForCutover: state.Apply.WaitingForCutover,
	state.Task.Recovering:        state.Apply.Running,
	state.Task.CuttingOver:       state.Apply.CuttingOver,
	state.Task.RevertWindow:      state.Apply.RevertWindow,
	state.Task.Reverting:         state.Apply.Reverting,
	state.Task.Completed:         state.Apply.Completed,
	state.Task.Failed:            state.Apply.Failed,
	state.Task.FailedRetryable:   state.Apply.FailedRetryable,
	state.Task.Stopped:           state.Apply.Stopped,
	state.Task.Reverted:          state.Apply.Reverted,
	state.Task.Cancelled:         state.Apply.Cancelled,
}

// tableChange is the kind of change a table is under, which decides how a
// table with no row copy reads.
type tableChange struct {
	changeType string
	isInstant  bool
}

// tableRows is how far a table's row copy has got.
type tableRows struct {
	copied, total, eta int64
	percent            int
}

// The CLI and the PR comment read one table the same way: for every engine,
// every task state, every kind of change, and a row copy that has not begun,
// is part way, or is done, the table's line and its rows line are the same
// text on both surfaces (UX-7).
func TestTableReadsTheSameInTheCLIAndThePRComment(t *testing.T) {
	engines := []string{storage.EngineSpirit, storage.EnginePlanetScale, storage.EngineStrata, storage.EnginePostgres}
	changes := []tableChange{{"alter", false}, {"alter", true}, {"create", true}, {"drop", false}}
	rowSets := map[string]tableRows{
		"no rows":  {},
		"part way": {copied: 420, total: 1000, eta: 120, percent: 42},
		"all rows": {copied: 1000, total: 1000, percent: 100},
	}
	taskStates := reflect.ValueOf(state.Task)
	for _, field := range taskStates.Fields() {
		status := field.String()
		applyState, ok := applyStateOfTask[status]
		require.True(t, ok, "task state %q has no apply state to read it under", status)
		for _, engine := range engines {
			for _, change := range changes {
				for rowsName, rows := range rowSets {
					name := fmt.Sprintf("%s/%s/%s instant=%t/%s", status, engine, change.changeType, change.isInstant, rowsName)
					t.Run(name, func(t *testing.T) {
						cliLine, cliDetail := cliSingleTableReading(t, engine, applyState, status, change, rows)
						prLine, prDetail := prSingleTableReading(t, engine, applyState, status, change, rows)
						assert.Equal(t, cliLine, prLine, "table line")
						assert.Equal(t, cliDetail, prDetail, "rows lines")
					})
				}
			}
		}
	}
}

// A table SchemaBot is retrying names the same attempt on both surfaces: the
// one under way, counted from 1 (UX-7).
func TestRetryingTableNamesTheSameAttemptInTheCLIAndThePRComment(t *testing.T) {
	rows := tableRows{copied: 420, total: 1000, eta: 120, percent: 42}
	change := tableChange{changeType: "alter"}
	cliLine, _ := cliSingleTableReading(t, storage.EngineSpirit, state.Apply.FailedRetryable, state.Task.FailedRetryable, change, rows)
	prLine, _ := prSingleTableReading(t, storage.EngineSpirit, state.Apply.FailedRetryable, state.Task.FailedRetryable, change, rows)
	want := fmt.Sprintf("🔄 Interrupted — retrying automatically (attempt 3/%d)", storage.MaxRecoveryAttempts)
	assert.Contains(t, cliLine, want)
	assert.Equal(t, cliLine, prLine)
}

const priorAttempts = 2

var (
	cliSingleTableLine = regexp.MustCompile(`(?m)^\s*[~+-] orders: (.*)$`)
	prSingleTableLine  = regexp.MustCompile("(?m)^\\*\\*`orders`\\*\\*: (.*)$")
	tableRowsLine      = regexp.MustCompile(`(?m)^\s*[•-] (Rows.*)$`)
)

// cliSingleTableReading renders one table through the CLI and returns its
// line and its rows lines.
func cliSingleTableReading(t *testing.T, engine, applyState, status string, change tableChange, rows tableRows) (string, []string) {
	t.Helper()
	out := renderRollout(t, ProgressData{
		ApplyID: "apply-7f3c", Database: "orders", Environment: "production", Engine: engine, State: applyState,
		Tables: []TableProgress{{
			Namespace: "orders", TableName: "orders", ChangeType: change.changeType, IsInstant: change.isInstant,
			RetryAttempt: retryAttempt(applyState, priorAttempts), DDL: addRegion, Status: status,
			RowsCopied: rows.copied, RowsTotal: rows.total, ETASeconds: rows.eta, PercentComplete: rows.percent,
		}},
	})
	return singleTableReading(t, out, cliSingleTableLine)
}

// prSingleTableReading renders one table through the PR comment and returns
// its line and its rows lines.
func prSingleTableReading(t *testing.T, engine, applyState, status string, change tableChange, rows tableRows) (string, []string) {
	t.Helper()
	out := webhooktemplates.RenderApplyStatusComment(webhooktemplates.ApplyStatusCommentData{
		Database: "orders", State: applyState, ApplyID: "apply-7f3c", Environment: "production", Engine: engine, Attempt: priorAttempts,
		Tables: []webhooktemplates.TableProgressData{{
			TableName: "orders", ChangeType: change.changeType, IsInstant: change.isInstant, DDL: addRegion, Status: status,
			RowsCopied: rows.copied, RowsTotal: rows.total, ETASeconds: rows.eta, PercentComplete: rows.percent,
		}},
	})
	return singleTableReading(t, out, prSingleTableLine)
}

func singleTableReading(t *testing.T, out string, tableLine *regexp.Regexp) (string, []string) {
	t.Helper()
	line := tableLine.FindStringSubmatch(out)
	require.NotNil(t, line, "no line for the table:\n%s", out)
	var details []string
	for _, row := range tableRowsLine.FindAllStringSubmatch(out, -1) {
		details = append(details, strings.TrimSpace(row[1]))
	}
	return strings.TrimSpace(line[1]), details
}
