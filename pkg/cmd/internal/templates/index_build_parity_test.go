package templates

import (
	"fmt"
	"maps"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/ui"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

// postgresProgress is a PostgreSQL apply running one statement, as the
// progress API reports it. changeType is the statement's operation, and
// metadata is the engine's build work, if any.
func postgresProgress(ddl, changeType string, percent int32, metadata map[string]string) *apitypes.ProgressResponse {
	md := map[string]string{"step": "1", "steps_total": "1", "statement": ddl}
	maps.Copy(md, metadata)
	return &apitypes.ProgressResponse{
		ApplyID: "apply-a1b2c3d4e5f6", Database: "shop", Environment: "staging",
		State: state.Apply.Running, Engine: storage.EnginePostgres, DatabaseType: storage.DatabaseTypePostgres,
		Metadata: md,
		Tables: []*apitypes.TableProgressResponse{{
			TableName: "users", DDL: ddl, ChangeType: changeType, Status: state.Task.Running, PercentComplete: percent,
		}},
	}
}

// An operator following a PostgreSQL index build from the terminal and from
// the PR sees one bar with one percent: both surfaces draw the engine's
// whole-build percent, with the two decimals the engine publishes, from the
// same progress response. The current phase's counters
// sit where a row copy puts its rows, on a detail line under the table's
// statement, and carry no percent of their own, so neither surface shows a
// second percent.
func TestIndexBuildProgressReadsTheSameOnEverySurface(t *testing.T) {
	resp := postgresProgress("CREATE INDEX CONCURRENTLY idx_users_email ON users (email)", "alter", 25, map[string]string{
		"executor_operation": "concurrent-index-build", "server_phase": "building index: scanning table",
		"blocks_done": "30000", "blocks_total": "48000", "percent": "25.37",
	})
	want := ui.ProgressBarRowCopy(25) + " 25.37%"
	const counters = "Building index: 30,000 of 48,000 blocks"

	cli := renderRollout(t, ParseProgressResponse(resp))
	pr := webhooktemplates.RenderApplyStatusComment(webhooktemplates.ApplyStatusFromProgress(resp, "octocat"))

	assert.Contains(t, cli, "users: "+want+"\n")
	assert.Contains(t, pr, "**`users`**: "+want+"\n")
	assert.Regexp(t, `CREATE INDEX CONCURRENTLY idx_users_email ON users USING btree \(email\);?\n\s+• `+counters+"\n", cli,
		"CLI: the counters follow the statement as a detail line")
	assert.Regexp(t, "```\n- "+counters+"\n", pr, "PR comment: the counters follow the statement as a detail line")
	assert.Equal(t, 1, strings.Count(cli, counters), "CLI shows the counters once")
	assert.Equal(t, 1, strings.Count(pr, counters), "PR comment shows the counters once")
	assert.NotContains(t, cli, "62%", "the counters must not show a phase-local percent beside the bar")
	assert.NotContains(t, pr, "62%", "the counters must not show a phase-local percent beside the bar")
}

// A PostgreSQL statement that is not an index build has no percent to show,
// so neither surface draws a bar for it, and both name it the same way.
func TestPostgresPlainDDLDrawsNoBar(t *testing.T) {
	resp := postgresProgress("ALTER TABLE users ADD COLUMN nickname text", "alter", 0, nil)

	data := ParseProgressResponse(resp)
	require.Len(t, data.Tables, 1)
	assert.False(t, data.Tables[0].BuildWork.IsConcurrentIndexBuild())
	assert.Contains(t, FormatTableProgress(data.Tables[0]), "users: Running...\n")

	pr := webhooktemplates.RenderApplyStatusComment(webhooktemplates.ApplyStatusFromProgress(resp, "octocat"))
	assert.Contains(t, pr, "**`users`**: Running...\n")
}

// While a PostgreSQL step waits outside the lock queue for another session's
// conflicting lock to end, an operator following it from the terminal and
// from the PR sees the same reading: the session it waits on and how long,
// in place of a statement that only reads "Running...".
func TestLockHolderWaitReadsTheSameOnEverySurface(t *testing.T) {
	resp := postgresProgress("ALTER TABLE users ADD COLUMN nickname text", "alter", 0, map[string]string{
		"lock_holder_pid": "9433", "lock_holder_table": "public.users",
		"lock_holder_waited_ms": "7944", "lock_holder_limit_ms": "30000",
	})
	const label = "⏳ Waiting for session 9433 to release its lock (7s of 30s)"
	const note = "Reads and writes continue. The step runs once session 9433 commits or rolls back."

	cli := renderRollout(t, ParseProgressResponse(resp))
	pr := webhooktemplates.RenderApplyStatusComment(webhooktemplates.ApplyStatusFromProgress(resp, "octocat"))

	assert.Contains(t, cli, "users: "+label+"\n")
	assert.Contains(t, cli, note)
	assert.NotContains(t, cli, "Running...")
	assert.Contains(t, pr, "**`users`**: "+label+"\n")
	assert.Contains(t, pr, note)
	assert.NotContains(t, pr, "Running...")
}

// indexBuildTarget is one target of a concurrent index build rolled out
// across targets.
type indexBuildTarget struct {
	opState, status string
	percent         int
	// building marks a target whose operation runs the index build, as its
	// recorded build work says.
	building bool
	// throttled marks a target whose engine's throttler is holding the
	// build back.
	throttled bool
}

// An operator following a concurrent index build rolled out across targets
// sees one bar with one percent on both surfaces: the mean of each target's
// whole-build percent, with finished targets counted whole and queued ones as
// not started. A throttled target marks the line throttled on both. The apply
// carries no build work of its own here, so the CLI
// draws the bar only by reading each table's build work from the operation
// that runs it (UX-5).
func TestRolledUpIndexBuildReadsTheSameInTheCLIAndThePRComment(t *testing.T) {
	running := func(pct int) indexBuildTarget {
		return indexBuildTarget{opState: state.ApplyOperation.Running, status: state.Task.Running, percent: pct, building: true}
	}
	completed := indexBuildTarget{opState: state.ApplyOperation.Completed, status: state.Task.Completed, percent: 100, building: true}
	queued := indexBuildTarget{opState: state.ApplyOperation.Pending, status: state.Task.Pending, building: true}
	cases := []struct {
		name    string
		targets []indexBuildTarget
		percent int
		suffix  string
	}{
		{name: "two targets building", targets: []indexBuildTarget{running(20), running(30)}, percent: 25},
		{name: "one target done beside one building", targets: []indexBuildTarget{completed, running(50)}, percent: 75},
		{name: "one target queued", targets: []indexBuildTarget{running(60), queued, completed}, percent: 53},
		{name: "one target throttled", targets: []indexBuildTarget{running(20), {opState: state.ApplyOperation.Running, status: state.Task.Running, percent: 30, building: true, throttled: true}},
			percent: 25, suffix: ui.ThrottledSuffix(true)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := ui.IndexBuildProgress(tc.percent, apitypes.BuildWork{}) + tc.suffix
			cli := renderRollout(t, ParseProgressResponse(indexBuildRolloutResponse(tc.targets)))
			pr := indexBuildRolloutComment(tc.targets)
			assert.Contains(t, cli, "orders: "+want+"\n", "CLI")
			assert.Contains(t, pr, "**`orders`**: "+want+"\n", "PR comment")
		})
	}
}

const createOrdersIndex = "CREATE INDEX CONCURRENTLY idx_orders_created ON orders (created_at)"

func indexBuildRolloutResponse(targets []indexBuildTarget) *apitypes.ProgressResponse {
	resp := &apitypes.ProgressResponse{
		ApplyID: "apply-7f3c", Database: "orders", Environment: "production",
		State: state.Apply.Running, Engine: "PostgreSQL", DatabaseType: storage.DatabaseTypePostgres,
	}
	for i, target := range targets {
		name := fmt.Sprintf("payments-%03d", i+1)
		op := &apitypes.ProgressOperationResponse{
			Deployment: "prod", Target: name, OperationKey: name, State: target.opState,
			CutoverPolicy: storage.CutoverPolicyParallel, OnFailure: storage.OnFailureContinue,
		}
		if target.building {
			op.Metadata = map[string]string{"executor_operation": "concurrent-index-build"}
		}
		resp.Operations = append(resp.Operations, op)
		resp.Tables = append(resp.Tables, &apitypes.TableProgressResponse{
			Deployment: "prod", Target: name, OperationKey: name, Keyspace: "orders",
			TableName: "orders", DDL: createOrdersIndex, ChangeType: "create", Status: target.status,
			PercentComplete: int32(target.percent), Throttled: target.throttled,
		})
	}
	return resp
}

func indexBuildRolloutComment(targets []indexBuildTarget) string {
	ops := make([]presentation.Operation, len(targets))
	details := make([]*webhooktemplates.ApplyStatusCommentData, len(targets))
	for i, target := range targets {
		name := fmt.Sprintf("payments-%03d", i+1)
		ops[i] = presentation.Operation{Deployment: "prod", Target: name, State: target.opState, Parallel: true, ContinueOnFailure: true}
		detail := &webhooktemplates.ApplyStatusCommentData{
			Database: strings.ReplaceAll(name, "-", "_"), State: state.Apply.Running, ApplyID: "apply-7f3c", Environment: "production",
			Engine: storage.EnginePostgres,
			Tables: []webhooktemplates.TableProgressData{{
				TableName: "orders", DDL: createOrdersIndex, Status: target.status, PercentComplete: target.percent,
				Throttled: target.throttled,
			}},
		}
		if target.building {
			detail.BuildWork = apitypes.BuildWork{Operation: "concurrent-index-build"}
		}
		details[i] = detail
	}
	return webhooktemplates.RenderMultiDeploymentApplyComment(webhooktemplates.MultiDeploymentApplyData{
		Model: presentation.Derive(ops), ApplyID: "apply-7f3c", Environment: "production", Details: details,
	})
}
