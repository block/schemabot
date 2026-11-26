package templates

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

const (
	dupesReason   = "Rows on the shard already share a value in the new unique key."
	timeoutReason = "The schema change ran out of time before it finished."
)

// shardFailureShards is the names of the 16 shards shardFailureResponse
// splits shop_sharded into, in key order: -10, 10-20, … f0-.
func shardFailureShards() []string {
	names := make([]string, 16)
	for i := range names {
		lo, hi := fmt.Sprintf("%x0", i), fmt.Sprintf("%x0", i+1)
		if i == 0 {
			lo = ""
		}
		if i == 15 {
			hi = ""
		}
		names[i] = lo + "-" + hi
	}
	return names
}

// shardFailure is how one shard of shardFailureResponse ended up.
type shardFailure struct {
	reason   string
	retrying bool
}

// shardFailureResponse is a sharded apply adding a unique key to `mutes`
// across 16 shards, as the progress API reports it: the first shards fail as
// failures says, the next running shards copy, and the rest wait.
func shardFailureResponse(applyState string, running int, failures ...shardFailure) *apitypes.ProgressResponse {
	var ops []*apitypes.ProgressOperationResponse
	var shards []*apitypes.ShardProgressResponse
	for i, name := range shardFailureShards() {
		op := &apitypes.ProgressOperationResponse{
			Deployment: "data-plane", OperationKey: "shop_sharded/" + name + "/mutes",
			OperationKind: storage.ApplyOperationKindWork, State: state.ApplyOperation.Pending,
		}
		sh := &apitypes.ShardProgressResponse{Shard: name, Status: state.Task.Pending}
		switch {
		case i < len(failures):
			op.State, op.ErrorMessage, sh.Status = state.ApplyOperation.Failed, failures[i].reason, state.Task.Failed
			if failures[i].retrying {
				op.State = state.ApplyOperation.FailedRetryable
			}
		case i < len(failures)+running:
			op.State, sh.Status, sh.RowsCopied, sh.RowsTotal = state.ApplyOperation.Running, state.Task.Running, 400, 1000
		}
		ops = append(ops, op)
		shards = append(shards, sh)
	}
	return &apitypes.ProgressResponse{
		ApplyID: "apply-a1b2c3d4e5f6", Database: "shop", DatabaseType: storage.DatabaseTypeMySQL,
		Environment: "production", State: applyState, Sharded: true, Operations: ops,
		Tables: []*apitypes.TableProgressResponse{{
			TableName: "mutes", Keyspace: "shop_sharded", Deployment: "data-plane", ChangeType: "alter",
			DDL:    "ALTER TABLE `mutes` ADD UNIQUE KEY `uk_target`(`target_id`)",
			Status: state.Task.Failed, PlannedShards: 16, Shards: shards,
		}},
	}
}

func failedFor(reason string) shardFailure { return shardFailure{reason: reason} }

func watchShardFailure(t *testing.T, resp *apitypes.ProgressResponse) string {
	t.Helper()
	return ansiEscape.ReplaceAllString(captureStdout(t, func() { WriteProgress(ParseProgressResponse(resp)) }), "")
}

// A change that fails the same way on many shards reads as one reason under
// the box, not as the first shard's error alone, and a second reason gets its
// own line naming the shards it failed. The failed table's line says how far
// the failure reaches instead of drawing an empty bar.
func TestWriteProgressShardedGroupsFailuresByReason(t *testing.T) {
	output := watchShardFailure(t, shardFailureResponse(state.Apply.Failed, 0,
		failedFor(dupesReason), failedFor(timeoutReason), failedFor(dupesReason), failedFor(dupesReason)))

	assert.Contains(t, output, "  ❌ Failed on 4 of 16 shards:\n"+
		"    - shards -10, 20-30, 30-40: "+dupesReason+"\n"+
		"    - shard 10-20: "+timeoutReason+"\n")
	assert.Equal(t, 1, strings.Count(output, dupesReason), "each reason renders once")
	assert.NotContains(t, output, "shard -10: "+dupesReason, "the grouped reasons stand in for the first shard's error")
	assert.Contains(t, output, "~ mutes: ❌ Failed on 4 of 16 shards\n")
	assert.NotContains(t, output, "⬜", "a failed sharded table draws no bar")
	assert.Contains(t, output, "To recover:")
	assert.NotContains(t, output, presentation.RetryOnceSettledNote, "a failed apply can be retried now")
}

// Shards that all fail for one reason name it on the headline, and a single
// failed shard keeps the apply's error, which already names it.
func TestWriteProgressShardedFailureReasonRidesOnTheHeadline(t *testing.T) {
	output := watchShardFailure(t, shardFailureResponse(state.Apply.Failed, 0, failedFor(dupesReason), failedFor(dupesReason)))
	assert.Contains(t, output, "  ❌ Failed on shards -10, 10-20: "+dupesReason+"\n")

	single := watchShardFailure(t, shardFailureResponse(state.Apply.Failed, 0, failedFor(dupesReason)))
	assert.Contains(t, single, "  shard -10: "+dupesReason+"\n")
	assert.NotContains(t, single, "❌ Failed on shard", "one failed shard needs no grouped callout")
	assert.Contains(t, single, "~ mutes: ❌ Failed on 1 of 16 shards\n")
}

// More failed shards than a line can name read as a count against the
// apply's shards.
func TestWriteProgressShardedFailureCountsManyShards(t *testing.T) {
	failures := make([]shardFailure, 12)
	for i := range failures {
		failures[i] = failedFor(dupesReason)
	}
	output := watchShardFailure(t, shardFailureResponse(state.Apply.Failed, 0, failures...))
	assert.Contains(t, output, "  ❌ Failed on 12 of 16 shards: "+dupesReason+"\n")
}

// Shards that are all retrying their failure read as attention, not as a
// stopped failure (UX-5).
func TestWriteProgressShardedRetryingShardsReadAsAttention(t *testing.T) {
	output := watchShardFailure(t, shardFailureResponse(state.Apply.Running, 2,
		shardFailure{reason: timeoutReason, retrying: true}, shardFailure{reason: timeoutReason, retrying: true}))
	assert.Contains(t, output, "  ⚠️ Retrying after a failure on shards -10, 10-20: "+timeoutReason+"\n")
}

// A shard that fails while others still copy is named under the box, and the
// view says when a new apply can retry it. Watch offers the stop key, so no
// stop command is printed.
func TestWriteProgressShardedFailureWhileOthersRun(t *testing.T) {
	output := watchShardFailure(t, shardFailureResponse(state.Apply.Running, 4,
		failedFor(dupesReason), failedFor(dupesReason), failedFor(dupesReason)))

	assert.Contains(t, output, "  ❌ Failed on shards -10, 10-20, 20-30: "+dupesReason+"\n")
	assert.Contains(t, output, "\n"+presentation.RetryOnceSettledNote+"\n")
	assert.NotContains(t, output, "To stop")
	assert.NotContains(t, output, "To recover:")
}

// The CLI and the PR comment read a sharded apply's failed shards the same
// way: the same headline, and each reason with the same shards.
func TestShardFailureReasonsReadTheSameOnEverySurface(t *testing.T) {
	reasons := []string{dupesReason, timeoutReason, dupesReason, dupesReason}
	failures := make([]shardFailure, len(reasons))
	for i, r := range reasons {
		failures[i] = failedFor(r)
	}
	cli := watchShardFailure(t, shardFailureResponse(state.Apply.Failed, 0, failures...))

	names := shardFailureShards()
	shards := make([]webhooktemplates.ShardStatus, len(names))
	for i, name := range names {
		shards[i] = webhooktemplates.ShardStatus{Shard: name, Emoji: "⏸️", Label: "halted — -10 failed", State: state.ApplyOperation.Pending}
		if i < len(reasons) {
			shards[i] = webhooktemplates.ShardStatus{Shard: name, Emoji: "❌", Label: "failed", State: state.ApplyOperation.Failed, Error: reasons[i]}
		}
	}
	pr := webhooktemplates.RenderShardedApplyComment(webhooktemplates.ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "shop", ApplyID: "apply-a1b2c3d4e5f6",
		Keyspaces: []webhooktemplates.ShardedKeyspace{{Keyspace: "shop_sharded", Shards: shards}},
	})
	prPlain := strings.NewReplacer("> ", "", "**", "", "`", "").Replace(pr)

	for _, line := range []string{
		"❌ Failed on 4 of 16 shards:",
		"- shards -10, 20-30, 30-40: " + dupesReason,
		"- shard 10-20: " + timeoutReason,
	} {
		assert.Contains(t, cli, line, "CLI")
		assert.Contains(t, prPlain, line, "PR comment")
	}
}

// Keyspaces split on the same key ranges share shard names, so the CLI and the
// PR comment both name each failed shard with its keyspace when a name repeats.
func TestShardFailureNamesQualifyRepeatedShardsOnEverySurface(t *testing.T) {
	type failedOp struct{ keyspace, shard, reason string }
	failed := []failedOp{{"shop_a", "-80", dupesReason}, {"shop_a", "80-", timeoutReason}, {"shop_b", "-80", dupesReason}}
	resp := &apitypes.ProgressResponse{
		ApplyID: "apply-a1b2c3d4e5f6", Database: "shop", DatabaseType: storage.DatabaseTypeMySQL,
		Environment: "production", State: state.Apply.Failed, Sharded: true,
	}
	var keyspaces []webhooktemplates.ShardedKeyspace
	for _, ks := range []string{"shop_a", "shop_b"} {
		table := &apitypes.TableProgressResponse{
			TableName: "mutes", Keyspace: ks, Deployment: "data-plane", ChangeType: "alter",
			DDL: "ALTER TABLE `mutes` ADD UNIQUE KEY `uk_target`(`target_id`)", Status: state.Task.Failed,
		}
		var shards []webhooktemplates.ShardStatus
		for _, f := range failed {
			if f.keyspace != ks {
				continue
			}
			resp.Operations = append(resp.Operations, &apitypes.ProgressOperationResponse{
				Deployment: "data-plane", OperationKey: ks + "/" + f.shard + "/mutes",
				OperationKind: storage.ApplyOperationKindWork, State: state.ApplyOperation.Failed, ErrorMessage: f.reason,
			})
			table.Shards = append(table.Shards, &apitypes.ShardProgressResponse{Shard: f.shard, Status: state.Task.Failed})
			shards = append(shards, webhooktemplates.ShardStatus{Shard: f.shard, Emoji: "❌", Label: "failed", State: state.ApplyOperation.Failed, Error: f.reason})
		}
		resp.Tables = append(resp.Tables, table)
		keyspaces = append(keyspaces, webhooktemplates.ShardedKeyspace{Keyspace: ks, Shards: shards})
	}
	cli := watchShardFailure(t, resp)
	pr := webhooktemplates.RenderShardedApplyComment(webhooktemplates.ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "shop", ApplyID: "apply-a1b2c3d4e5f6", Keyspaces: keyspaces,
	})
	prPlain := strings.NewReplacer("> ", "", "**", "", "`", "").Replace(pr)

	for _, line := range []string{
		"- shards shop_a/-80, shop_b/-80: " + dupesReason,
		"- shard shop_a/80-: " + timeoutReason,
	} {
		assert.Contains(t, cli, line, "CLI")
		assert.Contains(t, prPlain, line, "PR comment")
	}
}
