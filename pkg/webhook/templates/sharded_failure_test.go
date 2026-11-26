package templates

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
)

// wideShards is n shards named s00, s01, … in one keyspace, each running
// mutesDDL, with the state, label, and error that status gives the shard at
// its index; a shard past the end of statuses is halted behind s00.
func wideShards(n int, statuses ...ShardStatus) []ShardedKeyspace {
	shards := make([]ShardStatus, n)
	cells := make([]ShardCell, n)
	for i := range n {
		name := fmt.Sprintf("s%02d", i)
		s := ShardStatus{Emoji: "⏸️", Label: "halted — s00 failed", State: state.ApplyOperation.Pending}
		if i < len(statuses) {
			s = statuses[i]
		}
		s.Shard = name
		shards[i] = s
		cells[i] = mutesCell(name)
	}
	return oneKeyspace(shards, cells)
}

func failedShard(reason string) ShardStatus {
	return ShardStatus{Emoji: "❌", Label: "failed", State: state.ApplyOperation.Failed, Error: reason}
}

func runningShard() ShardStatus {
	return ShardStatus{Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running}
}

// A change that fails the same way on many shards reads as one reason on the
// failure callout, not as the same error once per shard, and a second reason
// gets its own line naming the shards it failed.
func TestRenderShardedApplyComment_GroupsFailuresByReason(t *testing.T) {
	const dupes = "Rows on the shard already share a value in the new unique key."
	const timeout = "The schema change ran out of time before it finished."
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "cdb_resolute", ApplyID: "apply-x",
		Keyspaces: wideShards(16, failedShard(dupes), failedShard(timeout), failedShard(dupes), failedShard(dupes)),
	})

	assert.Contains(t, out, "> ❌ **Failed on 4 of 16 shards:**\n"+
		"> - shards `s00`, `s02`, `s03`: "+dupes+"\n"+
		"> - shard `s01`: "+timeout+"\n")
	assert.Equal(t, 1, strings.Count(out, ": "+dupes+"\n"), "each reason renders once in the callout")
	assert.NotContains(t, out, "First failure")
	assert.Contains(t, out, "| `s03` | ❌ failed — "+dupes+" |", "each failed shard's reason stays reachable")

	single := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "cdb_resolute", ApplyID: "apply-x",
		Keyspaces: wideShards(16, failedShard(dupes), failedShard(dupes)),
	})
	assert.Contains(t, single, "> ❌ **Failed on shards `s00`, `s01`:** "+dupes+"\n", "one reason rides on the headline")
}

// Failure reasons are untrusted engine text, so each one is sanitized to a
// single line and HTML-escaped before it reaches the callout (AV-8), and two
// raw errors that read the same once sanitized share a line.
func TestRenderShardedApplyComment_FailureReasonsAreSanitized(t *testing.T) {
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "cdb_resolute", ApplyID: "apply-x",
		Keyspaces: wideShards(16,
			failedShard("bad <script>\nrow"),
			failedShard("bad <script> row"),
			failedShard("other"),
		),
	})

	assert.Contains(t, out, "> - shards `s00`, `s01`: bad &lt;script&gt; row\n")
	assert.NotContains(t, out, "<script>")
}

// Shards that each fail for their own reason cannot wall the callout: past
// the limit, the remaining reasons fold into one line with their shard count.
func TestRenderShardedApplyComment_CapsFailureReasons(t *testing.T) {
	statuses := make([]ShardStatus, failureReasonLimit+3)
	for i := range statuses {
		statuses[i] = failedShard(fmt.Sprintf("reason %d", i))
	}
	statuses[0] = failedShard("reason 0")
	statuses[1] = failedShard("reason 0")
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "cdb_resolute", ApplyID: "apply-x",
		Keyspaces: wideShards(16, statuses...),
	})

	assert.Contains(t, out, "> - shards `s00`, `s01`: reason 0\n", "the most common reason leads")
	assert.Equal(t, failureReasonLimit, strings.Count(out, ": reason "))
	assert.Contains(t, out, "> - …and 2 more reasons on 2 shards\n")
	for i := 2; i < len(statuses); i++ {
		assert.Contains(t, out, fmt.Sprintf("| `s%02d` | ❌ failed — reason %d |", i, i), "a reason past the limit stays reachable in the failed shards' table")
	}
}

// While every failed shard is still retrying on its own, the callout reads as
// attention rather than as a stopped failure.
func TestRenderShardedApplyComment_RetryingShardsReadAsAttention(t *testing.T) {
	retrying := ShardStatus{Emoji: "⚠️", Label: "retrying", State: state.ApplyOperation.FailedRetryable, Error: "lost connection"}
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.FailedRetryable, Environment: "production", Database: "cdb_resolute", ApplyID: "apply-x",
		Keyspaces: wideShards(16, retrying, retrying),
	})

	assert.Contains(t, out, "> ⚠️ **Retrying after a failure on shards `s00`, `s01`:** lost connection\n")
}

// A keyspace wider than the inline limit lists its shards under one collapsed
// line per state, failures first, instead of one table row per shard. The
// failed line expands into a table carrying each failed shard's error.
func TestRenderShardedApplyComment_WideKeyspaceCollapsesShardsByState(t *testing.T) {
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "production", Database: "cdb_resolute", ApplyID: "apply-x",
		Keyspaces: wideShards(16, runningShard(), failedShard("dupes"), runningShard()),
	})

	failed := strings.Index(out, "<summary>❌ 1 failed</summary>")
	running := strings.Index(out, "<summary>🔄 2 running table copy</summary>")
	halted := strings.Index(out, "<summary>⏸️ 13 halted</summary>")
	assert.Positive(t, failed)
	assert.Greater(t, running, failed, "failed shards lead")
	assert.Greater(t, halted, running)
	assert.Contains(t, out, "s00  s02\n", "the running line expands into its shards")
	assert.Contains(t, out, "<summary>❌ 1 failed</summary>\n\n| Shard | Status |\n| --- | --- |\n| `s01` | ❌ failed — dupes |\n\n</details>")
	assert.Equal(t, 1, strings.Count(out, "| Shard | Status |"), "only the failed line expands into a table")

	narrow := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "cdb_resolute", ApplyID: "apply-x",
		Keyspaces: wideShards(shardNamesInlineLimit, failedShard("dupes")),
	})
	assert.Contains(t, narrow, "| `s00` | ❌ failed — dupes |", "a narrow keyspace keeps its status table")
}

// A table whose change failed on some shards while others still copy says how
// far the failure reaches, since the failure leads the table's rollup.
func TestRenderShardedApplyComment_FailedTableCountsFailedShards(t *testing.T) {
	keyspaces := wideShards(16, failedShard("dupes"), runningShard())
	keyspaces[0].Tables = []ShardedTableStatus{{Table: "mutes", Status: state.Task.Failed, Shards: []ShardProgressData{
		{Shard: "s00", Status: state.Task.Failed},
		{Shard: "s01", Status: state.Task.Running},
		{Shard: "s02", Status: state.Task.Pending},
	}}}
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "production", Database: "cdb_resolute", ApplyID: "apply-x",
		Keyspaces: keyspaces,
	})

	assert.Contains(t, out, "**`mutes`**: ❌ Failed on 1 of 3 shards\n")

	keyspaces[0].Tables[0].PlannedShards = 16
	planned := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "production", Database: "cdb_resolute", ApplyID: "apply-x",
		Keyspaces: keyspaces,
	})
	assert.Contains(t, planned, "**`mutes`**: ❌ Failed on 1 of 16 shards\n", "shards yet to attach count toward the total")

	keyspaces[0].Tables[0].Shards = keyspaces[0].Tables[0].Shards[:1]
	onlyAttached := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "production", Database: "cdb_resolute", ApplyID: "apply-x",
		Keyspaces: keyspaces,
	})
	assert.Contains(t, onlyAttached, "**`mutes`**: ❌ Failed on 1 of 16 shards\n", "the only attached shard failing is not every shard failing")
}

// Keyspaces split on the same key ranges share shard names, so the failure
// callout names each shard with its keyspace when a name repeats across
// keyspaces, and leaves names bare when they cannot be confused.
func TestRenderShardedApplyComment_FailureCalloutQualifiesRepeatedShardNames(t *testing.T) {
	keyspace := func(name string, shards ...ShardStatus) ShardedKeyspace {
		return ShardedKeyspace{Keyspace: name, Shards: shards}
	}
	failedOn := func(shard, reason string) ShardStatus {
		s := failedShard(reason)
		s.Shard = shard
		return s
	}
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "shop", ApplyID: "apply-x",
		Keyspaces: []ShardedKeyspace{
			keyspace("shop_a", failedOn("-80", "dupes"), failedOn("80-", "timeout")),
			keyspace("shop_b", failedOn("-80", "dupes")),
		},
	})
	assert.Contains(t, out, "> ❌ **Failed on all 3 shards:**\n"+
		"> - shards `shop_a/-80`, `shop_b/-80`: dupes\n"+
		"> - shard `shop_a/80-`: timeout\n")

	distinct := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "shop", ApplyID: "apply-x",
		Keyspaces: []ShardedKeyspace{
			keyspace("shop_a", failedOn("-40", "dupes")),
			keyspace("shop_b", failedOn("40-", "dupes")),
		},
	})
	assert.Contains(t, distinct, "> ❌ **Failed on shards `-40`, `40-`:** dupes\n")
}

// A running sharded apply ends with the stop command. While a failed shard
// waits on the rest, the footer says a retry opens up once the apply settles,
// and a failed apply keeps its retry command.
func TestRenderShardedApplyComment_FooterOffersStopWhileRunning(t *testing.T) {
	render := func(applyState string, statuses ...ShardStatus) string {
		return RenderShardedApplyComment(ShardedApplyData{
			State: applyState, Environment: "production", Database: "cdb_resolute", ApplyID: "apply-x", Tenant: "acme",
			Keyspaces: wideShards(4, statuses...),
		})
	}

	healthy := render(state.Apply.Running, runningShard(), runningShard(), runningShard(), runningShard())
	assert.Contains(t, healthy, "To stop this schema change:\n```\nschemabot stop apply-x -e production --tenant acme\n```\n")
	assert.NotContains(t, healthy, "A new apply can retry")

	pastFailure := render(state.Apply.Running, failedShard("dupes"), runningShard())
	assert.Contains(t, pastFailure, "schemabot stop apply-x -e production --tenant acme")
	assert.Contains(t, pastFailure, "A new apply can retry the failure once this one finishes or is stopped")

	failed := render(state.Apply.Failed, failedShard("dupes"))
	assert.Contains(t, failed, "To retry:\n```\nschemabot apply -e production --tenant acme\n```\n")
	assert.NotContains(t, failed, "schemabot stop")
}
