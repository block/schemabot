package planetscale

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/mysqlerr"
	"github.com/block/schemabot/pkg/state"
)

// The message a failed shard reports quotes the statement that failed, so it
// carries a row out of the customer's table. What reaches the pull request is
// assembled from SchemaBot's own sentences instead, keyed off the error code.
const notNullFailure = "Error 1048 (23000): vttablet: rpc error: code = Unknown desc = " +
	"Column 'lending_product' cannot be null (errno 1048) (sqlstate 23000) " +
	"during query: insert into `_vt_vrp_related_external_entities` " +
	"(`entity_token`,`customer_id`) values ('ent_abc123', 'cst_xyz789')"

func TestFailedShard(t *testing.T) {
	t.Run("reports the shard that stopped", func(t *testing.T) {
		rows := []vitessMigrationRow{
			{Keyspace: "commerce", Shard: "-80", Table: "orders", Status: state.Vitess.Failed, Message: notNullFailure},
		}

		failed, ok := failedShard(rows)
		require.True(t, ok)
		assert.Equal(t, "-80", failed.Shard)
		assert.Equal(t, notNullFailure, failed.Message, "the raw message is kept for the server log")
	})

	// A running shard carries a message too — a throttle note, a retry — and
	// none of them mean the schema change stopped.
	t.Run("no failed shard", func(t *testing.T) {
		rows := []vitessMigrationRow{
			{Keyspace: "commerce", Shard: "-80", Table: "orders", Status: state.Vitess.Running, Message: "throttled"},
			{Keyspace: "commerce", Shard: "80-", Table: "orders", Status: state.Vitess.Complete, Message: "done"},
		}
		_, ok := failedShard(rows)
		assert.False(t, ok)
	})

	t.Run("one shard failed among healthy shards", func(t *testing.T) {
		rows := []vitessMigrationRow{
			{Keyspace: "commerce", Shard: "-80", Table: "orders", Status: state.Vitess.Running, Message: "throttled"},
			{Keyspace: "commerce", Shard: "80-", Table: "orders", Status: state.Vitess.Failed, Message: notNullFailure},
		}

		failed, ok := failedShard(rows)
		require.True(t, ok)
		assert.Equal(t, "80-", failed.Shard)
	})

	t.Run("failed shard Vitess left no message for", func(t *testing.T) {
		rows := []vitessMigrationRow{
			{Keyspace: "commerce", Shard: "-80", Table: "orders", Status: state.Vitess.Failed, Message: "   "},
		}
		_, ok := failedShard(rows)
		assert.False(t, ok)
	})

	// Shards report in whatever order the keyspaces answer, so an unstable pick
	// would make successive polls rewrite the pull request with a different
	// reason for the same failure.
	t.Run("picks the same shard whatever order the rows arrive in", func(t *testing.T) {
		forward := []vitessMigrationRow{
			{Keyspace: "commerce", Shard: "-80", Table: "orders", Status: state.Vitess.Failed, Message: "first"},
			{Keyspace: "commerce", Shard: "80-", Table: "orders", Status: state.Vitess.Failed, Message: "second"},
			{Keyspace: "zebra", Shard: "-80", Table: "orders", Status: state.Vitess.Failed, Message: "third"},
		}
		reversed := []vitessMigrationRow{forward[2], forward[1], forward[0]}

		first, ok := failedShard(forward)
		require.True(t, ok)
		assert.Equal(t, "first", first.Message)

		again, ok := failedShard(reversed)
		require.True(t, ok)
		assert.Equal(t, "first", again.Message)
	})
}

// A shard can stop while the deploy request is still working — Vitess retries
// it — and reporting that as the apply's failure would tell a pull request its
// change failed while the change is still running.
func TestAdoptedFailureReason(t *testing.T) {
	failed := vitessMigrationRow{
		Keyspace: "commerce", Shard: "-80", Table: "orders",
		Status: state.Vitess.Failed, Message: notNullFailure,
	}
	wantReason := mysqlerr.ReasonFromText(notNullFailure)

	tests := []struct {
		name        string
		engineState engine.State
		row         vitessMigrationRow
		shardFailed bool
		want        string
	}{
		{
			name:        "deploy request failed and a shard reported why",
			engineState: engine.StateFailed,
			row:         failed, shardFailed: true,
			want: wantReason,
		},
		{
			name:        "shard stopped while the change is still running",
			engineState: engine.StateRunning,
			row:         failed, shardFailed: true,
			want: "",
		},
		{
			name:        "shard stopped and the change went on to complete",
			engineState: engine.StateCompleted,
			row:         failed, shardFailed: true,
			want: "",
		},
		{
			name:        "deploy request failed with no shard to explain it",
			engineState: engine.StateFailed,
			shardFailed: false,
			want:        "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := adoptedFailureReason(tt.engineState, tt.row, tt.shardFailed)
			assert.Equal(t, tt.want, got)
			assert.NotContains(t, got, "cst_xyz789", "a customer value reached the pull request")
			assert.NotContains(t, got, "lending_product")
		})
	}
}

// The reason points an operator at this log, so the log has to carry the
// target's words — bounded, because the statement quoted after them is as wide
// as the table.
func TestClampTargetMessage(t *testing.T) {
	t.Run("keeps a message whole", func(t *testing.T) {
		assert.Equal(t, notNullFailure, clampTargetMessage(notNullFailure))
	})

	t.Run("bounds a message a wide table made enormous", func(t *testing.T) {
		got := clampTargetMessage(strings.Repeat("x", maxLoggedTargetMessageLen*3))
		assert.True(t, strings.HasSuffix(got, "… (truncated)"))
		assert.Len(t, []rune(got), maxLoggedTargetMessageLen+len([]rune("… (truncated)")))
	})

	t.Run("does not split a rune", func(t *testing.T) {
		got := clampTargetMessage(strings.Repeat("é", maxLoggedTargetMessageLen*2))
		assert.True(t, utf8.ValidString(got))
	})
}
