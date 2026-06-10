package planetscale

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
)

func TestAggregateShardProgress(t *testing.T) {
	t.Run("two shards one table", func(t *testing.T) {
		rows := []vitessMigrationRow{
			{MigrationUUID: "uuid-1", Keyspace: "commerce", Shard: "-80", Table: "orders", Status: "running", RowsCopied: 5000, TableRows: 10000, Progress: 50},
			{MigrationUUID: "uuid-1", Keyspace: "commerce", Shard: "80-", Table: "orders", Status: "running", RowsCopied: 3000, TableRows: 10000, Progress: 30},
		}

		tables, overall := aggregateShardProgress(rows)
		require.Len(t, tables, 1)
		assert.Equal(t, "orders", tables[0].Table)
		assert.Equal(t, state.Vitess.Running, tables[0].State)
		assert.Equal(t, int64(8000), tables[0].RowsCopied)
		assert.Equal(t, int64(20000), tables[0].RowsTotal)
		assert.Equal(t, 40, tables[0].Progress) // 8000/20000
		assert.Equal(t, 40, overall)
		require.Len(t, tables[0].Shards, 2)
		// Shards sorted by key range
		assert.Equal(t, "-80", tables[0].Shards[0].Shard)
		assert.Equal(t, "80-", tables[0].Shards[1].Shard)
	})

	t.Run("instant DDL", func(t *testing.T) {
		rows := []vitessMigrationRow{
			{MigrationUUID: "uuid-2", Keyspace: "commerce", Shard: "-80", Table: "items", Status: "complete", IsImmediate: true},
			{MigrationUUID: "uuid-2", Keyspace: "commerce", Shard: "80-", Table: "items", Status: "complete", IsImmediate: true},
		}

		tables, overall := aggregateShardProgress(rows)
		require.Len(t, tables, 1)
		assert.Equal(t, state.Vitess.Complete, tables[0].State)
		assert.Equal(t, 100, tables[0].Progress)
		assert.True(t, tables[0].IsInstant)
		assert.Equal(t, 100, overall)
	})

	t.Run("mixed running and complete", func(t *testing.T) {
		rows := []vitessMigrationRow{
			{MigrationUUID: "uuid-1", Keyspace: "commerce", Shard: "-80", Table: "orders", Status: "running", RowsCopied: 9000, TableRows: 10000},
			{MigrationUUID: "uuid-1", Keyspace: "commerce", Shard: "80-", Table: "orders", Status: "complete", RowsCopied: 10000, TableRows: 10000},
		}

		tables, _ := aggregateShardProgress(rows)
		require.Len(t, tables, 1)
		// One shard running means table is running
		assert.Equal(t, state.Vitess.Running, tables[0].State)
	})

	t.Run("failed shard overrides", func(t *testing.T) {
		rows := []vitessMigrationRow{
			{MigrationUUID: "uuid-1", Keyspace: "commerce", Shard: "-80", Table: "orders", Status: "running"},
			{MigrationUUID: "uuid-1", Keyspace: "commerce", Shard: "80-", Table: "orders", Status: "failed"},
		}

		tables, _ := aggregateShardProgress(rows)
		require.Len(t, tables, 1)
		assert.Equal(t, state.Vitess.Failed, tables[0].State)
	})

	t.Run("ready_to_complete derived state", func(t *testing.T) {
		rows := []vitessMigrationRow{
			{MigrationUUID: "uuid-1", Keyspace: "commerce", Shard: "-80", Table: "orders", Status: "running", ReadyToComplete: true},
			{MigrationUUID: "uuid-1", Keyspace: "commerce", Shard: "80-", Table: "orders", Status: "running", ReadyToComplete: true},
		}

		tables, _ := aggregateShardProgress(rows)
		require.Len(t, tables, 1)
		assert.Equal(t, state.Vitess.ReadyToComplete, tables[0].State)
		// Shards should show derived state
		assert.Equal(t, state.Vitess.ReadyToComplete, tables[0].Shards[0].State)
	})

	t.Run("multiple tables", func(t *testing.T) {
		rows := []vitessMigrationRow{
			{MigrationUUID: "uuid-1", Keyspace: "commerce", Shard: "0", Table: "orders", Status: "complete", RowsCopied: 100, TableRows: 100},
			{MigrationUUID: "uuid-2", Keyspace: "commerce", Shard: "0", Table: "items", Status: "running", RowsCopied: 50, TableRows: 200},
		}

		tables, overall := aggregateShardProgress(rows)
		require.Len(t, tables, 2)
		assert.Equal(t, "orders", tables[0].Table)
		assert.Equal(t, "items", tables[1].Table)
		assert.Equal(t, 50, overall) // 150/300
	})

	t.Run("ready_to_complete clamps progress to 100%", func(t *testing.T) {
		// Vitess row counts can lag behind (concurrent inserts during copy).
		// When a shard reaches ready_to_complete, the copy is done — show 100%.
		rows := []vitessMigrationRow{
			{MigrationUUID: "uuid-1", Keyspace: "commerce", Shard: "-80", Table: "orders", Status: "running", ReadyToComplete: true, Progress: 98, RowsCopied: 9800, TableRows: 10000},
			{MigrationUUID: "uuid-1", Keyspace: "commerce", Shard: "80-", Table: "orders", Status: "running", ReadyToComplete: true, Progress: 97, RowsCopied: 9700, TableRows: 10000},
		}

		tables, _ := aggregateShardProgress(rows)
		require.Len(t, tables, 1)
		assert.Equal(t, state.Vitess.ReadyToComplete, tables[0].State)
		assert.Equal(t, 100, tables[0].Progress)
		// Each shard should show 100% and clamped rows
		for _, sh := range tables[0].Shards {
			assert.Equal(t, 100, sh.Progress, "shard %s should be 100%%", sh.Shard)
			assert.Equal(t, int64(10000), sh.RowsCopied, "shard %s rows should be clamped", sh.Shard)
			assert.Equal(t, int64(10000), sh.RowsTotal, "shard %s total", sh.Shard)
		}
	})

	t.Run("complete shard clamps progress to 100%", func(t *testing.T) {
		rows := []vitessMigrationRow{
			{MigrationUUID: "uuid-1", Keyspace: "commerce", Shard: "-", Table: "users", Status: "complete", RowsCopied: 284953, TableRows: 284953},
		}

		tables, overall := aggregateShardProgress(rows)
		require.Len(t, tables, 1)
		assert.Equal(t, 100, tables[0].Progress)
		assert.Equal(t, 100, tables[0].Shards[0].Progress)
		assert.Equal(t, 100, overall)
	})

	t.Run("mixed ready_to_complete and running", func(t *testing.T) {
		// One shard done, one still copying — table should be running, not ready_to_complete
		rows := []vitessMigrationRow{
			{MigrationUUID: "uuid-1", Keyspace: "commerce", Shard: "-80", Table: "orders", Status: "running", ReadyToComplete: true, Progress: 99, RowsCopied: 10000, TableRows: 10000},
			{MigrationUUID: "uuid-1", Keyspace: "commerce", Shard: "80-", Table: "orders", Status: "running", ReadyToComplete: false, Progress: 50, RowsCopied: 5000, TableRows: 10000},
		}

		tables, _ := aggregateShardProgress(rows)
		require.Len(t, tables, 1)
		assert.Equal(t, state.Vitess.Running, tables[0].State)
		// First shard (ready_to_complete) should be clamped to 100%
		assert.Equal(t, 100, tables[0].Shards[0].Progress)
		assert.Equal(t, int64(10000), tables[0].Shards[0].RowsCopied)
		// Second shard (still running) should show actual progress
		assert.Equal(t, 50, tables[0].Shards[1].Progress)
		assert.Equal(t, int64(5000), tables[0].Shards[1].RowsCopied)
	})

	t.Run("rows_copied exceeding table_rows clamps to 100%", func(t *testing.T) {
		// While a shard is still copying, rows_copied can momentarily exceed the
		// estimated table_rows because of concurrent inserts. Table and overall
		// progress must never exceed 100%.
		rows := []vitessMigrationRow{
			{MigrationUUID: "uuid-1", Keyspace: "commerce", Shard: "-", Table: "orders", Status: "running", Progress: 99, RowsCopied: 12000, TableRows: 10000},
		}

		tables, overall := aggregateShardProgress(rows)
		require.Len(t, tables, 1)
		assert.Equal(t, state.Vitess.Running, tables[0].State)
		assert.Equal(t, 100, tables[0].Progress)
		assert.Equal(t, 100, overall)
	})
}

func TestParseProgressPercent(t *testing.T) {
	t.Run("fractional value rounds to nearest int", func(t *testing.T) {
		pct, err := parseProgressPercent("54.35")
		require.NoError(t, err)
		assert.Equal(t, 54, pct)
	})

	t.Run("rounds half up", func(t *testing.T) {
		pct, err := parseProgressPercent("54.5")
		require.NoError(t, err)
		assert.Equal(t, 55, pct)
	})

	t.Run("empty value is zero", func(t *testing.T) {
		pct, err := parseProgressPercent("")
		require.NoError(t, err)
		assert.Equal(t, 0, pct)
	})

	t.Run("clamps above 100", func(t *testing.T) {
		pct, err := parseProgressPercent("101.7")
		require.NoError(t, err)
		assert.Equal(t, 100, pct)
	})

	t.Run("clamps below 0", func(t *testing.T) {
		pct, err := parseProgressPercent("-3.2")
		require.NoError(t, err)
		assert.Equal(t, 0, pct)
	})

	t.Run("non-numeric value errors", func(t *testing.T) {
		_, err := parseProgressPercent("notanumber")
		require.Error(t, err)
	})
}

func TestValidateMigrationContext(t *testing.T) {
	assert.NoError(t, validateMigrationContext("singularity:abc-123"))
	assert.NoError(t, validateMigrationContext("localscale:42"))
	assert.Error(t, validateMigrationContext("has'quote"))
	assert.Error(t, validateMigrationContext(`has"double`))
	assert.Error(t, validateMigrationContext("has`backtick"))
	assert.Error(t, validateMigrationContext(`has\backslash`))
}

func TestShardLess(t *testing.T) {
	assert.True(t, shardLess("-80", "80-"))
	assert.False(t, shardLess("80-", "-80"))
	assert.True(t, shardLess("-40", "40-80"))
	assert.True(t, shardLess("40-80", "80-c0"))
	assert.False(t, shardLess("80-c0", "40-80"))
}
