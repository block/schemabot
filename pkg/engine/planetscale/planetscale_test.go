package planetscale

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
)

func TestDeployStateToEngineState(t *testing.T) {
	tests := []struct {
		deployState   string
		expectedState engine.State
		expectedProg  int
	}{
		{"pending", engine.StatePending, 0},
		{"ready", engine.StatePending, 0},
		{"no_changes", engine.StateCompleted, 100},
		{"queued", engine.StateRunning, 5},
		{"submitting", engine.StateRunning, 5},
		{"in_progress", engine.StateRunning, 50},
		{"in_progress_vschema", engine.StateRunning, 50},
		{"pending_cutover", engine.StateWaitingForCutover, 90},
		{"in_progress_cutover", engine.StateCuttingOver, 95},
		{"complete", engine.StateCompleted, 100},
		{"complete_pending_revert", engine.StateRevertWindow, 100},
		{"complete_error", engine.StateFailed, 0},
		{"error", engine.StateFailed, 0},
		{"failed", engine.StateFailed, 0},
		{"in_progress_cancel", engine.StateStopped, 0},
		{"cancelled", engine.StateStopped, 0},
		{"complete_cancel", engine.StateStopped, 0},
		{"in_progress_revert", engine.StateRunning, 50},
		{"in_progress_revert_vschema", engine.StateRunning, 50},
		{"complete_revert", engine.StateReverted, 100},
		{"complete_revert_error", engine.StateFailed, 0},
		{"unknown_state", engine.StateRunning, 25},
	}

	for _, tt := range tests {
		t.Run(tt.deployState, func(t *testing.T) {
			state, progress := deployStateToEngineState(tt.deployState)
			assert.Equal(t, tt.expectedState, state)
			assert.Equal(t, tt.expectedProg, progress)
		})
	}
}

func TestDeployStateToMessage(t *testing.T) {
	tests := []struct {
		deployState string
		contains    string
	}{
		{"pending", "Validating"},
		{"ready", "validation complete"},
		{"no_changes", "No changes"},
		{"queued", "queued"},
		{"submitting", "Submitting"},
		{"in_progress", "in progress"},
		{"in_progress_vschema", "VSchema"},
		{"pending_cutover", "cutover"},
		{"in_progress_cutover", "Cutover"},
		{"complete", "complete"},
		{"complete_pending_revert", "revert available"},
		{"failed", "failed"},
		{"cancelled", "cancelled"},
		{"in_progress_revert", "Revert in progress"},
		{"complete_revert", "reverted"},
		{"complete_revert_error", "Revert failed"},
		{"something_new", "something_new"},
	}

	for _, tt := range tests {
		t.Run(tt.deployState, func(t *testing.T) {
			msg := deployStateToMessage(tt.deployState)
			assert.Contains(t, msg, tt.contains)
		})
	}
}

func TestVolumeToThrottleRatio(t *testing.T) {
	tests := []struct {
		volume   int32
		expected float64
	}{
		{0, 0.95},  // below min, clamped to max throttle
		{1, 0.95},  // max throttle
		{2, 0.85},  // default volume
		{6, 0.45},  // mid-range
		{10, 0.05}, // near no throttle
		{11, 0.0},  // no throttle
		{12, 0.0},  // above max, clamped to no throttle
	}

	for _, tt := range tests {
		ratio := volumeToThrottleRatio(tt.volume)
		assert.InDelta(t, tt.expected, ratio, 0.001, "volume %d", tt.volume)
	}
}

func TestGenerateBranchName(t *testing.T) {
	tests := []struct {
		name     string
		database string
		planID   string
		expected string
	}{
		{
			name:     "basic",
			database: "mydb",
			planID:   "plan-12345678",
			expected: "schemabot-mydb-12345678",
		},
		{
			name:     "underscores replaced",
			database: "my_cool_db",
			planID:   "plan-abcdefgh",
			expected: "schemabot-my-cool-db-abcdefgh",
		},
		{
			name:     "long database name truncated",
			database: "this_is_a_very_long_database_name",
			planID:   "plan-xyz12345",
			expected: "schemabot-this-is-a-very-long--xyz12345",
		},
		{
			name:     "short plan ID",
			database: "db",
			planID:   "abc",
			expected: "schemabot-db-abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := generateBranchName(tt.database, tt.planID)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestPSMetadataEncodeDecode(t *testing.T) {
	original := &psMetadata{
		BranchName:       "schemabot-mydb-12345678",
		DeployRequestID:  42,
		DeployRequestURL: "https://app.planetscale.com/org/db/deploy-requests/42",
	}

	encoded, err := encodePSMetadata(original)
	require.NoError(t, err)
	assert.Contains(t, encoded, "schemabot-mydb-12345678")
	assert.Contains(t, encoded, "42")

	decoded, err := decodePSMetadata(encoded)
	require.NoError(t, err)
	assert.Equal(t, original.BranchName, decoded.BranchName)
	assert.Equal(t, original.DeployRequestID, decoded.DeployRequestID)
	assert.Equal(t, original.DeployRequestURL, decoded.DeployRequestURL)
}

func TestDecodePSMetadata_Empty(t *testing.T) {
	_, err := decodePSMetadata("")
	assert.Error(t, err)
}

func TestDecodePSMetadata_Invalid(t *testing.T) {
	_, err := decodePSMetadata("not json")
	assert.Error(t, err)
}

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

func TestSplitStatements(t *testing.T) {
	stmts, err := ddl.SplitStatements("CREATE TABLE `a` (id INT); ALTER TABLE `b` ADD COLUMN x INT;")
	require.NoError(t, err)
	assert.Len(t, stmts, 2)

	// Empty input
	stmts, err = ddl.SplitStatements("")
	require.NoError(t, err)
	assert.Empty(t, stmts)

	// Semicolons with no valid statements are a parse error
	_, err = ddl.SplitStatements("  ;  ;  ")
	assert.Error(t, err)
}
