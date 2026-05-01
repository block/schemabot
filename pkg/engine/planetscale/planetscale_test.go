package planetscale

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/statement"
	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/lint"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/spirit/pkg/table"
)

func TestDeployStateToEngineState(t *testing.T) {
	tests := []struct {
		deployState   string
		expectedState engine.State
	}{
		{"pending", engine.StatePending},
		{"ready", engine.StatePending},
		{"no_changes", engine.StateCompleted},
		{"queued", engine.StateRunning},
		{"submitting", engine.StateRunning},
		{"in_progress", engine.StateRunning},
		{"in_progress_vschema", engine.StateRunning},
		{"pending_cutover", engine.StateWaitingForCutover},
		{"in_progress_cutover", engine.StateCuttingOver},
		{"complete", engine.StateCompleted},
		{"complete_pending_revert", engine.StateRevertWindow},
		{"complete_error", engine.StateFailed},
		{"error", engine.StateFailed},
		{"failed", engine.StateFailed},
		{"in_progress_cancel", engine.StateStopped},
		{"cancelled", engine.StateStopped},
		{"complete_cancel", engine.StateStopped},
		{"in_progress_revert", engine.StateRunning},
		{"in_progress_revert_vschema", engine.StateRunning},
		{"complete_revert", engine.StateReverted},
		{"complete_revert_error", engine.StateFailed},
		{"unknown_state", engine.StateRunning},
	}

	for _, tt := range tests {
		t.Run(tt.deployState, func(t *testing.T) {
			got := deployStateToEngineState(tt.deployState)
			assert.Equal(t, tt.expectedState, got)
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

func TestPSMetadataEncodeDecode_DeferredDeploy(t *testing.T) {
	original := &psMetadata{
		BranchName:       "schemabot-mydb-12345678",
		DeployRequestID:  42,
		DeployRequestURL: "https://app.planetscale.com/org/db/deploy-requests/42",
		IsInstant:        true,
		DeferredDeploy:   true,
	}

	encoded, err := encodePSMetadata(original)
	require.NoError(t, err)

	decoded, err := decodePSMetadata(encoded)
	require.NoError(t, err)
	assert.Equal(t, original.BranchName, decoded.BranchName)
	assert.Equal(t, original.DeployRequestID, decoded.DeployRequestID)
	assert.True(t, decoded.IsInstant)
	assert.True(t, decoded.DeferredDeploy)
}

func TestStart_RejectsNonDeferredDeploy(t *testing.T) {
	e := &Engine{}

	// Non-deferred metadata — Start should return "not supported"
	meta, err := encodePSMetadata(&psMetadata{
		BranchName:      "schemabot-mydb-abc",
		DeployRequestID: 1,
		DeferredDeploy:  false,
	})
	require.NoError(t, err)

	_, err = e.Start(t.Context(), &engine.ControlRequest{
		ResumeState: &engine.ResumeState{Metadata: meta},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func TestStart_AcceptsDeferredDeploy(t *testing.T) {
	// Start with DeferredDeploy=true should attempt to deploy
	// (will fail because no PS client, but validates dispatch logic)
	e := &Engine{}

	meta, err := encodePSMetadata(&psMetadata{
		BranchName:      "schemabot-mydb-abc",
		DeployRequestID: 1,
		IsInstant:       true,
		DeferredDeploy:  true,
	})
	require.NoError(t, err)

	_, err = e.Start(t.Context(), &engine.ControlRequest{
		ResumeState: &engine.ResumeState{Metadata: meta},
	})
	// Fails because no PS client configured — but proves it didn't reject as "not supported"
	assert.Error(t, err)
	assert.NotContains(t, err.Error(), "not supported")
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

func TestIsSnapshotInProgress(t *testing.T) {
	assert.True(t, isSnapshotInProgress(fmt.Errorf("Cannot update VSchema while a schema snapshot is in progress.")))
	assert.True(t, isSnapshotInProgress(fmt.Errorf("wrapped: schema snapshot is in progress")))
	assert.False(t, isSnapshotInProgress(fmt.Errorf("connection refused")))
	assert.False(t, isSnapshotInProgress(nil))
}

func TestRetryDelay(t *testing.T) {
	t.Run("normal backoff is exponential", func(t *testing.T) {
		d0 := retryDelay(0, fmt.Errorf("connection refused"))
		d1 := retryDelay(1, fmt.Errorf("connection refused"))
		d2 := retryDelay(2, fmt.Errorf("connection refused"))
		// Base: 2s, 4s, 8s (plus up to 2s jitter)
		assert.GreaterOrEqual(t, d0, 2*time.Second)
		assert.Less(t, d0, 5*time.Second)
		assert.GreaterOrEqual(t, d1, 4*time.Second)
		assert.GreaterOrEqual(t, d2, 8*time.Second)
	})

	t.Run("snapshot backoff is longer", func(t *testing.T) {
		snapshotErr := fmt.Errorf("schema snapshot is in progress")
		d0 := retryDelay(0, snapshotErr)
		d1 := retryDelay(1, snapshotErr)
		// Base: 10s, 20s (plus up to 5s jitter)
		assert.GreaterOrEqual(t, d0, 10*time.Second)
		assert.Less(t, d0, 16*time.Second)
		assert.GreaterOrEqual(t, d1, 20*time.Second)
	})

	t.Run("snapshot backoff caps at 60s", func(t *testing.T) {
		snapshotErr := fmt.Errorf("schema snapshot is in progress")
		d10 := retryDelay(10, snapshotErr)
		assert.LessOrEqual(t, d10, 65*time.Second)
	})
}

func TestDiffKeyspace_DetectsSchemaChanges(t *testing.T) {
	e := &Engine{
		linter: lint.New(),
		logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}

	t.Run("matching schemas produce no changes", func(t *testing.T) {
		currentSchema := map[string][]table.TableSchema{
			"myapp": {
				{Name: "users", Schema: "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
			},
		}
		desired := &schema.Namespace{
			Files: map[string]string{
				"users.sql": "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
			},
		}
		changes, _, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema)
		require.NoError(t, err)
		assert.Empty(t, changes, "matching schemas should produce no changes")
	})

	t.Run("missing column detected as ALTER", func(t *testing.T) {
		currentSchema := map[string][]table.TableSchema{
			"myapp": {
				{Name: "users", Schema: "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
			},
		}
		desired := &schema.Namespace{
			Files: map[string]string{
				"users.sql": "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  `phone` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
			},
		}
		changes, _, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema)
		require.NoError(t, err)
		require.Len(t, changes, 1, "should detect one ALTER TABLE change")
		assert.Equal(t, "users", changes[0].Table)
		assert.Contains(t, changes[0].DDL, "phone")
	})

	t.Run("extra column on branch detected as ALTER DROP", func(t *testing.T) {
		currentSchema := map[string][]table.TableSchema{
			"myapp": {
				{Name: "users", Schema: "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  `stale_col` varchar(100) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
			},
		}
		desired := &schema.Namespace{
			Files: map[string]string{
				"users.sql": "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
			},
		}
		changes, _, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema)
		require.NoError(t, err)
		require.Len(t, changes, 1, "should detect DROP COLUMN for stale column")
		assert.Equal(t, "users", changes[0].Table)
		assert.Contains(t, changes[0].DDL, "stale_col")
	})

	t.Run("missing table detected as CREATE", func(t *testing.T) {
		currentSchema := map[string][]table.TableSchema{
			"myapp": {},
		}
		desired := &schema.Namespace{
			Files: map[string]string{
				"users.sql": "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
			},
		}
		changes, _, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema)
		require.NoError(t, err)
		require.Len(t, changes, 1, "should detect CREATE TABLE")
		assert.Equal(t, "users", changes[0].Table)
		assert.Equal(t, statement.StatementCreateTable, changes[0].Operation)
	})
}

func TestIsRetryablePSError(t *testing.T) {
	t.Run("PS SDK ErrRetry is retryable", func(t *testing.T) {
		assert.True(t, isRetryablePSError(&ps.Error{Code: ps.ErrRetry}))
	})

	t.Run("PS SDK ErrInternal is retryable", func(t *testing.T) {
		assert.True(t, isRetryablePSError(&ps.Error{Code: ps.ErrInternal}))
	})

	t.Run("PS SDK ErrResponseMalformed is retryable", func(t *testing.T) {
		assert.True(t, isRetryablePSError(&ps.Error{Code: ps.ErrResponseMalformed}))
	})

	t.Run("PS SDK ErrNotFound is NOT retryable", func(t *testing.T) {
		assert.False(t, isRetryablePSError(&ps.Error{Code: ps.ErrNotFound}))
	})

	t.Run("PS SDK ErrPermission is NOT retryable", func(t *testing.T) {
		assert.False(t, isRetryablePSError(&ps.Error{Code: ps.ErrPermission}))
	})

	t.Run("snapshot in progress is retryable", func(t *testing.T) {
		assert.True(t, isRetryablePSError(fmt.Errorf("schema snapshot is in progress")))
	})

	t.Run("plain error is NOT retryable", func(t *testing.T) {
		assert.False(t, isRetryablePSError(fmt.Errorf("DDL syntax error")))
	})

	t.Run("nil is NOT retryable", func(t *testing.T) {
		assert.False(t, isRetryablePSError(nil))
	})
}
