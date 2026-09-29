//go:build integration

package spirit

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/ui"
)

// directPolicyMetadata is the engine metadata an enabled direct execution
// policy resolves to, as the config layer would deliver it.
func directPolicyMetadata(maxTableRows int64) map[string]string {
	return map[string]string{
		"direct_execution":                "true",
		"direct_execution_max_table_rows": fmt.Sprintf("%d", maxTableRows),
	}
}

// directPolicyMetadataWithBytes is directPolicyMetadata with the optional byte
// bound set as well.
func directPolicyMetadataWithBytes(maxTableRows, maxTableBytes int64) map[string]string {
	md := directPolicyMetadata(maxTableRows)
	md["direct_execution_max_table_bytes"] = fmt.Sprintf("%d", maxTableBytes)
	return md
}

// createSeededPKTable creates a table with a single-column primary key and a
// secondary index, seeds it with the given number of rows, and refreshes its
// statistics so information_schema reports its size.
func createSeededPKTable(t *testing.T, db *sql.DB, table string, rows int) {
	t.Helper()
	dropTablesOnCleanup(t, db, table)
	_, err := db.ExecContext(t.Context(), "CREATE TABLE `"+table+"` (\n"+
		"  id INT NOT NULL AUTO_INCREMENT,\n"+
		"  tenant_id INT NOT NULL,\n"+
		"  note VARCHAR(255) NOT NULL DEFAULT '',\n"+
		"  PRIMARY KEY (id),\n"+
		"  KEY idx_note (note)\n"+
		")")
	require.NoError(t, err, "create %s", table)
	if rows > 0 {
		var inserts strings.Builder
		inserts.WriteString("INSERT INTO `" + table + "` (tenant_id, note) VALUES ")
		for i := range rows {
			if i > 0 {
				inserts.WriteString(",")
			}
			fmt.Fprintf(&inserts, "(1, '%s')", strings.Repeat("n", 200))
		}
		_, err = db.ExecContext(t.Context(), inserts.String())
		require.NoError(t, err, "seed %s", table)
	}
	_, err = db.ExecContext(t.Context(), "ANALYZE TABLE `"+table+"`")
	require.NoError(t, err, "analyze %s", table)
}

// pkReshapeSchema is the declared schema that widens createSeededPKTable's
// primary key, a reshape the engine refuses.
func pkReshapeSchema(table string) string {
	return "CREATE TABLE `" + table + "` (\n" +
		"  id INT NOT NULL AUTO_INCREMENT,\n" +
		"  tenant_id INT NOT NULL,\n" +
		"  note VARCHAR(255) NOT NULL DEFAULT '',\n" +
		"  PRIMARY KEY (id, tenant_id),\n" +
		"  KEY idx_note (note)\n" +
		")"
}

// dropTablesOnCleanup drops the named tables when the test finishes, using a
// context that survives the test context's cancellation so the shared test
// database stays clean for later tests.
func dropTablesOnCleanup(t *testing.T, db *sql.DB, tables ...string) {
	t.Helper()
	cleanupCtx := context.WithoutCancel(t.Context())
	t.Cleanup(func() {
		for _, table := range tables {
			_, err := db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS `"+table+"`")
			assert.NoError(t, err, "drop table %s", table)
		}
	})
}

// pkColumns returns the table's primary-key column names in ordinal order, so
// tests can assert a PK reshape actually landed on the target.
func pkColumns(t *testing.T, database, tableName string) []string {
	t.Helper()
	_, db := setupTestMySQL(t)
	rows, err := db.QueryContext(t.Context(), `
		SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION`, database, tableName)
	require.NoError(t, err, "query PK columns")
	defer func() { _ = rows.Close() }()
	var cols []string
	for rows.Next() {
		var col string
		require.NoError(t, rows.Scan(&col))
		cols = append(cols, col)
	}
	require.NoError(t, rows.Err())
	return cols
}

// With the direct execution policy enabled and the table within the size
// bound, the plan resolves a statement the engine refuses to the direct
// verdict, with the reason carrying the refusal and the measured row count —
// the operator sees exactly what will run natively and why before confirming.
func TestEngine_Plan_DirectVerdictWithinBound(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "direct_plan")

	_, err := db.ExecContext(t.Context(), `CREATE TABLE direct_plan (
		id INT NOT NULL AUTO_INCREMENT,
		tenant_id INT NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create direct_plan table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"direct_plan.sql": `CREATE TABLE direct_plan (
				id INT NOT NULL AUTO_INCREMENT,
				tenant_id INT NOT NULL,
				PRIMARY KEY (id, tenant_id)
			)`,
		}),
		Credentials: &engine.Credentials{
			DSN:      dsn,
			Metadata: directPolicyMetadata(100000),
		},
	})
	require.NoError(t, err, "Plan()")
	require.False(t, result.NoChanges)

	changes := result.FlatTableChanges()
	require.Len(t, changes, 1)
	assert.Contains(t, changes[0].DDL, "DROP PRIMARY KEY")
	assert.Equal(t, "direct", changes[0].ExecutionMode, "the refused statement resolves to the direct verdict")
	assert.Contains(t, changes[0].ModeReason, "dropping primary key is not supported")
	assert.Contains(t, changes[0].ModeReason, "runs as native MySQL DDL on a table with ~")
}

// A table whose row count exceeds max_table_rows keeps the blocked verdict
// even with the policy enabled: the bound is the fail-closed backstop against
// unbounded native rebuilds, and the reason names the configured limit so
// the operator sees why. The reason deliberately omits the measured count so
// the same verdict on every shard of a sharded plan renders as one entry.
func TestEngine_Plan_DirectBlockedAboveBound(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "direct_bound")

	_, err := db.ExecContext(t.Context(), `CREATE TABLE direct_bound (
		id INT NOT NULL AUTO_INCREMENT,
		tenant_id INT NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create direct_bound table")

	// Seed enough rows that the optimizer's estimate is safely above the tiny
	// bound, then refresh statistics so information_schema reflects them.
	var inserts strings.Builder
	inserts.WriteString("INSERT INTO direct_bound (tenant_id) VALUES (1)")
	for range 1999 {
		inserts.WriteString(",(1)")
	}
	_, err = db.ExecContext(t.Context(), inserts.String())
	require.NoError(t, err, "seed rows")
	_, err = db.ExecContext(t.Context(), "ANALYZE TABLE `direct_bound`")
	require.NoError(t, err, "analyze table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"direct_bound.sql": `CREATE TABLE direct_bound (
				id INT NOT NULL AUTO_INCREMENT,
				tenant_id INT NOT NULL,
				PRIMARY KEY (id, tenant_id)
			)`,
		}),
		Credentials: &engine.Credentials{
			DSN:      dsn,
			Metadata: directPolicyMetadata(10),
		},
	})
	require.NoError(t, err, "Plan()")

	changes := result.FlatTableChanges()
	require.Len(t, changes, 1)
	assert.Equal(t, "blocked", changes[0].ExecutionMode, "a table above the bound stays blocked")
	assert.Contains(t, changes[0].ModeReason, "dropping primary key is not supported")
	assert.Contains(t, changes[0].ModeReason, "above the configured limit of 10")
}

// A malformed direct execution policy fails the plan instead of silently
// disabling direct execution: enabling the policy without a size bound must
// surface as a config error, never as a mode downgrade.
func TestEngine_Plan_MalformedDirectPolicyFails(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "direct_malformed")

	_, err := db.ExecContext(t.Context(), `CREATE TABLE direct_malformed (
		id INT NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create direct_malformed table")

	eng := New(Config{})

	_, err = eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"direct_malformed.sql": `CREATE TABLE direct_malformed (
				id INT NOT NULL AUTO_INCREMENT,
				body TEXT,
				PRIMARY KEY (id)
			)`,
		}),
		Credentials: &engine.Credentials{
			DSN:      dsn,
			Metadata: map[string]string{"direct_execution": "true"},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "direct_execution_max_table_rows is not set")
}

// With direct execution enabled but the target unreachable, the verdict fails
// closed to blocked: no connection means no size gate, and an unmeasured
// table must never rebuild natively. The reason carries both the refusal and
// why direct execution declined it.
func TestResolveRefusedMode_UnreachableTargetBlocks(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	target := &lazyTargetDB{dsn: "root:nopass@tcp(127.0.0.1:1)/absent"}
	defer target.close()

	policy := directPolicy{Enabled: true, MaxTableRows: 1000}
	decision := eng.resolveRefusedMode(ctx, target, policy, "absent", "users", "dropping primary key is not supported")
	assert.Equal(t, engine.ExecutionModeBlocked, decision.mode)
	assert.Contains(t, decision.modeReason, "dropping primary key is not supported")
	assert.Contains(t, decision.modeReason, "size is unavailable")
}

// The size gate fails closed on every input it cannot measure: a table
// missing from information_schema, and a view — which is not a base table and
// has no size statistics — both resolve to errors instead of a zero size that
// would slip under any bound, whether or not the policy sets a byte bound.
func TestMeasureTableSize_FailClosedInputs(t *testing.T) {
	_, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "size_gate_base")

	_, err := db.ExecContext(t.Context(), `CREATE TABLE size_gate_base (
		id INT NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create size_gate_base table")

	_, err = db.ExecContext(t.Context(), "CREATE VIEW `size_gate_view` AS SELECT `id` FROM `size_gate_base`")
	require.NoError(t, err, "create size_gate_view")
	cleanupCtx := context.WithoutCancel(t.Context())
	t.Cleanup(func() {
		_, err := db.ExecContext(cleanupCtx, "DROP VIEW IF EXISTS `size_gate_view`")
		assert.NoError(t, err, "drop view size_gate_view")
	})

	policies := map[string]directPolicy{
		"row bound only":      {Enabled: true, MaxTableRows: 1000},
		"row and byte bounds": {Enabled: true, MaxTableRows: 1000, MaxTableBytes: 100 << 20},
	}
	for name, policy := range policies {
		t.Run(name+"/missing table", func(t *testing.T) {
			_, err := measureTableSize(t.Context(), db, "testdb", "size_gate_absent", policy)
			require.Error(t, err)
			assert.Equal(t, "table `testdb`.`size_gate_absent` not found in information_schema", err.Error())
		})
		t.Run(name+"/view has no statistics", func(t *testing.T) {
			_, err := measureTableSize(t.Context(), db, "testdb", "size_gate_view", policy)
			require.Error(t, err)
			assert.Equal(t, "table `testdb`.`size_gate_view` not found in information_schema", err.Error())
		})
	}
}

// One statistics read returns every figure the size gate and the plan's size
// display need, for several tables at once: the row estimate and the data and
// index footprints. A table that is not a base table, or does not exist, is
// absent from the result rather than reported as zero.
func TestReadTableStatistics(t *testing.T) {
	_, db := setupTestMySQL(t)
	createSeededPKTable(t, db, "stats_small", 0)
	createSeededPKTable(t, db, "stats_seeded", 2000)

	stats, err := readTableStatistics(t.Context(), db, "testdb", []string{"stats_small", "stats_seeded", "stats_absent"})
	require.NoError(t, err)
	require.Len(t, stats, 2, "only the two base tables that exist are reported")

	small, seeded := stats["stats_small"], stats["stats_seeded"]
	for name, s := range map[string]tableStatistics{"stats_small": small, "stats_seeded": seeded} {
		assert.True(t, s.rows.Valid, "%s TABLE_ROWS", name)
		assert.True(t, s.dataBytes.Valid, "%s DATA_LENGTH", name)
		assert.True(t, s.indexBytes.Valid, "%s INDEX_LENGTH", name)
		assert.Positive(t, s.dataBytes.Int64, "%s: even an empty InnoDB table allocates a clustered-index page", name)
		assert.Positive(t, s.indexBytes.Int64, "%s: the secondary index allocates a page", name)
	}
	// TABLE_ROWS is an estimate; allow statistics slop around the seeded count.
	assert.InDelta(t, 2000, float64(seeded.rows.Int64), 500)
	assert.Greater(t, seeded.dataBytes.Int64, small.dataBytes.Int64, "seeded rows grow the clustered index")
	assert.Greater(t, seeded.indexBytes.Int64, small.indexBytes.Int64, "seeded rows grow the secondary index")

	empty, err := readTableStatistics(t.Context(), db, "testdb", nil)
	require.NoError(t, err)
	assert.Empty(t, empty, "no tables means no query and no statistics")
}

// With a byte bound set alongside the row bound, a table within both still
// resolves to the direct verdict, and the reason reports the measured data
// and index size next to the row count so the operator sees how large the
// table the native rebuild touches is.
func TestEngine_Plan_DirectVerdictWithinByteBound(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	createSeededPKTable(t, db, "direct_bytes_ok", 500)

	stats, err := readTableStatistics(t.Context(), db, "testdb", []string{"direct_bytes_ok"})
	require.NoError(t, err)
	measured := stats["direct_bytes_ok"].dataBytes.Int64 + stats["direct_bytes_ok"].indexBytes.Int64

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database:    "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{"direct_bytes_ok.sql": pkReshapeSchema("direct_bytes_ok")}),
		Credentials: &engine.Credentials{
			DSN:      dsn,
			Metadata: directPolicyMetadataWithBytes(100000, 100<<20),
		},
	})
	require.NoError(t, err, "Plan()")

	changes := result.FlatTableChanges()
	require.Len(t, changes, 1)
	assert.Equal(t, engine.ExecutionModeDirect, changes[0].ExecutionMode, "a table within both bounds resolves to direct")
	assert.Contains(t, changes[0].ModeReason, "dropping primary key is not supported")
	assert.Contains(t, changes[0].ModeReason, "; runs as native MySQL DDL on a table with ~500 rows and "+
		ui.FormatApproxBytes(measured)+" of data and indexes")
}

// A table within the row bound but above the byte bound is blocked: the two
// bounds combine, and a table must be within both to run directly. The
// reason names only the configured byte limit, never the measured size, so
// two tables of different sizes (or the same table on different shards)
// render one identical reason and collapse into one row in the PR summary.
func TestEngine_Plan_DirectBlockedAboveByteBound(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	createSeededPKTable(t, db, "direct_bytes_empty", 0)
	createSeededPKTable(t, db, "direct_bytes_big", 2000)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"direct_bytes_empty.sql": pkReshapeSchema("direct_bytes_empty"),
			"direct_bytes_big.sql":   pkReshapeSchema("direct_bytes_big"),
		}),
		Credentials: &engine.Credentials{
			DSN: dsn,
			// The row bound admits both tables; only the byte bound blocks.
			Metadata: directPolicyMetadataWithBytes(100000, 1<<10),
		},
	})
	require.NoError(t, err, "Plan()")

	changes := result.FlatTableChanges()
	require.Len(t, changes, 2)
	const wantSuffix = "; direct execution is enabled but the table is above the configured limit of 1.0 KiB of data and indexes"
	for _, change := range changes {
		assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode, "%s is above the byte bound", change.Table)
		assert.Contains(t, change.ModeReason, "dropping primary key is not supported", change.Table)
		assert.True(t, strings.HasSuffix(change.ModeReason, wantSuffix), "%s reason %q names only the configured limit", change.Table, change.ModeReason)
	}
	assert.Equal(t, changes[0].ModeReason, changes[1].ModeReason, "tables of different sizes render one identical reason")
}

// An apply re-evaluates the byte bound at routing time, so a table that is
// above it when the apply runs fails before anything executes and is never
// rebuilt natively, even though its row count is within the row bound.
func TestEngine_ExecuteAlterPhase_AboveByteBoundFailsFast(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	createSeededPKTable(t, db, "direct_bytes_grew", 2000)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"direct_bytes_grew"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database,
		[]string{"ALTER TABLE `direct_bytes_grew` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)"}, false,
		directPolicy{Enabled: true, MaxTableRows: 100000, MaxTableBytes: 1 << 10})

	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	errorMessage := eng.runningSchemaChange.errorMessage
	eng.mu.Unlock()

	assert.Equal(t, engine.StateFailed, finalState)
	assert.Contains(t, errorMessage, "above the configured limit of 1.0 KiB of data and indexes")
	assert.Equal(t, []string{"id"}, pkColumns(t, database, "direct_bytes_grew"), "the target is untouched")
}

// A policy with a byte bound blocks a table whose size cannot be measured,
// exactly as the row bound does: the byte bound never turns an unmeasured
// table into an approval.
func TestEngine_ResolveRefusedMode_UnknownSizeBlockedWithByteBound(t *testing.T) {
	dsn, _ := setupTestMySQL(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	target := &lazyTargetDB{dsn: targetDSN(host, username, password, database)}
	defer target.close()

	decision := eng.resolveRefusedMode(t.Context(), target,
		directPolicy{Enabled: true, MaxTableRows: 100000, MaxTableBytes: 100 << 20},
		database, "direct_bytes_missing", "dropping primary key is not supported")
	assert.Equal(t, engine.ExecutionModeBlocked, decision.mode)
	assert.Equal(t, "blocked_size_unknown", decision.outcome)
	assert.Equal(t, "dropping primary key is not supported; direct execution is enabled but the table's size is unavailable", decision.modeReason)
}

// The exact bounded count confirms a direct verdict without trusting the
// optimizer's statistics: it counts real rows but never scans past the
// configured bound, and a missing table is an error rather than a zero count.
func TestExactRowCountWithin(t *testing.T) {
	_, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "exact_count")

	_, err := db.ExecContext(t.Context(), `CREATE TABLE exact_count (
		id INT NOT NULL AUTO_INCREMENT,
		tenant_id INT NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create exact_count table")

	var inserts strings.Builder
	inserts.WriteString("INSERT INTO exact_count (tenant_id) VALUES (1)")
	for range 19 {
		inserts.WriteString(",(1)")
	}
	_, err = db.ExecContext(t.Context(), inserts.String())
	require.NoError(t, err, "seed rows")

	count, err := exactRowCountWithin(t.Context(), db, "testdb", "exact_count", 100)
	require.NoError(t, err)
	assert.Equal(t, int64(20), count, "a table within the bound reports its exact count")

	count, err = exactRowCountWithin(t.Context(), db, "testdb", "exact_count", 10)
	require.NoError(t, err)
	assert.Equal(t, int64(11), count, "a table above the bound reports limit+1: the scan stops at the cap")

	_, err = exactRowCountWithin(t.Context(), db, "testdb", "exact_count_absent", 10)
	require.Error(t, err, "a missing table is an error, never a zero count")
}

// With the policy enabled, an apply routes a statement the engine refuses —
// a primary-key reshape — to direct execution and drives it to completion:
// the PK actually changes on the target, the schema change ends completed,
// and progress reports the statement as a completed direct entry.
func TestEngine_ExecuteAlterPhase_DirectApply(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "direct_apply")

	_, err := db.ExecContext(t.Context(), `CREATE TABLE direct_apply (
		id INT NOT NULL AUTO_INCREMENT,
		tenant_id INT NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create direct_apply table")
	for i := range 10 {
		_, err := db.ExecContext(t.Context(), `INSERT INTO direct_apply (tenant_id) VALUES (?)`, i)
		require.NoError(t, err, "insert data")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	ddlStatements := []string{
		"ALTER TABLE `direct_apply` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)",
	}

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"direct_apply"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database, ddlStatements, false,
		directPolicy{Enabled: true, MaxTableRows: 100000})

	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	eng.mu.Unlock()
	require.Equal(t, engine.StateCompleted, finalState)

	assert.Equal(t, []string{"id", "tenant_id"}, pkColumns(t, database, "direct_apply"),
		"the reshaped primary key landed on the target")

	var rowCount int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM direct_apply`).Scan(&rowCount))
	assert.Equal(t, 10, rowCount, "data survives the native rebuild")

	progress, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
	require.NoError(t, err, "Progress()")
	var directEntry *engine.TableProgress
	for i, tp := range progress.Tables {
		if tp.ProgressDetail == "direct execution (native MySQL DDL)" {
			directEntry = &progress.Tables[i]
		}
	}
	require.NotNil(t, directEntry, "progress reports the direct statement")
	assert.Equal(t, "direct_apply", directEntry.Table)
	assert.Equal(t, "completed", directEntry.State)
	assert.NotNil(t, directEntry.CompletedAt)
}

// A single apply carrying both a refused reshape and an ordinary
// engine-driven statement routes each to its own path: the reshape runs as
// native DDL, the engine applies the rest, and both land on the target.
func TestEngine_ExecuteAlterPhase_MixedRouting(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "mixed_direct", "mixed_spirit")

	_, err := db.ExecContext(t.Context(), `CREATE TABLE mixed_direct (
		id INT NOT NULL AUTO_INCREMENT,
		tenant_id INT NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create mixed_direct table")
	_, err = db.ExecContext(t.Context(), `CREATE TABLE mixed_spirit (
		id INT NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create mixed_spirit table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	ddlStatements := []string{
		"ALTER TABLE `mixed_direct` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)",
		"ALTER TABLE `mixed_spirit` ADD COLUMN `email` varchar(255) NULL",
	}

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"mixed_direct", "mixed_spirit"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database, ddlStatements, false,
		directPolicy{Enabled: true, MaxTableRows: 100000})

	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	eng.mu.Unlock()
	require.Equal(t, engine.StateCompleted, finalState)

	assert.Equal(t, []string{"id", "tenant_id"}, pkColumns(t, database, "mixed_direct"),
		"the direct-routed reshape landed")

	var columnCount int
	require.NoError(t, db.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'mixed_spirit' AND COLUMN_NAME = 'email'`,
		database).Scan(&columnCount))
	assert.Equal(t, 1, columnCount, "the engine-driven ADD COLUMN landed")
}

// Without the policy, an apply carrying a refused statement fails fast before
// any engine work starts: the schema change ends failed with a reason naming
// the table and pointing at the disabled policy, and the target is untouched.
func TestEngine_ExecuteAlterPhase_PolicyOffFailsFast(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "direct_off")

	_, err := db.ExecContext(t.Context(), `CREATE TABLE direct_off (
		id INT NOT NULL AUTO_INCREMENT,
		tenant_id INT NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create direct_off table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	ddlStatements := []string{
		"ALTER TABLE `direct_off` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)",
	}

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"direct_off"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database, ddlStatements, false, directPolicy{})

	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	errorMessage := eng.runningSchemaChange.errorMessage
	eng.mu.Unlock()

	assert.Equal(t, engine.StateFailed, finalState)
	assert.Contains(t, errorMessage, "direct_off")
	assert.Contains(t, errorMessage, "direct execution is not enabled")

	assert.Equal(t, []string{"id"}, pkColumns(t, database, "direct_off"), "the target is untouched")
}

// An apply whose refused statement exceeds the size bound fails fast at
// routing time, re-evaluating the plan-time predicate so a table that grew
// past the bound between plan and apply never rebuilds natively.
func TestEngine_ExecuteAlterPhase_AboveBoundFailsFast(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "direct_grew")

	_, err := db.ExecContext(t.Context(), `CREATE TABLE direct_grew (
		id INT NOT NULL AUTO_INCREMENT,
		tenant_id INT NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create direct_grew table")

	var inserts strings.Builder
	inserts.WriteString("INSERT INTO direct_grew (tenant_id) VALUES (1)")
	for range 1999 {
		inserts.WriteString(",(1)")
	}
	_, err = db.ExecContext(t.Context(), inserts.String())
	require.NoError(t, err, "seed rows")
	_, err = db.ExecContext(t.Context(), "ANALYZE TABLE `direct_grew`")
	require.NoError(t, err, "analyze table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	ddlStatements := []string{
		"ALTER TABLE `direct_grew` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)",
	}

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"direct_grew"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database, ddlStatements, false,
		directPolicy{Enabled: true, MaxTableRows: 10})

	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	errorMessage := eng.runningSchemaChange.errorMessage
	eng.mu.Unlock()

	assert.Equal(t, engine.StateFailed, finalState)
	assert.Contains(t, errorMessage, "above the configured limit of 10")
	assert.Equal(t, []string{"id"}, pkColumns(t, database, "direct_grew"), "the target is untouched")
}

// A direct statement never stalls behind a busy table: when an open
// transaction holds the table's metadata lock, the session's bounded lock
// wait expires and the apply fails fast with an operator-actionable
// busy-table error — instead of queueing on the lock indefinitely with all
// new table traffic queueing behind the DDL. The bound comes from the
// policy's configured lock wait, and the failure message reports that
// configured value.
func TestEngine_ExecuteAlterPhase_BusyTableFailsFast(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "direct_busy")

	_, err := db.ExecContext(t.Context(), `CREATE TABLE direct_busy (
		id INT NOT NULL AUTO_INCREMENT,
		tenant_id INT NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create direct_busy table")
	_, err = db.ExecContext(t.Context(), `INSERT INTO direct_busy (tenant_id) VALUES (1)`)
	require.NoError(t, err, "insert data")

	// Hold the table's metadata lock: a transaction that has read the table
	// keeps its shared MDL until the transaction ends, so the ALTER's
	// exclusive MDL request queues behind it for as long as it stays open.
	holder, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err, "begin lock-holding transaction")
	defer func() { _ = holder.Rollback() }()
	var id int
	require.NoError(t, holder.QueryRowContext(t.Context(),
		"SELECT `id` FROM `direct_busy` LIMIT 1 FOR UPDATE").Scan(&id), "acquire the metadata lock")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	ddlStatements := []string{
		"ALTER TABLE `direct_busy` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)",
	}

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"direct_busy"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	// The bounded session lock wait must fail the statement well inside this
	// deadline; if it expires instead, the schema change ends stopped rather
	// than failed and the assertions below diagnose the stall.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	const lockWaitSeconds = 2
	eng.executeSchemaChange(ctx, host, username, password, database, ddlStatements, false,
		directPolicy{Enabled: true, MaxTableRows: 100000, LockAcquisitionTimeoutSeconds: lockWaitSeconds})

	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	errorMessage := eng.runningSchemaChange.errorMessage
	eng.mu.Unlock()

	assert.Equal(t, engine.StateFailed, finalState)
	assert.Contains(t, errorMessage, `Table "direct_busy" is busy`)
	assert.Contains(t, errorMessage, fmt.Sprintf("could not acquire the metadata lock within %ds", lockWaitSeconds))
	assert.Contains(t, errorMessage, "Retry when long-running transactions on the table have finished")
	assert.NotContains(t, errorMessage, "Lock wait timeout exceeded",
		"the driver's own words are for the server log, not the pull request")

	require.NoError(t, holder.Rollback(), "release the metadata lock")
	assert.Equal(t, []string{"id"}, pkColumns(t, database, "direct_busy"), "the target is untouched")
}

// Routing classifies each statement against its table's current definition,
// because some of the engine's refusals depend on the existing column types. A
// table whose definition cannot be read fails the apply before any statement
// runs: classifying without it would narrow the refusals the engine reports and
// hand a refused statement to the engine anyway. The failure names the table but
// not the database's answer — that error reaches a PR comment, so the driver and
// target detail it carries stays in the logs.
func TestEngine_RouteAlterStatements_UnreadableTableBlocked(t *testing.T) {
	dsn, _ := setupTestMySQL(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	target := &lazyTargetDB{dsn: targetDSN(host, username, password, database)}
	defer target.close()

	_, err = eng.routeAlterStatements(t.Context(), target, database,
		[]string{"ALTER TABLE `direct_missing` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)"},
		directPolicy{Enabled: true, MaxTableRows: 100000})
	require.Error(t, err)

	// What reaches the pull request is the marked operator message, and it
	// carries none of the target's answer.
	operatorMsg, marked := engine.OperatorMessageOf(err)
	require.True(t, marked, "the routing failure is not safe to render: %v", err)
	assert.Contains(t, operatorMsg, "could not read the current definition of table \"direct_missing\"")
	assert.Contains(t, operatorMsg, "see the server logs")
	assert.NotContains(t, operatorMsg, "SHOW CREATE TABLE", "the raw query and driver error belong in the logs")
	assert.NotContains(t, operatorMsg, "Error 1146", "the raw query and driver error belong in the logs")

	// The error itself keeps the cause, because the logs are where an operator
	// goes to find out which query failed and what the target said.
	assert.Contains(t, err.Error(), "SHOW CREATE TABLE")
	assert.Contains(t, err.Error(), "Error 1146")
}

// A refused statement whose table size cannot be estimated is blocked: an
// unestimated table must never rebuild natively, so the size gate resolves to
// the blocked mode with the unavailable-estimate reason instead of permitting
// the statement to run directly.
func TestEngine_ResolveRefusedMode_UnknownSizeBlocked(t *testing.T) {
	dsn, _ := setupTestMySQL(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	target := &lazyTargetDB{dsn: targetDSN(host, username, password, database)}
	defer target.close()

	decision := eng.resolveRefusedMode(t.Context(), target, directPolicy{Enabled: true, MaxTableRows: 100000},
		database, "direct_missing", "dropping primary key is not supported")
	assert.Equal(t, engine.ExecutionModeBlocked, decision.mode)
	assert.Equal(t, "blocked_size_unknown", decision.outcome)
	assert.Contains(t, decision.modeReason, "dropping primary key is not supported")
	assert.Contains(t, decision.modeReason, "size is unavailable")
}

// An engine that plans a schema change itself and drives this engine against
// each target records the verdict through ExecutionVerdicts. For the same
// statement on the same target, that verdict matches what Plan records, both
// when the policy routes the refused statement to direct execution and when
// it leaves the statement blocked. A reviewer of such an engine's plan
// therefore sees what the apply will actually do.
func TestExecutionVerdicts_RecordMatchesPlan(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "direct_verdicts")
	_, err := db.ExecContext(t.Context(), `CREATE TABLE direct_verdicts (
		id INT NOT NULL AUTO_INCREMENT,
		tenant_id INT NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create direct_verdicts table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})

	for _, tc := range []struct {
		name     string
		metadata map[string]string
		mode     string
	}{
		{name: "policy enabled within bound", metadata: directPolicyMetadata(100000), mode: engine.ExecutionModeDirect},
		{name: "policy disabled", metadata: nil, mode: engine.ExecutionModeBlocked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			creds := &engine.Credentials{DSN: dsn, Metadata: tc.metadata}
			result, err := eng.Plan(t.Context(), &engine.PlanRequest{
				Database: "testdb",
				SchemaFiles: testSchemaFiles(map[string]string{
					"direct_verdicts.sql": `CREATE TABLE direct_verdicts (
						id INT NOT NULL AUTO_INCREMENT,
						tenant_id INT NOT NULL,
						PRIMARY KEY (id, tenant_id)
					)`,
				}),
				Credentials: creds,
			})
			require.NoError(t, err, "Plan()")
			planned := result.FlatTableChanges()
			require.Len(t, planned, 1)
			require.Equal(t, tc.mode, planned[0].ExecutionMode)

			verdicts, err := eng.NewExecutionVerdicts(creds)
			require.NoError(t, err)
			defer verdicts.Close()
			change := engine.TableChange{Table: planned[0].Table, Operation: planned[0].Operation, DDL: planned[0].DDL}
			require.NoError(t, verdicts.Record(t.Context(), &change))
			assert.Equal(t, planned[0].ExecutionMode, change.ExecutionMode)
			assert.Equal(t, planned[0].ModeReason, change.ModeReason)
			assert.Contains(t, change.ModeReason, "dropping primary key is not supported")
		})
	}
}

// A statement the engine runs on its default path gets no verdict: an ALTER
// the engine accepts is recorded with an empty mode. Record sets the whole
// verdict, so a mode the change already carried does not survive it.
func TestExecutionVerdicts_AcceptedAlterHasNoVerdict(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "direct_accepted")
	_, err := db.ExecContext(t.Context(), "CREATE TABLE direct_accepted (id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (id))")
	require.NoError(t, err, "create direct_accepted table")

	verdicts, err := New(Config{}).NewExecutionVerdicts(&engine.Credentials{DSN: dsn, Metadata: directPolicyMetadata(100000)})
	require.NoError(t, err)
	defer verdicts.Close()
	change := engine.TableChange{
		Table:         "direct_accepted",
		Operation:     ddl.StatementAlterTable,
		DDL:           "ALTER TABLE `direct_accepted` ADD COLUMN `note` varchar(64)",
		ExecutionMode: engine.ExecutionModeBlocked,
		ModeReason:    "stale",
	}
	require.NoError(t, verdicts.Record(t.Context(), &change))
	assert.Empty(t, change.ExecutionMode)
	assert.Empty(t, change.ModeReason)
}

// The verdict follows the statement the apply will run, not the Operation the
// caller labelled it with. A caller that plans the statement itself and leaves
// Operation unset still learns that the engine refuses it, so the plan does not
// admit an apply the engine then refuses.
func TestExecutionVerdicts_JudgesTheStatementNotItsOperation(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "direct_unlabelled")
	_, err := db.ExecContext(t.Context(), "CREATE TABLE direct_unlabelled (id INT NOT NULL, tenant_id INT NOT NULL, PRIMARY KEY (id))")
	require.NoError(t, err, "create direct_unlabelled table")

	verdicts, err := New(Config{}).NewExecutionVerdicts(&engine.Credentials{DSN: dsn})
	require.NoError(t, err)
	defer verdicts.Close()
	change := engine.TableChange{
		Table: "direct_unlabelled",
		DDL:   "ALTER TABLE `direct_unlabelled` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)",
	}
	require.Equal(t, ddl.StatementUnknown, change.Operation)
	require.NoError(t, verdicts.Record(t.Context(), &change))
	assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode)
	assert.Contains(t, change.ModeReason, "dropping primary key is not supported")
}

// An ALTER for a table the target cannot describe fails rather than recording
// no verdict: the refusal check needs the table's current definition, and
// judging without it would report the default path for a statement the apply
// might refuse.
func TestExecutionVerdicts_UnreadableTableFails(t *testing.T) {
	dsn, _ := setupTestMySQL(t)
	verdicts, err := New(Config{}).NewExecutionVerdicts(&engine.Credentials{DSN: dsn})
	require.NoError(t, err)
	defer verdicts.Close()
	change := engine.TableChange{
		Table:         "direct_absent",
		Operation:     ddl.StatementAlterTable,
		DDL:           "ALTER TABLE `direct_absent` DROP PRIMARY KEY",
		ExecutionMode: engine.ExecutionModeDirect,
		ModeReason:    "stale",
	}
	err = verdicts.Record(t.Context(), &change)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `execution verdict for table "direct_absent"`)
	assert.Contains(t, err.Error(), "read current definition")
	assert.Empty(t, change.ExecutionMode)
	assert.Empty(t, change.ModeReason)
}
