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

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/utils"
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

// byteBoundPolicyMetadata is the engine metadata of an enabled policy bounded
// by the table's data plus index size instead of its row count.
func byteBoundPolicyMetadata(maxTableBytes int64) map[string]string {
	return map[string]string{
		"direct_execution":                 "true",
		"direct_execution_max_table_bytes": fmt.Sprintf("%d", maxTableBytes),
	}
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
			_, err := db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS "+sqlescape.EscapeIdentifier(table))
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
	assert.Regexp(t, `^the table has ~\d+ rows$`, changes[0].ModeReason,
		"a direct verdict states the table's size, not the refusal it runs past")
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
	assert.Contains(t, err.Error(), "neither direct_execution_max_table_rows nor direct_execution_max_table_bytes is set")
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
		"row bound only":  {Enabled: true, MaxTableRows: 1000},
		"byte bound only": {Enabled: true, MaxTableBytes: 100 << 20},
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

// planPKReshape plans a primary-key reshape of each named table under the
// given policy metadata and returns the table changes keyed by table.
func planPKReshape(t *testing.T, dsn string, metadata map[string]string, tables ...string) map[string]engine.TableChange {
	t.Helper()
	files := make(map[string]string, len(tables))
	for _, table := range tables {
		files[table+".sql"] = pkReshapeSchema(table)
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})
	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database:    "testdb",
		SchemaFiles: testSchemaFiles(files),
		Credentials: &engine.Credentials{DSN: dsn, Metadata: metadata},
	})
	require.NoError(t, err, "Plan()")
	changes := make(map[string]engine.TableChange, len(tables))
	for _, change := range result.FlatTableChanges() {
		changes[change.Table] = change
	}
	require.Len(t, changes, len(tables))
	return changes
}

// measuredBytes is the data plus index footprint the size gate reads for a
// table.
func measuredBytes(t *testing.T, db *sql.DB, table string) int64 {
	t.Helper()
	stats, err := readTableStatistics(t.Context(), db, "testdb", []string{table})
	require.NoError(t, err)
	require.Contains(t, stats, table)
	return stats[table].dataBytes.Int64 + stats[table].indexBytes.Int64
}

// Under a byte-bound policy, a table within the bound resolves to the direct
// verdict, and the reason reports the measured data and index size so the
// operator sees how large the table the native rebuild touches is.
func TestEngine_Plan_DirectVerdictWithinByteBound(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	createSeededPKTable(t, db, "direct_bytes_only", 500)
	measured := measuredBytes(t, db, "direct_bytes_only")

	change := planPKReshape(t, dsn, byteBoundPolicyMetadata(100<<20), "direct_bytes_only")["direct_bytes_only"]

	assert.Equal(t, engine.ExecutionModeDirect, change.ExecutionMode)
	assert.Equal(t, "the table has "+ui.FormatApproxBytes(measured)+" of data and indexes", change.ModeReason)
}

// A table above the byte bound is blocked. The reason names only the
// configured limit, never the measured size, so two tables of different sizes
// (or the same table on different shards) render one identical reason and
// collapse into one row in the PR summary.
func TestEngine_Plan_DirectBlockedAboveByteBound(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	createSeededPKTable(t, db, "direct_bytes_mid", 1500)
	createSeededPKTable(t, db, "direct_bytes_big", 2000)

	changes := planPKReshape(t, dsn, byteBoundPolicyMetadata(1<<10), "direct_bytes_mid", "direct_bytes_big")

	const wantSuffix = "; direct execution is enabled but the table is above the configured limit of 1.0 KiB of data and indexes"
	for table, change := range changes {
		assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode, "%s is above the byte bound", table)
		assert.Contains(t, change.ModeReason, "dropping primary key is not supported", table)
		assert.True(t, strings.HasSuffix(change.ModeReason, wantSuffix), "%s reason %q names only the configured limit", table, change.ModeReason)
	}
	assert.Equal(t, changes["direct_bytes_mid"].ModeReason, changes["direct_bytes_big"].ModeReason,
		"tables of different sizes render one identical reason")
}

// runPKReshapeApply drives a direct-execution apply of a primary-key reshape
// on table under policy and returns the final state and error message.
func runPKReshapeApply(t *testing.T, dsn, table string, policy directPolicy) (engine.State, string) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{table},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database,
		[]string{"ALTER TABLE `" + table + "` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)"}, false, policy)

	eng.mu.Lock()
	defer eng.mu.Unlock()
	return eng.runningSchemaChange.state, eng.runningSchemaChange.errorMessage
}

// An apply under a byte-bound policy re-evaluates the bound at routing time.
// A table within it runs directly whatever its row count, and the reshaped
// primary key lands on the target.
func TestEngine_ExecuteAlterPhase_ByteBoundApprovesDirectApply(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	createSeededPKTable(t, db, "direct_bytes_apply", 2000)

	state, errorMessage := runPKReshapeApply(t, dsn, "direct_bytes_apply",
		directPolicy{Enabled: true, MaxTableBytes: 100 << 20})

	require.Equal(t, engine.StateCompleted, state, "error message: %s", errorMessage)
	assert.Equal(t, []string{"id", "tenant_id"}, pkColumns(t, "testdb", "direct_bytes_apply"),
		"the reshaped primary key landed on the target")
}

// A table above the byte bound when the apply runs fails before anything
// executes and is never rebuilt natively.
func TestEngine_ExecuteAlterPhase_AboveByteBoundFailsFast(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	createSeededPKTable(t, db, "direct_bytes_grew", 2000)

	state, errorMessage := runPKReshapeApply(t, dsn, "direct_bytes_grew",
		directPolicy{Enabled: true, MaxTableBytes: 1 << 10})

	assert.Equal(t, engine.StateFailed, state)
	assert.Contains(t, errorMessage, "above the configured limit of 1.0 KiB of data and indexes")
	assert.Equal(t, []string{"id"}, pkColumns(t, "testdb", "direct_bytes_grew"), "the target is untouched")
}

// A byte-bound policy blocks a table whose size cannot be measured, exactly
// as a row-bound policy does: neither turns an unmeasured table into an
// approval.
func TestEngine_ResolveRefusedMode_UnknownSizeBlockedWithByteBound(t *testing.T) {
	dsn, _ := setupTestMySQL(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	target := &lazyTargetDB{dsn: targetDSN(host, username, password, database)}
	defer target.close()

	decision := eng.resolveRefusedMode(t.Context(), target, directPolicy{Enabled: true, MaxTableBytes: 100 << 20},
		database, "direct_bytes_missing", "dropping primary key is not supported")
	assert.Equal(t, engine.ExecutionModeBlocked, decision.mode)
	assert.Equal(t, "blocked_size_unknown", decision.outcome)
	assert.Equal(t, "dropping primary key is not supported; direct execution is enabled but the table's size is unavailable", decision.modeReason)
}

// A row-bound policy blocks when the exact row count fails, even though the
// estimate is within the bound: the estimate never approves on its own. Here
// the target account can see the table's statistics but cannot read its rows.
func TestEngine_ResolveRefusedMode_FailedRowCountBlocks(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	createSeededPKTable(t, db, "direct_count_denied", 100)

	const user, password = "direct_insert_only", "insert-only-pw"
	_, err := db.ExecContext(t.Context(), "CREATE USER IF NOT EXISTS '"+user+"'@'%' IDENTIFIED BY '"+password+"'")
	require.NoError(t, err, "create insert-only user")
	cleanupCtx := context.WithoutCancel(t.Context())
	t.Cleanup(func() {
		_, err := db.ExecContext(cleanupCtx, "DROP USER IF EXISTS '"+user+"'@'%'")
		assert.NoError(t, err, "drop insert-only user")
	})
	_, err = db.ExecContext(t.Context(), "GRANT INSERT ON `testdb`.`direct_count_denied` TO '"+user+"'@'%'")
	require.NoError(t, err, "grant insert on direct_count_denied")
	// The kill that direct execution relies on reads performance_schema and
	// innodb_trx; the grants keep the byte-bound control below from blocking
	// on those instead.
	_, err = db.ExecContext(t.Context(), "GRANT SELECT ON `performance_schema`.* TO '"+user+"'@'%'")
	require.NoError(t, err, "grant select on performance_schema")
	_, err = db.ExecContext(t.Context(), "GRANT PROCESS ON *.* TO '"+user+"'@'%'")
	require.NoError(t, err, "grant process")

	host, _, _, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")
	target := &lazyTargetDB{dsn: targetDSN(host, user, password, database)}
	defer target.close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})

	decision := eng.resolveRefusedMode(t.Context(), target,
		directPolicy{Enabled: true, MaxTableRows: 100000},
		database, "direct_count_denied", "dropping primary key is not supported")
	assert.Equal(t, engine.ExecutionModeBlocked, decision.mode)
	assert.Equal(t, "blocked_size_unknown", decision.outcome)
	assert.Equal(t, "dropping primary key is not supported; direct execution is enabled but the table's size is unavailable", decision.modeReason)

	approved := eng.resolveRefusedMode(t.Context(), target,
		directPolicy{Enabled: true, MaxTableBytes: 100 << 20},
		database, "direct_count_denied", "dropping primary key is not supported")
	assert.Equal(t, engine.ExecutionModeDirect, approved.mode,
		"the same account's statistics approve the table under a byte bound, so the block above is the failed count")
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

// The exact bounded count counts the table a name containing a backtick
// names: a legal name is not a syntax error, and a name shaped like SQL
// cannot turn the count into a query over something else.
func TestExactRowCountWithinEscapesIdentifiers(t *testing.T) {
	_, db := setupTestMySQL(t)
	const plain = "ord`ers"
	const crafted = "big` WHERE 0) x #"
	dropTablesOnCleanup(t, db, plain, crafted)

	for _, name := range []string{plain, crafted} {
		_, err := db.ExecContext(t.Context(), "CREATE TABLE "+sqlescape.EscapeIdentifier(name)+" (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT NOT NULL)")
		require.NoError(t, err, "create %s", name)
		var inserts strings.Builder
		inserts.WriteString("INSERT INTO " + sqlescape.EscapeIdentifier(name) + " (v) VALUES (1)")
		for range 19 {
			inserts.WriteString(",(1)")
		}
		_, err = db.ExecContext(t.Context(), inserts.String())
		require.NoError(t, err, "seed %s", name)
	}

	count, err := exactRowCountWithin(t.Context(), db, "testdb", plain, 100)
	require.NoError(t, err, "a name containing a backtick is counted, not a syntax error")
	assert.Equal(t, int64(20), count)

	count, err = exactRowCountWithin(t.Context(), db, "testdb", crafted, 10)
	require.NoError(t, err)
	assert.Equal(t, int64(11), count, "a table above the bound reports limit+1 whatever its name")

	_, err = exactRowCountWithin(t.Context(), db, "testdb", "ord`ers_absent", 10)
	require.Error(t, err, "a missing table is an error, never a zero count")
	assert.Contains(t, err.Error(), "count rows of `testdb`.`ord``ers_absent` (bounded at 11)")
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

// directReshapeTable creates a small table whose primary key a direct
// statement can reshape, dropped when the test ends.
func directReshapeTable(t *testing.T, db *sql.DB, name string) {
	t.Helper()
	dropTablesOnCleanup(t, db, name)
	_, err := db.ExecContext(t.Context(), fmt.Sprintf(`CREATE TABLE %s (
		id INT NOT NULL AUTO_INCREMENT,
		tenant_id INT NOT NULL,
		PRIMARY KEY (id)
	)`, sqlescape.EscapeIdentifier(name)))
	require.NoError(t, err, "create %s table", name)
	_, err = db.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s (tenant_id) VALUES (1), (2), (3)", sqlescape.EscapeIdentifier(name)))
	require.NoError(t, err, "insert into %s", name)
}

// runDirectReshape drives an apply whose only statement reshapes table's
// primary key under an enabled direct execution policy with the given lock
// bound and row limit, and returns the schema change's final state and error message. The
// apply runs under a bounded context: if it stalls past it, the schema change
// ends stopped rather than completed or failed, and the caller's assertions
// on the state diagnose the stall.
func runDirectReshape(t *testing.T, dsn, tableName string, lockWaitSeconds, maxTableRows int64) (engine.State, string) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{tableName},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	stmt := fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)", sqlescape.EscapeIdentifier(tableName))
	eng.executeSchemaChange(ctx, host, username, password, database, []string{stmt}, false,
		directPolicy{Enabled: true, MaxTableRows: maxTableRows, LockAcquisitionTimeoutSeconds: lockWaitSeconds})

	eng.mu.Lock()
	defer eng.mu.Unlock()
	return eng.runningSchemaChange.state, eng.runningSchemaChange.errorMessage
}

// sessionExists reports whether the MySQL session with the given connection ID
// is still connected.
func sessionExists(t *testing.T, db *sql.DB, connectionID int64) bool {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM performance_schema.threads WHERE processlist_id = ?", connectionID).Scan(&n))
	return n > 0
}

// A direct statement does not wait out a transaction that blocks it. An
// application transaction that has read the table holds a shared metadata
// lock until it ends, and the ALTER's exclusive lock request would otherwise
// queue behind it, with all new table traffic queueing behind the ALTER, until
// the bounded wait expires. Instead, most of the way into the wait, the
// statement kills the blocking session and takes the lock: the apply
// completes, the reshape lands, and the blocker is gone.
func TestEngine_ExecuteAlterPhase_KillsMetadataLockBlocker(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	directReshapeTable(t, db, "direct_blocked")

	blockerConn, err := db.Conn(t.Context())
	require.NoError(t, err, "acquire the blocking session")
	defer utils.CloseAndLog(blockerConn)
	blocker, err := blockerConn.BeginTx(t.Context(), nil)
	require.NoError(t, err, "begin the blocking transaction")
	// Returning the conn waits for its transaction to end, so the transaction
	// is ended first, including on an early failure while it still holds the
	// metadata lock. Once the kill lands the rollback fails on the dead
	// connection, which is the expected outcome, not a teardown failure.
	defer func() { _ = blocker.Rollback() }()
	var blockerID int64
	require.NoError(t, blocker.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&blockerID))
	var rows int
	require.NoError(t, blocker.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM `direct_blocked`").Scan(&rows),
		"read the table, taking its shared metadata lock until the transaction ends")
	require.Equal(t, 3, rows)

	const lockWaitSeconds = 2
	started := time.Now()
	state, errorMessage := runDirectReshape(t, dsn, "direct_blocked", lockWaitSeconds, 100000)
	elapsed := time.Since(started)

	require.Equal(t, engine.StateCompleted, state, "the apply completes once the blocker is killed: %s", errorMessage)
	_, _, _, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")
	assert.Equal(t, []string{"id", "tenant_id"}, pkColumns(t, database, "direct_blocked"),
		"the reshaped primary key landed on the target")
	assert.GreaterOrEqual(t, elapsed, time.Duration(float64(lockWaitSeconds)*0.9*float64(time.Second)),
		"the blocker is given most of the bounded wait to finish on its own before it is killed")

	require.Eventually(t, func() bool { return !sessionExists(t, db, blockerID) }, 10*time.Second, 50*time.Millisecond,
		"the blocking session is killed")
	assert.Error(t, blocker.QueryRowContext(t.Context(), "SELECT 1").Scan(new(int)),
		"the blocking transaction's connection is gone")
}

// A session holding an explicit LOCK TABLES is never killed: it is not a
// transaction that rolls back, so killing it could interrupt work that
// depends on the lock being held. The direct statement leaves it alone, each
// bounded attempt times out, and the apply fails with the operator-actionable
// busy-table error — naming the kind of blocker it will not kill and asking
// for a retry — rather than the driver's own words. The lock holder keeps its
// session and the target is untouched.
func TestEngine_ExecuteAlterPhase_ExplicitTableLockFailsBusy(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	directReshapeTable(t, db, "direct_locked")

	locker, err := db.Conn(t.Context())
	require.NoError(t, err, "acquire the locking session")
	defer utils.CloseAndLog(locker)
	_, err = locker.ExecContext(t.Context(), "LOCK TABLES `direct_locked` READ")
	require.NoError(t, err, "take an explicit table lock")
	var lockerID int64
	require.NoError(t, locker.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&lockerID))

	const lockWaitSeconds = 1
	state, errorMessage := runDirectReshape(t, dsn, "direct_locked", lockWaitSeconds, 100000)

	assert.Equal(t, engine.StateFailed, state)
	assert.Contains(t, errorMessage, `Table "direct_locked" is busy`)
	assert.Contains(t, errorMessage, fmt.Sprintf("Each attempt waits up to %ds, and SchemaBot makes up to %d attempts.", lockWaitSeconds, directMaxAttempts))
	assert.Contains(t, errorMessage, "not a session holding an explicit LOCK TABLES")
	assert.Contains(t, errorMessage, "unless its database user has PROCESS and CONNECTION_ADMIN",
		"a kill that fails for lack of privilege lands on this same message, so it names that remedy too")
	assert.Contains(t, errorMessage, "Retry when those sessions have finished")
	assert.NotContains(t, errorMessage, "Lock wait timeout exceeded",
		"the driver's own words are for the server log, not the pull request")

	assert.True(t, sessionExists(t, db, lockerID), "the explicit lock holder is not killed")
	_, err = locker.ExecContext(t.Context(), "UNLOCK TABLES")
	require.NoError(t, err, "release the explicit table lock")
	_, _, _, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")
	assert.Equal(t, []string{"id"}, pkColumns(t, database, "direct_locked"), "the target is untouched")
}

// A direct statement finds the sessions blocking it through
// performance_schema. A target user that cannot read those tables would reach
// apply time unable to kill anything, and the statement would queue on the
// lock while table traffic stalls behind it. So the verdict fails closed to
// blocked instead, and the reason says why without the database's own error
// text.
func TestResolveRefusedMode_ForceKillUnavailableBlocks(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	directReshapeTable(t, db, "direct_nokill")
	host, _, _, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})
	// The test context is cancelled before cleanup runs, so each drop gets its
	// own bounded context that outlives it.
	cleanupCtx := context.WithoutCancel(t.Context())
	for _, tc := range []struct {
		name string
		// grants are given on top of the user's own database.
		grants      []string
		wantMode    string
		wantOutcome string
	}{
		{
			name:        "no performance_schema access",
			wantMode:    engine.ExecutionModeBlocked,
			wantOutcome: "blocked_force_kill_unavailable",
		},
		{
			// MySQL checks PROCESS for innodb_trx only when it fills the
			// table, so a probe that can return no rows never asks for it.
			name:        "no PROCESS",
			grants:      []string{"SELECT ON performance_schema.*"},
			wantMode:    engine.ExecutionModeBlocked,
			wantOutcome: "blocked_force_kill_unavailable",
		},
		{
			name:     "both grants",
			grants:   []string{"SELECT ON performance_schema.*", "PROCESS ON *.*"},
			wantMode: engine.ExecutionModeDirect,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			user := fmt.Sprintf("direct_nokill_%d", len(tc.grants))
			const password = "direct_nokill_pw"
			_, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", user, password))
			require.NoError(t, err, "create user %s", user)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(cleanupCtx, 10*time.Second)
				defer cancel()
				_, err := db.ExecContext(ctx, fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", user))
				assert.NoError(t, err, "drop user %s", user)
			})
			_, err = db.ExecContext(t.Context(), fmt.Sprintf("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%%'", sqlescape.EscapeIdentifier(database), user))
			require.NoError(t, err, "grant the user its database")
			for _, grant := range tc.grants {
				_, err = db.ExecContext(t.Context(), fmt.Sprintf("GRANT %s TO '%s'@'%%'", grant, user))
				require.NoError(t, err, "grant %s", grant)
			}

			target := &lazyTargetDB{dsn: targetDSN(host, user, password, database)}
			defer target.close()
			for name, policy := range map[string]directPolicy{
				"row bound":  {Enabled: true, MaxTableRows: 100000},
				"byte bound": {Enabled: true, MaxTableBytes: 100 << 20},
			} {
				t.Run(name, func(t *testing.T) {
					decision := eng.resolveRefusedMode(t.Context(), target, policy,
						database, "direct_nokill", "dropping primary key is not supported")
					assert.Equal(t, tc.wantMode, decision.mode)
					assert.Equal(t, tc.wantOutcome, decision.outcome)
					if tc.wantOutcome != "" {
						assert.Equal(t, "dropping primary key is not supported"+blockedForceKillUnavailableReason, decision.modeReason)
					}
				})
			}
		})
	}
}

// A check of the kill's grants that fails without the target denying one, here
// on a cancelled context, says nothing about the grants. The statement is
// still blocked, but as unknown, with a reason that asks for a fresh plan
// rather than a grant the user may already have.
func TestRequireForceKill_FailedCheckBlocksAsUnknown(t *testing.T) {
	dsn, _ := setupTestMySQL(t)
	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")
	target := &lazyTargetDB{dsn: targetDSN(host, username, password, database)}
	defer target.close()
	_, err = target.get(t.Context())
	require.NoError(t, err, "connect to the target as the size gate would")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	decision := eng.requireForceKill(ctx, target, database, "orders", "dropping primary key is not supported",
		refusedModeDecision{mode: engine.ExecutionModeDirect})
	assert.Equal(t, engine.ExecutionModeBlocked, decision.mode)
	assert.Equal(t, "blocked_force_kill_unknown", decision.outcome)
	assert.Equal(t, "dropping primary key is not supported"+blockedForceKillUnknownReason, decision.modeReason)
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
		name       string
		metadata   map[string]string
		mode       string
		wantReason string
	}{
		{name: "policy enabled within bound", metadata: directPolicyMetadata(100000), mode: engine.ExecutionModeDirect, wantReason: "the table has ~"},
		{name: "policy disabled", metadata: nil, mode: engine.ExecutionModeBlocked, wantReason: "dropping primary key is not supported"},
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
			assert.Contains(t, change.ModeReason, tc.wantReason)
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

// A direct statement only kills sessions while it is waiting for its metadata
// lock. Once it holds the lock and is rebuilding the table, concurrent
// application traffic is expected and is not blocking it. A transaction that
// starts after the rebuild begins, reads the table, and commits before the
// rebuild ends must survive, even though the rebuild outlasts the kill delay.
func TestEngine_ExecuteAlterPhase_SparesTrafficDuringRebuild(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	const tableName = "direct_rebuild"
	dropTablesOnCleanup(t, db, tableName)
	_, err := db.ExecContext(t.Context(), `CREATE TABLE direct_rebuild (
		id INT NOT NULL AUTO_INCREMENT,
		tenant_id INT NOT NULL,
		pad VARCHAR(255) NOT NULL,
		PRIMARY KEY (id),
		KEY pad_idx (pad),
		KEY tenant_pad_idx (tenant_id, pad)
	)`)
	require.NoError(t, err, "create the table")
	_, err = db.ExecContext(t.Context(), "INSERT INTO direct_rebuild (tenant_id, pad) SELECT 1, REPEAT(MD5(RAND()), 7)")
	require.NoError(t, err, "seed the first row")
	for range 19 {
		_, err = db.ExecContext(t.Context(), "INSERT INTO direct_rebuild (tenant_id, pad) SELECT tenant_id + 1, REPEAT(MD5(RAND()), 7) FROM direct_rebuild")
		require.NoError(t, err, "double the table")
	}

	type result struct {
		state        engine.State
		errorMessage string
	}
	done := make(chan result, 1)
	started := time.Now()
	go func() {
		state, errorMessage := runDirectReshape(t, dsn, tableName, 1, 1<<20)
		t.Logf("direct apply finished after %s", time.Since(started))
		done <- result{state, errorMessage}
	}()

	// alterState reports what the ALTER is doing. "altering table" is the
	// rebuild itself: the ALTER has taken its lock and not yet asked to
	// upgrade it, so no session can be blocking it. Earlier states come before
	// the lock is taken, and the final upgrade waits on every open reader,
	// the bystander included, which the statement is right to kill.
	alterState := func() string {
		var state string
		err := db.QueryRowContext(t.Context(), `SELECT COALESCE(MAX(state), '') FROM information_schema.processlist
			WHERE info LIKE 'ALTER TABLE `+"`direct_rebuild`"+`%'`).Scan(&state)
		require.NoError(t, err, "read the ALTER's state")
		return state
	}
	require.Eventually(t, func() bool { return alterState() == "altering table" },
		10*time.Second, 10*time.Millisecond, "the ALTER starts rebuilding")

	bystanderConn, err := db.Conn(t.Context())
	require.NoError(t, err, "acquire the bystander session")
	defer utils.CloseAndLog(bystanderConn)
	bystander, err := bystanderConn.BeginTx(t.Context(), nil)
	require.NoError(t, err, "begin the bystander transaction")
	defer func() { _ = bystander.Rollback() }()
	var rows int
	require.NoError(t, bystander.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM direct_rebuild WHERE id = 1").Scan(&rows))
	// Hold the transaction across the kill delay, then commit while the
	// rebuild is still running.
	time.Sleep(1200 * time.Millisecond)
	stillRebuilding := alterState() == "altering table"
	commitErr := bystander.Commit()

	var r result
	select {
	case r = <-done:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the direct apply did not finish")
	}
	require.True(t, stillRebuilding, "the rebuild must outlast the bystander, without waiting on it, for this scenario")
	require.Equal(t, engine.StateCompleted, r.state, "the apply completes: %s", r.errorMessage)
	assert.NoError(t, commitErr, "a transaction that never blocked the statement is not killed")
}
