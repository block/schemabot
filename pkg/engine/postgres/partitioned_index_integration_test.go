//go:build integration

package postgres

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// plannedPartitionedIndexDDL is the statement the planner writes for an index
// on a partitioned table's parent.
const plannedPartitionedIndexDDL = "CREATE INDEX events_kind_idx ON ONLY public.events USING btree (kind)"

// createPartitionedEvents creates public.events partitioned by day, with two
// partitions holding rows, so an index build has partitions to work through.
func createPartitionedEvents(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.events (id bigint NOT NULL, day date NOT NULL, kind text) PARTITION BY RANGE (day);
		CREATE TABLE public.events_2025 PARTITION OF public.events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
		CREATE TABLE public.events_2026 PARTITION OF public.events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
		INSERT INTO public.events (id, day, kind)
			SELECT g, DATE '2025-01-01' + (g % 700), 'kind_' || (g % 5) FROM generate_series(1, 2000) g`)
	require.NoError(t, err)
}

// assertPartitionedIndexComplete proves the parent's index is valid and every
// partition carries a valid index attached to it.
func assertPartitionedIndexComplete(t *testing.T, db *sql.DB, index string, partitions ...string) {
	t.Helper()
	var valid bool
	err := db.QueryRowContext(t.Context(),
		`SELECT i.indisvalid FROM pg_index i WHERE i.indexrelid = to_regclass($1)`, "public."+index).Scan(&valid)
	require.NoError(t, err)
	assert.True(t, valid, "the parent's index must be valid, which the server allows only once every partition has one attached")

	rows, err := db.QueryContext(t.Context(), `
		SELECT t.relname, i.indisvalid
		FROM pg_inherits h
		JOIN pg_index i ON i.indexrelid = h.inhrelid
		JOIN pg_class t ON t.oid = i.indrelid
		WHERE h.inhparent = to_regclass($1)
		ORDER BY t.relname`, "public."+index)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	attached := map[string]bool{}
	for rows.Next() {
		var table string
		var partitionValid bool
		require.NoError(t, rows.Scan(&table, &partitionValid))
		attached[table] = partitionValid
	}
	require.NoError(t, rows.Err())
	want := map[string]bool{}
	for _, partition := range partitions {
		want[partition] = true
	}
	assert.Equal(t, want, attached, "every partition must carry a valid index attached to the parent's")
}

// An index added to a partitioned table in a schema file plans as one
// executable step, and applying it builds the index one partition at a time:
// the parent's index ends valid with every partition's index attached, and a
// second plan finds nothing left to do.
func TestEnginePlanAndApplyPartitionedIndexBuild(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "partitioned_index_test")
	createPartitionedEvents(t, db)
	req := &engine.PlanRequest{
		Database: "partitioned_index_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"events.sql": `CREATE TABLE events (id bigint NOT NULL, day date NOT NULL, kind text) PARTITION BY RANGE (day);
CREATE INDEX events_kind_idx ON events (kind);`,
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	eng := New()
	plan, err := eng.Plan(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1)
	step := plan.Changes[0].TableChanges[0]
	assert.Equal(t, "events", step.Table)
	assert.Empty(t, step.ExecutionMode, "the partitioned index build must be executable, not blocked: %s", step.ModeReason)
	assert.Equal(t, plannedPartitionedIndexDDL, step.DDL, "the plan shows the parent's statement, which the build runs partition by partition")

	_, err = eng.Apply(t.Context(), applyRequest(dsn, "events", step.DDL))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "events")
	require.Equal(t, engine.StateCompleted, progress.State, "progress: %+v", progress)
	assertPartitionedIndexComplete(t, db, "events_kind_idx", "events_2025", "events_2026")

	converged, err := eng.Plan(t.Context(), req)
	require.NoError(t, err)
	assert.True(t, converged.NoChanges, "the built index must converge the plan: %+v", converged.Changes)
}

// A partitioned index build interrupted after the parent's index and one
// partition's index exist leaves an invalid parent index. Applying the same
// statement again resumes from there: the finished partition is kept, the
// remaining one is built, and the parent's index turns valid.
func TestEngineApplyPartitionedIndexBuildResumes(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "partitioned_index_resume_test")
	createPartitionedEvents(t, db)
	_, err := db.ExecContext(t.Context(), `
		CREATE INDEX events_kind_idx ON ONLY public.events (kind);
		CREATE INDEX events_2025_kind_idx ON public.events_2025 (kind);
		ALTER INDEX public.events_kind_idx ATTACH PARTITION public.events_2025_kind_idx`)
	require.NoError(t, err)
	var parentValid bool
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT indisvalid FROM pg_index WHERE indexrelid = 'public.events_kind_idx'::regclass`).Scan(&parentValid))
	require.False(t, parentValid, "the interrupted build must leave the parent's index invalid for the test to prove a resume")

	eng := New()
	_, err = eng.Apply(t.Context(), applyRequest(dsn, "events", plannedPartitionedIndexDDL))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "events")
	require.Equal(t, engine.StateCompleted, progress.State, "progress: %+v", progress)
	assertPartitionedIndexComplete(t, db, "events_kind_idx", "events_2025", "events_2026")
}

// A plan that routed an index to the partitioned build can meet a table that
// is no longer partitioned by the time it is applied. The apply refuses before
// it touches the target, so the statement never runs as a CREATE INDEX that
// blocks writes, and the operator is told to plan again.
func TestEngineApplyPartitionedIndexBuildRefusesAPlainTable(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "partitioned_index_plain_test")
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.events (id bigint PRIMARY KEY, kind text)")
	require.NoError(t, err)

	eng := New()
	_, err = eng.Apply(t.Context(), applyRequest(dsn, "events", plannedPartitionedIndexDDL))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "events")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "refused", progress.Metadata["phase"])
	assert.False(t, progress.Retryable)
	assert.Equal(t, `"events" is no longer partitioned, so the index planned for its partitions was not built; re-plan against the current schema`, progress.ErrorMessage)

	var exists bool
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT to_regclass('public.events_kind_idx') IS NOT NULL`).Scan(&exists))
	assert.False(t, exists, "the refused statement must not have built an index")
}

// A unique index on a partitioned table whose rows in one partition duplicate
// its key: that partition's build fails on its rows and leaves an invalid
// index on the partition. The apply is refused naming the SQLSTATE, the
// engine drops the partition's leftover the same proven way it drops a plain
// table's, and the application can write the duplicate the live schema
// allows into that partition.
func TestEngineApplyPartitionedUniqueIndexOnDuplicateRowsDropsThePartitionsLeftover(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "partitioned_data_fault_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.ledger (ref text NOT NULL, day date NOT NULL) PARTITION BY RANGE (day);
		CREATE TABLE public.ledger_2025 PARTITION OF public.ledger FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
		CREATE TABLE public.ledger_2026 PARTITION OF public.ledger FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
		INSERT INTO public.ledger (ref, day) VALUES
			('a', '2025-03-01'), ('b', '2025-03-01'),
			('c', '2026-03-01'), ('c', '2026-03-01')`)
	require.NoError(t, err)

	eng := New()
	_, err = eng.Apply(t.Context(), applyRequest(dsn, "ledger",
		"CREATE UNIQUE INDEX ledger_ref_key ON ONLY public.ledger USING btree (ref, day)"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "ledger")
	require.Equal(t, engine.StateFailed, progress.State, "progress: %+v", progress)
	assert.Equal(t, "refused", progress.Metadata["phase"])
	assert.False(t, progress.Retryable, "a build the partition's rows violate fails the same way on every rerun")
	// The partition's index carries a name derived from the partition's OID,
	// so the detail is matched around it; the partition it names is where
	// the rows to correct are.
	assert.Regexp(t, `^index "public"\."ledger_ref_key_p\d+" \("ledger_2026"\) was not built: rows duplicate its key \(23505\), `+
		`and its invalid leftover was dropped; correct the rows, then re-run$`, progress.ErrorMessage)
	assertNoInvalidIndexOn(t, db, "ledger_2026")

	_, err = db.ExecContext(t.Context(), "INSERT INTO public.ledger (ref, day) VALUES ('c', '2026-04-01'), ('c', '2026-04-01')")
	require.NoError(t, err, "a write the live schema allows is not rejected by the failed build's index")
}
