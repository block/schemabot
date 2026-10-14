//go:build integration

package pendingdrops

import (
	"bytes"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// recordQuarantine records that applyIdentifier quarantined testdb.table as the
// copy named for quarantinedAt, and returns that copy's name.
func recordQuarantine(t *testing.T, db *sql.DB, applyIdentifier, table string, quarantinedAt time.Time) string {
	t.Helper()
	moved, err := Destinations([]TableMove{{SchemaName: "testdb", TableName: table}}, quarantinedAt)
	require.NoError(t, err)
	require.NoError(t, RecordQuarantines(t.Context(), db, applyIdentifier, moved, quarantinedAt))
	return moved[0].QuarantineTable
}

func recordedNames(t *testing.T, db *sql.DB, applyIdentifier, table string) []string {
	t.Helper()
	recorded, err := RecordedQuarantines(t.Context(), db, applyIdentifier, "testdb", table)
	require.NoError(t, err)
	names := make([]string, 0, len(recorded))
	for _, r := range recorded {
		names = append(names, r.QuarantineTable)
	}
	return names
}

// An apply quarantines `orders` twice, the second time on a replay that chose a
// new destination, and another apply quarantines a table of the same name. The
// log answers for the apply that asks: both of its destinations, oldest first,
// and none of the other apply's.
func TestQuarantineLogAnswersForTheApplyThatAsks(t *testing.T) {
	db := setupCleanerTest(t)
	now := time.Now()

	assert.Empty(t, recordedNames(t, db, "apply-a", "orders"), "a target with no log has no records")

	first := recordQuarantine(t, db, "apply-a", "orders", now.Add(-time.Minute))
	second := recordQuarantine(t, db, "apply-a", "orders", now)
	recordQuarantine(t, db, "apply-b", "orders", now.Add(time.Second))
	recordQuarantine(t, db, "apply-a", "customers", now)

	assert.Equal(t, []string{first, second}, recordedNames(t, db, "apply-a", "orders"))
	assert.Empty(t, recordedNames(t, db, "apply-c", "orders"))
	assert.Empty(t, recordedNames(t, db, "apply-a", "Orders"), "source names compare exactly")
}

// An apply that could not tell whether its records landed writes them again
// and ends with one record per copy. A copy already recorded for another apply,
// or for another source table, is refused rather than overwritten, and the
// record it already has is kept.
func TestQuarantineLogAcceptsARepeatAndRefusesAnotherOwner(t *testing.T) {
	db := setupCleanerTest(t)
	now := time.Now()
	moved, err := Destinations([]TableMove{
		{SchemaName: "testdb", TableName: "orders"},
		{SchemaName: "testdb", TableName: "customers"},
	}, now)
	require.NoError(t, err)

	require.NoError(t, RecordQuarantines(t.Context(), db, "apply-a", moved, now))
	require.NoError(t, RecordQuarantines(t.Context(), db, "apply-a", moved, now))
	assert.Equal(t, []string{moved[0].QuarantineTable}, recordedNames(t, db, "apply-a", "orders"))
	assert.Equal(t, []string{moved[1].QuarantineTable}, recordedNames(t, db, "apply-a", "customers"))

	err = RecordQuarantines(t.Context(), db, "apply-b", moved, now)
	require.Error(t, err)
	assert.Contains(t, err.Error(), moved[0].QuarantineTable)
	assert.Contains(t, err.Error(), "apply-a")
	assert.Empty(t, recordedNames(t, db, "apply-b", "orders"))

	otherSource := []QuarantinedTable{{SchemaName: "testdb", TableName: "invoices", QuarantineSchema: Database, QuarantineTable: moved[0].QuarantineTable}}
	err = RecordQuarantines(t.Context(), db, "apply-a", otherSource, now)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "testdb.orders")
	assert.Empty(t, recordedNames(t, db, "apply-a", "invoices"))
	assert.Equal(t, []string{moved[0].QuarantineTable}, recordedNames(t, db, "apply-a", "orders"), "the existing record is untouched")
}

// A quarantine log the cleaner cannot read fails the pass under its own
// reason, after the expired copies have still been dropped. When a drop fails
// in the same pass, the error names both failures, so neither is hidden
// behind the other.
func TestCleaner_ReportsAQuarantineLogItCannotRead(t *testing.T) {
	old := time.Now().Add(-8 * 24 * time.Hour)

	t.Run("the log alone", func(t *testing.T) {
		db := setupCleanerTest(t)
		expired := createQuarantinedTable(t, db, "old_orders", old)
		breakQuarantineLog(t, db)
		reader := newManualMetricReader(t)

		err := testCleaner(t, DefaultRetention, false).Run(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), LogTable)
		assert.Equal(t, map[string]string{"testdb": "log_error"}, cleanupErrorReasonsByDatabase(t, reader))
		assert.NotContains(t, quarantinedTables(t, db), expired, "the copies are still dropped")
	})

	t.Run("with a copy that cannot be dropped", func(t *testing.T) {
		db := setupCleanerTest(t)
		expired := createUndroppableQuarantinedTable(t, db, "parent_table", old)
		breakQuarantineLog(t, db)

		err := testCleaner(t, DefaultRetention, false).Run(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "drop expired pending drops tables")
		assert.Contains(t, err.Error(), "prune "+Database+"."+LogTable)
		assert.Contains(t, quarantinedTables(t, db), expired)
	})
}

// The quarantine log lives in the pending drops database beside the copies it
// describes. The cleaner never drops it or reports it unparseable, deletes the
// record of a copy once that copy is gone and past retention, whether the
// cleaner dropped it or its RENAME never landed, and keeps every record whose
// copy still exists or is still within retention. A dry run deletes nothing
// and reports the records a live pass would delete.
func TestCleaner_KeepsTheQuarantineLogInStepWithTheCopies(t *testing.T) {
	now := time.Now()
	old := now.Add(-8 * 24 * time.Hour)

	seed := func(t *testing.T) (*sql.DB, map[string]string) {
		db := setupCleanerTest(t)
		names := map[string]string{
			"expired copy":       createQuarantinedTable(t, db, "old_orders", old),
			"fresh copy":         createQuarantinedTable(t, db, "new_orders", now),
			"expired, no copy":   recordQuarantine(t, db, "apply-a", "never_moved", old),
			"fresh, no copy yet": recordQuarantine(t, db, "apply-a", "moving", now),
		}
		require.NoError(t, RecordQuarantines(t.Context(), db, "apply-a", []QuarantinedTable{
			{SchemaName: "testdb", TableName: "old_orders", QuarantineSchema: Database, QuarantineTable: names["expired copy"]},
			{SchemaName: "testdb", TableName: "new_orders", QuarantineSchema: Database, QuarantineTable: names["fresh copy"]},
		}, now))
		return db, names
	}
	logged := func(t *testing.T, db *sql.DB) []string {
		var names []string
		for _, table := range []string{"old_orders", "new_orders", "never_moved", "moving"} {
			names = append(names, recordedNames(t, db, "apply-a", table)...)
		}
		return names
	}

	t.Run("a cleaning pass", func(t *testing.T) {
		db, names := seed(t)
		reader := newManualMetricReader(t)
		require.NoError(t, testCleaner(t, DefaultRetention, false).Run(t.Context()))

		assert.Contains(t, quarantinedTables(t, db), LogTable, "the log is never dropped")
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(t.Context(), &rm))
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				assert.NotEqual(t, "schemabot.pending_drops.cleanup_skipped_total", m.Name, "the log is not reported as an unparseable quarantine")
			}
		}
		assert.Empty(t, cleanupErrorReasonsByDatabase(t, reader))
		assert.ElementsMatch(t, []string{names["fresh copy"], names["fresh, no copy yet"]}, logged(t, db))
	})

	t.Run("a dry run", func(t *testing.T) {
		db, names := seed(t)
		var logs bytes.Buffer
		require.NoError(t, testCleanerLoggingTo(t, &logs, DefaultRetention, true).Run(t.Context()))

		assert.ElementsMatch(t, []string{names["expired copy"], names["fresh copy"], names["expired, no copy"], names["fresh, no copy yet"]}, logged(t, db))
		assert.Contains(t, logs.String(), "would_prune_log_records=1")
		assert.Contains(t, logs.String(), "quarantine_table="+names["expired, no copy"], "the record a live pass would delete is named")
		assert.NotContains(t, logs.String(), "quarantine_table="+names["expired copy"], "a copy a dry run leaves in place keeps its record")
	})
}
