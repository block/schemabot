//go:build integration

package pendingdrops

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
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
	require.NoError(t, RecordQuarantines(t.Context(), db, slog.Default(), applyIdentifier, moved, quarantinedAt))
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

	require.NoError(t, RecordQuarantines(t.Context(), db, slog.Default(), "apply-a", moved, now))
	require.NoError(t, RecordQuarantines(t.Context(), db, slog.Default(), "apply-a", moved, now))
	assert.Equal(t, []string{moved[0].QuarantineTable}, recordedNames(t, db, "apply-a", "orders"))
	assert.Equal(t, []string{moved[1].QuarantineTable}, recordedNames(t, db, "apply-a", "customers"))

	err = RecordQuarantines(t.Context(), db, slog.Default(), "apply-b", moved, now)
	require.Error(t, err)
	assert.Contains(t, err.Error(), moved[0].QuarantineTable)
	assert.Contains(t, err.Error(), "apply-a")
	assert.Empty(t, recordedNames(t, db, "apply-b", "orders"))

	otherSource := []QuarantinedTable{{SchemaName: "testdb", TableName: "invoices", QuarantineSchema: Database, QuarantineTable: moved[0].QuarantineTable}}
	err = RecordQuarantines(t.Context(), db, slog.Default(), "apply-a", otherSource, now)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "testdb.orders")
	assert.Empty(t, recordedNames(t, db, "apply-a", "invoices"))
	assert.Equal(t, []string{moved[0].QuarantineTable}, recordedNames(t, db, "apply-a", "orders"), "the existing record is untouched")
}

// One DROP TABLE names `cafe` and `café`, two tables MySQL keeps apart. The
// log keeps their records apart as well, so the quarantine moves both, while a
// destination that differs from a recorded one only by case is still the same
// copy, as it is on a server that folds table names.
func TestQuarantineLogTellsDestinationsApartAsMySQLDoes(t *testing.T) {
	db := setupCleanerTest(t)
	now := time.Now()
	moved, err := Destinations([]TableMove{
		{SchemaName: "testdb", TableName: "cafe"},
		{SchemaName: "testdb", TableName: "café"},
	}, now)
	require.NoError(t, err)
	require.NotEqual(t, moved[0].QuarantineTable, moved[1].QuarantineTable)

	require.NoError(t, RecordQuarantines(t.Context(), db, slog.Default(), "apply-a", moved, now))
	assert.Equal(t, []string{moved[0].QuarantineTable}, recordedNames(t, db, "apply-a", "cafe"))
	assert.Equal(t, []string{moved[1].QuarantineTable}, recordedNames(t, db, "apply-a", "café"))

	for _, stmt := range []string{
		"CREATE TABLE `testdb`.`cafe` (id INT)",
		"CREATE TABLE `testdb`.`café` (id INT)",
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() {
		_, err := db.ExecContext(context.WithoutCancel(t.Context()), "DROP TABLE IF EXISTS `testdb`.`cafe`, `testdb`.`café`")
		assert.NoError(t, err, "drop accent tables")
	})
	require.NoError(t, MoveTablesTo(t.Context(), db, slog.Default(), moved))
	assert.Contains(t, quarantinedTables(t, db), moved[0].QuarantineTable)
	assert.Contains(t, quarantinedTables(t, db), moved[1].QuarantineTable)

	folded := []QuarantinedTable{{SchemaName: "testdb", TableName: "cafe", QuarantineSchema: Database, QuarantineTable: strings.ToUpper(moved[0].QuarantineTable)}}
	err = RecordQuarantines(t.Context(), db, slog.Default(), "apply-b", folded, now)
	require.Error(t, err, "a destination differing only by case is the recorded copy")
	assert.Contains(t, err.Error(), "apply-a")
}

// A resume reads the quarantine log with whatever credentials it has. A target
// with no pending drops database, or one without the log, has no records. A
// log the account cannot read is an error, never an empty log: taking it for
// empty would let a DROP TABLE IF EXISTS of a table this apply already moved
// report the drop done without finding its copy.
func TestRecordedQuarantinesDistinguishesNoLogFromAnUnreadableOne(t *testing.T) {
	db := setupCleanerTest(t)

	t.Run("no pending drops database", func(t *testing.T) {
		assert.Empty(t, recordedNames(t, db, "apply-a", "orders"))
	})

	t.Run("pending drops database without the log", func(t *testing.T) {
		_, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE DATABASE `%s`", Database))
		require.NoError(t, err)
		assert.Empty(t, recordedNames(t, db, "apply-a", "orders"))
	})

	t.Run("a log the account cannot read", func(t *testing.T) {
		recordQuarantine(t, db, "apply-a", "orders", time.Now())

		const user, password = "pending_drops_blind", "no-pending-drops-pw"
		_, err := db.ExecContext(t.Context(), "CREATE USER IF NOT EXISTS '"+user+"'@'%' IDENTIFIED BY '"+password+"'")
		require.NoError(t, err, "create user without pending drops access")
		t.Cleanup(func() {
			_, err := db.ExecContext(context.WithoutCancel(t.Context()), "DROP USER IF EXISTS '"+user+"'@'%'")
			assert.NoError(t, err, "drop user without pending drops access")
		})
		_, err = db.ExecContext(t.Context(), "GRANT SELECT ON `testdb`.* TO '"+user+"'@'%'")
		require.NoError(t, err, "grant select on testdb")

		cfg, err := mysql.ParseDSN(sharedDSN)
		require.NoError(t, err)
		cfg.User, cfg.Passwd = user, password
		limited, err := sql.Open("block-mysql", cfg.FormatDSN())
		require.NoError(t, err)
		t.Cleanup(func() { utils.CloseAndLog(limited) })
		require.NoError(t, limited.PingContext(t.Context()))

		recorded, err := RecordedQuarantines(t.Context(), limited, "apply-a", "testdb", "orders")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "read recorded quarantines of testdb.orders for apply apply-a")
		assert.Empty(t, recorded)
	})
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

// makeQuarantineLogReadOnly replaces the quarantine log with a view that lists
// the given records but refuses deletes, as a log the account can read but not
// write does.
func makeQuarantineLogReadOnly(t *testing.T, db *sql.DB, records ...string) {
	t.Helper()
	for _, stmt := range []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", Database),
		fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", Database, LogTable),
		fmt.Sprintf("CREATE TABLE `%s`.`log_base` (quarantine_table varchar(64) NOT NULL)", Database),
		fmt.Sprintf("CREATE VIEW `%s`.`%s` AS SELECT DISTINCT quarantine_table FROM `%s`.`log_base`", Database, LogTable, Database),
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}
	for _, name := range records {
		_, err := db.ExecContext(t.Context(),
			fmt.Sprintf("INSERT INTO `%s`.`log_base` (quarantine_table) VALUES (?)", Database), name)
		require.NoError(t, err)
	}
}

// makeQuarantineLogWritable replaces the read-only quarantine log built by
// makeQuarantineLogReadOnly with a view over the same records that accepts
// deletes, as when the account is given write access to the log.
func makeQuarantineLogWritable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(t.Context(),
		fmt.Sprintf("CREATE OR REPLACE VIEW `%s`.`%s` AS SELECT quarantine_table FROM `%s`.`log_base`", Database, LogTable, Database))
	require.NoError(t, err, "make the quarantine log writable")
}

// quarantineLogRecords returns the copies the quarantine log lists.
func quarantineLogRecords(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(),
		fmt.Sprintf("SELECT quarantine_table FROM `%s`.`%s` ORDER BY quarantine_table", Database, LogTable))
	require.NoError(t, err, "read the quarantine log")
	defer utils.CloseAndLog(rows)

	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name), "scan quarantine log record")
		names = append(names, name)
	}
	require.NoError(t, rows.Err(), "iterate quarantine log records")
	return names
}

// A pass drops an expired copy and then cannot delete that copy's quarantine
// log record, because it can read the log but not write it. The pass fails
// under log_error, naming the record in its error and in its log, and the
// record stays. Once the log can be written, the next pass deletes the record
// and drops nothing more.
func TestCleaner_ReportsALogRecordItCannotDelete(t *testing.T) {
	db := setupCleanerTest(t)
	ctx := boundedContext(t)
	expired := createQuarantinedTable(t, db, "old_orders", time.Now().Add(-8*24*time.Hour))
	makeQuarantineLogReadOnly(t, db, expired)
	reader := newManualMetricReader(t)
	var logs bytes.Buffer
	cleaner := testCleanerLoggingTo(t, &logs, DefaultRetention, false)

	err := cleaner.Run(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "prune "+Database+"."+LogTable)
	assert.Contains(t, err.Error(), "delete log record of "+expired)
	assert.Equal(t, map[string]string{"testdb": "log_error"}, cleanupErrorReasonsByDatabase(t, reader))
	assert.NotContains(t, quarantinedTables(t, db), expired, "the copy is still dropped")
	assert.Equal(t, int64(1), counterTotal(t, reader, "schemabot.pending_drops.cleanup_dropped_total"))
	assert.Equal(t, []string{expired}, quarantineLogRecords(t, db), "the record the pass could not delete is still there")
	assert.Contains(t, logs.String(), "could not delete the quarantine log record of a copy that is gone")
	assert.Contains(t, logs.String(), "quarantine_table="+expired)

	makeQuarantineLogWritable(t, db)
	require.NoError(t, cleaner.Run(ctx))
	assert.Empty(t, quarantineLogRecords(t, db), "a later pass that can write the log deletes the record")
	assert.Equal(t, int64(1), counterTotal(t, reader, "schemabot.pending_drops.cleanup_dropped_total"), "the later pass drops nothing more")
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
		require.NoError(t, RecordQuarantines(t.Context(), db, slog.Default(), "apply-a", []QuarantinedTable{
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
