//go:build integration

package spirit

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// testDatabase is the schema every target in this package's integration tests
// serves, and the one the engine is told it is applying to.
const testDatabase = "testdb"

// recordedLog is one line the engine emitted, reduced to the parts an operator
// alert keys on: how loud it was, what it said, and the attributes carrying the
// identifiers to triage from.
type recordedLog struct {
	level slog.Level
	msg   string
	attrs map[string]string
}

// logSink collects the lines a capturingHandler and every handler derived from
// it record.
type logSink struct {
	mu   sync.Mutex
	logs []recordedLog
}

func (s *logSink) record(entry recordedLog) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs = append(s.logs, entry)
}

func (s *logSink) recorded() []recordedLog {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedLog(nil), s.logs...)
}

// capturingHandler records what the engine logs so a test can assert on the
// level and attributes operator alerting keys on, not only on the decision
// behind them. Attributes bound with Logger.With are merged into each line the
// derived logger emits, the way a text handler renders them.
type capturingHandler struct {
	sink  *logSink
	bound []slog.Attr
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, rec slog.Record) error {
	entry := recordedLog{level: rec.Level, msg: rec.Message, attrs: make(map[string]string)}
	for _, a := range h.bound {
		entry.attrs[a.Key] = a.Value.String()
	}
	rec.Attrs(func(a slog.Attr) bool {
		entry.attrs[a.Key] = a.Value.String()
		return true
	})
	h.sink.record(entry)
	return nil
}

func (h *capturingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &capturingHandler{sink: h.sink, bound: append(append([]slog.Attr(nil), h.bound...), attrs...)}
}

func (h *capturingHandler) WithGroup(string) slog.Handler { return h }

// capturingLogger returns a logger that records to the sink it also returns.
func capturingLogger() (*slog.Logger, *logSink) {
	sink := &logSink{}
	return slog.New(&capturingHandler{sink: sink}), sink
}

// onlyCopyReportLine returns the single line the copy report emitted, failing
// when the report said nothing or said more than one thing. The disclosure is
// one line per apply by design: an operator scanning an apply's log for what
// happened to a copy must not have to reconcile several.
func onlyCopyReportLine(t *testing.T, sink *logSink) recordedLog {
	t.Helper()

	var lines []recordedLog
	for _, line := range sink.recorded() {
		if _, ok := line.attrs["checkpoint_table"]; ok {
			lines = append(lines, line)
		}
	}
	require.Len(t, lines, 1, "the copy report emits exactly one line")
	return lines[0]
}

// The disclosure an operator alerts on is the log line itself, so its level and
// its attributes are part of the contract, not just the adopt/discard decision
// behind them. A discard is a warning that names the cause, the tables whose
// copy is being destroyed, and the whole batch that costs them.
func TestReportExistingCopyWarnsOnADiscardAndNamesItsCause(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const batch = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	seedCopy(t, db, []string{"xfers"}, utils.CheckpointTableName("xfers"), batch)

	logger, sink := capturingLogger()
	eng := New(Config{Logger: logger})
	defer eng.Drain()

	widened := "ALTER TABLE `xfers` ADD INDEX r_token (r_token, id)"
	eng.reportExistingCopy(t.Context(), dsn, testDatabase, widened, []string{"xfers"})

	line := onlyCopyReportLine(t, sink)
	assert.Equal(t, slog.LevelWarn, line.level,
		"a discard destroys work already paid for, so it must be loud enough to alert on")
	assert.Equal(t, "discarding an existing copy on the target; its tables are re-copied from zero", line.msg)
	assert.Equal(t, engine.DiscardStatementDiffers, line.attrs["reason"])
	assert.Equal(t, testDatabase, line.attrs["database"])
	assert.Equal(t, "[xfers]", line.attrs["copied_tables"])
	assert.Equal(t, "[xfers]", line.attrs["batch_tables"])
	assert.Equal(t, "_xfers_chkpnt", line.attrs["checkpoint_table"])
	assert.Contains(t, line.attrs, "checkpoint_age", "the checkpoint was read, so its age is a fact")
}

// A copy the apply continues is reported, but as information rather than a
// warning: nothing is destroyed, so it must not reach an alert keyed on the
// discard warning.
func TestReportExistingCopyLogsAnAdoptedCopyAtInfo(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const batch = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	seedCopy(t, db, []string{"xfers"}, utils.CheckpointTableName("xfers"), batch)

	logger, sink := capturingLogger()
	eng := New(Config{Logger: logger})
	defer eng.Drain()

	eng.reportExistingCopy(t.Context(), dsn, testDatabase, batch, []string{"xfers"})

	line := onlyCopyReportLine(t, sink)
	assert.Equal(t, slog.LevelInfo, line.level)
	assert.Equal(t, "continuing an existing copy on the target", line.msg)
	assert.NotContains(t, line.attrs, "reason", "there is no cause to report when nothing is discarded")
}

// A shadow table whose checkpoint this batch cannot read is still a copy the
// apply destroys, so it is still reported. Its age is not reported at all: the
// zero value would read as "written moments ago" and understate a copy that may
// be days old.
func TestReportExistingCopyOmitsTheAgeOfAnUnreachableCheckpoint(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	seedCopy(t, db, []string{"xfers"}, "", "")

	logger, sink := capturingLogger()
	eng := New(Config{Logger: logger})
	defer eng.Drain()

	eng.reportExistingCopy(t.Context(), dsn, testDatabase,
		"ALTER TABLE `xfers` ADD INDEX r_token (r_token)", []string{"xfers"})

	line := onlyCopyReportLine(t, sink)
	assert.Equal(t, slog.LevelWarn, line.level)
	assert.Equal(t, engine.DiscardStatementDiffers, line.attrs["reason"])
	assert.NotContains(t, line.attrs, "checkpoint_age",
		"no checkpoint was read, so there is no age to state as a fact")
}

// findCopy looks for a copy on the target the way the engine does, on a pool of
// its own that lives no longer than the lookup.
func findCopy(t *testing.T, dsn string, tables []string) (*existingCopy, error) {
	t.Helper()

	target := &lazyTargetDB{dsn: dsn}
	defer target.close()
	return findExistingCopy(t.Context(), target, tables)
}

// seedCopy puts a Spirit copy on the target: a shadow table for each of tables,
// and a checkpoint under checkpointTable recording the batch it was made for.
// An empty checkpointTable seeds the shadow tables alone, which is what a copy
// whose checkpoint this batch cannot reach looks like.
func seedCopy(t *testing.T, db *sql.DB, tables []string, checkpointTable, statement string) {
	t.Helper()

	for _, table := range tables {
		_, err := db.ExecContext(t.Context(),
			fmt.Sprintf("CREATE TABLE `%s` (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)", utils.NewTableName(table)))
		require.NoError(t, err, "create shadow table for %s", table)
	}
	if checkpointTable == "" {
		return
	}

	cp := checkpoint.NewTable(db, checkpointTable, checkpoint.Transient)
	require.NoError(t, cp.Create(t.Context()), "create checkpoint table %s", checkpointTable)
	require.NoError(t, cp.Write(t.Context(), checkpoint.Record{
		Statement:       statement,
		CopierWatermark: `{"Key":["id"],"LowerBound":3952903346}`,
		Position:        "mysql-bin.024891:19443021",
	}), "write checkpoint row")
}

// A batch that meets its own copy on the target continues it: Spirit resumes
// from the checkpoint rather than re-copying, so the operator is told the copy
// is being adopted and nothing is destroyed.
func TestExistingCopyAdoptsMatchingBatch(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const batch = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	seedCopy(t, db, []string{"xfers"}, utils.CheckpointTableName("xfers"), batch)

	found, err := findCopy(t, dsn, []string{"xfers"})
	require.NoError(t, err, "findExistingCopy")
	require.NotNil(t, found, "a copy of xfers is on the target")
	assert.Equal(t, []string{"xfers"}, found.CopiedTables)
	assert.Equal(t, "_xfers_chkpnt", found.CheckpointTable)
	assert.Equal(t, batch, found.Statement)

	disposition, reason := found.Disposition(batch, DefaultCheckpointMaxAge)
	assert.Equal(t, engine.CopyAdopt, disposition)
	assert.Empty(t, reason)
}

// A target with no copy has nothing to disclose, so the apply proceeds
// silently, exactly as it does today.
func TestExistingCopyAbsentOnCleanTarget(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	found, err := findCopy(t, dsn, []string{"xfers"})
	require.NoError(t, err, "findExistingCopy")
	require.Nil(t, found, "no copy of xfers is on the target")

	disposition, reason := found.Disposition("ALTER TABLE `xfers` ADD INDEX r_token (r_token)", DefaultCheckpointMaxAge)
	assert.Equal(t, engine.CopyNone, disposition)
	assert.Empty(t, reason)
}

// Spirit's checkpoint identity is the whole joined batch, not the statement for
// the table being copied. A batch that gains or loses an unrelated table
// therefore discards the copy even when the ALTER for the copied table is
// unchanged, which is the case the operator most needs warned about because the
// remedy is to restore the batch rather than to narrow it.
func TestExistingCopyDiscardsOnBatchDrift(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const xfersAlter = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	const ledgerAlter = "ALTER TABLE `ledger_entries` ADD COLUMN note VARCHAR(64)"
	seedCopy(t, db, []string{"xfers", "ledger_entries"}, sharedCheckpointTable, xfersAlter+"; "+ledgerAlter)

	t.Run("the copy's own batch adopts it", func(t *testing.T) {
		found, err := findCopy(t, dsn, []string{"xfers", "ledger_entries"})
		require.NoError(t, err, "findExistingCopy")
		require.NotNil(t, found)
		assert.Equal(t, sharedCheckpointTable, found.CheckpointTable,
			"a batch of two or more reads the schema-level checkpoint")

		disposition, _ := found.Disposition(xfersAlter+"; "+ledgerAlter, DefaultCheckpointMaxAge)
		assert.Equal(t, engine.CopyAdopt, disposition)
	})

	t.Run("narrowing to the copied table discards it", func(t *testing.T) {
		found, err := findCopy(t, dsn, []string{"xfers"})
		require.NoError(t, err, "findExistingCopy")
		require.NotNil(t, found, "the shadow table is still a copy this batch will destroy")
		assert.Equal(t, []string{"xfers"}, found.CopiedTables)
		assert.Equal(t, "_xfers_chkpnt", found.CheckpointTable,
			"a batch of one reads the per-table checkpoint, which this copy did not write")
		assert.Empty(t, found.Statement, "the copy's checkpoint is unreachable from a batch of one")

		disposition, reason := found.Disposition(xfersAlter, DefaultCheckpointMaxAge)
		assert.Equal(t, engine.CopyDiscard, disposition)
		assert.Equal(t, engine.DiscardStatementDiffers, reason)
	})
}

// Dropping one table from a batch of three leaves a batch of two, so both the
// copy and the batch that meets it read the schema-level checkpoint: the
// checkpoint is reachable and its statement is intact. The copy is still
// discarded, on the joined statement alone, because Spirit's identity is the
// whole batch. This is the drift an operator sees when a table's change moves
// to its own PR while the tables around it keep copying.
func TestExistingCopyDiscardsWhenAnUnrelatedTableLeavesTheBatch(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const xfersAlter = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	const ledgerAlter = "ALTER TABLE `ledger_entries` ADD COLUMN note VARCHAR(64)"
	const accountsAlter = "ALTER TABLE `accounts` ADD COLUMN region VARCHAR(16)"
	seedCopy(t, db, []string{"xfers", "ledger_entries", "accounts"}, sharedCheckpointTable,
		xfersAlter+"; "+ledgerAlter+"; "+accountsAlter)

	found, err := findCopy(t, dsn, []string{"xfers", "ledger_entries"})
	require.NoError(t, err, "findExistingCopy")
	require.NotNil(t, found)
	assert.Equal(t, []string{"xfers", "ledger_entries"}, found.CopiedTables,
		"the batch reports the copied tables it names, not the ones it dropped")
	assert.Equal(t, sharedCheckpointTable, found.CheckpointTable,
		"both batches have two or more statements, so both read the schema-level checkpoint")
	require.NotEmpty(t, found.Statement,
		"the checkpoint is reachable, so the discard rests on the statement comparison alone")

	disposition, reason := found.Disposition(xfersAlter+"; "+ledgerAlter, DefaultCheckpointMaxAge)
	assert.Equal(t, engine.CopyDiscard, disposition)
	assert.Equal(t, engine.DiscardStatementDiffers, reason)

	// The ALTER for each surviving table is unchanged; only the batch around
	// them shrank.
	assert.Contains(t, found.Statement, xfersAlter)
	assert.Contains(t, found.Statement, ledgerAlter)
}

// Spirit reads the shadow table of every table in the batch before it resumes,
// and rebuilds all of them if any one is missing. A batch that adds a table to
// an existing copy therefore destroys the copy it already had, even though the
// checkpoint is current and its statement matches this batch byte for byte.
// This is the one discard an operator cannot see coming from the schema change
// alone, so predicting adopt here would promise survival to a copy that is
// about to be dropped.
func TestExistingCopyDiscardsWhenTheBatchCoversMoreTablesThanTheCopy(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const xfersAlter = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	const ledgerAlter = "ALTER TABLE `ledger_entries` ADD COLUMN note VARCHAR(64)"
	batch := xfersAlter + "; " + ledgerAlter

	// The checkpoint is for the whole batch, but only one of its tables ever got
	// a shadow table: the copy came down before Spirit built the second.
	seedCopy(t, db, []string{"xfers"}, sharedCheckpointTable, batch)

	found, err := findCopy(t, dsn, []string{"xfers", "ledger_entries"})
	require.NoError(t, err, "findExistingCopy")
	require.NotNil(t, found)
	assert.Equal(t, []string{"xfers"}, found.CopiedTables, "only one table was copied")
	assert.Equal(t, []string{"xfers", "ledger_entries"}, found.BatchTables,
		"the batch alters both, so Spirit reads both shadow tables")
	require.Equal(t, batch, found.Statement,
		"the checkpoint matches this batch exactly; only the missing shadow table disqualifies it")

	disposition, reason := found.Disposition(batch, DefaultCheckpointMaxAge)
	assert.Equal(t, engine.CopyDiscard, disposition)
	assert.Equal(t, engine.DiscardCopyIncomplete, reason)
}

// A checkpoint older than the configured maximum cannot be replayed, so Spirit
// starts fresh and the copy is lost. The operator is warned that applying
// re-copies from zero.
func TestExistingCopyDiscardsExpiredCheckpoint(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const batch = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	seedCopy(t, db, []string{"xfers"}, utils.CheckpointTableName("xfers"), batch)

	found, err := findCopy(t, dsn, []string{"xfers"})
	require.NoError(t, err, "findExistingCopy")
	require.NotNil(t, found)
	assert.Equal(t, batch, found.Statement, "the statement still matches; only the age disqualifies it")

	// The checkpoint was written moments ago, so bound the age below its own
	// age rather than waiting for a real expiry.
	disposition, reason := found.Disposition(batch, -time.Second)
	assert.Equal(t, engine.CopyDiscard, disposition)
	assert.Equal(t, engine.DiscardCheckpointExpired, reason)

	// Spirit's bound is inclusive, so a checkpoint exactly at the limit is
	// already too old. Predicting adopt at the boundary would be optimistic in
	// the destructive direction, since Spirit re-measures the age later than
	// this does and can only find it larger.
	atTheBound, reason := found.Disposition(batch, found.Age)
	assert.Equal(t, engine.CopyDiscard, atTheBound)
	assert.Equal(t, engine.DiscardCheckpointExpired, reason)
}

// A shadow table with no checkpoint at all is an orphan from an apply that
// failed before its first dump. It is still a copy this batch destroys, so it
// is disclosed rather than reported as an empty target.
func TestExistingCopyWithoutCheckpointDiscards(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	seedCopy(t, db, []string{"xfers"}, "", "")

	found, err := findCopy(t, dsn, []string{"xfers"})
	require.NoError(t, err, "findExistingCopy")
	require.NotNil(t, found, "a shadow table with no checkpoint is still a copy")
	assert.Empty(t, found.Statement)
	assert.Zero(t, found.Age)

	disposition, reason := found.Disposition("ALTER TABLE `xfers` ADD INDEX r_token (r_token)", DefaultCheckpointMaxAge)
	assert.Equal(t, engine.CopyDiscard, disposition)
	assert.Equal(t, engine.DiscardStatementDiffers, reason)
}

// An ungrouped apply drives one table at a time, so a plan covering several
// tables meets each table's copy on its own terms: the table being copied
// resumes from its own checkpoint, and the tables that have not started yet
// have nothing at stake. This is the ordinary mid-flight shape of a multi-table
// schema change, and reading it as one all-or-nothing batch would tell the
// operator that confirming the apply destroys a copy that it in fact continues.
func TestPlannedExistingCopiesFollowsTheGroupingTheApplyWillUse(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const xfersAlter = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	const ledgerAlter = "ALTER TABLE `ledger_entries` ADD COLUMN note VARCHAR(64)"
	// One table of the plan is partway through its own copy; the other has not
	// been touched.
	seedCopy(t, db, []string{"xfers"}, utils.CheckpointTableName("xfers"), xfersAlter)

	changes := []engine.TableChange{
		engineRun("xfers", xfersAlter),
		engineRun("ledger_entries", ledgerAlter),
	}

	logger, _ := capturingLogger()
	eng := New(Config{Logger: logger})
	defer eng.Drain()

	t.Run("ungrouped continues the table already copying and says nothing of the rest", func(t *testing.T) {
		target := &lazyTargetDB{dsn: dsn}
		defer target.close()

		copies := eng.plannedExistingCopies(t.Context(), target, testDatabase, changes, false)
		require.Len(t, copies, 1, "only the table with a copy has anything at stake")
		assert.Equal(t, engine.CopyAdopt, copies[0].Disposition,
			"the apply hands Spirit this table's own ALTER, which is what its checkpoint records")
		assert.Empty(t, copies[0].Reason)
		assert.Equal(t, []string{"xfers"}, copies[0].Tables)
		assert.Equal(t, xfersAlter, copies[0].Statement)
	})

	t.Run("grouped destroys the copy because the batch covers a table with none", func(t *testing.T) {
		target := &lazyTargetDB{dsn: dsn}
		defer target.close()

		copies := eng.plannedExistingCopies(t.Context(), target, testDatabase, changes, true)
		require.Len(t, copies, 1, "the batch is one unit, so its copy is one disclosure")
		assert.Equal(t, engine.CopyDiscard, copies[0].Disposition,
			"Spirit rebuilds every table in the batch when one of them has no shadow table")
		assert.Equal(t, engine.DiscardCopyIncomplete, copies[0].Reason)
		assert.Equal(t, []string{"xfers"}, copies[0].Tables)
	})
}

// Each ungrouped batch is measured against its own statement and its own
// checkpoint, so which table of the plan holds the copy must not matter: a
// copy on the plan's second table is continued on that table's terms, exactly
// as a copy on its first would be.
func TestPlannedExistingCopiesMeetsEachTableOnItsOwnTerms(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const xfersAlter = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	const ledgerAlter = "ALTER TABLE `ledger_entries` ADD COLUMN note VARCHAR(64)"
	seedCopy(t, db, []string{"ledger_entries"}, utils.CheckpointTableName("ledger_entries"), ledgerAlter)

	changes := []engine.TableChange{
		engineRun("xfers", xfersAlter),
		engineRun("ledger_entries", ledgerAlter),
	}

	logger, _ := capturingLogger()
	eng := New(Config{Logger: logger})
	defer eng.Drain()

	target := &lazyTargetDB{dsn: dsn}
	defer target.close()

	copies := eng.plannedExistingCopies(t.Context(), target, testDatabase, changes, false)
	require.Len(t, copies, 1, "only the table with a copy has anything at stake")
	assert.Equal(t, engine.CopyAdopt, copies[0].Disposition,
		"the copy matches its own table's ALTER, not the plan's first")
	assert.Empty(t, copies[0].Reason)
	assert.Equal(t, []string{"ledger_entries"}, copies[0].Tables)
	assert.Equal(t, ledgerAlter, copies[0].Statement)
}

// An ungrouped plan can have work at stake on several tables at once, and
// every one of them is disclosed: a copy silently left out of the comment is a
// copy the operator consents to destroying without knowing it exists.
func TestPlannedExistingCopiesDisclosesEveryTableWithACopy(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const xfersAlter = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	const ledgerAlter = "ALTER TABLE `ledger_entries` ADD COLUMN note VARCHAR(64)"
	seedCopy(t, db, []string{"xfers"}, utils.CheckpointTableName("xfers"), xfersAlter)
	// The second table's copy was made for a different change, so its
	// disposition differs from the first's — pinning that each disclosure is
	// the verdict for its own table, not a repeat of the first.
	seedCopy(t, db, []string{"ledger_entries"}, utils.CheckpointTableName("ledger_entries"),
		"ALTER TABLE `ledger_entries` ADD COLUMN memo VARCHAR(64)")

	changes := []engine.TableChange{
		engineRun("xfers", xfersAlter),
		engineRun("ledger_entries", ledgerAlter),
	}

	logger, _ := capturingLogger()
	eng := New(Config{Logger: logger})
	defer eng.Drain()

	target := &lazyTargetDB{dsn: dsn}
	defer target.close()

	copies := eng.plannedExistingCopies(t.Context(), target, testDatabase, changes, false)
	require.Len(t, copies, 2, "every table with a copy is disclosed")
	assert.Equal(t, engine.CopyAdopt, copies[0].Disposition)
	assert.Equal(t, []string{"xfers"}, copies[0].Tables)
	assert.Equal(t, engine.CopyDiscard, copies[1].Disposition)
	assert.Equal(t, engine.DiscardStatementDiffers, copies[1].Reason)
	assert.Equal(t, []string{"ledger_entries"}, copies[1].Tables)
}

// Spirit's declarative diff can emit several ALTERs for one table, and an
// ungrouped apply runs them one at a time. The first of them consumes the
// table's copy — resuming or discarding it — so the ones after it meet a table
// with nothing at stake. The plan discloses the copy once, as the first batch
// will treat it, rather than rendering the same table under contradictory
// verdicts.
func TestPlannedExistingCopiesDisclosesATableOnceAcrossItsBatches(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const addIndex = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	const addColumn = "ALTER TABLE `xfers` ADD COLUMN note VARCHAR(64)"
	seedCopy(t, db, []string{"xfers"}, utils.CheckpointTableName("xfers"), addIndex)

	changes := []engine.TableChange{
		engineRun("xfers", addIndex),
		engineRun("xfers", addColumn),
	}

	logger, _ := capturingLogger()
	eng := New(Config{Logger: logger})
	defer eng.Drain()

	target := &lazyTargetDB{dsn: dsn}
	defer target.close()

	copies := eng.plannedExistingCopies(t.Context(), target, testDatabase, changes, false)
	require.Len(t, copies, 1, "one table, one verdict, however many batches alter it")
	assert.Equal(t, engine.CopyAdopt, copies[0].Disposition,
		"the first batch is the one that meets the copy, and its statement matches")
	assert.Equal(t, []string{"xfers"}, copies[0].Tables)
	assert.Equal(t, addIndex, copies[0].Statement)
}
