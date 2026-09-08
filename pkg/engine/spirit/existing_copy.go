// existing_copy.go answers one question about a target before a batch of
// ALTERs runs: is there already a Spirit row copy there, and will this batch
// continue it or throw it away?
//
// Spirit decides that for itself, by comparing the statement it was given
// against the one stored in the checkpoint, byte for byte. Nothing here
// changes that decision or coordinates with it. This only predicts it, so the
// operator can be told what an apply is about to cost before it runs, instead
// of learning afterwards that days of copying were dropped.
//
// Two rules govern everything in this file:
//
//   - Never normalize. Spirit compares raw bytes, so any canonicalization here
//     disagrees with it by construction.
//   - Never re-derive. The statement to compare comes from the same routing the
//     apply uses, and the checkpoint is read with Spirit's own reader.
package spirit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/mysqlconn"
)

// sharedCheckpointTable is the schema-level checkpoint a batch of two or more
// statements writes. A batch of one writes utils.CheckpointTableName(table)
// instead. Spirit picks between them on statement count alone, so a copy made
// by a different-sized batch than the one being planned now lives under a name
// this batch will never read.
const sharedCheckpointTable = "_spirit_checkpoint"

// copyDetectionTimeout bounds one existing-copy lookup — two small reads
// against a target that is already being used for the plan or apply around it
// — and equally the whole plan-side prediction across every batch it covers.
// Generous enough that a loaded target still answers, short enough that an
// unresponsive one costs a log line instead of a stalled plan or apply, no
// matter how many batches the plan predicts.
const copyDetectionTimeout = 10 * time.Second

// existingCopy is a Spirit row copy found on a target: the shadow tables that
// hold the copied rows, and the checkpoint that says what batch they were made
// for. A copy with no reachable checkpoint still has shadow tables, which is
// why CopiedTables is populated independently of Statement.
type existingCopy struct {
	// CopiedTables are the tables in the planned batch that already have a
	// shadow table on the target, in the order the batch names them.
	CopiedTables []string
	// BatchTables are all the tables the planned batch alters, in batch order.
	// Spirit reads the shadow table of every one of them before it resumes, so
	// this is what says whether CopiedTables is the whole batch or part of it.
	BatchTables []string
	// CheckpointTable is the checkpoint this batch will read, whether or not it
	// exists. Named for logs and comments, never parsed.
	CheckpointTable string
	// CheckpointFound reports whether that checkpoint was there to read. When it
	// was not, Statement and Age describe nothing and must not be reported as
	// though they described the copy.
	CheckpointFound bool
	// Statement is the batch the copy was made for, verbatim, empty when this
	// batch cannot reach the copy's checkpoint.
	Statement string
	// Age is how long ago the checkpoint was written. Zero when no checkpoint
	// was found.
	Age time.Duration
}

// Disposition reports what Spirit will do with c when it runs statement, and
// why. statement must be the exact string the apply will hand Spirit for this
// batch — every ALTER joined when the apply groups them, the one table's ALTER
// when it drives a table at a time.
//
// Adopt is the narrow case. It requires that every table in the batch already
// has a copy, that the checkpoint this batch reads exists, that its statement
// matches byte for byte, and that it is young enough to replay from. Everything
// else destroys the copy. A copy whose checkpoint this batch cannot reach at
// all — because a batch of a different size reads the other checkpoint name —
// has an empty Statement here, and so resolves to the same statement-drift
// discard.
//
// The checks run in the order Spirit runs them, so the reason reported is the
// one Spirit would hit first.
func (c *existingCopy) Disposition(statement string, maxAge time.Duration) (engine.CopyDisposition, string) {
	if c == nil || len(c.CopiedTables) == 0 {
		return engine.CopyNone, ""
	}
	// Spirit reads the shadow table of every table in the batch before it will
	// resume, and rebuilds all of them if any one is unreadable. A batch that
	// only partly overlaps an existing copy therefore destroys the part that
	// exists, even when the checkpoint is current and its statement matches.
	if len(c.CopiedTables) != len(c.BatchTables) {
		return engine.CopyDiscard, engine.DiscardCopyIncomplete
	}
	if c.Statement != statement {
		return engine.CopyDiscard, engine.DiscardStatementDiffers
	}
	// Spirit's own bound is inclusive, and it evaluates the age later than this
	// does, so anything this reports as adoptable must be strictly younger.
	if c.Age >= maxAge {
		return engine.CopyDiscard, engine.DiscardCheckpointExpired
	}
	return engine.CopyAdopt, ""
}

// planned converts the copy into the engine-agnostic disclosure a plan carries
// to the surfaces that render it, as the one-element list a single-database
// engine can produce. It returns nothing when applying statement destroys
// nothing, so a plan meeting a clean target carries no disclosure.
func (c *existingCopy) planned(namespace, statement string, maxAge time.Duration) []*engine.ExistingCopy {
	disposition, reason := c.Disposition(statement, maxAge)
	if disposition == engine.CopyNone {
		return nil
	}
	return []*engine.ExistingCopy{{
		Namespace:   namespace,
		Disposition: disposition,
		Reason:      reason,
		Tables:      c.CopiedTables,
		Age:         c.Age,
		Statement:   c.Statement,
	}}
}

// plannedExistingCopies predicts what applying these planned changes will do
// with a row copy already on the target, for the plan to disclose before anyone
// confirms it. Spirit plans one database at a time and reports one disclosure
// per batch the apply will run, so a grouped apply yields at most one and an
// ungrouped one at most a copy per table; a caller planning several databases
// concatenates their results.
//
// A plan is a read: it must describe the target, never fail because of it. A
// target that cannot be read is logged and the plan carries no disclosure,
// which leaves the plan exactly as it is without this check.
//
// Two things keep this a prediction rather than a fact, and both fail quiet
// rather than wrong. The target may be unreadable, as above. And the batch is
// the one this plan expects: routing runs again at apply time against the
// target as it is then, so a statement this plan reads as directly executable —
// or as refused outright — can reach Spirit after all, in a batch that no
// longer matches the one predicted here. The prediction runs again at apply
// time against the routing the apply actually took, and Spirit decides for
// itself regardless, so a plan that discloses nothing is never the last word on
// whether a copy survives.
func (e *Engine) plannedExistingCopies(ctx context.Context, target *lazyTargetDB, database string, changes []engine.TableChange, grouped bool) []*engine.ExistingCopy {
	batches, ok := plannedSpiritBatches(changes, grouped)
	if !ok {
		e.logger.Debug("plan has no statements that reach the engine's copy path, so no existing copy is at stake",
			"database", database, "grouped_execution", grouped)
		return nil
	}

	// One deadline covers the whole prediction: the per-lookup bound inside
	// findExistingCopy cannot extend past it, so an unresponsive target costs
	// the plan one timeout regardless of how many batches it runs.
	ctx, cancel := context.WithTimeout(ctx, copyDetectionTimeout)
	defer cancel()

	var copies []*engine.ExistingCopy
	evaluated := make(map[string]bool, len(batches))
	for _, batch := range batches {
		// An apply drives these batches in order, and the first batch that
		// touches a table consumes whatever copy is there — it either resumes
		// it through cutover or discards it before copying. A later batch on
		// the same table therefore meets a target with nothing at stake, so
		// only a table's first batch has a disposition worth disclosing.
		if allTablesEvaluated(batch.Tables, evaluated) {
			e.logger.Debug("an earlier batch of this plan consumes any copy of these tables, so this batch discloses nothing",
				"database", database, "tables", batch.Tables)
			continue
		}
		for _, table := range batch.Tables {
			evaluated[table] = true
		}

		found, err := findExistingCopy(ctx, target, batch.Tables)
		if err != nil {
			// The raw error carries target infrastructure detail and a plan is
			// rendered into a PR comment, so it stays in the logs. One batch
			// failing says nothing about the others: an apply runs them
			// independently, so the rest are still worth disclosing.
			e.logger.Warn("cannot tell whether applying this plan continues or discards an existing copy; this batch discloses nothing",
				"database", database, "tables", batch.Tables, "error", err)
			continue
		}
		copies = append(copies, found.planned(database, batch.Statement, e.checkpointMaxAge)...)
	}
	return copies
}

// allTablesEvaluated reports whether every table in tables has already been
// covered by an earlier batch of the same prediction.
func allTablesEvaluated(tables []string, evaluated map[string]bool) bool {
	for _, table := range tables {
		if !evaluated[table] {
			return false
		}
	}
	return true
}

// spiritBatch is one unit of work an apply hands Spirit: the statement Spirit
// executes and the tables that statement alters. A checkpoint belongs to a
// batch, so batches are what an existing copy has to be predicted against.
type spiritBatch struct {
	Statement string
	Tables    []string
}

// plannedSpiritBatches returns the ALTER batches the apply will hand Spirit for
// these planned changes, in plan order. A grouped apply gives Spirit every
// ALTER at once, so it is one batch covering every table; an ungrouped apply
// drives one table at a time, so it is one single-table batch each.
//
// Which shape runs decides both halves of the prediction: the checkpoint name
// Spirit reads, and how much of the batch must already be copied for the copy
// to be resumable. Predicting the grouped shape for an ungrouped apply looks
// under a checkpoint that apply never reads and calls a whole batch incomplete
// whenever fewer than all its tables have been copied, which is the ordinary
// state of an ungrouped apply partway through.
//
// It reports ok=false when the plan reaches no copy at all, either because it
// has no engine-run ALTER or because it carries a statement the engine refuses
// and the database's policy will not route directly — that apply fails during
// routing, before Spirit sees a checkpoint, so nothing on the target is at
// stake.
func plannedSpiritBatches(changes []engine.TableChange, grouped bool) ([]spiritBatch, bool) {
	var alters, tables []string
	for _, change := range changes {
		if change.Operation != ddl.StatementAlterTable {
			continue
		}
		switch change.ExecutionMode {
		case engine.ExecutionModeBlocked:
			return nil, false
		case engine.ExecutionModeDirect:
			continue
		}
		alters = append(alters, change.DDL)
		tables = append(tables, change.Table)
	}
	if len(alters) == 0 {
		return nil, false
	}
	if grouped {
		return []spiritBatch{{Statement: strings.Join(alters, "; "), Tables: tables}}, true
	}
	batches := make([]spiritBatch, len(alters))
	for i, alter := range alters {
		batches[i] = spiritBatch{Statement: alter, Tables: []string{tables[i]}}
	}
	return batches, true
}

// reportExistingCopy records what this batch is about to do with a row copy
// already on the target, before Spirit runs and makes the decision for real.
//
// A discard destroys work that can be days old, and today the only trace is one
// Spirit info line, so nobody learns the copy existed. This makes it a first
// class signal an operator can alert on and find in the logs afterwards.
//
// The apply proceeds either way. This reads the target to describe what is
// about to happen, it does not decide anything, so a read failure must not turn
// a working apply into a failed one. It is logged and the apply continues,
// which leaves behavior exactly as it is without this check.
func (e *Engine) reportExistingCopy(ctx context.Context, dsn, database, statement string, tables []string) {
	logger := e.changeLogger()

	// The pool exists only for the two reads below. The apply that follows can
	// run for hours on Spirit's own connections, so a pool held for its duration
	// would be a connection spent on nothing.
	target := &lazyTargetDB{dsn: dsn}
	defer target.close()

	found, err := findExistingCopy(ctx, target, tables)
	if err != nil {
		// The raw error carries target infrastructure detail, so it stays in the
		// logs and never reaches a comment.
		logger.Warn("cannot tell whether this schema change continues or discards an existing copy; the apply proceeds and Spirit decides",
			"database", database, "tables", tables, "error", err)
		return
	}

	disposition, reason := found.Disposition(statement, e.checkpointMaxAge)
	if disposition == engine.CopyNone {
		logger.Debug("no existing copy on the target for any table in this schema change",
			"database", database, "tables", tables)
		return
	}

	attrs := []any{
		"database", database,
		"copied_tables", found.CopiedTables,
		"checkpoint_table", found.CheckpointTable,
	}
	// A copy whose checkpoint this batch cannot read has no age to report. Zero
	// would read as "written moments ago", which is the opposite of what an
	// unreachable checkpoint tells the operator about what is being lost.
	if found.CheckpointFound {
		attrs = append(attrs, "checkpoint_age", found.Age.String())
	}
	if disposition == engine.CopyAdopt {
		logger.Info("continuing an existing copy on the target", attrs...)
		return
	}
	// The batch is named on a discard so the copy_incomplete case reads without
	// a second lookup: the tables in the batch that are absent from
	// copied_tables are the ones whose missing copy costs the others theirs.
	logger.Warn("discarding an existing copy on the target; its tables are re-copied from zero",
		append(attrs, "reason", reason, "batch_tables", found.BatchTables)...)
}

// checkpointTableForBatch returns the checkpoint table Spirit reads for a batch
// covering these tables, mirroring the runner's own rule: a batch of two or
// more shares one schema-level table, a batch of one uses its table's name.
func checkpointTableForBatch(tables []string) string {
	if len(tables) == 1 {
		return utils.CheckpointTableName(tables[0])
	}
	return sharedCheckpointTable
}

// findExistingCopy looks for a row copy on the target for any table in tables,
// which must be the batch's tables in batch order. It returns nil when the
// target holds no copy for any of them.
//
// Shadow tables and the checkpoint are looked up separately on purpose. A
// shadow table is what makes the copy real and expensive; the checkpoint is
// only what decides whether it can be continued. A copy whose checkpoint this
// batch cannot reach still costs the same to lose, and reporting it as "no
// copy" is how that loss goes unnoticed today.
//
// The shadow tables are looked up on the connection the caller already has.
// Only the checkpoint needs a connection of its own, and only once a copy is
// known to exist, so the common case of a clean target costs one query and no
// new connection; see openCheckpointReader for why the checkpoint cannot share
// the caller's.
func findExistingCopy(ctx context.Context, target *lazyTargetDB, tables []string) (*existingCopy, error) {
	if len(tables) == 0 {
		return nil, nil
	}

	// The lookup describes an apply and decides nothing, so it must never be
	// what holds one up. An apply's context carries no deadline — it runs in
	// the background for hours — so without a bound of its own, a target that
	// accepts a connection and then stops answering would stall the fresh
	// pool's dial or either query for as long as the operating system allows.
	// A plan bounds its whole prediction with the same constant, and this
	// nested deadline cannot extend that one.
	ctx, cancel := context.WithTimeout(ctx, copyDetectionTimeout)
	defer cancel()

	targetDB, err := target.get(ctx)
	if err != nil {
		return nil, err
	}
	copied, err := copiedTables(ctx, targetDB, tables)
	if err != nil {
		return nil, err
	}
	if len(copied) == 0 {
		return nil, nil
	}

	found := &existingCopy{
		CopiedTables:    copied,
		BatchTables:     tables,
		CheckpointTable: checkpointTableForBatch(tables),
	}

	db, err := openCheckpointReader(ctx, target.dsn)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(db)

	rec, ok, err := readCheckpoint(ctx, db, found.CheckpointTable)
	if err != nil {
		return nil, err
	}
	if ok {
		found.CheckpointFound = true
		found.Statement = rec.Statement
		found.Age = rec.Age()
	}
	return found, nil
}

// openCheckpointReader opens a connection to the target with the Go MySQL
// driver's time parsing disabled. Spirit's checkpoint reader scans created_at
// into a string and parses the layout itself, so a pool that hands back
// time.Time values makes every checkpoint on that target unreadable. The
// target's own DSN decides everything else, and a checkpoint written by Spirit
// is UTC regardless of session time zone because DATETIME carries none.
func openCheckpointReader(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := mysqlconn.Open(dsn, func(cfg *mysql.Config) { cfg.ParseTime = false })
	if err != nil {
		return nil, fmt.Errorf("open target database to read its checkpoint: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		utils.CloseAndLog(db)
		return nil, fmt.Errorf("ping target database to read its checkpoint: %w", err)
	}
	return db, nil
}

// copiedTables returns the subset of tables that already have a Spirit shadow
// table on the target, in the order they were given. The whole batch is asked
// for in one query so that checking a batch against a clean target — the
// ordinary case — costs one query whether it covers one table or fifty.
func copiedTables(ctx context.Context, db *sql.DB, tables []string) ([]string, error) {
	shadowOf := make(map[string]string, len(tables))
	placeholders := make([]string, len(tables))
	args := make([]any, len(tables))
	for i, table := range tables {
		shadow := utils.NewTableName(table)
		shadowOf[shadow] = table
		placeholders[i] = "?"
		args[i] = shadow
	}

	query := fmt.Sprintf(`
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name IN (%s)`, strings.Join(placeholders, ", "))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query information_schema for shadow tables of %v: %w", tables, err)
	}
	defer utils.CloseAndLog(rows)

	present := make(map[string]bool, len(tables))
	for rows.Next() {
		var shadow string
		if err := rows.Scan(&shadow); err != nil {
			return nil, fmt.Errorf("scan shadow table name for %v: %w", tables, err)
		}
		present[shadowOf[shadow]] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read shadow tables of %v: %w", tables, err)
	}

	// Rebuild from the caller's slice rather than the result set: the batch
	// order is what names the copy, and the server is free to return rows in
	// any order.
	var copied []string
	for _, table := range tables {
		if present[table] {
			copied = append(copied, table)
		}
	}
	return copied, nil
}

// readCheckpoint reads the latest checkpoint row from name using Spirit's own
// reader. It reports ok=false when there is nothing to resume from: the table
// does not exist, was written by an incompatible Spirit version, or holds no
// row. Any other failure is returned, so an unreadable target can never be
// mistaken for a target with no checkpoint.
func readCheckpoint(ctx context.Context, db *sql.DB, name string) (checkpoint.Record, bool, error) {
	rec, err := checkpoint.NewTable(db, name, checkpoint.Transient).ReadLatest(ctx)
	switch {
	case err == nil:
		return rec, true, nil
	case errors.Is(err, checkpoint.ErrNotFound), checkpoint.IsIncompatible(err):
		return checkpoint.Record{}, false, nil
	default:
		return checkpoint.Record{}, false, fmt.Errorf("read checkpoint table %q: %w", name, err)
	}
}
