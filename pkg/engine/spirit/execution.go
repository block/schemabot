// execution.go runs Spirit migrations: dispatching DDL statements to instant DDL,
// CREATE/DROP execution, or Spirit's online migration runner depending on type.
package spirit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/dbconn"
	spiritflags "github.com/block/spirit/pkg/flags"
	spiritmigration "github.com/block/spirit/pkg/migration"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/mysqlerr"
)

// maxCommitLatency is the commit-latency throttle threshold: the copy backs
// off when the target's observed average commit latency exceeds it. It must
// be set explicitly — Spirit treats a zero value as "throttler disabled", and
// in redo-aware mode this throttler is also the backstop that lets the
// write-thread autoscaler grow above its starting value.
const maxCommitLatency = 100 * time.Millisecond

// classifyRunnerError marks runner failures that a retry would repeat as
// permanent, so operator retries are not spent repeating a schema change that
// cannot finish:
//   - a checksum verdict a retry would reach again, such as a lossy schema
//     change (see retryRepeatsChecksumVerdict).
//   - a row the copy could not write into the new table definition (see
//     rowDataErrorCodes). Every attempt copies the same rows into the same
//     definition, so only changing the data or the statement clears it.
//
// Everything else remains retryable. A run refused the table's advisory lock is
// marked ErrTargetHeld: another run is still working on the table, so the
// refusal says nothing about this schema change and it can start once that run
// lets go.
func classifyRunnerError(err error) error {
	if retryRepeatsChecksumVerdict(err) {
		return &engine.PermanentError{Err: err}
	}
	if isRowDataError(err) {
		return &engine.PermanentError{Err: err}
	}
	if errors.Is(err, dbconn.ErrLockHeld) {
		return fmt.Errorf("%w: %w", engine.ErrTargetHeld, err)
	}
	return err
}

// retryRepeatsChecksumVerdict reports whether err is a checksum verdict that a
// retry of the schema change would reach again. Spirit's checksum.IsReproducible
// decides which checksum failures are verdicts about the data, such as the
// checksum finding a difference again after it was repaired (a lossy ALTER).
// Failures it does not count stay retryable: attempts that errored before a
// repair was re-read, and a pass budget that ran out with ranges still
// changing too fast to verify.
//
// One reproducible verdict is still retryable here. The continuous checksum
// during the deferred cutover wait reports checksum.ErrPermanentDivergence
// without repairing, because a cutover may be imminent. Re-reading the data
// finds it again, but a retry resumes the schema change, and the resumed run's
// initial checksum repairs the range.
func retryRepeatsChecksumVerdict(err error) bool {
	if errors.Is(err, checksum.ErrPermanentDivergence) {
		return false
	}
	return checksum.IsReproducible(err)
}

// rowDataErrorCodes are the MySQL codes that say a row's value cannot be
// stored in the column the schema change gives it. Spirit copies with INSERT
// IGNORE, which downgrades these to warnings, and fails the copy on the
// warning; the code reaches here as a typed MySQL error either way. Each one is
// a property of the rows and the target definition alone, so a retry replays
// it. Lock waits, deadlocks, timeouts, and connection loss are deliberately
// absent: those describe the moment, not the data, and retrying clears them.
//
// Duplicate key (1062) is absent too, though a change that collapses unique
// values is just as deterministic: the copy ignores duplicate-key warnings by
// design, because a resumed copy re-inserts rows it already wrote, so that
// change surfaces as reproducible checksum differences instead, which
// classifyRunnerError already treats as permanent.
var rowDataErrorCodes = []uint16{
	1048, // ER_BAD_NULL_ERROR: a NULL in a column the change makes NOT NULL.
	1264, // ER_WARN_DATA_OUT_OF_RANGE: a value outside the range of a narrowed numeric type.
	1265, // WARN_DATA_TRUNCATED: a value the new type or length can only store truncated.
	1366, // ER_TRUNCATED_WRONG_VALUE_FOR_FIELD: a value the new type or character set cannot represent.
	1406, // ER_DATA_TOO_LONG: a value longer than the new column allows.
}

// isRowDataError reports whether err is a MySQL error saying a row cannot be
// stored under the new table definition.
func isRowDataError(err error) bool {
	// Read through mysqlerr rather than asserting a driver type: two MySQL
	// drivers are linked and their error types are not interchangeable. See
	// pkg/mysqlerr/number.go.
	return mysqlerr.Is(err, rowDataErrorCodes...)
}

// newSpiritMigration builds the Spirit migration for a statement against the
// target with the engine's copy, durability, and throttling settings.
// Callers layer statement-specific fields (DeferCutOver) onto the result.
//
// On Aurora, Spirit sizes the thread pools from the instance and scales them
// on throttler feedback, so apply throughput tracks the target instance rather
// than a fixed constant. Other MySQL targets run at the configured copier
// threads and Spirit's default write threads. The copy is verified with
// Spirit's default (lockless) checksum.
func (e *Engine) newSpiritMigration(host, username, password, database, stmt string) *spiritmigration.Migration {
	threads, lockTimeout := e.threads, e.lockWaitTimeout
	return &spiritmigration.Migration{
		Host:      host,
		Username:  username,
		Password:  &password,
		Database:  database,
		Statement: stmt,
		Common: spiritflags.Common{
			Threads:           threads,
			WriteThreads:      spiritflags.DefaultWriteThreads, // autoscaling sizes it on Aurora
			InterpolateParams: true,
			CheckpointMaxAge:  e.checkpointMaxAge,
			MaxCommitLatency:  maxCommitLatency,
		},
		Cutover: spiritflags.Cutover{
			LockWaitTimeout: lockTimeout,
		},
	}
}

// executeSchemaChange runs the Spirit schema change synchronously.
// CREATE and DROP statements are executed individually through Spirit. ALTER
// statements Spirit accepts are combined for atomic multi-table execution;
// ALTERs Spirit refuses run as native MySQL DDL when the direct execution
// policy permits, and fail the schema change otherwise.
func (e *Engine) executeSchemaChange(ctx context.Context, host, username, password, database string, ddlStatements []string, deferCutover bool, policy directPolicy) {
	logger := e.changeLogger()
	phases, err := classifyDDLPhases(ddlStatements)
	if err != nil {
		logger.Error("failed to classify statement", "error", err)
		e.setSchemaChangeFailed(err)
		return
	}

	// Execute CREATE TABLE statements first (each individually through Spirit).
	if !e.executeCreateStatements(ctx, host, username, password, database, phases.creates) {
		return
	}

	// Execute ALTER statements (routed per statement between Spirit and
	// direct execution).
	if len(phases.alters) > 0 {
		if !e.executeAlterPhase(ctx, host, username, password, database, phases.alters, deferCutover, policy) {
			return
		}
	}

	// Execute DROP TABLE statements last. Each table is dropped directly
	// through Spirit unless the deployment enabled the pending drops
	// quarantine, in which case it is renamed into the pending drops database
	// instead.
	if !e.executeDropStatements(ctx, host, username, password, database, phases.drops) {
		return
	}

	// Every phase ran every statement, so the change completed, even if a stop
	// or cancel arrived after the last one landed (see completeSchemaChange).
	e.completeSchemaChange(ctx, logger, database)
}

// resumeSchemaChange continues a stopped schema change to completion.
//
// When an ALTER was in flight, its statements are resumed from Spirit's
// checkpoint using the stored verbatim combined statement (not the original DDL
// list) so the statement string matches the checkpoint exactly — re-parsing
// through statement.New() can normalize formatting and trip Spirit's "alter
// statement does not match" guard. CREATE statements always run before the ALTER
// phase, so once an ALTER has started they are already applied; only the DROP
// phase remains and must run after the resumed ALTER completes. When no ALTER was
// in flight, the whole plan is run from the start.
func (e *Engine) resumeSchemaChange(ctx context.Context, host, username, password, database string, originalDDLs []string, combinedStatement string, deferCutover bool, policy directPolicy) {
	logger := e.changeLogger()
	if combinedStatement == "" {
		e.executeSchemaChange(ctx, host, username, password, database, originalDDLs, deferCutover, policy)
		return
	}

	if err := e.executeSpiritMigration(ctx, host, username, password, database, combinedStatement, deferCutover); err != nil {
		return // Failure already logged and recorded; a stop leaves the state Stopped.
	}

	phases, err := classifyDDLPhases(originalDDLs)
	if err != nil {
		logger.Error("failed to classify statements on resume", "database", database, "error", err)
		e.setSchemaChangeFailed(err)
		return
	}

	if !e.executeDropStatements(ctx, host, username, password, database, phases.drops) {
		return
	}

	e.completeSchemaChange(ctx, logger, database)
}

// completeSchemaChange records a change whose every phase ran every statement
// as completed. Completion is signalled only after the last phase, so a poller
// never observes StateCompleted while a later DROP phase is still pending.
//
// It is recorded even when a stop or cancel cancelled the context after the
// last statement landed, typically a Spirit cutover that committed just before
// the cancel reached it. The target then holds the whole change, so recording
// the stop the caller asked for would describe a resumable pause of a change
// that already landed. Stop and Cancel read the state again once the runner has
// returned, and answer that the change completed (see settleAfterRunnerExit).
// A stop that lands before the last statement keeps the change stopped, since
// that statement or a later phase returns early on the cancelled context.
func (e *Engine) completeSchemaChange(ctx context.Context, logger *slog.Logger, database string) {
	if ctx.Err() != nil {
		logger.Info("schema change completed after its context was cancelled by a stop, cancel, or shutdown; recording it completed, since the target holds the whole change",
			"database", database,
			"reason", ctx.Err(),
		)
	}
	e.setSchemaChangeCompleted()
}

// ddlPhases groups a plan's statements into the order Spirit requires:
// CREATE first, then ALTER (combined), then DROP.
type ddlPhases struct {
	creates []string
	alters  []string
	drops   []string
}

// classifyDDLPhases groups DDL statements into CREATE/ALTER/DROP phases using
// AST-based classification. RENAME and unrecognized statements run in the ALTER
// phase, matching Spirit's handling.
func classifyDDLPhases(ddlStatements []string) (ddlPhases, error) {
	var phases ddlPhases
	for _, stmt := range ddlStatements {
		stmtType, _, err := ddl.ClassifyStatement(stmt)
		if err != nil {
			return ddlPhases{}, fmt.Errorf("classify statement %q: %w", stmt, err)
		}
		switch stmtType {
		case ddl.StatementCreateTable:
			phases.creates = append(phases.creates, stmt)
		case ddl.StatementDropTable:
			phases.drops = append(phases.drops, stmt)
		default:
			phases.alters = append(phases.alters, stmt)
		}
	}
	return phases, nil
}

// runStatementPhase executes each statement through run, stopping the phase as
// soon as run returns an error. A cancelled context leaves the state Stopped; a
// genuine failure transitions to StateFailed. stopLabel names the DDL phase in
// the stop log; failLabel names it in the failure log and error (the DROP phase
// keeps its distinct "phase" wording in failure messages). It returns false when
// the caller must not run later phases.
func (e *Engine) runStatementPhase(ctx context.Context, database, stopLabel, failLabel string, statements []string, run func(context.Context, string) error) bool {
	logger := e.changeLogger()
	for _, stmt := range statements {
		if err := run(ctx, stmt); err != nil {
			if ctx.Err() != nil {
				logger.Info("schema change stopped during "+stopLabel,
					"database", database,
					"reason", ctx.Err(),
				)
				return false
			}
			logger.Error(failLabel+" failed", "database", database, "error", err)
			e.setSchemaChangeFailed(fmt.Errorf("%s failed: %w", failLabel, err))
			return false
		}
	}
	return true
}

// executeCreateStatements runs each CREATE TABLE through Spirit. It returns false
// when execution should stop: a cancelled context leaves the state Stopped, a
// genuine failure transitions to StateFailed. The caller must not run later
// phases when this returns false.
func (e *Engine) executeCreateStatements(ctx context.Context, host, username, password, database string, creates []string) bool {
	return e.runStatementPhase(ctx, database, "CREATE TABLE", "CREATE TABLE", creates, func(ctx context.Context, stmt string) error {
		return e.executeSingleStatement(ctx, host, username, password, database, stmt)
	})
}

// executeAlterPhase routes each ALTER between the Spirit runner and direct
// execution, runs the direct statements first (each is synchronous and fast
// relative to an online copy, and a routing or direct failure should surface
// before hours of copying), then combines the Spirit-accepted statements into
// one runner. It returns true when every statement ran and the caller may
// proceed to the DROP phase. It returns false when the phase did not finish:
// after a genuine failure (the failing step has already set StateFailed), or
// after a stop, which leaves the state Stopped.
func (e *Engine) executeAlterPhase(ctx context.Context, host, username, password, database string, alters []string, deferCutover bool, policy directPolicy) bool {
	target := &lazyTargetDB{dsn: targetDSN(host, username, password, database)}
	defer target.close()

	logger := e.changeLogger()
	routing, err := e.routeAlterStatements(ctx, target, database, alters, policy)
	if err != nil {
		logger.Error("ALTER routing failed", "database", database, "error", err)
		e.setSchemaChangeFailed(err)
		return false
	}

	if len(routing.direct) > 0 {
		if !e.executeDirectStatements(ctx, target, database, routing.direct, policy) {
			return false
		}
	}

	if len(routing.spiritAlters) == 0 {
		return true
	}

	combinedStatement := strings.Join(routing.spiritAlters, "; ")

	logger.Info("executing ALTER via Spirit",
		"database", database,
		"tables", routing.spiritTables,
		"ddl_count", len(routing.spiritAlters),
		"defer_cutover", deferCutover,
	)

	return e.executeSpiritMigration(ctx, host, username, password, database, combinedStatement, deferCutover) == nil
}

// executeDropStatements runs the DROP TABLE phase. Each table is dropped
// directly through Spirit unless the deployment enabled the pending drops
// quarantine, in which case it is renamed into the pending drops database so
// its data stays recoverable until the retention period expires. Both the
// initial-apply DROP phase and the resume DROP phase call this helper, so a
// resumed DROP behaves exactly like an initial one. It returns false when
// execution should stop: a cancelled context leaves the state Stopped, a
// genuine failure transitions to StateFailed.
func (e *Engine) executeDropStatements(ctx context.Context, host, username, password, database string, drops []string) bool {
	return e.runStatementPhase(ctx, database, "DROP TABLE", "DROP TABLE phase", drops, func(ctx context.Context, stmt string) error {
		return e.executeDropStatement(ctx, host, username, password, database, stmt)
	})
}

// executeDropStatement runs a single DROP TABLE statement, dropping the table
// outright unless the pending drops quarantine is enabled for this deployment.
func (e *Engine) executeDropStatement(ctx context.Context, host, username, password, database, stmt string) error {
	if e.disablePendingDrops {
		if err := e.executeDropDirectly(ctx, host, username, password, database, stmt); err != nil {
			return fmt.Errorf("drop table directly: %w", err)
		}
		return nil
	}
	if err := e.quarantineDroppedTables(ctx, host, username, password, database, stmt); err != nil {
		return fmt.Errorf("quarantine dropped table: %w", err)
	}
	return nil
}

// executeSingleStatement runs a single DDL statement through Spirit.
// Used for CREATE/DROP TABLE which Spirit requires to be individual statements.
func (e *Engine) executeSingleStatement(ctx context.Context, host, username, password, database, stmt string) error {
	parsed, err := statement.New(stmt)
	if err != nil {
		return fmt.Errorf("parse statement: %w", err)
	}
	if len(parsed) == 0 {
		return fmt.Errorf("no statement parsed")
	}

	// Determine statement type from parsed result
	stmtType := "DDL"
	if parsed[0].IsCreateTable() {
		stmtType = "CREATE TABLE"
	} else if parsed[0].IsAlterTable() {
		stmtType = "ALTER TABLE"
	}
	// Note: Spirit doesn't expose IsDropTable/IsRenameTable but handles them

	e.changeLogger().Info("executing single DDL through Spirit",
		"database", database,
		"table", parsed[0].Table,
		"type", stmtType,
	)

	migration := e.newSpiritMigration(host, username, password, database, stmt)

	runner, err := spiritmigration.NewRunner(migration)
	if err != nil {
		return fmt.Errorf("create runner: %w", err)
	}
	// Use spiritLogger so Spirit's log lines reach the apply log stream and
	// respect the runtime debug-log filter, same as the ALTER path. The table
	// attr gives every routed line its table context so multi-table applies
	// stay attributable in the apply log stream.
	runner.SetLogger(e.changeSpiritLogger().With("database", database, "table", parsed[0].Table))
	// Run opens the runner's connection pool and background routines; they are
	// only released by Close. Close on every return path so each single DDL
	// statement leaves no leaked connections or goroutines behind.
	defer utils.CloseAndLog(runner)

	if err := runner.Run(ctx); err != nil {
		return fmt.Errorf("run Spirit: %w", classifyRunnerError(err))
	}

	return nil
}

// errSchemaChangeStopped is returned by executeSpiritMigration when the runner
// returned because a stop or cancel cancelled its context. The state is left
// as the stop or cancel set it.
var errSchemaChangeStopped = errors.New("schema change stopped before the ALTER finished")

// cancelledRunnerError is what a Spirit runner error returned on a cancelled
// context says about the target, read from the evidence the runner retained
// for the run (see classifyCancelledRunnerError).
type cancelledRunnerError int

const (
	// cancelledBeforeCutover: the cutover never committed, so the cancel
	// stopped the change and the target holds none of it.
	cancelledBeforeCutover cancelledRunnerError = iota
	// cancelledAfterCutover: the cutover committed, so the target holds the
	// change and the error came from Spirit's cleanup after it.
	cancelledAfterCutover
	// cancelledCutoverUnverified: Spirit could not tell whether the cutover
	// committed, so neither outcome is proven.
	cancelledCutoverUnverified
)

// classifyCancelledRunnerError reads whether a runner that returned an error on
// a cancelled context had committed its cutover. Spirit marks the run's result
// with a durable mutation the moment the cutover commits, and every error it
// returns after that comes from cleanup of a change the target already holds.
// An ownership-ambiguous result outranks it: Spirit could not tell which table
// is live, so nothing is proven landed.
func classifyCancelledRunnerError(result status.WorkflowResult) cancelledRunnerError {
	if result.OwnershipAmbiguous() {
		return cancelledCutoverUnverified
	}
	if result.DurableMutation {
		return cancelledAfterCutover
	}
	return cancelledBeforeCutover
}

// executeSpiritMigration runs Spirit for ALTER statements. A nil return means
// the runner finished the ALTER, cutover included, even when the context was
// cancelled while it ran, and even when Spirit's cleanup after a committed
// cutover then failed on that cancelled context.
func (e *Engine) executeSpiritMigration(ctx context.Context, host, username, password, database, combinedStatement string, deferCutover bool) error {
	logger := e.changeLogger()
	// Parse statements to extract table names
	parsed, err := statement.New(combinedStatement)
	if err != nil {
		logger.Error("failed to parse combined statement", "error", err)
		e.setSchemaChangeFailed(fmt.Errorf("failed to parse combined statement: %w", err))
		return err
	}
	var tables []string
	var ddls []string
	for _, p := range parsed {
		tables = append(tables, p.Table)
		ddls = append(ddls, p.Statement)
	}

	// Every batch Spirit runs arrives here, whether it is a first apply whose
	// statements routing has just re-evaluated against the target's live schema
	// or an operator resuming a stopped one from its stored statement. Both hand
	// Spirit exactly this string, so this is where what Spirit compares against
	// the checkpoint is known, and the only place a disclosure covers both. The
	// resumed apply is the one that matters most: a copy stopped for days is
	// discarded the moment it starts again, and the operator asked for that
	// start.
	e.reportExistingCopy(ctx, targetDSN(host, username, password, database), database, combinedStatement, tables)

	migration := e.newSpiritMigration(host, username, password, database, combinedStatement)
	// Only a deferred apply waits on the sentinel table. A sentinel left in
	// the schema by an earlier cancelled or failed deferred apply never holds
	// a non-deferred apply, whose cutover SchemaBot could neither surface nor
	// release.
	migration.DeferCutOver = deferCutover

	runner, err := spiritmigration.NewRunner(migration)
	if err != nil {
		logger.Error("failed to create Spirit runner",
			"error", err,
			"statement", combinedStatement,
		)
		e.setSchemaChangeFailed(fmt.Errorf("failed to create Spirit runner: %w", err))
		return err
	}

	// Use spiritLogger which filters noisy debug logs unless DebugLogs is
	// enabled. The table attr gives every routed line its table context so
	// multi-table applies stay attributable in the apply log stream.
	spiritLogger := e.changeSpiritLogger().With("database", database, "table", uniqueTableList(tables))
	runner.SetLogger(spiritLogger)

	// Track schema change state
	e.mu.Lock()
	if e.runningSchemaChange != nil {
		e.runningSchemaChange.tables = tables
		e.runningSchemaChange.ddls = ddls
		e.runningSchemaChange.combinedStatement = combinedStatement
		e.runningSchemaChange.runners = []*spiritmigration.Runner{runner}
		e.runningSchemaChange.progressCallback = func() string {
			return runner.Progress().Summary
		}
	}
	e.mu.Unlock()

	logger.Info("starting Spirit runner",
		"database", database,
		"tables", tables,
		"defer_cutover", deferCutover,
		"statement_len", len(combinedStatement),
	)

	// On every exit path the tracked state must be terminal before the runner
	// is closed: Close() flips Spirit's status to close while teardown is
	// still in flight, and a progress poll must observe the recorded outcome,
	// never infer one from a runner mid-teardown.
	if err := runner.Run(ctx); err != nil {
		if ctx.Err() == nil {
			logger.Error("schema change failed",
				"error", err,
			)
			classifiedErr := classifyRunnerError(err)
			e.setSchemaChangeFailed(fmt.Errorf("schema change failed: %w", classifiedErr))
			utils.CloseAndLog(runner)
			return classifiedErr
		}
		// A stop, cancel, or shutdown cancelled the context. Whether that
		// stopped the change is Spirit's to say: an error after the cutover
		// committed comes from cleanup of a change the target already holds.
		switch classifyCancelledRunnerError(runner.Result()) {
		case cancelledAfterCutover:
			// The change landed, so it settles exactly as a runner that
			// returned nil on the cancelled context does, below. The error is
			// cleanup Spirit could not finish; it is logged, never recorded as
			// the change's outcome.
			logger.Warn("the schema change's cutover committed before its context was cancelled by a stop, cancel, or shutdown, but Spirit's cleanup after the cutover failed; settling the ALTER as finished, since the target holds it",
				"database", database,
				"tables", tables,
				"reason", ctx.Err(),
				"error", err,
			)
		case cancelledCutoverUnverified:
			// Neither outcome is proven, so the change is not recorded
			// completed. Stopped claims nothing about the target: a resume
			// re-plans against the live schema first and settles a change that
			// already landed instead of running it again.
			logger.Warn("schema change stopped, but Spirit could not verify whether its cutover committed; leaving it stopped, since a resume re-plans against the live schema before running anything",
				"database", database,
				"tables", tables,
				"reason", ctx.Err(),
				"error", err,
			)
			utils.CloseAndLog(runner)
			return errSchemaChangeStopped
		case cancelledBeforeCutover:
			logger.Info("schema change stopped",
				"reason", ctx.Err(),
			)
			// Don't change state - Stop() already set it to StateStopped
			utils.CloseAndLog(runner)
			return errSchemaChangeStopped
		default:
			// Only a proven cutover settles the change as landed.
			logger.Error("schema change stopped with an unrecognized classification of the cancelled run; leaving it stopped",
				"database", database,
				"tables", tables,
				"reason", ctx.Err(),
				"error", err,
			)
			utils.CloseAndLog(runner)
			return errSchemaChangeStopped
		}
	}

	utils.CloseAndLog(runner)
	if ctx.Err() != nil {
		// The runner finished the ALTER on a context a stop, cancel, or halt
		// had cancelled, so Spirit's own drop of the swapped-out originals,
		// which runs on that context, may have been refused. Finish it here
		// so the completed change leaves the target as an uninterrupted run
		// would have.
		e.dropCutoverOriginalsLeftByCancel(context.WithoutCancel(ctx), targetDSN(host, username, password, database), database, tables)
	}
	logger.Info("ALTER phase completed", "database", database, "tables", tables)
	return nil
}

// cutoverOriginalDropTimeout bounds the drop of the swapped-out originals a
// cancelled run left behind. It runs on a detached context, since the one the
// run had is already cancelled, so it needs a bound of its own: a drop can wait
// on the table's metadata lock, and the stop or cancel that is waiting for the
// runner should not wait on a server that stopped responding.
const cutoverOriginalDropTimeout = 2 * time.Minute

// dropCutoverOriginalsLeftByCancel drops the _old tables a completed cutover
// left on the target because the run's context was cancelled before Spirit
// dropped them. Spirit drops each swapped-out original on the run's own context
// after the cutover commits, and only logs a refusal, so a cancel that lands
// between the commit and that drop leaves the whole original table on the
// target while the runner still returns having finished the change. Dropping it
// here mirrors what the uninterrupted run does: the change completed, so the
// original is disposed of the way every completed cutover disposes of it. A
// failure is logged with the tables left behind, never returned: the change has
// completed whatever became of its leftovers.
func (e *Engine) dropCutoverOriginalsLeftByCancel(ctx context.Context, dsn, database string, tables []string) {
	ctx, cancel := context.WithTimeout(ctx, cutoverOriginalDropTimeout)
	defer cancel()
	logger := e.changeLogger()

	originals := make([]string, 0, len(tables))
	for _, table := range tables {
		originals = append(originals, utils.OldTableName(table))
	}
	leftover, err := func() ([]string, error) {
		db, err := mysqlconn.Open(dsn)
		if err != nil {
			return originals, fmt.Errorf("open database %s: %w", database, err)
		}
		defer utils.CloseAndLog(db)
		if err := db.PingContext(ctx); err != nil {
			return originals, fmt.Errorf("connect to database %s: %w", database, err)
		}
		present, err := existingTables(ctx, db, database, originals)
		if err != nil {
			return originals, err
		}
		if len(present) == 0 {
			return nil, nil
		}
		logger.Info("dropping the swapped-out originals Spirit left behind because the run's context was cancelled after the cutover committed",
			"database", database,
			"tables", present,
		)
		discarded, err := discardArtifacts(ctx, db, database, present)
		return present[len(discarded):], err
	}()
	if err != nil {
		logger.Warn("the schema change completed but its swapped-out originals could not be dropped; drop them by hand to reclaim their space",
			"database", database,
			"tables", leftover,
			"error", err,
		)
	}
}

func (e *Engine) setSchemaChangeCompleted() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runningSchemaChange != nil {
		e.runningSchemaChange.state = engine.StateCompleted
	}
}

// setSchemaChangeFailed sets the state to failed with a reason an operator can
// read. Every caller has already logged err with the target identifiers, so the
// detail this drops is still available where it is safe to keep it. An err
// classified permanent (see classifyRunnerError) is recorded as such, so
// progress tells the drive a retry would only repeat the same failure.
func (e *Engine) setSchemaChangeFailed(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runningSchemaChange != nil {
		e.runningSchemaChange.state = engine.StateFailed
		if err != nil {
			e.runningSchemaChange.errorMessage = failureReason(err)
			e.runningSchemaChange.permanentFailure = !engine.IsRetryable(err)
			e.runningSchemaChange.targetHeld = errors.Is(err, engine.ErrTargetHeld)
		}
	}
}

// failureReason renders why the schema change failed, for a pull request to
// show. The errors reaching here are mixed — a message SchemaBot wrote, a
// MySQL error, a dial failure, a library's own text — and all but the first
// carry text that must not be published: a dial failure names the target host,
// and a MySQL error quotes the row that could not be copied.
//
// So the message is taken only from an error explicitly marked as SchemaBot's
// own words. Everything else is classified by mysqlerr, which reads the error
// for its code and renders a sentence from its own vocabulary.
func failureReason(err error) string {
	if msg, ok := engine.OperatorMessageOf(err); ok {
		return msg
	}
	return mysqlerr.Reason(err)
}
