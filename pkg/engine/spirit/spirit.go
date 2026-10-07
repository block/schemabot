// Package spirit implements the schema change engine for MySQL databases using Spirit.
//
// Spirit performs online schema changes using a gh-ost-style approach:
// - Creates a shadow table with the new schema
// - Copies data in chunks while capturing changes
// - Atomically swaps tables at cutover
//
// For simple changes that MySQL can execute instantly (instant DDL), Spirit
// detects this and uses instant DDL instead of a full table copy.
//
// The Plan operation uses the differ package to compute schema differences.
// The Apply operation uses Spirit's runner to execute changes.
package spirit

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	spiritlint "github.com/block/spirit/pkg/lint"
	spiritmigration "github.com/block/spirit/pkg/migration"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/lint"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/pendingdrops"
	"github.com/block/schemabot/pkg/targetauth"
)

// DefaultThreads is the default number of concurrent copier threads. On
// Aurora, Spirit's autoscaler sizes the pools from the instance and adjusts
// them from throttler feedback during the copy, overriding this value; on
// other MySQL targets it is the fixed copier thread count.
const DefaultThreads = 2

// DefaultLockWaitTimeout is how long Spirit waits for table locks. Spirit's
// ForceKill (on by default) kills blocking transactions at 90% of this
// timeout. It does not kill an explicit LOCK TABLES holder or a transaction
// heavier than dbconn.TransactionWeightThreshold — those are left to finish
// naturally within the timeout, since killing them is considered unsafe.
const DefaultLockWaitTimeout = 10 * time.Second

// Engine implements the engine.Engine interface for MySQL using Spirit.
type Engine struct {
	logger       *slog.Logger
	spiritLogger *slog.Logger // Logger for Spirit (may filter debug logs)
	linter       *lint.Linter

	// Configuration; immutable after New. Every schema change this engine runs
	// starts from these values.
	threads         int
	lockWaitTimeout time.Duration
	// debugLogs is toggled at runtime and read from the Spirit log filter on
	// every line a running schema change emits, so it is read without taking
	// the engine lock a logging path must never contend on.
	debugLogs           atomic.Bool
	disablePendingDrops bool

	// Resolved Settings applied to every Spirit run; immutable after New.
	checkpointMaxAge time.Duration

	// onLog routes Spirit logs to the ApplyLogStore, with table context. It is
	// swapped as drives hand the engine over and read from the log filter on
	// every line, so it is held in an atomic slot: a caller clearing it while a
	// runner is still logging must not race the read, and a load that hands back
	// a callback must hand back one whole enough to call.
	onLog atomic.Pointer[logCallback]

	// Running schema change state
	mu                  sync.Mutex
	runningSchemaChange *runningSchemaChange
	// drainedOutcome retains the terminal result of the last schema change
	// Drain released, so a progress poll that lands after the drain still
	// observes the outcome. Apply releases it when it accepts new work.
	drainedOutcome *drainedOutcome

	// drainRaceWindow is a test seam invoked between the drained goroutine's
	// exit and Drain's release of the tracked state, so tests can interleave
	// engine activity into that window deterministically.
	drainRaceWindow func()

	// stopCheckpointWindow is a test seam invoked between Stop's checkpoint
	// dump and its write of the stopped state, so tests can land an outcome in
	// that window deterministically.
	stopCheckpointWindow func()

	// sizeProbeFault is a test seam invoked with the plan-time size probe's
	// context when the probe starts. A non-nil error fails the probe, so tests
	// can prove a failed or slow probe never fails or stalls a plan.
	sizeProbeFault func(ctx context.Context) error

	// sizeProbeSQL is a test seam that rewrites each statement the size probe
	// sends, given the probe's context, so tests can make the server slow to
	// answer and prove every statement runs under the probe's budget.
	sizeProbeSQL func(ctx context.Context, stmt string) string
}

// runningSchemaChange tracks the state of an in-progress schema change.
type runningSchemaChange struct {
	// logger is the logger scoped to this schema change, carrying the caller's triage
	// identity (apply id, repo, PR, environment); spiritLogger is the
	// filtered Spirit logger derived from it. Both fall back to the engine's
	// loggers when the caller did not provide one.
	logger       *slog.Logger
	spiritLogger *slog.Logger

	database          string            // MySQL database name parsed from DSN
	tableNamespace    map[string]string // table name → namespace (from ApplyRequest.Changes)
	tables            []string
	ddls              []string // DDL statement for each table
	originalDDLs      []string // Full statement list from Apply, in execution order; never overwritten so resume can run the whole plan
	combinedStatement string   // Original combined statement passed to Spirit (for checkpoint-safe restart)
	runners           []*spiritmigration.Runner
	progressCallback  func() string // returns Summary from Spirit's Progress API
	state             engine.State
	errorMessage      string // Error details when state is StateFailed
	permanentFailure  bool   // The failure reproduces on every retry, so it is reported as not retryable
	targetHeld        bool   // The run was refused the table because another run holds it (engine.ErrTargetHeld)
	started           time.Time
	deferCutover      bool // Whether to defer cutover until manual trigger

	// lastLiveTables is the most recent per-table progress built from a live
	// runner poll. The runners are closed by the time a drained outcome is
	// served, so this snapshot is how the outcome keeps the change's last
	// observed row counters instead of reporting zeroes.
	lastLiveTables []engine.TableProgress

	// directPolicy is the direct execution policy snapshotted at Apply, so a
	// resumed schema change routes with the same policy the apply started
	// with. directStatements tracks each direct-routed statement's lifecycle
	// for progress reporting.
	directPolicy     directPolicy
	directStatements []*directStatementProgress

	// quarantinedDrops records every table this attempt of the schema change
	// set out to move into pending drops, keyed by source table, so a DROP
	// phase replayed through Start skips the tables it already moved and still
	// fails on a table that vanished some other way. It lives in this process
	// only; a resume that goes through Apply starts with an empty record.
	quarantinedDrops map[dropTarget]pendingdrops.QuarantinedTable

	// owner is the drive the run belongs to (engine.WithWorkOwner), set when
	// Apply starts the run and again when Start resumes it, under Engine.mu.
	owner string

	// For resume support
	cancelFunc context.CancelFunc
	host       string
	username   string
	password   string

	// For waiting on schema change to finish. active counts the run goroutines
	// still executing, so a halt can tell work that is still running from work
	// that has already ended.
	wg     sync.WaitGroup
	active atomic.Int32
}

// goRun runs the schema change's background work, tracked by both wg and
// active.
func (rm *runningSchemaChange) goRun(run func()) {
	rm.active.Add(1)
	rm.wg.Go(func() {
		defer rm.active.Add(-1)
		run()
	})
}

// Compile-time check that Engine implements the interface.
var _ engine.Engine = (*Engine)(nil)
var _ engine.Drainer = (*Engine)(nil)
var _ engine.ShutdownHalter = (*Engine)(nil)
var _ engine.OwnedWorkHalter = (*Engine)(nil)
var _ engine.DeferredCutoverSignalChecker = (*Engine)(nil)
var _ engine.CancelledArtifactReleaser = (*Engine)(nil)

// Config holds configuration for the Spirit engine.
type Config struct {
	Logger          *slog.Logger
	Threads         int
	LockWaitTimeout time.Duration
	DebugLogs       bool // Enable Spirit's verbose debug logs (replication events, etc.)

	// DisablePendingDrops executes DROP TABLE statements directly instead of
	// quarantining the table in the pending drops database. Quarantine is the
	// default because it keeps dropped table data recoverable until the
	// retention period expires.
	DisablePendingDrops bool

	// Settings tunes the Spirit runs the engine starts; zero-value fields
	// resolve to the fleet defaults (see settings.go).
	Settings Settings
}

// New creates a new Spirit engine.
func New(cfg Config) *Engine {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	threads := cfg.Threads
	if threads == 0 {
		threads = DefaultThreads
	}

	lockWaitTimeout := cfg.LockWaitTimeout
	if lockWaitTimeout == 0 {
		lockWaitTimeout = DefaultLockWaitTimeout
	}

	checkpointMaxAge := cfg.Settings.CheckpointMaxAge
	if checkpointMaxAge == 0 {
		checkpointMaxAge = DefaultCheckpointMaxAge
	}

	eng := &Engine{
		logger:              logger,
		linter:              lint.New(),
		threads:             threads,
		lockWaitTimeout:     lockWaitTimeout,
		disablePendingDrops: cfg.DisablePendingDrops,
		checkpointMaxAge:    checkpointMaxAge,
	}
	eng.debugLogs.Store(cfg.DebugLogs)

	// Create Spirit logger with filter that checks debugLogs at runtime
	// and routes logs to ApplyLogStore via onLog callback
	eng.spiritLogger = slog.New(&spiritLogFilter{
		handler: logger.Handler(),
		debug:   &eng.debugLogs,
		onLog:   &eng.onLog,
	})

	return eng
}

func (e *Engine) Name() string {
	return "spirit"
}

// resolveChangeLoggers resolves the loggers for one schema change from the
// caller's optional request logger. When the caller provides a logger — bound
// with its triage identity (apply id, repo, PR, environment) — every engine
// line and every routed Spirit runner line for the change inherits that
// identity; otherwise the engine's configured loggers are used. The Spirit
// logger is rebuilt on the request logger's handler so it keeps the runtime
// debug-log filter and apply-log routing.
func (e *Engine) resolveChangeLoggers(reqLogger *slog.Logger) (logger, spiritLogger *slog.Logger) {
	if reqLogger == nil {
		return e.logger, e.spiritLogger
	}
	return reqLogger, slog.New(&spiritLogFilter{
		handler: reqLogger.Handler(),
		debug:   &e.debugLogs,
		onLog:   &e.onLog,
	})
}

// changeLogger returns the logger for the tracked schema change: the
// change-scoped logger bound by Apply when the caller provided one, or the
// engine's configured logger otherwise. Execution paths use it so every line
// about the running change carries the caller's triage identity.
func (e *Engine) changeLogger() *slog.Logger {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runningSchemaChange != nil && e.runningSchemaChange.logger != nil {
		return e.runningSchemaChange.logger
	}
	return e.logger
}

// schemaChangeLogger returns the logger bound to the given schema change,
// falling back to the engine's configured logger when the change is nil or
// carries no caller-bound logger. Control paths that hold a specific change
// use it instead of changeLogger so their lines carry that change's triage
// identity even when the engine has since started tracking a different one.
// The change's logger is set once at creation, so no lock is needed.
func (e *Engine) schemaChangeLogger(rm *runningSchemaChange) *slog.Logger {
	if rm != nil && rm.logger != nil {
		return rm.logger
	}
	return e.logger
}

// changeSpiritLogger returns the filtered Spirit logger for the tracked
// schema change, falling back to the engine-level Spirit logger. Runner log
// lines routed through it inherit the change's triage identity.
func (e *Engine) changeSpiritLogger() *slog.Logger {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runningSchemaChange != nil && e.runningSchemaChange.spiritLogger != nil {
		return e.runningSchemaChange.spiritLogger
	}
	return e.spiritLogger
}

// SetLogCallback sets a callback that receives Spirit log messages.
// Only INFO level and above are routed (DEBUG logs are filtered).
// The callback receives the log level, table name (if available), and message.
// Set to nil to disable log routing.
func (e *Engine) SetLogCallback(cb logCallback) {
	e.onLog.Store(&cb)
}

// SetDebugLogs enables or disables verbose Spirit debug logs at runtime.
// When disabled, noisy logs like "Received unknown event type" are filtered.
func (e *Engine) SetDebugLogs(enabled bool) {
	e.debugLogs.Store(enabled)
}

// DebugLogs returns whether verbose Spirit debug logs are enabled.
func (e *Engine) DebugLogs() bool {
	return e.debugLogs.Load()
}

// RegistersWorkSynchronously reports that Apply records the accepted schema
// change on this engine before it returns, so there is no window in which Spirit
// has accepted work it cannot yet describe. Spirit executes in a goroutine of
// this process with nothing to provision first, and the tracked state is
// published under the engine mutex before Apply returns; Drain and the cancel
// path are the only writers that clear it, and both mean the work is not coming
// back. A pending progress report for a task a driver believes is in flight is
// therefore conclusive rather than a phase to wait out.
func (e *Engine) RegistersWorkSynchronously() bool {
	return true
}

// Drain waits for any in-flight schema change goroutine to complete and clears
// the running schema change state. This ensures DB connections from a previous
// run are fully released before new operations begin.
//
// A schema change that ran to completion or failed is retained as a drained
// outcome so a progress poll that lands after the drain still observes the
// result instead of concluding no schema change ever ran. Draining an engine
// with nothing running changes nothing: an already-retained outcome stays
// retained.
//
// Drain waits without a bound, for a caller that owns the engine outright. A
// drive waits through DrainContext instead, so it never outlives its claim.
func (e *Engine) Drain() {
	rm, raceWindow := e.trackedForDrain()
	if rm == nil {
		return
	}
	rm.wg.Wait()
	e.releaseDrained(rm, raceWindow)
}

// DrainContext is Drain bounded by ctx. When ctx ends before the schema change
// exits, it returns an error and leaves the change tracked, so the engine
// still reports it as holding the target.
func (e *Engine) DrainContext(ctx context.Context) error {
	rm, raceWindow := e.trackedForDrain()
	if rm == nil {
		return nil
	}
	if err := waitForRunExit(ctx, rm); err != nil {
		return fmt.Errorf("drain schema change on database %s tables %v: still running after %w", rm.database, rm.tables, err)
	}
	e.releaseDrained(rm, raceWindow)
	return nil
}

// trackedForDrain returns the schema change a drain waits for, if any.
func (e *Engine) trackedForDrain() (*runningSchemaChange, func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runningSchemaChange, e.drainRaceWindow
}

// releaseDrained stops tracking rm once its run has exited, keeping its
// outcome for a later progress poll.
func (e *Engine) releaseDrained(rm *runningSchemaChange, raceWindow func()) {
	if raceWindow != nil {
		raceWindow()
	}

	e.mu.Lock()
	if e.runningSchemaChange != rm {
		// Another flow released or replaced the tracked state while this
		// drain waited — a newer accepted schema change or a concurrent
		// drain — and that flow owns the engine's state now.
		e.mu.Unlock()
		e.schemaChangeLogger(rm).Debug("drained schema change is no longer tracked; leaving the engine's current state in place",
			"database", rm.database, "tables", rm.tables)
		return
	}
	if retainsDrainedOutcome(rm.state) {
		e.drainedOutcome = newDrainedOutcome(rm)
	}
	e.runningSchemaChange = nil
	e.mu.Unlock()
}

// installRunningSchemaChange publishes rm as the engine's tracked schema
// change. Accepting new work releases the previous change's drained outcome in
// the same critical section, so no poll can ever observe the new change's
// tracked state alongside an older change's result — one schema change's
// outcome never bleeds into the next one's progress.
func (e *Engine) installRunningSchemaChange(rm *runningSchemaChange) {
	e.mu.Lock()
	e.runningSchemaChange = rm
	e.drainedOutcome = nil
	e.mu.Unlock()
}

// drainedOutcome is the retained result of a schema change that reached a
// terminal outcome before Drain released its tracked state. Progress serves it
// so a poll that lands after the drain reports the real result — the drain
// must never make a finished change look like one that never started. It lives
// under Engine.mu next to the tracked state it stands in for.
type drainedOutcome struct {
	state        engine.State
	database     string
	message      string
	errorMessage string // Failure details when state is StateFailed
	permanent    bool   // The failure reproduces on every retry
	targetHeld   bool   // The run was refused the table because another run holds it
	tables       []engine.TableProgress
}

// failureIsRetryable reports whether a progress result in state s offers the
// drive a retry: only a failure does, and only when retrying it could succeed.
// A permanent failure, such as a copy whose checksum keeps finding lost rows,
// would repeat the full table copy on every attempt and fail the same way.
func failureIsRetryable(s engine.State, permanent bool) bool {
	return s == engine.StateFailed && !permanent
}

// retainsDrainedOutcome reports whether a drained schema change's final state
// must stay observable to progress polls after the tracked state is released:
// the change ran to completion or failed. Stopped changes stay resumable
// through their stored checkpoint and cancelled changes resolve through the
// cancel call itself, so neither is retained.
func retainsDrainedOutcome(s engine.State) bool {
	return s == engine.StateCompleted || s == engine.StateFailed
}

// newDrainedOutcome snapshots the terminal result of a drained schema change.
// The runners are closed by the time the snapshot is served, so each table
// keeps the counters from the last live poll — a failure's last-known copy
// position is the operator's only remaining measure of how far it got, and a
// later sync must not overwrite the recorded position with zeroes. A change
// drained before any live poll observed per-table counters falls back to
// identity, DDL, and final state alone. The caller must hold e.mu.
func newDrainedOutcome(rm *runningSchemaChange) *drainedOutcome {
	var tables []engine.TableProgress
	if len(rm.lastLiveTables) > 0 {
		tables = make([]engine.TableProgress, 0, len(rm.lastLiveTables)+len(rm.directStatements))
		for _, tp := range rm.lastLiveTables {
			tp.State = string(rm.state)
			// ETA and throttle describe live pacing; a terminal outcome has
			// neither.
			tp.ETASeconds = 0
			tp.Throttled = false
			tp.ThrottleReason = ""
			if rm.state == engine.StateCompleted {
				tp.Progress = 100
				// A completed change copied everything, so the copied count is
				// ground truth: reconcile the estimated total to it the way the
				// live path does.
				tp.RowsTotal = tp.RowsCopied
			}
			tables = append(tables, tp)
		}
	} else {
		tables = tableIdentityProgress(rm, string(rm.state))
		if rm.state == engine.StateCompleted {
			for i := range tables {
				tables[i].Progress = 100
			}
		}
	}
	tables = append(tables, directStatementTableProgress(rm)...)
	return &drainedOutcome{
		state:        rm.state,
		database:     rm.database,
		message:      fmt.Sprintf("Schema change %s", rm.state),
		errorMessage: rm.errorMessage,
		permanent:    rm.permanentFailure,
		targetHeld:   rm.targetHeld,
		tables:       tables,
	}
}

// HaltForShutdown brings this instance's in-flight schema change down so the
// process can exit without leaving the target held. Spirit runs the copy in a
// goroutine of this process and holds an advisory lock on the table for as long
// as that goroutine lives, so a process that exits without halting it keeps the
// lock while no longer renewing the apply's lease — every driver that then
// reclaims the apply is refused the lock and burns a recovery attempt.
//
// A drive that hands its apply back for another driver halts its own run the
// same way, through HaltWorkOwnedBy, for the same reason: the work must not
// outlive the claim it was started under.
//
// This is not an operator stop. The tracked state is left alone so nothing
// reads the halt as an operator's decision to park the apply: the schema change
// is checkpointed and the apply stays active for another driver to claim and
// resume.
func (e *Engine) HaltForShutdown(ctx context.Context) error {
	return e.halt(ctx, func(*runningSchemaChange) bool { return true })
}

// HaltWorkOwnedBy halts like HaltForShutdown, but only a run that owner
// started. The run is selected and its owner compared under the same lock that
// publishes a new run, so a run another drive starts in its place is never the
// one this halt reaches.
func (e *Engine) HaltWorkOwnedBy(ctx context.Context, owner string) error {
	return e.halt(ctx, func(rm *runningSchemaChange) bool {
		if rm.owner == owner {
			return true
		}
		e.schemaChangeLogger(rm).Debug("schema change belongs to another drive; leaving it running",
			"database", rm.database, "tables", rm.tables)
		return false
	})
}

// halt brings the tracked run down when selected reports it is one to halt.
// selected is called under e.mu.
func (e *Engine) halt(ctx context.Context, selected func(*runningSchemaChange) bool) error {
	e.mu.Lock()
	rm := e.runningSchemaChange
	if rm == nil {
		e.mu.Unlock()
		return nil
	}
	if !selected(rm) {
		e.mu.Unlock()
		return nil
	}
	if rm.active.Load() == 0 {
		database, tables, state := rm.database, rm.tables, rm.state
		e.mu.Unlock()
		e.schemaChangeLogger(rm).Debug("schema change has already ended; nothing to halt",
			"database", database, "tables", tables, "state", state)
		return nil
	}
	runners := rm.runners
	database := rm.database
	tables := rm.tables
	cancelRun := rm.cancelFunc
	outcome := rm.state
	e.mu.Unlock()

	logger := e.schemaChangeLogger(rm)

	// A change that has reached its outcome is only tearing down: checkpointing
	// it would write progress for work that is over, and cancelling would cut
	// short a teardown that releases the table on its own. Wait for it instead.
	if outcome.IsTerminal() {
		return waitForSchemaChangeExit(ctx, rm, database, tables)
	}

	// Checkpoint before cancelling. Spirit checkpoints on its own interval, so
	// without this the driver that reclaims the apply resumes from the last
	// periodic checkpoint and recopies everything written since.
	if len(runners) > 0 && runners[0] != nil {
		if err := runners[0].DumpCheckpoint(ctx); err != nil {
			logger.Warn("could not checkpoint before halting for shutdown; the schema change resumes from its last periodic checkpoint",
				"database", database, "tables", tables, "error", err)
		}
	}

	if cancelRun != nil {
		cancelRun()
	}

	if err := waitForSchemaChangeExit(ctx, rm, database, tables); err != nil {
		return err
	}
	logger.Info("schema change halted; the target's lock is released and the apply stays active for another driver",
		"database", database, "tables", tables)
	return nil
}

// waitForSchemaChangeExit waits for the change's run goroutines to return. It
// waits off the calling goroutine so a runner that will not come down bounds
// the caller at ctx rather than blocking it forever; the wait goroutine ends
// with the runner it is waiting on.
func waitForSchemaChangeExit(ctx context.Context, rm *runningSchemaChange, database string, tables []string) error {
	if err := waitForRunExit(ctx, rm); err != nil {
		return fmt.Errorf("halt schema change on database %s tables %v: still running after %w; the target may still be locked", database, tables, err)
	}
	return nil
}

// waitForRunExit waits off the calling goroutine for the change's run
// goroutines to return, or for ctx to end. The wait goroutine ends with the
// runner it is waiting on.
func waitForRunExit(ctx context.Context, rm *runningSchemaChange) error {
	done := make(chan struct{})
	go func() {
		rm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Plan computes the schema changes needed by diffing current schema against desired.
func (e *Engine) Plan(ctx context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
	if req.Credentials == nil || req.Credentials.DSN == "" {
		return nil, fmt.Errorf("DSN credentials required for Spirit engine")
	}

	// Extract database name from DSN (DSN is the source of truth for actual database)
	_, _, _, database, err := parseDSN(req.Credentials.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse DSN: %w", err)
	}

	e.logger.Info("computing plan",
		"database", database,
		"schema_files", len(req.SchemaFiles),
	)

	// Fetch current schema from database (use database from DSN, not req.Database)
	ignored := engine.NewIgnoredTables(req.IgnoreTables)
	currentSchema, withheld, err := e.fetchCurrentSchema(ctx, req.Credentials.DSN, database, ignored)
	if err != nil {
		return nil, fmt.Errorf("fetch current schema: %w", err)
	}
	e.logger.Debug("fetched current schema", "database", database, "tables", len(currentSchema))
	for i, ts := range currentSchema {
		e.logger.Debug("current schema table", "index", i, "stmt", ts.Schema[:min(200, len(ts.Schema))])
	}
	exemptTables := exemptWithheldTables(ignored, req, database, withheld)

	// Build list of desired table schemas from all namespaces. Every namespace
	// is diffed as one set against the database, so a table two schema files
	// declare, in one namespace or across them, is refused; files are read in
	// sorted order so the refusal names the same pair on every run.
	var desiredSchemas []table.TableSchema
	var declared ddl.TableDeclarations
	declaredByNamespace := make(map[string][]string, len(req.SchemaFiles))
	for _, namespace := range slices.Sorted(maps.Keys(req.SchemaFiles)) {
		ns := req.SchemaFiles[namespace]
		for _, filename := range slices.Sorted(maps.Keys(ns.Files)) {
			content := ns.Files[filename]
			stmts, err := ddl.SplitStatements(content)
			if err != nil {
				return nil, fmt.Errorf("split statements in %s/%s: %w", namespace, filename, err)
			}
			for _, stmt := range stmts {
				ct, err := statement.ParseCreateTable(stmt)
				if err != nil {
					return nil, fmt.Errorf("parse desired schema in %s/%s: %w", namespace, filename, err)
				}
				// Validate semantic correctness (e.g., index columns exist)
				if err := ddl.ValidateCreateTable(ct); err != nil {
					return nil, fmt.Errorf("SQL usage error in %s/%s: %w", namespace, filename, err)
				}
				if err := declared.Declare(namespace+"/"+filename, ct.TableName); err != nil {
					return nil, err
				}
				desiredSchemas = append(desiredSchemas, table.TableSchema{Name: ct.TableName, Schema: stmt})
				declaredByNamespace[namespace] = append(declaredByNamespace[namespace], ct.TableName)
			}
		}
	}

	// A table the config withholds that a schema file also declares is a
	// contradiction the diff below would resolve as a CREATE TABLE for a table
	// that already exists. Refuse in namespace order so the error names the
	// same namespace on every run.
	for _, namespace := range slices.Sorted(maps.Keys(declaredByNamespace)) {
		if err := ignored.RefuseDeclared(namespace, declaredByNamespace[namespace]); err != nil {
			return nil, err
		}
	}

	// Use Spirit's PlanChanges to diff + lint in one call.
	// This combines DeclarativeToImperative (diff) with RunLinters (lint),
	// returning per-statement lint results with severity levels.
	plan, err := lint.PlanChanges(currentSchema, desiredSchemas, nil, e.linter.SpiritConfig())
	if err != nil {
		return nil, err
	}

	// The direct execution policy resolves refused statements to a direct or
	// blocked verdict below. A malformed policy fails the plan: silently
	// treating it as disabled would record blocked verdicts the apply-time
	// routing might not agree with.
	verdicts, err := e.NewExecutionVerdicts(req.Credentials)
	if err != nil {
		return nil, err
	}
	// The target connection opens lazily, so a plan with no changes never
	// opens it. The size probe, the policy bound's row estimates, the
	// collation report's charset defaults, and the existing-copy disclosure
	// below all read the target through it.
	defer verdicts.Close()
	target := verdicts.target

	if !plan.HasChanges() {
		// The exemption travels on a no-changes plan too: this is exactly where
		// a reviewer needs to tell a withheld live table from an unchanged one.
		// A plan with nothing to apply meets no copy, so it is checked by
		// construction.
		return &engine.PlanResult{
			PlanID:                engine.NewPlanID(),
			NoChanges:             true,
			ExistingCopiesChecked: true,
			ExemptTables:          exemptTables,
		}, nil
	}

	// The engine's refusal checks compare a redeclared column against its
	// current type, so each ALTER is classified alongside the table's current
	// definition. The diff only emits an ALTER for a table present in
	// currentSchema, so every ALTER below has an entry here.
	currentByTable := make(map[string]string, len(currentSchema))
	for _, ts := range currentSchema {
		currentByTable[ts.Name] = ts.Schema
	}
	// The collation report names the unique indexes that cover a re-collated
	// column as the desired definition leaves them.
	desiredByTable := make(map[string]string, len(desiredSchemas))
	for _, ts := range desiredSchemas {
		desiredByTable[ts.Name] = ts.Schema
	}
	collationDefaults := &targetCollationDefaults{target: target}
	defaultCollation := func(charset string) (string, error) {
		return collationDefaults.defaultCollation(ctx, charset)
	}

	// Best-effort per-table size estimates for plan display, read only for the
	// tables this plan touches so a database with many unrelated tables does
	// not pay for them. Sizes are informational — a failed or slow read must
	// not fail or stall the plan, so the probe runs under its own budget, and
	// a miss logs and renders the plan without sizes.
	// A table the plan creates has no size to read yet, so only existing
	// tables are probed: a plan that only creates tables never connects to
	// the target for sizes.
	sizedTables, createdTables := partitionByExistence(e.plannedTableNames(database, plan.Changes), currentByTable)
	if len(createdTables) > 0 {
		e.logger.Debug("tables the plan creates have no size estimate yet",
			"database", database, "tables", createdTables)
	}
	probeCtx, cancelProbe := context.WithTimeout(ctx, engine.TableSizeProbeTimeout)
	sizeEstimates, err := e.fetchTableSizeEstimates(probeCtx, target, database, sizedTables)
	cancelProbe()
	if err != nil {
		e.logger.Warn("table size estimates unavailable; the plan will omit table sizes",
			"database", database, "tables", sizedTables, "error", err)
		sizeEstimates = nil
	} else {
		e.logMissingSizeEstimates(database, sizedTables, sizeEstimates)
	}

	// Convert PlannedChanges to engine types
	var lintViolations []engine.LintViolation
	changes := make([]engine.TableChange, 0, len(plan.Changes))
	for _, pc := range plan.Changes {
		stmtType, _, err := ddl.ClassifyStatement(pc.Statement)
		if err != nil {
			return nil, err
		}
		change := engine.TableChange{
			Table:     pc.TableName,
			Operation: stmtType,
			DDL:       pc.Statement,
		}

		// Attach the plan-time size estimates statistics reported. A table
		// being created does not exist yet and gets none.
		if est, ok := sizeEstimates[pc.TableName]; ok {
			change.EstimatedRows = copyInt64(est.rows)
			change.EstimatedBytes = copyInt64(est.bytes)
		}

		// Error-severity violations mark the change as unsafe
		if errViolations := pc.Errors(); len(errViolations) > 0 {
			change.IsUnsafe = true
			msgs := make([]string, len(errViolations))
			for i, v := range errViolations {
				msgs[i] = v.Message
			}
			change.UnsafeReason = strings.Join(msgs, "; ")
		}

		// Execution-mode verdict: surface statements Spirit deterministically
		// refuses so the operator learns at plan time how the apply will
		// behave — routed to direct execution when the policy permits, or
		// guaranteed to fail when it doesn't. The diff emits only CREATE,
		// ALTER, and DROP, so every statement that can be refused here is an
		// ALTER.
		if verdictApplies(stmtType) {
			currentCreateTable, ok := currentByTable[pc.TableName]
			if !ok {
				return nil, fmt.Errorf("plan produced an ALTER for table %q, which has no current definition in database %q", pc.TableName, database)
			}
			if err := verdicts.record(ctx, &change, currentCreateTable); err != nil {
				return nil, err
			}
			desiredCreateTable, ok := desiredByTable[pc.TableName]
			if !ok {
				return nil, fmt.Errorf("plan produced an ALTER for table %q, which no schema file declares", pc.TableName)
			}
			change.CollationChanges, err = plannedCollationChanges(e.logger, pc.Statement, currentCreateTable, desiredCreateTable, defaultCollation)
			if err != nil {
				return nil, fmt.Errorf("resolve collation changes for table %q: %w", pc.TableName, err)
			}
		}

		changes = append(changes, change)

		// Collect lint violations from all severity levels
		for _, v := range pc.Violations {
			lintViolations = append(lintViolations, engine.LintViolation{
				Table:    pc.TableName,
				Linter:   v.Linter.Name(),
				Message:  v.Message,
				Severity: strings.ToLower(v.Severity.String()),
			})
		}
	}

	// Build per-namespace SchemaChanges.
	// Spirit operates on a single database, but we group table changes by the
	// namespace they belong to (from SchemaFiles keys) for consistency with
	// multi-namespace engines like PlanetScale.
	changesByNS := make(map[string][]engine.TableChange)
	for _, tc := range changes {
		ns, err := namespaceForTable(tc.Table, req.SchemaFiles)
		if err != nil {
			return nil, fmt.Errorf("namespace lookup for table %q: %w", tc.Table, err)
		}
		changesByNS[ns] = append(changesByNS[ns], tc)
	}
	originalFilesByNS := make(map[string]map[string]string, len(changesByNS))
	for ns := range changesByNS {
		originalFilesByNS[ns] = map[string]string{}
	}
	if len(req.SchemaFiles) == 1 {
		for ns := range req.SchemaFiles {
			for _, ts := range currentSchema {
				originalFilesByNS[ns][ts.Name+".sql"] = ts.Schema
			}
		}
	} else {
		for _, ts := range currentSchema {
			ns, err := namespaceForTable(ts.Name, req.SchemaFiles)
			if err != nil {
				return nil, fmt.Errorf("namespace lookup for original table %q: %w", ts.Name, err)
			}
			if _, ok := originalFilesByNS[ns]; ok {
				originalFilesByNS[ns][ts.Name+".sql"] = ts.Schema
			}
		}
	}
	var schemaChanges []engine.SchemaChange
	for ns, tableChanges := range changesByNS {
		schemaChanges = append(schemaChanges, engine.SchemaChange{
			Namespace:             ns,
			TableChanges:          tableChanges,
			OriginalFiles:         originalFilesByNS[ns],
			OriginalFilesCaptured: true,
		})
	}

	// Applying this plan can meet a copy an earlier schema change left on the
	// target and continue it or destroy it. Disclose which, so that is known
	// before anyone confirms rather than after the copy is gone.
	existingCopies, copiesChecked := e.plannedExistingCopies(ctx, target, database, changes, req.GroupedExecution)
	return &engine.PlanResult{
		PlanID:                engine.NewPlanID(),
		Changes:               schemaChanges,
		LintViolations:        lintViolations,
		ExistingCopies:        existingCopies,
		ExistingCopiesChecked: copiesChecked,
		ExemptTables:          exemptTables,
	}, nil
}

// exemptWithheldTables builds the plan's ignore_tables disclosure. A withheld
// table has no declaring file, so the file-based attribution the table changes
// use cannot place it in a namespace: with one namespace in the request it
// belongs to that namespace, and with several the unit being diffed is the
// database itself, which is what the disclosure then names.
func exemptWithheldTables(ignored engine.IgnoredTables, req *engine.PlanRequest, database string, withheld []string) []*engine.ExemptTables {
	namespace := database
	if len(req.SchemaFiles) == 1 {
		for ns := range req.SchemaFiles {
			namespace = ns
		}
	}
	exemption := ignored.Exemption(namespace, withheld)
	if exemption == nil {
		return nil
	}
	return []*engine.ExemptTables{exemption}
}

// Apply starts executing a schema change plan using Spirit.
func (e *Engine) Apply(ctx context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	// Check for defer_cutover option
	deferCutover := req.Options["defer_cutover"] == "true"

	logger, spiritLogger := e.resolveChangeLoggers(req.Logger)

	logger.Info("applying plan",
		"database", req.Database,
		"ddl_count", len(req.FlatDDL()),
		"defer_cutover", deferCutover,
		"options", req.Options,
	)

	if req.Credentials == nil || req.Credentials.DSN == "" {
		return nil, fmt.Errorf("DSN credentials required for Spirit engine")
	}

	if len(req.FlatDDL()) == 0 {
		// An accepted no-op is still accepted work: release the previous
		// change's drained outcome so a poll after this accept reads an idle
		// engine rather than another change's result as its own.
		e.mu.Lock()
		e.drainedOutcome = nil
		e.mu.Unlock()
		return &engine.ApplyResult{
			Accepted: true,
			Message:  "No changes to apply",
		}, nil
	}

	// Resolve the direct execution policy up front so a malformed policy
	// rejects the apply before any state is created, and snapshot it on the
	// running change below so resume routes with the same policy.
	directExecPolicy, err := directPolicyFromMetadata(req.Credentials.Metadata)
	if err != nil {
		return nil, fmt.Errorf("resolve direct execution policy: %w", err)
	}

	// Parse DSN to extract connection info (DSN is the source of truth for actual database)
	host, username, password, database, err := parseDSN(req.Credentials.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse DSN: %w", err)
	}

	// Wait for any in-flight schema change to fully exit before starting a new
	// one. This ensures the old Spirit runner's DB connections are released.
	// The wait ends with the caller's context, so a drive whose claim is gone
	// does not sit behind a run it can no longer act on.
	if err := e.DrainContext(ctx); err != nil {
		return nil, fmt.Errorf("wait for the previous schema change to exit: %w", err)
	}

	// Initialize running state and start background execution.
	// Build a table→namespace lookup from the apply request. Each SchemaChange
	// carries a namespace and a list of table changes. Spirit flattens all DDLs
	// into one execution, so we need to map each table back to its namespace
	// for progress key matching.
	tableNamespace := make(map[string]string)
	for _, sc := range req.Changes {
		for _, tc := range sc.TableChanges {
			tableNamespace[tc.Table] = sc.Namespace
		}
	}

	// Start schema change in background with cancellable context.
	// Use WithoutCancel to preserve context values (tracing) without inheriting
	// the request deadline — the schema change must outlive the API call.
	// Stop() and HaltForShutdown cancel via rm.cancelFunc, which is in place
	// before the run is published, so a halt that finds the run active always
	// reaches it.
	bgCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	rm := &runningSchemaChange{
		logger:         logger,
		spiritLogger:   spiritLogger,
		database:       database,
		tableNamespace: tableNamespace,
		tables:         nil, // Tables will be populated by executeSchemaChange
		originalDDLs:   req.FlatDDL(),
		state:          engine.StateRunning,
		started:        time.Now(),
		deferCutover:   deferCutover,
		directPolicy:   directExecPolicy,
		host:           host,
		username:       username,
		password:       password,
		cancelFunc:     cancel,
		owner:          engine.WorkOwnerFromContext(ctx),
	}
	e.installRunningSchemaChange(rm)

	rm.goRun(func() {
		defer cancel()
		e.executeSchemaChange(bgCtx, host, username, password, database, req.FlatDDL(), deferCutover, directExecPolicy)
	})

	return &engine.ApplyResult{
		Accepted:    true,
		Message:     fmt.Sprintf("Started schema change with %d DDL statements", len(req.FlatDDL())),
		ResumeState: req.ResumeState,
	}, nil
}

// Progress returns the current schema change status.
// Uses Spirit's Progress API which returns a Summary string like "12.5% copyRows ETA 1h 30m"
func (e *Engine) Progress(ctx context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.runningSchemaChange == nil {
		// A retained drained outcome is still this engine's answer for the
		// last schema change it ran; only a truly idle engine reports pending.
		if d := e.drainedOutcome; d != nil {
			return &engine.ProgressResult{
				State:        d.state,
				Message:      d.message,
				ErrorMessage: d.errorMessage,
				Retryable:    failureIsRetryable(d.state, d.permanent),
				TargetHeld:   d.state == engine.StateFailed && d.targetHeld,
				Tables:       slices.Clone(d.tables),
				ResumeState:  req.ResumeState,
			}, nil
		}
		return &engine.ProgressResult{
			State:   engine.StatePending,
			Message: "No active schema change",
		}, nil
	}

	rm := e.runningSchemaChange

	// Get progress from the single runner (handles all tables together)
	message := fmt.Sprintf("Schema change %s", rm.state)
	var spiritState status.State
	var spiritProgress status.Progress
	if len(rm.runners) > 0 && rm.runners[0] != nil {
		spiritProgress = rm.runners[0].Progress()
		if spiritProgress.Summary != "" {
			message = spiritProgress.Summary
		}
		spiritState = spiritProgress.CurrentState
	}

	// Build table progress using Spirit's per-table progress if available
	var tableProgress []engine.TableProgress
	stateStr := spiritState.String()

	// Create a map of DDLs by table name for lookup
	ddlByTable := make(map[string]string)
	for i, tableName := range rm.tables {
		if i < len(rm.ddls) {
			ddlByTable[tableName] = rm.ddls[i]
		}
	}

	// If Spirit provides per-table progress, use it
	if len(spiritProgress.Tables) > 0 {
		tableProgress = buildSpiritTableProgress(spiritProgress, spiritState, ddlByTable, rm.tableNamespace)
		rm.lastLiveTables = slices.Clone(tableProgress)
	} else {
		// Fallback: no per-table progress available
		tableProgress = tableIdentityProgress(rm, stateStr)
		// Only show progress detail on first table
		if len(tableProgress) > 0 {
			tableProgress[0].ProgressDetail = message
		}
	}

	tableProgress = append(tableProgress, directStatementTableProgress(rm)...)

	state := progressState(rm, spiritState)

	// If state was overridden and message is still the default fallback, update message to match
	defaultMessage := fmt.Sprintf("Schema change %s", rm.state)
	if state != rm.state && message == defaultMessage {
		message = fmt.Sprintf("Schema change %s", state)
	}

	return &engine.ProgressResult{
		State:                 state,
		Message:               message,
		ErrorMessage:          rm.errorMessage,
		Retryable:             failureIsRetryable(state, rm.permanentFailure),
		TargetHeld:            state == engine.StateFailed && rm.targetHeld,
		Tables:                tableProgress,
		ResumeState:           req.ResumeState,
		ResumedFromCheckpoint: spiritProgress.Resume,
	}, nil
}

// tableIdentityProgress renders one entry per tracked table carrying identity,
// index-aligned DDL, and the given state — the shape shared by a live poll
// with no per-table progress from Spirit yet and a drained outcome captured
// before any live poll observed counters. Keeping the DDL index alignment in
// one place keeps the two consumers from drifting apart. The caller must hold
// e.mu.
func tableIdentityProgress(rm *runningSchemaChange, stateStr string) []engine.TableProgress {
	entries := make([]engine.TableProgress, 0, len(rm.tables))
	for i, tableName := range rm.tables {
		tp := engine.TableProgress{
			Namespace: rm.tableNamespace[tableName],
			Table:     tableName,
			State:     stateStr,
		}
		if i < len(rm.ddls) {
			tp.DDL = rm.ddls[i]
		}
		entries = append(entries, tp)
	}
	return entries
}

// directStatementTableProgress renders the schema change's direct-routed
// statements as table progress entries. Direct statements run outside the
// Spirit runner, so they report their own explicit lifecycle entries: no row
// counts or ETA, just the per-statement state transitions the executor
// recorded. The caller must hold e.mu.
func directStatementTableProgress(rm *runningSchemaChange) []engine.TableProgress {
	entries := make([]engine.TableProgress, 0, len(rm.directStatements))
	for _, ds := range rm.directStatements {
		tp := engine.TableProgress{
			Namespace:      rm.tableNamespace[ds.table],
			Table:          ds.table,
			DDL:            ds.ddl,
			State:          ds.state,
			ProgressDetail: "direct execution (native MySQL DDL)",
		}
		started := ds.startedAt
		tp.StartedAt = &started
		if ds.completedAt != nil {
			completed := *ds.completedAt
			tp.CompletedAt = &completed
		}
		if ds.state == directStateCompleted {
			tp.Progress = 100
		}
		entries = append(entries, tp)
	}
	return entries
}

// buildSpiritTableProgress maps Spirit's per-table progress into engine
// TableProgress. Spirit reports a single remaining row-copy estimate for the
// whole runner, so the ETA is surfaced on the tables still copying that have an
// established row total; completed tables keep 0, as do tables without a total
// yet and estimates that aren't ready (still measuring the copy rate, or
// essentially done).
//
// Spirit's per-table RowsTotal is a statistics-based estimate, not a count, so
// a finished copy can land above or below it. Once the copy is complete the
// copied count is ground truth: the total is reconciled to it so the table
// reports a consistent 100% instead of a full bar contradicted by its own rows
// line (estimate high) or an over-100 ratio (estimate low).
func buildSpiritTableProgress(prog status.Progress, spiritState status.State, ddlByTable, tableNamespace map[string]string) []engine.TableProgress {
	stateStr := spiritState.String()
	var etaSeconds int64
	if prog.ETA.State == status.ETAReady {
		etaSeconds = int64(prog.ETA.Duration.Seconds())
	}
	tableProgress := make([]engine.TableProgress, 0, len(prog.Tables))
	for _, st := range prog.Tables {
		tp := engine.TableProgress{
			Namespace:  tableNamespace[st.TableName],
			Table:      st.TableName,
			DDL:        ddlByTable[st.TableName],
			State:      stateStr,
			RowsCopied: int64(st.RowsCopied),
			RowsTotal:  int64(st.RowsTotal),
		}
		if st.IsComplete {
			tp.RowsTotal = tp.RowsCopied
			tp.Progress = 100
			// While the runner works through its post-copy phases the table's
			// interesting state is that phase (applying accumulated changes,
			// verifying, cutting over), not the fact that its copy finished.
			// Keep the runner phase so consumers can surface post-copy work;
			// outside those phases the table is simply done copying while
			// other tables copy.
			if !spiritPostCopyPhase(spiritState) {
				tp.State = "completed"
			}
		} else if st.RowsTotal > 0 {
			// Clamp to 100 — concurrent inserts can push RowsCopied past the estimate.
			tp.Progress = min(int(float64(st.RowsCopied)/float64(st.RowsTotal)*100), 100)
			tp.ETASeconds = etaSeconds
		}
		// Spirit reports a single runner-wide checksum estimate (rows verified so
		// far / total to verify), populated only during the verify phase and zero
		// otherwise. Every table copy is complete by the time the verify phase
		// runs, so the estimate is stamped on all tables unconditionally.
		tp.ChecksumRowsChecked = int64(prog.Checksum.RowsChecked)
		tp.ChecksumRowsTotal = int64(prog.Checksum.RowsTotal)
		// Spirit's throttle status is likewise runner-wide and already scoped to
		// the paced phases (the row copy and the checksum verify; zero-valued
		// everywhere else). Stamp it on the tables participating in that paced
		// work — a table still copying, or every table during the verify — so a
		// completed table is never rendered as paused by another table's copy.
		// The reason is stamped only with the flag, keeping the contract that
		// an unthrottled table carries no reason.
		if tableInPacedPhase(st.IsComplete, spiritState) && prog.Throttle.Throttled {
			tp.Throttled = true
			tp.ThrottleReason = engine.SanitizeThrottleReason(prog.Throttle.Reason)
		}
		tableProgress = append(tableProgress, tp)
	}
	return tableProgress
}

// tableInPacedPhase reports whether a table is doing work the runner's
// throttler paces: its own row copy while incomplete, or the runner-wide
// checksum verify (which runs only after every copy finished).
func tableInPacedPhase(copyComplete bool, spiritState status.State) bool {
	return !copyComplete || spiritState == status.Checksum
}

// spiritPostCopyPhase reports whether the runner is in one of the active
// phases between finishing the row copy and finishing the cutover: applying
// the accumulated changeset, restoring deferred indexes, analyzing, verifying
// via checksum, or the cutover itself. During these phases the runner state is
// the whole schema change's state, so it is surfaced per table instead of
// "completed". Waiting states, the post-cutover reverse window, and teardown
// are excluded — those are reported through the apply-level state.
func spiritPostCopyPhase(s status.State) bool {
	switch s {
	case status.ApplyChangeset, status.RestoreSecondaryIndexes, status.AnalyzeTable,
		status.Checksum, status.PostChecksum, status.CutOver:
		return true
	default:
		return false
	}
}

// progressState resolves the state reported for a progress poll. The tracked
// state is authoritative for terminal outcomes: they are recorded before the
// runner is closed, so a runner observed mid-teardown never changes the
// recorded outcome. Spirit's status only refines a non-terminal state, e.g.
// surfacing the sentinel wait for a deferred cutover.
func progressState(rm *runningSchemaChange, spiritState status.State) engine.State {
	state := rm.state
	if !state.IsTerminal() && spiritState == status.WaitingOnSentinelTable && rm.deferCutover {
		state = engine.StateWaitingForCutover
	}
	return state
}

// liveSchemaFilterOptions returns the loader options for a live-schema read.
//
// The loader drops an excluded table before reading its definition, so an
// exclusion moved to this side of it costs one SHOW CREATE TABLE per table it
// would have dropped — and turns a table that cannot be read into a failed
// plan, where the loader would never have looked at it. The archive exclusion
// therefore stays with the loader unless the config names a table the archive
// convention also excludes, which is the only case where the two orderings
// disagree about what the plan discloses.
//
// The test is on the entry's shape, not on whether it names a live table, so
// one archive-shaped entry takes the exclusion off the whole read even when it
// matches nothing — and the convention it matches is the one that partition
// rotation produces, where the affected set can be large. The loader decides
// before it has a name list to compare against, so shape is all this side can
// test; an exclusion that kept the cheap path for every archive table the
// config does not name would have to come from the loader, which already holds
// the full list from SHOW TABLES before it reads a definition.
//
// The leading-underscore exclusion stays with the loader unconditionally: it
// discards the names it drops, so an entry naming one cannot be disclosed as
// withheld from here whatever the ordering.
func liveSchemaFilterOptions(ignored engine.IgnoredTables) []table.FilterOption {
	opts := []table.FilterOption{table.WithoutUnderscoreTables, table.WithStrippedAutoIncrement}
	if !ignored.NamesAny(table.IsArchiveTable) {
		opts = append(opts, table.WithoutArchiveTables)
	}
	return opts
}

// fetchCurrentSchema retrieves table schemas from the database, filtering out
// internal tables (Spirit shadow/checkpoint tables and other _-prefixed tables)
// and archive tables that are maintained outside declarative schema files.
//
// ignored withholds the tables the repository's ignore_tables config names. The
// second return value is what it actually withheld, sorted, for the plan to
// disclose: the diff never sees these tables, so without the disclosure a
// withheld table would be indistinguishable from an unchanged one.
func (e *Engine) fetchCurrentSchema(ctx context.Context, dsn, database string, ignored engine.IgnoredTables) ([]table.TableSchema, []string, error) {
	db, err := mysqlconn.Open(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open target database: %w", err)
	}
	defer utils.CloseAndLog(db)

	// Open only parsed the DSN; this ping is the first dial, so it is where the
	// target can refuse the session and the only error worth classifying.
	if err := db.PingContext(ctx); err != nil {
		return nil, nil, fmt.Errorf("ping target database: %w", targetauth.Wrap(err))
	}

	tables, err := table.LoadSchemaFromDB(ctx, db, liveSchemaFilterOptions(ignored)...)
	if err != nil {
		return nil, nil, fmt.Errorf("load schema: %w", err)
	}

	// The archive-naming exclusion is applied here when the loader was not
	// asked for it, so that the config's own entries are matched against the
	// target's catalog first. An entry naming a table the archive convention
	// also excludes is then disclosed as withheld, the same as on every other
	// engine, instead of counting as an entry that withheld nothing because
	// another exclusion reached it first. When the loader did apply it
	// this pass finds nothing, since it is the same predicate.
	kept := make([]table.TableSchema, 0, len(tables))
	var withheld []string
	for _, ts := range tables {
		if ignored.Withholds(ts.Name) {
			withheld = append(withheld, ts.Name)
			continue
		}
		if table.IsArchiveTable(ts.Name) {
			continue
		}
		kept = append(kept, ts)
	}
	slices.Sort(withheld)
	if len(withheld) > 0 {
		// The plan discloses this too; the log is where an operator tracing
		// "why does the plan not mention table X" finds the answer without a
		// plan comment in front of them.
		e.logger.Info("live tables withheld from the plan by ignore_tables",
			"database", database, "tables", withheld)
	}
	return kept, withheld, nil
}

// tableSizeEstimate is one table's approximate plan-time size read from
// information_schema statistics: row count and on-disk footprint (data plus
// indexes). Either is nil when statistics did not report it, and the other is
// still kept. Display only; statistics are estimates.
type tableSizeEstimate struct {
	rows  *int64
	bytes *int64
}

// copyInt64 returns a pointer to a copy of *v, or nil for nil, so plan changes
// for the same table never share one estimate.
func copyInt64(v *int64) *int64 {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

// plannedTableNames returns the distinct tables a plan touches, in first-seen
// order, as parameters for the size probe.
func (e *Engine) plannedTableNames(database string, changes []spiritlint.PlannedChange) []string {
	seen := make(map[string]bool, len(changes))
	names := make([]string, 0, len(changes))
	for _, pc := range changes {
		if pc.TableName == "" {
			e.logger.Warn("planned statement names no table; the plan will omit its size estimate",
				"database", database, "statement", pc.Statement)
			continue
		}
		if seen[pc.TableName] {
			// A plan can carry several statements for one table (a
			// partition-type change needs its own REMOVE PARTITIONING
			// statement); the table is probed once.
			continue
		}
		seen[pc.TableName] = true
		names = append(names, pc.TableName)
	}
	return names
}

// sizeProbeStatement returns stmt as the size probe sends it: unchanged
// outside tests, rewritten by the sizeProbeSQL seam inside them.
func (e *Engine) sizeProbeStatement(ctx context.Context, stmt string) string {
	if e.sizeProbeSQL == nil {
		return stmt
	}
	return e.sizeProbeSQL(ctx, stmt)
}

// fetchTableSizeEstimates reads the approximate row count and on-disk
// footprint of the named base tables from information_schema, through the
// plan's own lazily opened connection to the target. The read is scoped to the
// planned tables so the cost does not scale with the size of the schema, and
// it disables statistics caching as the direct execution size gate does, so a
// plan never displays a size older than the one its own gate reads. Estimates
// are display-only plan context: the caller treats a failure as "no sizes"
// rather than failing the plan.
func (e *Engine) fetchTableSizeEstimates(ctx context.Context, target *lazyTargetDB, database string, tables []string) (map[string]tableSizeEstimate, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	if e.sizeProbeFault != nil {
		if err := e.sizeProbeFault(ctx); err != nil {
			return nil, fmt.Errorf("size probe fault: %w", err)
		}
	}

	db, err := target.get(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect for size estimates: %w", err)
	}
	// A dedicated connection, so the session setting below applies to the
	// query that follows it. The setting outlives this function on the pooled
	// connection, which is safe because the pool is the plan's own: it is
	// closed with the plan, and its other readers also want fresh statistics.
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection for size estimates: %w", err)
	}
	defer utils.CloseAndLog(conn)
	if _, err := conn.ExecContext(ctx, e.sizeProbeStatement(ctx, "SET SESSION information_schema_stats_expiry = 0")); err != nil {
		return nil, fmt.Errorf("disable cached statistics for size estimates: %w", err)
	}

	args := make([]any, 0, len(tables)+1)
	args = append(args, database)
	for _, name := range tables {
		args = append(args, name)
	}
	query := `
		SELECT table_name, table_rows, data_length + index_length
		FROM information_schema.tables
		WHERE table_schema = ? AND table_type = 'BASE TABLE'
		  AND table_name IN (?` + strings.Repeat(", ?", len(tables)-1) + `)`
	requested := requestedTableNames(tables)

	rows, err := conn.QueryContext(ctx, e.sizeProbeStatement(ctx, query), args...)
	if err != nil {
		return nil, fmt.Errorf("query size estimates for tables %v: %w", tables, err)
	}
	defer utils.CloseAndLog(rows)

	estimates := make(map[string]tableSizeEstimate)
	for rows.Next() {
		var name string
		var tableRows, tableBytes sql.NullInt64
		if err := rows.Scan(&name, &tableRows, &tableBytes); err != nil {
			return nil, fmt.Errorf("scan table size estimate: %w", err)
		}
		// information_schema can match a name case-insensitively and return it
		// in the server's case; the estimate is keyed by the name the plan
		// asked for, which is the name its changes carry.
		asked, ok := requested[strings.ToLower(name)]
		if !ok {
			e.logger.Warn("size probe returned a table the plan did not ask for; ignoring it",
				"database", database, "table", name)
			continue
		}
		name = asked
		est := tableSizeEstimate{
			rows:  e.validSizeStatistic(database, name, "table_rows", tableRows),
			bytes: e.validSizeStatistic(database, name, "data_length + index_length", tableBytes),
		}
		if est.rows == nil && est.bytes == nil {
			// validSizeStatistic logged why each one is missing.
			continue
		}
		estimates[name] = est
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table size estimates: %w", err)
	}
	return estimates, nil
}

// validSizeStatistic returns a base table's statistic as a display estimate,
// or nil with a warning when statistics could not report it. A NULL value is a
// base table whose statistics are unreadable (for example a discarded
// tablespace), and a negative one is a sentinel for "no real estimate", not a
// count, as it is for the direct execution size gate.
func (e *Engine) validSizeStatistic(database, table, statistic string, v sql.NullInt64) *int64 {
	if !v.Valid {
		e.logger.Warn("table statistics report no value; the plan will omit it from the table's size",
			"database", database, "table", table, "statistic", statistic)
		return nil
	}
	if v.Int64 < 0 {
		e.logger.Warn("table statistics report a negative value; the plan will omit it from the table's size",
			"database", database, "table", table, "statistic", statistic, "value", v.Int64)
		return nil
	}
	n := v.Int64
	return &n
}

// logMissingSizeEstimates reports the existing tables a successful probe
// returned no estimate for: their statistics were unreadable, which the plan
// renders as an unavailable size.
func (e *Engine) logMissingSizeEstimates(database string, tables []string, estimates map[string]tableSizeEstimate) {
	var missing []string
	for _, name := range tables {
		if _, ok := estimates[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		e.logger.Warn("existing tables have no size estimate; the plan will omit their sizes",
			"database", database, "tables", missing)
	}
}

// partitionByExistence splits the planned tables into those present in the
// current schema, whose sizes can be read, and those the plan creates.
func partitionByExistence(tables []string, currentByTable map[string]string) (existing, created []string) {
	for _, name := range tables {
		if _, ok := currentByTable[name]; ok {
			existing = append(existing, name)
		} else {
			created = append(created, name)
		}
	}
	return existing, created
}

// requestedTableNames indexes the probed table names by their lower-cased
// form, so a row information_schema returns in a different case maps back to
// the name the plan asked for.
func requestedTableNames(tables []string) map[string]string {
	requested := make(map[string]string, len(tables))
	for _, name := range tables {
		requested[strings.ToLower(name)] = name
	}
	return requested
}
