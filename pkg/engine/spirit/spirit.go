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
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	spiritmigration "github.com/block/spirit/pkg/migration"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/lint"
	"github.com/block/schemabot/pkg/mysqlconn"
)

// DefaultTargetChunkTime is the default time Spirit aims for per chunk.
// Lower values = faster copies but more load on the database.
// Spirit requires this to be in range 100ms-5s. Matches volume 3 (default).
const DefaultTargetChunkTime = 2 * time.Second

// DefaultThreads is the default number of concurrent copier threads.
// Matches volume 3 (default).
const DefaultThreads = 2

// DefaultLockWaitTimeout is how long Spirit waits for table locks.
const DefaultLockWaitTimeout = 30 * time.Second

// Engine implements the engine.Engine interface for MySQL using Spirit.
type Engine struct {
	logger       *slog.Logger
	spiritLogger *slog.Logger // Logger for Spirit (may filter debug logs)
	linter       *lint.Linter

	// Configuration. targetChunkTime, threads, and lockWaitTimeout are the
	// configured default Spirit copy settings; they are immutable after New.
	// Each schema change starts from these defaults and carries its own working
	// copy on runningSchemaChange, which Volume retunes for that change only.
	targetChunkTime     time.Duration
	threads             int
	lockWaitTimeout     time.Duration
	debugLogs           bool
	disablePendingDrops bool
	cpuHint             int // Inferred CPU count from innodb_buffer_pool_instances (0 = unknown); guarded by mu

	// Log callback for routing Spirit logs to ApplyLogStore (with table context)
	onLog func(level slog.Level, table, msg string)

	// Running schema change state
	mu                  sync.Mutex
	runningSchemaChange *runningSchemaChange
}

// runningSchemaChange tracks the state of an in-progress schema change.
type runningSchemaChange struct {
	database                string            // MySQL database name parsed from DSN
	tableNamespace          map[string]string // table name → namespace (from ApplyRequest.Changes)
	tables                  []string
	ddls                    []string // DDL statement for each table
	originalDDLs            []string // Full statement list from Apply, in execution order; never overwritten so resume can run the whole plan
	combinedStatement       string   // Original combined statement passed to Spirit (for checkpoint-safe restart)
	runners                 []*spiritmigration.Runner
	progressCallback        func() string // returns Summary from Spirit's Progress API
	state                   engine.State
	errorMessage            string // Error details when state is StateFailed
	started                 time.Time
	deferCutover            bool // Whether to defer cutover until manual trigger
	volumeRestartInProgress bool // Set while stored stopped state should still be exposed as running progress.

	// Spirit copy settings for this schema change. Initialized from the
	// engine's configured defaults and retuned by Volume for this change only;
	// they end with the change, so the next schema change starts from the
	// configured defaults again.
	threads         int
	targetChunkTime time.Duration
	lockWaitTimeout time.Duration
	volume          int32 // Explicit volume set via Volume for this change (0 = never set)

	// For resume support
	cancelFunc context.CancelFunc
	host       string
	username   string
	password   string

	// For waiting on schema change to finish (used by SetVolume)
	wg sync.WaitGroup
}

// Compile-time check that Engine implements the interface.
var _ engine.Engine = (*Engine)(nil)
var _ engine.Drainer = (*Engine)(nil)
var _ engine.DeferredCutoverSignalChecker = (*Engine)(nil)

// Config holds configuration for the Spirit engine.
type Config struct {
	Logger          *slog.Logger
	TargetChunkTime time.Duration
	Threads         int
	LockWaitTimeout time.Duration
	DebugLogs       bool // Enable Spirit's verbose debug logs (replication events, etc.)

	// DisablePendingDrops executes DROP TABLE statements directly instead of
	// quarantining the table in the pending drops database. Quarantine is the
	// default because it keeps dropped table data recoverable until the
	// retention period expires.
	DisablePendingDrops bool
}

// New creates a new Spirit engine.
func New(cfg Config) *Engine {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	targetChunkTime := cfg.TargetChunkTime
	if targetChunkTime == 0 {
		targetChunkTime = DefaultTargetChunkTime
	}

	threads := cfg.Threads
	if threads == 0 {
		threads = DefaultThreads
	}

	lockWaitTimeout := cfg.LockWaitTimeout
	if lockWaitTimeout == 0 {
		lockWaitTimeout = DefaultLockWaitTimeout
	}

	eng := &Engine{
		logger:              logger,
		linter:              lint.New(),
		targetChunkTime:     targetChunkTime,
		threads:             threads,
		lockWaitTimeout:     lockWaitTimeout,
		debugLogs:           cfg.DebugLogs,
		disablePendingDrops: cfg.DisablePendingDrops,
	}

	// Create Spirit logger with filter that checks debugLogs at runtime
	// and routes logs to ApplyLogStore via onLog callback
	eng.spiritLogger = slog.New(&spiritLogFilter{
		handler:  logger.Handler(),
		debugRef: &eng.debugLogs,
		onLogRef: &eng.onLog,
	})

	return eng
}

func (e *Engine) Name() string {
	return "spirit"
}

// SetLogCallback sets a callback that receives Spirit log messages.
// Only INFO level and above are routed (DEBUG logs are filtered).
// The callback receives the log level, table name (if available), and message.
// Set to nil to disable log routing.
func (e *Engine) SetLogCallback(cb func(level slog.Level, table, msg string)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onLog = cb
}

// SetDebugLogs enables or disables verbose Spirit debug logs at runtime.
// When disabled, noisy logs like "Received unknown event type" are filtered.
func (e *Engine) SetDebugLogs(enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.debugLogs = enabled
}

// DebugLogs returns whether verbose Spirit debug logs are enabled.
func (e *Engine) DebugLogs() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.debugLogs
}

// Drain waits for any in-flight migration goroutine to complete and clears the
// running migration state. This ensures DB connections from a previous run are
// fully released before new operations begin.
func (e *Engine) Drain() {
	e.mu.Lock()
	rm := e.runningSchemaChange
	if rm == nil {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()

	rm.wg.Wait()

	e.mu.Lock()
	e.runningSchemaChange = nil
	e.mu.Unlock()
}

// copySettings returns the Spirit copy settings for the tracked schema change,
// falling back to the engine's configured defaults when no change is tracked.
// Each schema change starts from the configured defaults and may be retuned by
// Volume; the defaults themselves never change, so a volume set on one schema
// change never affects a later one.
func (e *Engine) copySettings() (threads int, chunkTime, lockTimeout time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if rm := e.runningSchemaChange; rm != nil {
		return rm.threads, rm.targetChunkTime, rm.lockWaitTimeout
	}
	return e.threads, e.targetChunkTime, e.lockWaitTimeout
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
	currentSchema, err := e.fetchCurrentSchema(ctx, req.Credentials.DSN, database)
	if err != nil {
		return nil, fmt.Errorf("fetch current schema: %w", err)
	}
	e.logger.Debug("fetched current schema", "database", database, "tables", len(currentSchema))
	for i, ts := range currentSchema {
		e.logger.Debug("current schema table", "index", i, "stmt", ts.Schema[:min(200, len(ts.Schema))])
	}

	// Build list of desired table schemas from all namespaces
	var desiredSchemas []table.TableSchema
	for namespace, ns := range req.SchemaFiles {
		for filename, content := range ns.Files {
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
				desiredSchemas = append(desiredSchemas, table.TableSchema{Name: ct.TableName, Schema: stmt})
			}
		}
	}

	// Use Spirit's PlanChanges to diff + lint in one call.
	// This combines DeclarativeToImperative (diff) with RunLinters (lint),
	// returning per-statement lint results with severity levels.
	plan, err := lint.PlanChanges(currentSchema, desiredSchemas, nil, e.linter.SpiritConfig())
	if err != nil {
		return nil, err
	}

	if !plan.HasChanges() {
		return &engine.PlanResult{
			PlanID:    fmt.Sprintf("plan-%d", time.Now().UnixNano()),
			NoChanges: true,
		}, nil
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

		// Error-severity violations mark the change as unsafe
		if errViolations := pc.Errors(); len(errViolations) > 0 {
			change.IsUnsafe = true
			msgs := make([]string, len(errViolations))
			for i, v := range errViolations {
				msgs[i] = v.Message
			}
			change.UnsafeReason = strings.Join(msgs, "; ")
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

	return &engine.PlanResult{
		PlanID:         fmt.Sprintf("plan-%d", time.Now().UnixNano()),
		Changes:        schemaChanges,
		LintViolations: lintViolations,
	}, nil
}

// Apply starts executing a schema change plan using Spirit.
func (e *Engine) Apply(ctx context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	// Check for defer_cutover option
	deferCutover := req.Options["defer_cutover"] == "true"

	e.logger.Info("applying plan",
		"database", req.Database,
		"ddl_count", len(req.FlatDDL()),
		"defer_cutover", deferCutover,
		"options", req.Options,
	)

	if req.Credentials == nil || req.Credentials.DSN == "" {
		return nil, fmt.Errorf("DSN credentials required for Spirit engine")
	}

	if len(req.FlatDDL()) == 0 {
		return &engine.ApplyResult{
			Accepted: true,
			Message:  "No changes to apply",
		}, nil
	}

	// Parse DSN to extract connection info (DSN is the source of truth for actual database)
	host, username, password, database, err := parseDSN(req.Credentials.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse DSN: %w", err)
	}

	// Wait for any in-flight migration to fully exit before starting a new one.
	// This ensures the old Spirit runner's DB connections are released.
	e.Drain()

	// Query CPU hint for volume scaling (best-effort, falls back to fixed counts)
	e.mu.Lock()
	cpuHint := e.cpuHint
	e.mu.Unlock()
	if cpuHint == 0 {
		cpuHint = e.queryCPUHint(ctx, req.Credentials.DSN)
		e.mu.Lock()
		e.cpuHint = cpuHint
		e.mu.Unlock()
	}

	// Initialize running state and start background execution.
	e.mu.Lock()
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

	rm := &runningSchemaChange{
		database:        database,
		tableNamespace:  tableNamespace,
		tables:          nil, // Tables will be populated by executeSchemaChange
		originalDDLs:    req.FlatDDL(),
		state:           engine.StateRunning,
		started:         time.Now(),
		deferCutover:    deferCutover,
		host:            host,
		username:        username,
		password:        password,
		threads:         e.threads,
		targetChunkTime: e.targetChunkTime,
		lockWaitTimeout: e.lockWaitTimeout,
	}
	e.runningSchemaChange = rm
	e.mu.Unlock()

	// Start schema change in background with cancellable context.
	// Use WithoutCancel to preserve context values (tracing) without inheriting
	// the request deadline — the schema change must outlive the API call.
	// Stop() cancels via rm.cancelFunc.
	rm.wg.Go(func() {
		bgCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		defer cancel()
		e.mu.Lock()
		if e.runningSchemaChange != nil {
			e.runningSchemaChange.cancelFunc = cancel
		}
		e.mu.Unlock()
		e.executeSchemaChange(bgCtx, host, username, password, database, req.FlatDDL(), deferCutover)
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
		tableProgress = buildSpiritTableProgress(spiritProgress, stateStr, ddlByTable, rm.tableNamespace)
	} else {
		// Fallback: no per-table progress available
		for i, tableName := range rm.tables {
			ddl := ""
			if i < len(rm.ddls) {
				ddl = rm.ddls[i]
			}
			tp := engine.TableProgress{
				Namespace: rm.tableNamespace[tableName],
				Table:     tableName,
				DDL:       ddl,
				State:     stateStr,
			}
			// Only show progress detail on first table
			if i == 0 {
				tp.ProgressDetail = message
			}
			tableProgress = append(tableProgress, tp)
		}
	}

	state := progressState(rm, spiritState)

	// If state was overridden and message is still the default fallback, update message to match
	defaultMessage := fmt.Sprintf("Schema change %s", rm.state)
	if state != rm.state && message == defaultMessage {
		message = fmt.Sprintf("Schema change %s", state)
	}

	return &engine.ProgressResult{
		State:        state,
		Message:      message,
		ErrorMessage: rm.errorMessage,
		Retryable:    state == engine.StateFailed,
		Tables:       tableProgress,
		ResumeState:  req.ResumeState,
	}, nil
}

// buildSpiritTableProgress maps Spirit's per-table progress into engine
// TableProgress. Spirit reports a single remaining row-copy estimate for the
// whole runner, so the ETA is surfaced on the tables still copying that have an
// established row total; completed tables keep 0, as do tables without a total
// yet and estimates that aren't ready (still measuring the copy rate, or
// essentially done).
func buildSpiritTableProgress(prog status.Progress, stateStr string, ddlByTable, tableNamespace map[string]string) []engine.TableProgress {
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
		// Calculate percent (clamp to 100 — concurrent inserts can cause RowsCopied > RowsTotal)
		if st.RowsTotal > 0 {
			tp.Progress = min(int(float64(st.RowsCopied)/float64(st.RowsTotal)*100), 100)
			tp.ProgressDetail = fmt.Sprintf("%d/%d %d%% copyRows",
				st.RowsCopied, st.RowsTotal, tp.Progress)
		}
		if st.IsComplete {
			tp.State = "completed"
			tp.Progress = 100
		} else if st.RowsTotal > 0 {
			tp.ETASeconds = etaSeconds
		}
		// Spirit reports a single runner-wide checksum estimate (rows verified so
		// far / total to verify), populated only during the verify phase and zero
		// otherwise. Surface it on the tables still in flight so consumers can
		// render verify progress; completed tables keep zero.
		if !st.IsComplete {
			tp.ChecksumRowsChecked = int64(prog.Checksum.RowsChecked)
			tp.ChecksumRowsTotal = int64(prog.Checksum.RowsTotal)
		}
		tableProgress = append(tableProgress, tp)
	}
	return tableProgress
}

// progressState resolves the state reported for a progress poll. The tracked
// state is authoritative for terminal outcomes: they are recorded before the
// runner is closed, so a runner observed mid-teardown never changes the
// recorded outcome. Spirit's status only refines a non-terminal state, e.g.
// surfacing the sentinel wait for a deferred cutover. A stopped tracked state
// with a volume restart in flight reports running because the schema change
// is restarting with new settings.
func progressState(rm *runningSchemaChange, spiritState status.State) engine.State {
	state := rm.state
	if state == engine.StateStopped && rm.volumeRestartInProgress {
		state = engine.StateRunning
	}
	if !state.IsTerminal() && spiritState == status.WaitingOnSentinelTable && rm.deferCutover {
		state = engine.StateWaitingForCutover
	}
	return state
}

// fetchCurrentSchema retrieves table schemas from the database, filtering out
// internal tables (Spirit shadow/checkpoint tables and other _-prefixed tables)
// and archive tables that are maintained outside declarative schema files.
func (e *Engine) fetchCurrentSchema(ctx context.Context, dsn, _ string) ([]table.TableSchema, error) {
	db, err := mysqlconn.Open(dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping database: %w", err)
	}

	tables, err := table.LoadSchemaFromDB(ctx, db, table.WithoutUnderscoreTables, table.WithoutArchiveTables, table.WithStrippedAutoIncrement)
	if err != nil {
		return nil, fmt.Errorf("load schema: %w", err)
	}
	return tables, nil
}
