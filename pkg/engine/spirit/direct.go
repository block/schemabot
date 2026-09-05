// direct.go implements direct execution: routing ALTER statements Spirit
// deterministically refuses to native MySQL DDL when the database's policy
// permits it. The policy arrives as engine metadata, the plan-time verdict
// and the apply-time routing evaluate the same predicate, and everything
// fails closed — a refused statement never runs directly unless the policy
// is enabled and the table's measured size is within the configured bound.
package spirit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/ui"
)

// directPolicy is the resolved direct execution policy for a target database.
// The zero value is the fail-closed default: refused statements are blocked.
type directPolicy struct {
	Enabled      bool
	MaxTableRows int64
	// LockAcquisitionTimeoutSeconds bounds each direct statement's lock acquisition.
	// Zero means the policy did not set one; read the effective value through
	// lockAcquisitionTimeoutSeconds(), which applies the engine default.
	LockAcquisitionTimeoutSeconds int64
}

// lockAcquisitionTimeoutSeconds returns the effective lock-acquisition bound for
// direct statements: the policy's configured value, or the engine default
// when the policy leaves it unset.
func (p directPolicy) lockAcquisitionTimeoutSeconds() int64 {
	if p.LockAcquisitionTimeoutSeconds > 0 {
		return p.LockAcquisitionTimeoutSeconds
	}
	return defaultDirectLockAcquisitionTimeoutSeconds
}

// directPolicyFromMetadata parses the direct execution policy from engine
// metadata. Malformed values are errors rather than a silent fallback to
// disabled, so a misconfigured policy is surfaced instead of quietly turning
// planned direct changes into apply-time failures.
func directPolicyFromMetadata(md map[string]string) (directPolicy, error) {
	rawEnabled := md[engine.MetadataDirectExecution]
	if rawEnabled == "" {
		return directPolicy{}, nil
	}
	enabled, err := strconv.ParseBool(rawEnabled)
	if err != nil {
		return directPolicy{}, fmt.Errorf("invalid %s metadata value %q: %w", engine.MetadataDirectExecution, rawEnabled, err)
	}
	if !enabled {
		return directPolicy{}, nil
	}
	raw := md[engine.MetadataDirectExecutionMaxTableRows]
	if raw == "" {
		return directPolicy{}, fmt.Errorf("%s is enabled but %s is not set: the row bound is required so direct execution fails closed on large tables", engine.MetadataDirectExecution, engine.MetadataDirectExecutionMaxTableRows)
	}
	maxRows, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return directPolicy{}, fmt.Errorf("parse %s metadata value %q: %w", engine.MetadataDirectExecutionMaxTableRows, raw, err)
	}
	if maxRows <= 0 {
		return directPolicy{}, fmt.Errorf("%s must be positive, got %d", engine.MetadataDirectExecutionMaxTableRows, maxRows)
	}
	policy := directPolicy{Enabled: true, MaxTableRows: maxRows}
	if raw := md[engine.MetadataDirectExecutionLockAcquisitionTimeoutSeconds]; raw != "" {
		lockWait, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return directPolicy{}, fmt.Errorf("parse %s metadata value %q: %w", engine.MetadataDirectExecutionLockAcquisitionTimeoutSeconds, raw, err)
		}
		if lockWait <= 0 {
			return directPolicy{}, fmt.Errorf("%s must be positive, got %d", engine.MetadataDirectExecutionLockAcquisitionTimeoutSeconds, lockWait)
		}
		policy.LockAcquisitionTimeoutSeconds = lockWait
	}
	return policy, nil
}

// estimatedTableRows returns MySQL's estimated row count for the table from
// information_schema statistics. The read happens on a dedicated connection
// with statistics caching disabled: MySQL otherwise serves
// information_schema statistics cached for up to
// information_schema_stats_expiry seconds (a day by default), and a safety
// bound must not be decided on a day-old row count. Even uncached, the value
// is only the optimizer's estimate — InnoDB persistent statistics refresh in
// the background and can grossly undercount right after a bulk load — so
// callers trust it to block, never to approve: a verdict for direct
// execution is corroborated with an exact bounded row count.
func estimatedTableRows(ctx context.Context, db *sql.DB, schema, tableName string) (int64, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire connection for row estimate of `%s`.`%s`: %w", schema, tableName, err)
	}
	defer utils.CloseAndLog(conn)
	if _, err := conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		return 0, fmt.Errorf("disable cached statistics for row estimate of `%s`.`%s`: %w", schema, tableName, err)
	}
	var rows sql.NullInt64
	err = conn.QueryRowContext(ctx,
		"SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		schema, tableName).Scan(&rows)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("table `%s`.`%s` not found in information_schema", schema, tableName)
	}
	if err != nil {
		return 0, fmt.Errorf("query estimated row count for `%s`.`%s`: %w", schema, tableName, err)
	}
	if !rows.Valid {
		return 0, fmt.Errorf("estimated row count for `%s`.`%s` is unavailable", schema, tableName)
	}
	// A negative value is a sentinel for "no real estimate", not a count —
	// treat it as unavailable so the caller blocks instead of comparing a
	// sentinel against the bound.
	if rows.Int64 < 0 {
		return 0, fmt.Errorf("estimated row count for `%s`.`%s` is negative (%d), treating it as unavailable", schema, tableName, rows.Int64)
	}
	return rows.Int64, nil
}

// exactRowCountWithin returns the table's exact row count, capped at limit+1.
// The scan is bounded by the cap, so confirming a table within the policy
// bound stays cheap no matter how wrong the optimizer's estimate is; a return
// value of limit+1 means "more than limit rows", not a total.
func exactRowCountWithin(ctx context.Context, db *sql.DB, schema, tableName string, limit int64) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM (SELECT 1 FROM `%s`.`%s` LIMIT %d) bounded", schema, tableName, limit+1)
	var count int64
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("count rows of `%s`.`%s` (bounded at %d): %w", schema, tableName, limit+1, err)
	}
	return count, nil
}

// refusedModeDecision is how a statement the engine refuses will execute
// under the direct execution policy. The plan records mode and modeReason on
// the table change as an operator-facing preview; apply-time routing
// re-resolves the decision against live policy and table state rather than
// trusting the stored verdict.
type refusedModeDecision struct {
	mode       string // engine.ExecutionModeDirect or engine.ExecutionModeBlocked
	modeReason string // operator-facing reason, including row-count context
	outcome    string // metric outcome label when the decision blocks
	rows       int64  // measured rows when the size gate ran: exact for a direct verdict, the estimate when the estimate alone blocked
}

// blockedSizeUnknownReason is the mode-reason suffix when the size gate could
// not be evaluated at all: no target connection, no statistics row, or a
// failed count. Every such uncertainty blocks.
const blockedSizeUnknownReason = "; direct execution is enabled but the table's row count is unavailable"

// resolveRefusedMode decides whether the policy routes a refused statement to
// direct execution. Every uncertainty blocks: policy disabled, a size gate
// that cannot be evaluated, or a table above the configured bound. The size
// gate runs in two steps — the optimizer's estimate first, then an exact
// bounded row count — because the estimate can lag reality in both
// directions, so it is trusted to block but never to approve on its own.
// The above-bound reason names only the configured limit, not the measured
// count, so identical verdicts on different shards collapse into one row in
// PR-facing summaries.
func (e *Engine) resolveRefusedMode(ctx context.Context, target *lazyTargetDB, policy directPolicy, database, tableName, refusalReason string) refusedModeDecision {
	if !policy.Enabled {
		return refusedModeDecision{
			mode:       engine.ExecutionModeBlocked,
			modeReason: refusalReason,
			outcome:    "blocked_policy_disabled",
		}
	}
	db, err := target.get(ctx)
	if err != nil {
		// Fail closed: without a connection there is no size gate, and an
		// unmeasured table must never rebuild natively.
		e.logger.Warn("direct execution blocked: cannot connect to target for the size gate",
			"database", database, "table", tableName, "error", err)
		return refusedModeDecision{
			mode:       engine.ExecutionModeBlocked,
			modeReason: refusalReason + blockedSizeUnknownReason,
			outcome:    "blocked_size_unknown",
		}
	}
	rows, err := estimatedTableRows(ctx, db, database, tableName)
	if err != nil {
		// Fail closed: a table whose size cannot be measured must never
		// rebuild natively — block it and surface why in the mode reason.
		e.logger.Warn("direct execution blocked: estimated row count unavailable",
			"database", database, "table", tableName, "error", err)
		return refusedModeDecision{
			mode:       engine.ExecutionModeBlocked,
			modeReason: refusalReason + blockedSizeUnknownReason,
			outcome:    "blocked_size_unknown",
		}
	}
	aboveBoundReason := fmt.Sprintf("%s; direct execution is enabled but the table is above the configured limit of %s rows",
		refusalReason, ui.FormatNumber(policy.MaxTableRows))
	if rows > policy.MaxTableRows {
		e.logger.Info("direct execution blocked: estimated row count above the policy bound",
			"database", database, "table", tableName, "estimated_rows", rows, "max_table_rows", policy.MaxTableRows)
		return refusedModeDecision{
			mode:       engine.ExecutionModeBlocked,
			modeReason: aboveBoundReason,
			outcome:    "blocked_size_limit",
			rows:       rows,
		}
	}
	count, err := exactRowCountWithin(ctx, db, database, tableName, policy.MaxTableRows)
	if err != nil {
		e.logger.Warn("direct execution blocked: exact row count unavailable",
			"database", database, "table", tableName, "error", err)
		return refusedModeDecision{
			mode:       engine.ExecutionModeBlocked,
			modeReason: refusalReason + blockedSizeUnknownReason,
			outcome:    "blocked_size_unknown",
		}
	}
	if count > policy.MaxTableRows {
		e.logger.Info("direct execution blocked: exact row count above the policy bound despite a smaller estimate",
			"database", database, "table", tableName, "estimated_rows", rows, "max_table_rows", policy.MaxTableRows)
		return refusedModeDecision{
			mode:       engine.ExecutionModeBlocked,
			modeReason: aboveBoundReason,
			outcome:    "blocked_size_limit",
		}
	}
	return refusedModeDecision{
		mode: engine.ExecutionModeDirect,
		modeReason: fmt.Sprintf("%s; runs as native MySQL DDL on a table with ~%s rows",
			refusalReason, ui.FormatNumber(count)),
		rows: count,
	}
}

// Lifecycle states for a direct-routed statement's TableProgress entries.
// "completed" matches the literal Spirit-runner table progress already uses.
const (
	directStateRunning   = "running"
	directStateCompleted = "completed"
	directStateFailed    = "failed"
	directStateStopped   = "stopped"
)

// defaultDirectLockAcquisitionTimeoutSeconds bounds how long a direct statement
// waits to acquire its locks when the policy does not configure a bound.
// MySQL's default lock_wait_timeout lets DDL queue on the table's metadata
// lock essentially indefinitely, and every query arriving after the queued
// DDL queues behind it — a single long-running transaction would turn a
// direct statement into a table-wide stall. A short bound turns "the table
// is busy" into a fast, retryable failure instead.
const defaultDirectLockAcquisitionTimeoutSeconds = 10

// erLockWaitTimeout is MySQL error 1205 (ER_LOCK_WAIT_TIMEOUT), returned when
// a statement gives up waiting for a lock. For direct DDL this is almost
// always the table's metadata lock, held by an open transaction that has
// touched the table.
const erLockWaitTimeout = 1205

// isLockWaitTimeout reports whether err is MySQL's lock-wait timeout, i.e.
// the session's bounded lock_wait_timeout expired while the statement queued
// behind existing lock holders.
func isLockWaitTimeout(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == erLockWaitTimeout
}

// directStatementProgress tracks one direct-routed statement's lifecycle for
// progress reporting. Guarded by the engine mutex.
type directStatementProgress struct {
	table       string
	ddl         string
	state       string
	startedAt   time.Time
	completedAt *time.Time
}

// directRouted is an ALTER statement routed to direct execution, carrying the
// context the executor logs and reports.
type directRouted struct {
	stmt   string
	table  string
	reason string // the engine's refusal reason that routed it here
	rows   int64  // measured rows at routing time
}

// alterRouting partitions the ALTER phase between the Spirit runner and
// direct execution. The partition is also the execution order: all direct
// statements run first, then all Spirit-driven statements, regardless of
// their relative order in the plan. A statement in one partition that
// depends on one in the other therefore fails at apply time — the apply
// fails closed rather than reordering statements to satisfy the dependency.
type alterRouting struct {
	spiritAlters []string
	spiritTables []string
	direct       []directRouted
}

// routeAlterStatements re-evaluates the plan-time refusal predicate per
// statement at apply time, so a schema or policy change between plan and
// apply can never smuggle a refused statement past the policy: statements
// Spirit accepts run through Spirit, refused statements run directly only
// when the policy permits, and anything else fails the apply before any
// statement executes.
//
// Some of the engine's refusals depend on the table's current column types, so
// each statement is classified against the target's live definition of its
// table, read here rather than taken from the plan's snapshot: re-reading it is
// how a column type changed since the plan is caught. A definition that cannot
// be read fails the apply, because classifying without it would silently narrow
// the refusals Spirit reports and route a statement Spirit refuses onto Spirit
// anyway.
func (e *Engine) routeAlterStatements(ctx context.Context, target *lazyTargetDB, database string, alters []string, policy directPolicy) (alterRouting, error) {
	var routing alterRouting
	logger := e.changeLogger()
	schema := &targetSchema{target: target}
	for _, stmt := range alters {
		parsed, err := statement.New(stmt)
		if err != nil {
			return alterRouting{}, fmt.Errorf("parse ALTER statement %q: %w", stmt, err)
		}
		if len(parsed) == 0 {
			return alterRouting{}, fmt.Errorf("no statement parsed from %q", stmt)
		}
		table := parsed[0].Table

		currentCreateTable, err := schema.createTable(ctx, table)
		if err != nil {
			// The raw error carries target infrastructure detail (host, account,
			// driver internals) and this one surfaces in a PR comment, so the
			// apply fails with a fixed line and the cause stays in the logs.
			logger.Error("routing failed: cannot read the target's current table definition",
				"database", database, "table", table, "error", err)
			return alterRouting{}, engine.OperatorErrorf(err, "SchemaBot could not read the current definition of table %q on the target; see the server logs for the reason.", table)
		}
		reason, refused, err := check.StatementRefusal(ctx, stmt, currentCreateTable, logger)
		if err != nil {
			// The engine could not judge the statement — typically the current
			// definition no longer describes a column the statement redeclares.
			// That is not a refusal, so it must not be routed as one.
			logger.Error("routing failed: the engine cannot classify the statement against the current table definition",
				"database", database, "table", table, "error", err)
			return alterRouting{}, fmt.Errorf("route ALTER statement for table %q: %w", table, err)
		}
		if !refused {
			routing.spiritAlters = append(routing.spiritAlters, stmt)
			routing.spiritTables = append(routing.spiritTables, table)
			continue
		}

		decision := e.resolveRefusedMode(ctx, target, policy, database, table, reason)
		if decision.mode != engine.ExecutionModeDirect {
			metrics.RecordDirectExecution(ctx, database, decision.outcome)
			// A refusal reason is the schema change engine's own account of why
			// it will not run the statement, and SchemaBot's bound and
			// row-count context appended to it. It is not target output: the
			// engine's checks interpolate only the column and type names the
			// statement itself declares, which the plan preview already shows
			// on this pull request. That is what makes it publishable here,
			// and it is why a new refusal path has to be read before it is
			// marked rather than assumed to match this one.
			//
			// It names the schema change engine in full because failureReason
			// publishes this message verbatim as the apply's failure reason,
			// with no sentence around it to establish which engine is meant.
			if !policy.Enabled {
				return alterRouting{}, engine.OperatorErrorf(nil, "Statement on table %q is not supported by the schema change engine and direct execution is not enabled for this database: %s", table, reason)
			}
			return alterRouting{}, engine.OperatorErrorf(nil, "Statement on table %q cannot run directly: %s", table, decision.modeReason)
		}
		routing.direct = append(routing.direct, directRouted{stmt: stmt, table: table, reason: reason, rows: decision.rows})
	}
	return routing, nil
}

// executeDirectStatements runs each direct-routed ALTER verbatim as native
// MySQL DDL, one statement at a time in plan order. Each statement is
// synchronous — MySQL chooses the algorithm and lock level, writes to the
// table block while it runs, and there is no revert window — so progress is
// reported as explicit per-statement state transitions rather than row
// counts. It returns false when execution must stop: a cancelled context
// leaves the engine state Stopped, a genuine failure transitions to
// StateFailed.
func (e *Engine) executeDirectStatements(ctx context.Context, target *lazyTargetDB, database string, stmts []directRouted, policy directPolicy) bool {
	logger := e.changeLogger()
	lockWaitSeconds := policy.lockAcquisitionTimeoutSeconds()
	db, err := target.get(ctx)
	if err != nil {
		logger.Error("direct execution failed: cannot connect to target",
			"database", database, "error", err)
		e.setSchemaChangeFailed(fmt.Errorf("connect for direct execution: %w", err))
		return false
	}
	// Run every statement on one dedicated connection: pool connections have
	// no session affinity, so the session-level lock bound below would not
	// reliably apply to the DDL if both went through the pool.
	conn, err := db.Conn(ctx)
	if err != nil {
		logger.Error("direct execution failed: cannot acquire a dedicated connection",
			"database", database, "error", err)
		e.setSchemaChangeFailed(fmt.Errorf("acquire dedicated connection for direct execution: %w", err))
		return false
	}
	defer utils.CloseAndLog(conn)
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d, innodb_lock_wait_timeout = %d",
		lockWaitSeconds, lockWaitSeconds)); err != nil {
		logger.Error("direct execution failed: cannot bound the session lock wait timeouts",
			"database", database, "error", err)
		e.setSchemaChangeFailed(fmt.Errorf("bound session lock wait timeouts for direct execution: %w", err))
		return false
	}
	for _, ds := range stmts {
		progress := e.trackDirectStatement(ds.table, ds.stmt)
		logger.Info("executing statement directly as native MySQL DDL",
			"database", database, "table", ds.table, "reason", ds.reason, "estimated_rows", ds.rows)
		e.emitTableLog(ds.table, "executing statement as native MySQL DDL: writes to the table block while it runs; not revertible")
		if _, err := conn.ExecContext(ctx, ds.stmt); err != nil {
			if ctx.Err() != nil {
				// A cancelled context closes the connection, but MySQL may
				// finish the DDL server-side — the statement's outcome is
				// indeterminate until an operator inspects the table.
				e.setDirectStatementState(progress, directStateStopped)
				metrics.RecordDirectExecution(ctx, database, "stopped")
				logger.Info("schema change stopped during direct execution; the in-flight statement may still complete server-side",
					"database", database, "table", ds.table, "reason", ctx.Err())
				return false
			}
			e.setDirectStatementState(progress, directStateFailed)
			metrics.RecordDirectExecution(ctx, database, "failed")
			if isLockWaitTimeout(err) {
				logger.Error("direct execution failed: statement could not acquire the table's metadata lock within the bounded wait",
					"database", database, "table", ds.table, "lock_wait_timeout_seconds", lockWaitSeconds, "error", err)
				// The table name comes from the plan and the timeout from this
				// deployment's own configuration, so this sentence is safe to
				// show on the pull request that asked for the change.
				e.setSchemaChangeFailed(engine.OperatorErrorf(err,
					"Table %q is busy: the change could not acquire the metadata lock within %ds. Retry when long-running transactions on the table have finished.",
					ds.table, lockWaitSeconds))
				return false
			}
			logger.Error("direct execution failed",
				"database", database, "table", ds.table, "error", err)
			e.setSchemaChangeFailed(fmt.Errorf("direct execution of ALTER on table %q failed: %w", ds.table, err))
			return false
		}
		e.setDirectStatementState(progress, directStateCompleted)
		metrics.RecordDirectExecution(ctx, database, "completed")
		logger.Info("direct execution completed", "database", database, "table", ds.table)
		e.emitTableLog(ds.table, "statement completed as native MySQL DDL")
	}
	return true
}

// emitTableLog routes an operator-facing message for a table to the apply log
// store when a log callback is registered.
func (e *Engine) emitTableLog(table, msg string) {
	if onLog := loadLogCallback(&e.onLog); onLog != nil {
		onLog(slog.LevelInfo, table, msg)
	}
}

// trackDirectStatement registers a direct-routed statement on the running
// schema change so progress polls report its lifecycle.
func (e *Engine) trackDirectStatement(table, ddlStmt string) *directStatementProgress {
	p := &directStatementProgress{table: table, ddl: ddlStmt, state: directStateRunning, startedAt: time.Now()}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runningSchemaChange != nil {
		e.runningSchemaChange.directStatements = append(e.runningSchemaChange.directStatements, p)
	}
	return p
}

// setDirectStatementState records a direct statement's transition. Completed
// and failed are terminal for the statement; stopped leaves no completion
// time because the statement's server-side outcome is indeterminate.
func (e *Engine) setDirectStatementState(p *directStatementProgress, state string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p.state = state
	if state == directStateCompleted || state == directStateFailed {
		now := time.Now()
		p.completedAt = &now
	}
}

// targetDSN builds a DSN for the target database from the connection parts
// the execution path carries.
func targetDSN(host, username, password, database string) string {
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = host
	cfg.User = username
	cfg.Passwd = password
	cfg.DBName = database
	return cfg.FormatDSN()
}

// lazyTargetDB opens a connection to the target database on first use, so an
// apply that never reaches the target — one with no ALTER to route, or no
// statement at all — never pays for a connection. An apply carrying an ALTER
// always does: routing reads the table's current definition to classify it,
// whether or not the statement turns out to be refused.
type lazyTargetDB struct {
	dsn string
	db  *sql.DB
}

func (l *lazyTargetDB) get(ctx context.Context) (*sql.DB, error) {
	if l.db != nil {
		return l.db, nil
	}
	db, err := mysqlconn.Open(l.dsn)
	if err != nil {
		return nil, fmt.Errorf("open target database: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		utils.CloseAndLog(db)
		return nil, fmt.Errorf("ping target database: %w", err)
	}
	l.db = db
	return db, nil
}

func (l *lazyTargetDB) close() {
	if l.db != nil {
		utils.CloseAndLog(l.db)
		l.db = nil
	}
}

// targetSchema reads the target's current CREATE TABLE for the tables an apply
// touches, caching each one so routing several ALTERs against the same table
// costs a single round trip. It is not safe for concurrent use; routing
// classifies statements one at a time.
type targetSchema struct {
	target *lazyTargetDB
	cache  map[string]string
}

func (s *targetSchema) createTable(ctx context.Context, tableName string) (string, error) {
	if createTable, ok := s.cache[tableName]; ok {
		return createTable, nil
	}
	db, err := s.target.get(ctx)
	if err != nil {
		return "", err
	}
	var name, createTable string
	query := "SHOW CREATE TABLE " + sqlescape.EscapeIdentifier(tableName)
	if err := db.QueryRowContext(ctx, query).Scan(&name, &createTable); err != nil {
		return "", fmt.Errorf("%s: %w", query, err)
	}
	if s.cache == nil {
		s.cache = make(map[string]string)
	}
	s.cache[tableName] = createTable
	return createTable, nil
}
