// direct.go implements direct execution: routing ALTER statements Spirit
// deterministically refuses to native MySQL DDL when the database's policy
// permits it. The policy arrives as engine metadata, the plan-time verdict
// and the apply-time routing evaluate the same predicate through the shared
// resolver (pkg/engine/direct), and everything fails closed — a refused
// statement never runs directly unless the policy is enabled and the table's
// measured size is within the configured bound.
package spirit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/direct"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/mysqlconn"
)

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

// mysqlTableSizer implements the shared resolver's size-estimator contract
// against the target database: the optimizer's statistics estimate first,
// then an exact bounded count. It connects lazily through the shared target
// handle so verdicts that never reach the size gate never open a connection.
type mysqlTableSizer struct {
	target *lazyTargetDB
	schema string
}

func (s *mysqlTableSizer) EstimatedRows(ctx context.Context, table string) (int64, error) {
	db, err := s.target.get(ctx)
	if err != nil {
		return 0, fmt.Errorf("connect to the target for the size gate of `%s`.`%s`: %w", s.schema, table, err)
	}
	return estimatedTableRows(ctx, db, s.schema, table)
}

func (s *mysqlTableSizer) ExactRowsWithin(ctx context.Context, table string, limit int64) (int64, error) {
	db, err := s.target.get(ctx)
	if err != nil {
		return 0, fmt.Errorf("connect to the target for the size gate of `%s`.`%s`: %w", s.schema, table, err)
	}
	return exactRowCountWithin(ctx, db, s.schema, table, limit)
}

// resolveRefusedMode resolves a refused statement's execution-mode verdict
// through the shared resolver, measuring the table against the target
// database. See direct.Resolver.ResolveRefusedMode for the decision
// semantics; a non-nil error means the context was cancelled mid-gate, not a
// blocked verdict.
func (e *Engine) resolveRefusedMode(ctx context.Context, target *lazyTargetDB, policy direct.Policy, database, tableName, refusalReason string) (direct.Decision, error) {
	resolver := direct.Resolver{
		Logger:    e.logger,
		Estimator: &mysqlTableSizer{target: target, schema: database},
		RunsAs:    "native MySQL DDL",
	}
	return resolver.ResolveRefusedMode(ctx, policy, database, tableName, refusalReason)
}

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
	state       engine.State
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
func (e *Engine) routeAlterStatements(ctx context.Context, target *lazyTargetDB, database string, alters []string, policy direct.Policy) (alterRouting, error) {
	var routing alterRouting
	for _, stmt := range alters {
		parsed, err := statement.New(stmt)
		if err != nil {
			return alterRouting{}, fmt.Errorf("parse ALTER statement %q: %w", stmt, err)
		}
		if len(parsed) == 0 {
			return alterRouting{}, fmt.Errorf("no statement parsed from %q", stmt)
		}
		table := parsed[0].Table

		reason, refused, err := ddl.EngineRefusalReason(stmt)
		if err != nil {
			return alterRouting{}, fmt.Errorf("route ALTER statement for table %q: %w", table, err)
		}
		if !refused {
			routing.spiritAlters = append(routing.spiritAlters, stmt)
			routing.spiritTables = append(routing.spiritTables, table)
			continue
		}

		decision, err := e.resolveRefusedMode(ctx, target, policy, database, table, reason)
		if err != nil {
			return alterRouting{}, fmt.Errorf("route ALTER statement for table %q: %w", table, err)
		}
		if decision.Mode != engine.ExecutionModeDirect {
			metrics.RecordDirectExecution(ctx, database, decision.Outcome)
			if !policy.Enabled {
				return alterRouting{}, fmt.Errorf("statement on table %q is not supported by the schema-change engine and direct execution is not enabled for this database: %s", table, reason)
			}
			return alterRouting{}, fmt.Errorf("statement on table %q cannot run directly: %s", table, decision.ModeReason)
		}
		routing.direct = append(routing.direct, directRouted{stmt: stmt, table: table, reason: reason, rows: decision.Rows})
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
func (e *Engine) executeDirectStatements(ctx context.Context, target *lazyTargetDB, database string, stmts []directRouted, policy direct.Policy) bool {
	lockWaitSeconds := policy.EffectiveLockAcquisitionTimeoutSeconds(defaultDirectLockAcquisitionTimeoutSeconds)
	db, err := target.get(ctx)
	if err != nil {
		if ctx.Err() != nil {
			e.logger.Info("schema change stopped before direct execution connected to the target",
				"database", database, "reason", ctx.Err())
			return false
		}
		e.logger.Error("direct execution failed: cannot connect to target",
			"database", database, "error", err)
		e.setSchemaChangeFailed(fmt.Errorf("connect for direct execution: %w", err))
		return false
	}
	// Run every statement on one dedicated connection: pool connections have
	// no session affinity, so the session-level lock bound below would not
	// reliably apply to the DDL if both went through the pool.
	conn, err := db.Conn(ctx)
	if err != nil {
		if ctx.Err() != nil {
			e.logger.Info("schema change stopped before direct execution acquired its dedicated connection",
				"database", database, "reason", ctx.Err())
			return false
		}
		e.logger.Error("direct execution failed: cannot acquire a dedicated connection",
			"database", database, "error", err)
		e.setSchemaChangeFailed(fmt.Errorf("acquire dedicated connection for direct execution: %w", err))
		return false
	}
	defer utils.CloseAndLog(conn)
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d, innodb_lock_wait_timeout = %d",
		lockWaitSeconds, lockWaitSeconds)); err != nil {
		if ctx.Err() != nil {
			e.logger.Info("schema change stopped before direct execution bounded its session lock waits",
				"database", database, "reason", ctx.Err())
			return false
		}
		e.logger.Error("direct execution failed: cannot bound the session lock wait timeouts",
			"database", database, "error", err)
		e.setSchemaChangeFailed(fmt.Errorf("bound session lock wait timeouts for direct execution: %w", err))
		return false
	}
	for _, ds := range stmts {
		progress, claim := e.claimDirectStatement(ds.table, ds.stmt)
		switch claim {
		case directClaimSkipCompleted:
			// The statement already ran to completion on an earlier run of this
			// schema change (a resume re-enters this loop from the top).
			// Native DDL is not revertible, so a completed statement must
			// never execute twice.
			metrics.RecordDirectExecution(ctx, database, "skipped_completed")
			e.logger.Info("direct statement already completed on an earlier run; skipping",
				"database", database, "table", ds.table)
			e.emitTableLog(ds.table, "statement already completed as native MySQL DDL on an earlier run; skipping")
			continue
		case directClaimOutcomeUnknown:
			// An earlier run was interrupted mid-statement: the connection
			// dropped but MySQL may have finished the DDL server-side, so
			// whether the statement took effect is unknown. Re-executing
			// non-revertible DDL against an unknown outcome can silently
			// duplicate it (an unnamed FOREIGN KEY re-runs successfully as a
			// second constraint), so this fails closed instead.
			metrics.RecordDirectExecution(ctx, database, "blocked_outcome_unknown")
			e.logger.Error("direct execution failed: an earlier run left the statement's outcome unknown",
				"database", database, "table", ds.table)
			e.setSchemaChangeFailed(fmt.Errorf("direct statement on table %q was interrupted on an earlier run and its outcome is unknown — the DDL may have completed server-side; inspect the table and run a fresh plan to reconcile", ds.table))
			return false
		}
		e.logger.Info("executing statement directly as native MySQL DDL",
			"database", database, "table", ds.table, "reason", ds.reason, "estimated_rows", ds.rows)
		e.emitTableLog(ds.table, "executing statement as native MySQL DDL: writes to the table block while it runs; not revertible")
		if _, err := conn.ExecContext(ctx, ds.stmt); err != nil {
			if ctx.Err() != nil {
				// A cancelled context closes the connection, but MySQL may
				// finish the DDL server-side — the statement's outcome is
				// indeterminate until an operator inspects the table.
				e.setDirectStatementState(progress, engine.StateStopped)
				metrics.RecordDirectExecution(ctx, database, "stopped")
				e.logger.Info("schema change stopped during direct execution; the in-flight statement may still complete server-side",
					"database", database, "table", ds.table, "reason", ctx.Err())
				return false
			}
			e.setDirectStatementState(progress, engine.StateFailed)
			metrics.RecordDirectExecution(ctx, database, "failed")
			if isLockWaitTimeout(err) {
				e.logger.Error("direct execution failed: statement could not acquire the table's metadata lock within the bounded wait",
					"database", database, "table", ds.table, "lock_wait_timeout_seconds", lockWaitSeconds, "error", err)
				e.setSchemaChangeFailed(fmt.Errorf("table %q is busy: could not acquire the metadata lock within %ds; retry when long-running transactions on the table have finished: %w",
					ds.table, lockWaitSeconds, err))
				return false
			}
			e.logger.Error("direct execution failed",
				"database", database, "table", ds.table, "error", err)
			e.setSchemaChangeFailed(fmt.Errorf("direct execution of ALTER on table %q failed: %w", ds.table, err))
			return false
		}
		e.setDirectStatementState(progress, engine.StateCompleted)
		metrics.RecordDirectExecution(ctx, database, "completed")
		e.logger.Info("direct execution completed", "database", database, "table", ds.table)
		e.emitTableLog(ds.table, "statement completed as native MySQL DDL")
	}
	return true
}

// emitTableLog routes an operator-facing message for a table to the apply log
// store when a log callback is registered.
func (e *Engine) emitTableLog(table, msg string) {
	e.mu.Lock()
	onLog := e.onLog
	e.mu.Unlock()
	if onLog != nil {
		onLog(slog.LevelInfo, table, msg)
	}
}

// directClaim is how the executor should handle a direct-routed statement,
// resolved against any lifecycle an earlier run of this schema change already
// recorded for it.
type directClaim int

const (
	// directClaimRun executes the statement. Its progress entry is fresh, or a
	// reused failed entry — a failed MySQL DDL statement is atomic and left no
	// effect, so re-executing it on a resume is safe.
	directClaimRun directClaim = iota
	// directClaimSkipCompleted skips the statement: it already ran to
	// completion on an earlier run, and native DDL must never execute twice.
	directClaimSkipCompleted
	// directClaimOutcomeUnknown fails the schema change: an earlier run was
	// interrupted mid-statement, so whether the DDL took effect is unknown and
	// re-executing it could silently duplicate a non-revertible change.
	directClaimOutcomeUnknown
)

// claimDirectStatement resolves how a direct-routed statement should run given
// the lifecycle already recorded for it on this schema change. Each statement
// keeps a single progress entry across resumes — a re-run reuses the recorded
// entry instead of appending a duplicate, so a progress poll never lists the
// same statement twice.
func (e *Engine) claimDirectStatement(table, ddlStmt string) (*directStatementProgress, directClaim) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runningSchemaChange == nil {
		return &directStatementProgress{table: table, ddl: ddlStmt, state: engine.StateRunning, startedAt: time.Now()}, directClaimRun
	}
	for _, p := range e.runningSchemaChange.directStatements {
		if p.table != table || p.ddl != ddlStmt {
			continue
		}
		switch p.state {
		case engine.StateCompleted:
			return p, directClaimSkipCompleted
		case engine.StateFailed:
			p.state = engine.StateRunning
			p.startedAt = time.Now()
			p.completedAt = nil
			return p, directClaimRun
		default:
			// Stopped, or running with no recorded outcome: the earlier run was
			// interrupted before the statement's result was known.
			return p, directClaimOutcomeUnknown
		}
	}
	p := &directStatementProgress{table: table, ddl: ddlStmt, state: engine.StateRunning, startedAt: time.Now()}
	e.runningSchemaChange.directStatements = append(e.runningSchemaChange.directStatements, p)
	return p, directClaimRun
}

// setDirectStatementState records a direct statement's transition. Completed
// and failed are terminal for the statement; stopped leaves no completion
// time because the statement's server-side outcome is indeterminate.
func (e *Engine) setDirectStatementState(p *directStatementProgress, state engine.State) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p.state = state
	if state == engine.StateCompleted || state == engine.StateFailed {
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

// lazyTargetDB opens a connection to the target database on first use so
// callers that never need one (no refused statements, policy disabled with no
// direct work) never pay for a connection.
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
