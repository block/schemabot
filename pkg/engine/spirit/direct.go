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

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/mysqlerr"
	"github.com/block/schemabot/pkg/ui"
)

// directPolicy is the resolved direct execution policy for a target database.
// The zero value is the fail-closed default: refused statements are blocked.
type directPolicy struct {
	Enabled bool
	// MaxTableRows is the optional bound on the table's row count. Zero means
	// the policy sets no row bound.
	MaxTableRows int64
	// MaxTableBytes is the optional bound on the table's data plus index
	// footprint. Zero means the policy sets no byte bound. An enabled policy
	// sets exactly one of the two.
	MaxTableBytes int64
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
	maxRows, err := sizeBoundFromMetadata(md, engine.MetadataDirectExecutionMaxTableRows)
	if err != nil {
		return directPolicy{}, err
	}
	maxBytes, err := sizeBoundFromMetadata(md, engine.MetadataDirectExecutionMaxTableBytes)
	if err != nil {
		return directPolicy{}, err
	}
	if maxRows == 0 && maxBytes == 0 {
		return directPolicy{}, fmt.Errorf("%s is enabled but neither %s nor %s is set: a size bound is required so direct execution fails closed on large tables", engine.MetadataDirectExecution, engine.MetadataDirectExecutionMaxTableRows, engine.MetadataDirectExecutionMaxTableBytes)
	}
	if maxRows != 0 && maxBytes != 0 {
		return directPolicy{}, fmt.Errorf("%s sets both %s and %s: a policy sets exactly one size bound", engine.MetadataDirectExecution, engine.MetadataDirectExecutionMaxTableRows, engine.MetadataDirectExecutionMaxTableBytes)
	}
	policy := directPolicy{Enabled: true, MaxTableRows: maxRows, MaxTableBytes: maxBytes}
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

// sizeBoundFromMetadata parses one optional size bound: zero when the key is
// absent, an error when it is present but not a positive integer. Presence,
// not a non-empty value, states a bound: a key present with an empty value is
// a malformed bound, and reading it as absent would silently change the grant.
func sizeBoundFromMetadata(md map[string]string, key string) (int64, error) {
	raw, ok := md[key]
	if !ok {
		return 0, nil
	}
	bound, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s metadata value %q: %w", key, raw, err)
	}
	if bound <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %d", key, bound)
	}
	return bound, nil
}

// measuredTableSize is the size gate's reading of a table's statistics. Both
// figures are InnoDB statistics estimates, which is why the gate trusts them
// only to block.
type measuredTableSize struct {
	// estimatedRows is TABLE_ROWS, read only when the policy sets a row
	// bound; zero otherwise.
	estimatedRows int64
	// bytes is DATA_LENGTH + INDEX_LENGTH, read only when the policy sets a
	// byte bound; zero otherwise.
	bytes int64
}

// measureTableSize reads the table's statistics for the size gate in one
// query (see readTableStatistics for why it runs uncached on a dedicated
// connection). Each figure is read only when the policy sets the bound it is
// compared against, so a row-bound policy judges a table exactly as it would
// without the byte bound existing. A figure the gate needs that is missing,
// NULL, or negative is an error, and the caller blocks on it: unknown size is
// never assumed small.
func measureTableSize(ctx context.Context, db *sql.DB, schema, tableName string, policy directPolicy) (measuredTableSize, error) {
	all, err := readTableStatistics(ctx, db, schema, []string{tableName})
	if err != nil {
		return measuredTableSize{}, err
	}
	stats, ok := all[tableName]
	if !ok {
		return measuredTableSize{}, fmt.Errorf("table `%s`.`%s` not found in information_schema", schema, tableName)
	}
	var size measuredTableSize
	if policy.MaxTableRows > 0 {
		rows, err := usableStatistic(stats.rows, "TABLE_ROWS", schema, tableName)
		if err != nil {
			return measuredTableSize{}, err
		}
		size.estimatedRows = rows
	}
	if policy.MaxTableBytes > 0 {
		dataBytes, err := usableStatistic(stats.dataBytes, "DATA_LENGTH", schema, tableName)
		if err != nil {
			return measuredTableSize{}, err
		}
		indexBytes, err := usableStatistic(stats.indexBytes, "INDEX_LENGTH", schema, tableName)
		if err != nil {
			return measuredTableSize{}, err
		}
		size.bytes = dataBytes + indexBytes
	}
	return size, nil
}

// usableStatistic returns a statistics figure the size gate can compare
// against a bound. NULL means information_schema has no figure. A negative
// value is a sentinel for "no real estimate", not a size, so it is treated as
// unavailable rather than compared against the bound.
func usableStatistic(v sql.NullInt64, column, schema, tableName string) (int64, error) {
	if !v.Valid {
		return 0, fmt.Errorf("%s for `%s`.`%s` is unavailable", column, schema, tableName)
	}
	if v.Int64 < 0 {
		return 0, fmt.Errorf("%s for `%s`.`%s` is negative (%d), treating it as unavailable", column, schema, tableName, v.Int64)
	}
	return v.Int64, nil
}

// exactRowCountWithin returns the table's exact row count, capped at limit+1.
// The scan is bounded by the cap, so confirming a table within the policy
// bound stays cheap no matter how wrong the optimizer's estimate is; a return
// value of limit+1 means "more than limit rows", not a total.
func exactRowCountWithin(ctx context.Context, db *sql.DB, schema, tableName string, limit int64) (int64, error) {
	query := boundedRowCountQuery(schema, tableName, limit)
	var count int64
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("count rows of `%s`.`%s` (bounded at %d): %w", schema, tableName, limit+1, err)
	}
	return count, nil
}

// boundedRowCountQuery builds the capped row-count query exactRowCountWithin
// runs. Schema and table are escaped as identifiers, so a name containing a
// backtick is counted as the table it names rather than altering the query.
func boundedRowCountQuery(schema, tableName string, limit int64) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM (SELECT 1 FROM %s.%s LIMIT %d) bounded",
		sqlescape.EscapeIdentifier(schema), sqlescape.EscapeIdentifier(tableName), limit+1)
}

// refusedModeDecision is how a statement the engine refuses will execute
// under the direct execution policy. The plan records mode and modeReason on
// the table change as an operator-facing preview; apply-time routing
// re-resolves the decision against live policy and table state rather than
// trusting the stored verdict.
type refusedModeDecision struct {
	mode       string // engine.ExecutionModeDirect or engine.ExecutionModeBlocked
	modeReason string // operator-facing reason: a blocked verdict leads with the engine's refusal, a direct one states only the table's size
	outcome    string // metric outcome label when the decision blocks
	rows       int64  // measured rows under a row bound: exact for a direct verdict, the estimate when the estimate alone blocked
	bytes      int64  // estimated data plus index bytes under a byte bound
}

// blockedSizeUnknownReason is the mode-reason suffix when the size gate could
// not be evaluated at all: no target connection, no statistics row, a NULL or
// negative figure the policy's bound needs, or a failed count. Every such
// uncertainty blocks.
const blockedSizeUnknownReason = "; direct execution is enabled but the table's size is unavailable"

// blockedForceKillUnavailableReason is the mode-reason suffix when the target
// denies SchemaBot a grant the kill needs to end the sessions blocking a direct
// statement's metadata lock: SELECT on the performance_schema lock tables,
// PROCESS for information_schema.innodb_trx, or CONNECTION_ADMIN (or SUPER) to
// kill another user's session. The denial itself stays in the server log; the
// reason names the grants that fix it and the fresh plan that picks them up.
const blockedForceKillUnavailableReason = "; direct execution is enabled but SchemaBot lacks a grant it needs to end sessions blocking the statement: grant its database user SELECT on performance_schema, PROCESS, and CONNECTION_ADMIN (or SUPER), then plan again"

// blockedForceKillUnknownReason is the mode-reason suffix when checking those
// grants failed for a reason other than a denial, such as a lost connection, a
// deadline, or a failed read of the server's role settings. Nothing is known to be missing, so the reason asks
// for a fresh plan rather than naming a grant.
const blockedForceKillUnknownReason = "; direct execution is enabled but SchemaBot could not check that it can end sessions blocking the statement: plan again"

// Access-denied errors MySQL returns when a grant the kill depends on is
// missing: ER_DBACCESS_DENIED_ERROR (1044), ER_TABLEACCESS_DENIED_ERROR
// (1142), ER_COLUMNACCESS_DENIED_ERROR (1143), and
// ER_SPECIFIC_ACCESS_DENIED_ERROR (1227), which reading innodb_trx without
// PROCESS returns.
const (
	erDBAccessDenied       = 1044
	erTableAccessDenied    = 1142
	erColumnAccessDenied   = 1143
	erSpecificAccessDenied = 1227
)

// isAccessDenied reports whether err is MySQL refusing a grant, as opposed to
// a probe that could not run at all.
func isAccessDenied(err error) bool {
	return mysqlerr.Is(err, erDBAccessDenied, erTableAccessDenied, erColumnAccessDenied, erSpecificAccessDenied)
}

// deniesForceKillGrant reports whether the force-kill privilege check failed
// because the target lacks a grant, as opposed to a check that could not run.
// Spirit marks every grant it finds missing, including CONNECTION_ADMIN, which
// it reads from SHOW GRANTS rather than from a denied query, so its marker is
// the primary signal. A denial code Spirit does not mark is still a denial.
func deniesForceKillGrant(err error) bool {
	return errors.Is(err, dbconn.ErrForceKillPrivilegeMissing) || isAccessDenied(err)
}

// resolveRefusedMode decides whether the policy routes a refused statement to
// direct execution. The table runs directly when it is within the one size
// bound the policy sets and SchemaBot can end the sessions blocking the
// statement. Everything else blocks: policy disabled, a size gate that cannot
// be evaluated, a table above the bound, or a target on which the statement
// could not end the sessions blocking it.
//
// The two bounds are not equally strong, which is why a policy chooses one
// rather than combining them. The row bound runs in two steps, the TABLE_ROWS
// estimate first and then an exact bounded row count, because the estimate can
// lag reality in both directions: it is trusted to block, and approval rests
// on the exact count. The byte figure, DATA_LENGTH + INDEX_LENGTH, counts the
// pages InnoDB's persistent statistics last saw allocated, and nothing cheap
// measures it exactly, so the byte bound approves on that estimate alone. A
// table that grew since its statistics were last sampled can therefore pass
// the byte bound while above it.
//
// An above-bound reason names only the configured limit, not the measured
// size, so identical verdicts on different shards collapse into one row in
// PR-facing summaries.
//
// The refusal quotes identifiers the schema author declared, so it is
// neutralized here, where every mode reason this engine composes starts, and
// decodes as the one cause the engine issued.
func (e *Engine) resolveRefusedMode(ctx context.Context, target *lazyTargetDB, policy directPolicy, database, tableName, refusalReason string) refusedModeDecision {
	refusalReason = engine.SanitizeBlockedCause(refusalReason)
	decision := e.resolveSizeGate(ctx, target, policy, database, tableName, refusalReason)
	if decision.mode != engine.ExecutionModeDirect {
		return decision
	}
	return e.requireForceKill(ctx, target, database, tableName, refusalReason, decision)
}

// requireForceKill blocks a statement the size gate approved when SchemaBot
// lacks a grant the kill needs: reading the tables it uses to find the
// sessions blocking the statement's metadata lock, or killing another user's
// session. Without them the kill could only fail at apply
// time, after the operator confirmed, leaving the statement queued on the lock
// while table traffic stalls behind it. The statement is blocked rather than
// run without the kill: as unavailable when the target denies a grant, and as
// unknown when the check itself could not run.
func (e *Engine) requireForceKill(ctx context.Context, target *lazyTargetDB, database, tableName, refusalReason string, approved refusedModeDecision) refusedModeDecision {
	unavailable := refusedModeDecision{
		mode:       engine.ExecutionModeBlocked,
		modeReason: refusalReason + blockedForceKillUnavailableReason,
		outcome:    "blocked_force_kill_unavailable",
	}
	unknown := refusedModeDecision{
		mode:       engine.ExecutionModeBlocked,
		modeReason: refusalReason + blockedForceKillUnknownReason,
		outcome:    "blocked_force_kill_unknown",
	}
	// The size gate approved only after connecting, so this is the cached
	// connection; an error here still blocks rather than run without the kill.
	db, err := target.get(ctx)
	if err != nil {
		e.logger.Warn("direct execution blocked: cannot connect to target to check the grants the kill needs",
			"database", database, "table", tableName, "error", err)
		return unknown
	}
	err = dbconn.CheckForceKillPrivileges(ctx, db)
	if err == nil {
		return approved
	}
	if deniesForceKillGrant(err) {
		e.logger.Warn("direct execution blocked: the target denies a grant the kill needs to end sessions blocking the statement",
			"database", database, "table", tableName, "error", err)
		return unavailable
	}
	e.logger.Warn("direct execution blocked: checking the grants the kill needs failed",
		"database", database, "table", tableName, "error", err)
	return unknown
}

// resolveSizeGate applies the policy's size bound to a refused statement whose
// refusal reason is already sanitized.
func (e *Engine) resolveSizeGate(ctx context.Context, target *lazyTargetDB, policy directPolicy, database, tableName, refusalReason string) refusedModeDecision {
	if !policy.Enabled {
		return refusedModeDecision{
			mode:       engine.ExecutionModeBlocked,
			modeReason: refusalReason,
			outcome:    "blocked_policy_disabled",
		}
	}
	blockedSizeUnknown := refusedModeDecision{
		mode:       engine.ExecutionModeBlocked,
		modeReason: refusalReason + blockedSizeUnknownReason,
		outcome:    "blocked_size_unknown",
	}
	db, err := target.get(ctx)
	if err != nil {
		// Fail closed: without a connection there is no size gate, and an
		// unmeasured table must never rebuild natively.
		e.logger.Warn("direct execution blocked: cannot connect to target for the size gate",
			"database", database, "table", tableName, "error", err)
		return blockedSizeUnknown
	}
	size, err := measureTableSize(ctx, db, database, tableName, policy)
	if err != nil {
		// Fail closed: a table whose size cannot be measured must never
		// rebuild natively — block it and surface why in the mode reason.
		e.logger.Warn("direct execution blocked: table size statistics unavailable",
			"database", database, "table", tableName, "error", err)
		return blockedSizeUnknown
	}
	if policy.MaxTableBytes > 0 {
		return e.resolveByteBound(policy, database, tableName, refusalReason, size)
	}
	aboveRowBound := refusedModeDecision{
		mode: engine.ExecutionModeBlocked,
		modeReason: fmt.Sprintf("%s; direct execution is enabled but the table is above the configured limit of %s rows",
			refusalReason, ui.FormatNumber(policy.MaxTableRows)),
		outcome: "blocked_size_limit",
		rows:    size.estimatedRows,
	}
	if size.estimatedRows > policy.MaxTableRows {
		e.logger.Info("direct execution blocked: estimated row count above the policy bound",
			"database", database, "table", tableName, "estimated_rows", size.estimatedRows, "max_table_rows", policy.MaxTableRows)
		return aboveRowBound
	}
	count, err := exactRowCountWithin(ctx, db, database, tableName, policy.MaxTableRows)
	if err != nil {
		e.logger.Warn("direct execution blocked: exact row count unavailable",
			"database", database, "table", tableName, "error", err)
		return blockedSizeUnknown
	}
	if count > policy.MaxTableRows {
		e.logger.Info("direct execution blocked: exact row count above the policy bound despite a smaller estimate",
			"database", database, "table", tableName, "estimated_rows", size.estimatedRows, "max_table_rows", policy.MaxTableRows)
		return aboveRowBound
	}
	return refusedModeDecision{
		mode:       engine.ExecutionModeDirect,
		modeReason: fmt.Sprintf("the table has ~%s rows", ui.FormatNumber(count)),
		rows:       count,
	}
}

// resolveByteBound decides a refused statement under a byte-bound policy. The
// estimate is compared once: no exact count exists to corroborate it.
func (e *Engine) resolveByteBound(policy directPolicy, database, tableName, refusalReason string, size measuredTableSize) refusedModeDecision {
	if size.bytes > policy.MaxTableBytes {
		e.logger.Info("direct execution blocked: estimated data and index size above the policy bound",
			"database", database, "table", tableName, "estimated_bytes", size.bytes, "max_table_bytes", policy.MaxTableBytes)
		return refusedModeDecision{
			mode: engine.ExecutionModeBlocked,
			modeReason: fmt.Sprintf("%s; direct execution is enabled but the table is above the configured limit of %s of data and indexes",
				refusalReason, ui.FormatBytesBinary(policy.MaxTableBytes)),
			outcome: "blocked_size_limit",
			bytes:   size.bytes,
		}
	}
	return refusedModeDecision{
		mode:       engine.ExecutionModeDirect,
		modeReason: fmt.Sprintf("the table has %s of data and indexes", ui.FormatApproxBytes(size.bytes)),
		bytes:      size.bytes,
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

// ExecutionVerdicts records execution-mode verdicts, which say how a planned
// statement will run at apply time, for the table changes planned against one
// target database. It uses this engine's refusal check, direct execution
// policy, and size gate, so a verdict it records is the one this engine's own
// Plan records for the same statement. Both judge the statement against the
// table's SHOW CREATE TABLE output: Record reads it from the target as the
// apply's routing does, and Plan reads it into the schema snapshot it diffs.
// An engine that plans a schema change itself but drives this engine against
// each target, such as one that fans a sharded schema change out to its shard
// primaries, uses it to disclose the verdict at plan time without
// reimplementing the rule.
//
// It connects to the target only when a statement needs it, and it is not safe
// for concurrent use. Close releases the connection.
type ExecutionVerdicts struct {
	engine   *Engine
	policy   directPolicy
	target   *lazyTargetDB
	schema   *targetSchema
	database string
}

// NewExecutionVerdicts resolves the direct execution policy in creds.Metadata
// for the target database creds.DSN names. A malformed policy is an error, as
// it is for Plan: treating it as disabled would record blocked verdicts that
// the apply's routing might not agree with.
func (e *Engine) NewExecutionVerdicts(creds *engine.Credentials) (*ExecutionVerdicts, error) {
	if creds == nil || creds.DSN == "" {
		return nil, errors.New("execution verdicts: DSN credentials required for Spirit engine")
	}
	_, _, _, database, err := parseDSN(creds.DSN)
	if err != nil {
		return nil, fmt.Errorf("execution verdicts: parse DSN: %w", err)
	}
	policy, err := directPolicyFromMetadata(creds.Metadata)
	if err != nil {
		return nil, fmt.Errorf("execution verdicts for database %q: resolve direct execution policy: %w", database, err)
	}
	target := &lazyTargetDB{dsn: creds.DSN}
	return &ExecutionVerdicts{
		engine:   e,
		policy:   policy,
		target:   target,
		schema:   &targetSchema{target: target},
		database: database,
	}, nil
}

// Record sets change's ExecutionMode and ModeReason to the verdict for its DDL
// on the target. The statement is classified from change.DDL, the text the
// apply runs, rather than from change.Operation. The table's current
// definition is read from the target, as the apply's routing reads it. A
// statement the engine runs on its default path is left with an empty verdict,
// and so are CREATE TABLE and DROP TABLE, which the engine never refuses. A
// table the target cannot describe is an error rather than a verdict, as it is
// for the apply's routing, which fails the apply on it.
//
// A verdict is one target's, so change must be judged against exactly one
// target. A caller planning the same statement for several targets, such as
// one shard primary after another, records each target's verdict on its own
// change: judging one change against each in turn would let a target that
// accepts the statement erase the blocked verdict another target recorded,
// and the plan would admit an apply that target then refuses. Record sets the
// whole verdict, clearing any mode change already carries.
func (v *ExecutionVerdicts) Record(ctx context.Context, change *engine.TableChange) error {
	change.ExecutionMode = ""
	change.ModeReason = ""
	stmtType, _, err := ddl.ClassifyStatement(change.DDL)
	if err != nil {
		return fmt.Errorf("execution verdict for table %q in database %q: classify statement: %w", change.Table, v.database, err)
	}
	if !verdictApplies(stmtType) {
		return nil
	}
	currentCreateTable, err := v.schema.createTable(ctx, change.Table)
	if err != nil {
		return fmt.Errorf("execution verdict for table %q in database %q: read current definition: %w", change.Table, v.database, err)
	}
	return v.record(ctx, change, currentCreateTable)
}

// Close releases the target connection, if a verdict opened one.
func (v *ExecutionVerdicts) Close() {
	v.target.close()
}

// verdictApplies reports whether a statement of stmtType can carry an
// execution-mode verdict. The apply runs every statement other than CREATE
// TABLE and DROP TABLE in its ALTER phase (see classifyDDLPhases), where each
// one passes the engine's refusal check, so each of those can be refused.
func verdictApplies(stmtType ddl.StatementType) bool {
	return stmtType != ddl.StatementCreateTable && stmtType != ddl.StatementDropTable
}

// record sets change's verdict, judged against currentCreateTable. The
// engine's refusal checks compare a redeclared column against its current
// type, which is why the statement is classified alongside the table's
// definition.
func (v *ExecutionVerdicts) record(ctx context.Context, change *engine.TableChange, currentCreateTable string) error {
	logger := v.engine.logger
	reason, refused, err := check.StatementRefusal(ctx, change.DDL, currentCreateTable, logger)
	if err != nil {
		return fmt.Errorf("execution verdict for table %q in database %q: %w", change.Table, v.database, err)
	}
	if !refused {
		return nil
	}
	decision := v.engine.resolveRefusedMode(ctx, v.target, v.policy, v.database, change.Table, reason)
	change.ExecutionMode = decision.mode
	change.ModeReason = decision.modeReason
	if decision.mode == engine.ExecutionModeDirect {
		logger.Info("plan routes a statement the engine refuses to direct execution",
			"database", v.database, "table", change.Table, "reason", reason,
			"estimated_rows", decision.rows, "estimated_bytes", decision.bytes)
	} else {
		logger.Info("plan contains a statement the engine will refuse at apply time",
			"database", v.database, "table", change.Table, "reason", decision.modeReason)
	}
	return nil
}

// defaultDirectLockAcquisitionTimeoutSeconds bounds how long each attempt of a
// direct statement waits to acquire its locks when the policy does not
// configure a bound. MySQL's default lock_wait_timeout lets DDL queue on the
// table's metadata lock essentially indefinitely, and every query arriving
// after the queued DDL queues behind it — a single long-running transaction
// would turn a direct statement into a table-wide stall. A short bound keeps
// that stall short: once the statement has waited 90% of it for the lock, it
// kills the transactions blocking it and retries, as Spirit does for its own
// DDL. It never kills while it holds the lock and runs, so traffic to the
// table during a rebuild is left alone. A blocker it will not kill, an
// explicit LOCK TABLES or a transaction heavier than
// dbconn.TransactionWeightThreshold, or one the target denies it the KILL of,
// makes the apply fail with a retryable "table is busy" error.
const defaultDirectLockAcquisitionTimeoutSeconds = 10

// directMaxAttempts is how many times a direct statement tries to take its
// lock before the apply fails as busy. A blocker the kill does not end, an
// explicit LOCK TABLES, a transaction too heavy to roll back, or a session the
// target denies the KILL of, ends the attempts after the first.
const directMaxAttempts = 3

// directExecutionPoolSize caps the pool direct statements run on: one
// connection runs the statement, and the others serve the kill's
// performance_schema reads and KILLs while it waits. At one, the kill would
// wait on the statement's own connection and never run.
const directExecutionPoolSize = 3

// erLockWaitTimeout is MySQL error 1205 (ER_LOCK_WAIT_TIMEOUT), returned when
// a statement gives up waiting for a lock. For direct DDL this is almost
// always the table's metadata lock, held by an open transaction that has
// touched the table.
const erLockWaitTimeout = 1205

// isLockWaitTimeout reports whether err is MySQL's lock-wait timeout, i.e.
// the session's bounded lock_wait_timeout expired while the statement queued
// behind existing lock holders.
func isLockWaitTimeout(err error) bool {
	// Read through mysqlerr rather than asserting a driver type: two MySQL
	// drivers are linked and their error types are not interchangeable. See
	// pkg/mysqlerr/number.go.
	return mysqlerr.Is(err, erLockWaitTimeout)
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
	bytes  int64  // estimated data plus index bytes at routing time, when the policy sets a byte bound
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
		routing.direct = append(routing.direct, directRouted{stmt: stmt, table: table, reason: reason, rows: decision.rows, bytes: decision.bytes})
	}
	return routing, nil
}

// openDirectExecutionDB opens the pool direct statements run on, with the
// policy's lock bound set as session variables on every connection it opens.
// dbconn.ForceExec takes the statement's connection from the pool itself, so
// the bound has to be a property of the pool rather than a SET on one
// connection. The same pool serves the kill's performance_schema reads and
// KILLs on other connections while the statement waits, so it is capped at
// directExecutionPoolSize rather than one.
func openDirectExecutionDB(ctx context.Context, dsn string, lockWaitSeconds int64) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse target DSN: %w", err)
	}
	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	bound := strconv.FormatInt(lockWaitSeconds, 10)
	cfg.Params["lock_wait_timeout"] = bound
	cfg.Params["innodb_lock_wait_timeout"] = bound
	db, err := mysqlconn.Open(cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("open target database: %w", err)
	}
	db.SetMaxOpenConns(directExecutionPoolSize)
	// The ping opens the first connection, which is where MySQL accepts or
	// rejects the session lock bound.
	if err := db.PingContext(ctx); err != nil {
		utils.CloseAndLog(db)
		return nil, fmt.Errorf("ping target database with session lock wait timeouts of %ds: %w", lockWaitSeconds, err)
	}
	return db, nil
}

// directForceExecConfig is the dbconn configuration direct statements run
// under: Spirit's kill (once the statement has waited 90% of the lock wait),
// both lock waits taken from the policy, and directMaxAttempts attempts.
func directForceExecConfig(lockWaitSeconds int64) *dbconn.DBConfig {
	cfg := dbconn.NewDBConfig()
	cfg.LockWaitTimeout = int(lockWaitSeconds)
	cfg.InnodbLockWaitTimeout = int(lockWaitSeconds)
	cfg.MaxRetries = directMaxAttempts
	return cfg
}

// executeDirectStatements runs each direct-routed ALTER verbatim as native
// MySQL DDL, one statement at a time in plan order. Each statement is
// synchronous — MySQL chooses the algorithm and lock level, writes to the
// table block while it runs, and there is no revert window — so progress is
// reported as explicit per-statement state transitions rather than row
// counts. A statement that cannot take the table's metadata lock kills the
// transactions blocking it, as Spirit does for its own DDL; routing has
// already confirmed SchemaBot holds the grants that find and kill them. It returns false when execution must stop: a cancelled context
// leaves the engine state Stopped, a genuine failure transitions to
// StateFailed.
func (e *Engine) executeDirectStatements(ctx context.Context, target *lazyTargetDB, database string, stmts []directRouted, policy directPolicy) bool {
	logger := e.changeLogger()
	lockWaitSeconds := policy.lockAcquisitionTimeoutSeconds()
	db, err := openDirectExecutionDB(ctx, target.dsn, lockWaitSeconds)
	if err != nil {
		logger.Error("direct execution failed: cannot connect to target with bounded session lock waits",
			"database", database, "lock_wait_timeout_seconds", lockWaitSeconds, "error", err)
		e.setSchemaChangeFailed(fmt.Errorf("connect for direct execution: %w", err))
		return false
	}
	defer utils.CloseAndLog(db)
	forceExecConfig := directForceExecConfig(lockWaitSeconds)
	for _, ds := range stmts {
		progress := e.trackDirectStatement(ds.table, ds.stmt)
		logger.Info("executing statement directly as native MySQL DDL",
			"database", database, "table", ds.table, "reason", ds.reason, "estimated_rows", ds.rows, "estimated_bytes", ds.bytes,
			"lock_wait_timeout_seconds", lockWaitSeconds, "max_attempts", forceExecConfig.MaxRetries)
		e.emitTableLog(ds.table, "executing statement as native MySQL DDL: transactions blocking the table's metadata lock are killed; writes to the table block while it runs")
		// The ALTER is spliced in with %r so ForceExec's format string never
		// interprets it: a literal % in a comment or default value stays as
		// written.
		tables := []*table.TableInfo{table.NewTableInfo(db, database, ds.table)}
		killLogger := logger.With("database", database, "table", ds.table)
		if err := dbconn.ForceExec(ctx, db, tables, forceExecConfig, killLogger, "%r", sqlescape.RawSQL(ds.stmt)); err != nil {
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
				// The kill did not clear the blocker: an explicit LOCK TABLES
				// and a transaction too heavy to roll back are never killed, and
				// a KILL of a SYSTEM_USER account's session fails without
				// SYSTEM_USER, as does any KILL after a grant routing confirmed
				// is revoked. Spirit logs which of these it hit through the kill
				// logger above.
				logger.Error("direct execution failed: statement could not acquire the table's metadata lock; the force-kill did not clear the blocker",
					"database", database, "table", ds.table, "lock_wait_timeout_seconds", lockWaitSeconds,
					"max_attempts", forceExecConfig.MaxRetries, "error", err)
				// The table name comes from the plan and the timeout from this
				// deployment's own configuration, so this sentence is safe to
				// show on the pull request that asked for the change.
				e.setSchemaChangeFailed(engine.OperatorErrorf(err,
					"Table %q is busy: the change could not acquire the metadata lock. Each attempt waits up to %ds, and SchemaBot makes up to %d attempts, killing the transactions blocking the lock between them. It stops after the first attempt that meets a session it will not or cannot kill: a session holding an explicit LOCK TABLES, a transaction too large to roll back safely, or a session of a SYSTEM_USER account, which only a user that also has SYSTEM_USER may kill. Retry when those sessions have finished; the server log names the session that blocked the change.",
					ds.table, lockWaitSeconds, forceExecConfig.MaxRetries))
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
