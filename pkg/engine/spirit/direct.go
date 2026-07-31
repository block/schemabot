// direct.go implements the direct execution policy decision: whether an
// ALTER statement Spirit deterministically refuses may run as native MySQL
// DDL under the target database's policy. The policy arrives as engine
// metadata, the plan records the resulting per-statement verdict, and every
// uncertainty fails closed — a refused statement resolves to the direct
// verdict only when the policy is enabled and the table's measured size is
// within the configured bound.
package spirit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/ui"
)

// directPolicy is the resolved direct execution policy for a target database.
// The zero value is the fail-closed default: refused statements are blocked.
type directPolicy struct {
	Enabled      bool
	MaxTableRows int64
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
	return directPolicy{Enabled: true, MaxTableRows: maxRows}, nil
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
		}
	}
	if count > policy.MaxTableRows {
		e.logger.Info("direct execution blocked: exact row count above the policy bound despite a smaller estimate",
			"database", database, "table", tableName, "estimated_rows", rows, "max_table_rows", policy.MaxTableRows)
		return refusedModeDecision{
			mode:       engine.ExecutionModeBlocked,
			modeReason: aboveBoundReason,
		}
	}
	return refusedModeDecision{
		mode: engine.ExecutionModeDirect,
		modeReason: fmt.Sprintf("%s; runs as native MySQL DDL on a table with ~%s rows",
			refusalReason, ui.FormatNumber(count)),
		rows: count,
	}
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
