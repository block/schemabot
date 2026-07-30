// direct.go implements the direct execution policy decision: whether an
// ALTER statement Spirit deterministically refuses may run as native MySQL
// DDL under the target database's policy. The policy arrives as engine
// metadata, the plan records the resulting per-statement verdict, and every
// uncertainty fails closed — a refused statement resolves to the direct
// verdict only when the policy is enabled and the table's estimated size is
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

// Engine metadata keys carrying the direct execution policy. Config surfaces
// (server config, embedder assemblers) resolve their policy into these keys;
// the engine reads them from request credentials.
const (
	// metadataDirectExecution enables direct execution ("true") for ALTER
	// statements the engine deterministically refuses. Absent or "false"
	// leaves refused statements blocked.
	metadataDirectExecution = "direct_execution"

	// metadataDirectExecutionMaxTableRows bounds direct execution by the
	// target table's estimated row count. Required (a positive integer) when
	// direct execution is enabled, so a native table rebuild can never run
	// unbounded: above the bound — or when the size cannot be estimated —
	// the statement stays blocked.
	metadataDirectExecutionMaxTableRows = "direct_execution_max_table_rows"
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
	switch md[metadataDirectExecution] {
	case "", "false":
		return directPolicy{}, nil
	case "true":
	default:
		return directPolicy{}, fmt.Errorf("invalid %s metadata value %q: must be \"true\" or \"false\"", metadataDirectExecution, md[metadataDirectExecution])
	}
	raw := md[metadataDirectExecutionMaxTableRows]
	if raw == "" {
		return directPolicy{}, fmt.Errorf("%s is enabled but %s is not set: the row bound is required so direct execution fails closed on large tables", metadataDirectExecution, metadataDirectExecutionMaxTableRows)
	}
	maxRows, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return directPolicy{}, fmt.Errorf("parse %s metadata value %q: %w", metadataDirectExecutionMaxTableRows, raw, err)
	}
	if maxRows <= 0 {
		return directPolicy{}, fmt.Errorf("%s must be positive, got %d", metadataDirectExecutionMaxTableRows, maxRows)
	}
	return directPolicy{Enabled: true, MaxTableRows: maxRows}, nil
}

// estimatedTableRows returns MySQL's estimated row count for the table from
// information_schema statistics. The estimate is approximate (it tracks the
// optimizer's statistics, not an exact count), which is sufficient for the
// order-of-magnitude bound the direct execution policy enforces. The read
// happens on a dedicated connection with statistics caching disabled: MySQL
// otherwise serves information_schema statistics cached for up to
// information_schema_stats_expiry seconds (a day by default), and a safety
// bound must not be decided on a day-old row count.
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

// refusedModeDecision is how a statement the engine refuses will execute
// under the direct execution policy. The plan records mode and modeReason on
// the table change; the apply-time routing acts on the same decision.
type refusedModeDecision struct {
	mode       string // engine.ExecutionModeDirect or engine.ExecutionModeBlocked
	modeReason string // operator-facing reason, including row-estimate context
	rows       int64  // estimated rows when the estimate succeeded
}

// resolveRefusedMode decides whether the policy routes a refused statement to
// direct execution. Every uncertainty blocks: policy disabled, an estimated
// size above the bound, or a size that cannot be estimated at all.
func (e *Engine) resolveRefusedMode(ctx context.Context, target *lazyTargetDB, policy directPolicy, database, tableName, refusalReason string) refusedModeDecision {
	if !policy.Enabled {
		return refusedModeDecision{
			mode:       engine.ExecutionModeBlocked,
			modeReason: refusalReason,
		}
	}
	db, err := target.get(ctx)
	if err != nil {
		// Fail closed: without a connection there is no size estimate, and an
		// unestimated table must never rebuild natively.
		e.logger.Warn("direct execution blocked: cannot connect to target for the row estimate",
			"database", database, "table", tableName, "error", err)
		return refusedModeDecision{
			mode:       engine.ExecutionModeBlocked,
			modeReason: refusalReason + "; direct execution is enabled but the table's estimated row count is unavailable",
		}
	}
	rows, err := estimatedTableRows(ctx, db, database, tableName)
	if err != nil {
		// Fail closed: a table whose size cannot be estimated must never
		// rebuild natively — block it and surface why in the mode reason.
		e.logger.Warn("direct execution blocked: estimated row count unavailable",
			"database", database, "table", tableName, "error", err)
		return refusedModeDecision{
			mode:       engine.ExecutionModeBlocked,
			modeReason: refusalReason + "; direct execution is enabled but the table's estimated row count is unavailable",
		}
	}
	if rows > policy.MaxTableRows {
		return refusedModeDecision{
			mode: engine.ExecutionModeBlocked,
			modeReason: fmt.Sprintf("%s; direct execution is enabled but the table has ~%s rows, above the configured limit of %s",
				refusalReason, ui.FormatNumber(rows), ui.FormatNumber(policy.MaxTableRows)),
			rows: rows,
		}
	}
	return refusedModeDecision{
		mode: engine.ExecutionModeDirect,
		modeReason: fmt.Sprintf("%s; runs as native MySQL DDL on a table with ~%s rows",
			refusalReason, ui.FormatNumber(rows)),
		rows: rows,
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
