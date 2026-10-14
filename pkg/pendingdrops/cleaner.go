package pendingdrops

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/namedlock"
)

const (
	lockNamePrefix = "schemabot_pending_drops_"
	lockHashLen    = 16
)

// Target is one database the cleaner inspects. The DSN must be resolved
// (no secret references) and reachable from this process.
type Target struct {
	// Database is the SchemaBot database name, used for logs and metrics.
	Database string

	// Environment is the SchemaBot environment name, used for logs and metrics.
	Environment string

	// DSN is the resolved MySQL connection string for the target.
	DSN string

	// Locker is the advisory-lock implementation for this target's engine,
	// used to serialize cleanup across SchemaBot instances with a
	// session-scoped lock held on the pinned target connection. The producer
	// chooses it per target because one cleanup pass can span heterogeneous
	// engines. A target without a locker fails closed rather than assuming
	// MySQL semantics.
	Locker namedlock.Locker
}

// Cleaner permanently drops quarantined tables from _pending_drops once they
// are older than the retention period. Tables whose names do not carry a
// valid timestamp prefix are never dropped because their age is unknown.
type Cleaner struct {
	targets   []Target
	retention time.Duration
	dryRun    bool
	logger    *slog.Logger

	// nowFunc is overridable for testing.
	nowFunc func() time.Time
}

// NewCleaner creates a Cleaner for the given targets. When dryRun is true,
// the cleaner logs the tables it would drop without dropping them.
func NewCleaner(targets []Target, retention time.Duration, dryRun bool, logger *slog.Logger) *Cleaner {
	if retention <= 0 {
		retention = DefaultRetention
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Cleaner{
		targets:   targets,
		retention: retention,
		dryRun:    dryRun,
		logger:    logger,
		nowFunc:   time.Now,
	}
}

// Run performs one cleanup pass over every target. Targets are independent:
// a failure on one target is logged and counted, and the pass continues with
// the remaining targets. The last error is returned so callers can surface
// that the pass was incomplete.
func (c *Cleaner) Run(ctx context.Context) error {
	var lastErr error
	for _, target := range c.targets {
		if err := c.cleanTarget(ctx, target); err != nil {
			c.logger.Error("pending drops cleanup failed for target; continuing with remaining targets",
				"database", target.Database,
				"environment", target.Environment,
				"error", err,
			)
			lastErr = err
		}
	}
	return lastErr
}

// cleanTarget connects to one target, serializes against other SchemaBot
// instances with an advisory lock, and drops expired quarantined tables.
func (c *Cleaner) cleanTarget(ctx context.Context, target Target) error {
	// A missing locker is a producer wiring bug, not a connectivity failure:
	// it fails every pass until the producer is fixed, so it gets its own
	// metric reason instead of blending into transient target errors.
	if target.Locker == nil {
		metrics.RecordPendingDropsCleanupError(ctx, target.Database, target.Environment, "locker_missing")
		return fmt.Errorf("target %s/%s has no advisory locker; cleanup cannot serialize across instances without one", target.Database, target.Environment)
	}

	db, err := mysqlconn.Open(target.DSN)
	if err != nil {
		metrics.RecordPendingDropsCleanupError(ctx, target.Database, target.Environment, "target_error")
		return fmt.Errorf("open target %s/%s: %w", target.Database, target.Environment, err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		metrics.RecordPendingDropsCleanupError(ctx, target.Database, target.Environment, "target_error")
		return fmt.Errorf("ping target %s/%s: %w", target.Database, target.Environment, err)
	}

	// The advisory lock is session-scoped, so hold one connection for the
	// whole pass.
	conn, err := db.Conn(ctx)
	if err != nil {
		metrics.RecordPendingDropsCleanupError(ctx, target.Database, target.Environment, "target_error")
		return fmt.Errorf("acquire connection for %s/%s: %w", target.Database, target.Environment, err)
	}
	defer utils.CloseAndLog(conn)

	lockName := targetLockName(target)
	acquired, err := target.Locker.Acquire(ctx, conn, lockName, 0)
	if err != nil {
		metrics.RecordPendingDropsCleanupError(ctx, target.Database, target.Environment, "target_error")
		return fmt.Errorf("acquire advisory lock on %s/%s: %w", target.Database, target.Environment, err)
	}
	if !acquired {
		c.logger.Info("pending drops cleanup skipped: another instance holds the cleanup lock",
			"database", target.Database,
			"environment", target.Environment,
			"lock_name", lockName,
		)
		metrics.RecordPendingDropsCleanupLockSkipped(ctx, target.Database, target.Environment)
		return nil
	}
	defer func() {
		released, err := target.Locker.Release(context.WithoutCancel(ctx), conn, lockName)
		switch {
		case err != nil:
			c.logger.Warn("failed to release pending drops cleanup lock; the lock releases when the session closes",
				"database", target.Database,
				"environment", target.Environment,
				"lock_name", lockName,
				"error", err,
			)
		case !released:
			c.logger.Warn("pending drops cleanup lock was no longer held at release; another instance may have run concurrently against this target",
				"database", target.Database,
				"environment", target.Environment,
				"lock_name", lockName,
			)
		}
	}()

	exists, err := pendingDropsDatabaseExists(ctx, conn)
	if err != nil {
		metrics.RecordPendingDropsCleanupError(ctx, target.Database, target.Environment, "target_error")
		return fmt.Errorf("check %s database on %s/%s: %w", Database, target.Database, target.Environment, err)
	}
	if !exists {
		c.logger.Debug("pending drops cleanup: no quarantine database on target, nothing to clean",
			"database", target.Database,
			"environment", target.Environment,
		)
		return nil
	}

	tables, err := listPendingDropTables(ctx, conn)
	if err != nil {
		metrics.RecordPendingDropsCleanupError(ctx, target.Database, target.Environment, "target_error")
		return fmt.Errorf("list %s tables on %s/%s: %w", Database, target.Database, target.Environment, err)
	}

	now := c.nowFunc()
	cutoff := now.Add(-c.retention)
	var dropped, wouldDrop, kept, skipped int
	var dropErr error
	hasLog := false
	remaining := make([]string, 0, len(tables))

	for _, tableName := range tables {
		if tableName == LogTable {
			// The quarantine log records which apply moved each table here;
			// it is not a quarantined table and is never dropped.
			hasLog = true
			continue
		}
		quarantinedAt, ok := ParseTimestamp(tableName)
		if !ok {
			c.logger.Warn("pending drops cleanup: table name has no valid timestamp prefix, table will never be auto-dropped",
				"database", target.Database,
				"environment", target.Environment,
				"table", tableName,
			)
			metrics.RecordPendingDropsCleanupSkipped(ctx, target.Database, target.Environment)
			remaining = append(remaining, tableName)
			skipped++
			continue
		}
		if !quarantinedAt.Before(cutoff) {
			remaining = append(remaining, tableName)
			kept++
			continue
		}

		age := now.Sub(quarantinedAt).Round(time.Hour)
		if c.dryRun {
			c.logger.Info("pending drops cleanup dry run: table would be dropped",
				"database", target.Database,
				"environment", target.Environment,
				"table", tableName,
				"age", age,
			)
			remaining = append(remaining, tableName)
			wouldDrop++
			continue
		}

		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", quoteIdentifier(Database), quoteIdentifier(tableName))
		if _, err := conn.ExecContext(ctx, dropSQL); err != nil {
			c.logger.Error("pending drops cleanup: drop failed, table remains quarantined until the next pass",
				"database", target.Database,
				"environment", target.Environment,
				"table", tableName,
				"error", err,
			)
			metrics.RecordPendingDropsCleanupError(ctx, target.Database, target.Environment, "drop_error")
			remaining = append(remaining, tableName)
			dropErr = err
			continue
		}
		c.logger.Info("pending drops cleanup: dropped expired quarantined table",
			"database", target.Database,
			"environment", target.Environment,
			"table", tableName,
			"age", age,
		)
		metrics.RecordPendingDropsCleanupDropped(ctx, target.Database, target.Environment)
		dropped++
	}

	prunedRecords, logErr := c.pruneQuarantineLog(ctx, conn, target, hasLog, remaining, cutoff)

	attrs := []any{
		"database", target.Database,
		"environment", target.Environment,
		"dry_run", c.dryRun,
		"kept", kept,
		"skipped_unparseable", skipped,
	}
	if c.dryRun {
		attrs = append(attrs, "would_drop", wouldDrop, "would_prune_log_records", prunedRecords)
	} else {
		attrs = append(attrs, "dropped", dropped, "pruned_log_records", prunedRecords)
	}
	c.logger.Info("pending drops cleanup pass complete", attrs...)

	// Both causes are kept: a pass that could neither drop a copy nor read
	// the log has two things for an operator to look at.
	var errs []error
	if dropErr != nil {
		errs = append(errs, fmt.Errorf("drop expired pending drops tables on %s/%s: %w", target.Database, target.Environment, dropErr))
	}
	if logErr != nil {
		errs = append(errs, fmt.Errorf("prune %s.%s on %s/%s: %w", Database, LogTable, target.Database, target.Environment, logErr))
	}
	return errors.Join(errs...)
}

// pruneQuarantineLog deletes the quarantine log records of copies past
// retention that no longer exist: copies this or an earlier pass dropped, and
// copies whose RENAME never landed. remaining lists the pending drops tables
// still present after this pass. A dry run reports how many records it would
// delete and deletes none, and a target with no log has nothing to prune.
func (c *Cleaner) pruneQuarantineLog(ctx context.Context, conn *sql.Conn, target Target, hasLog bool, remaining []string, cutoff time.Time) (int, error) {
	if !hasLog {
		return 0, nil
	}
	expired, err := expiredLogRecords(ctx, conn, remaining, cutoff)
	if err != nil {
		metrics.RecordPendingDropsCleanupError(ctx, target.Database, target.Environment, "log_error")
		return 0, err
	}
	if c.dryRun {
		for _, name := range expired {
			c.logger.Info("pending drops cleanup dry run: quarantine log record of a copy that is gone would be deleted",
				"database", target.Database,
				"environment", target.Environment,
				"quarantine_table", name,
			)
		}
		return len(expired), nil
	}
	deleteSQL := "DELETE FROM `" + Database + "`.`" + LogTable + "` WHERE quarantine_table = ?"
	pruned := 0
	for _, name := range expired {
		if _, err := conn.ExecContext(ctx, deleteSQL, name); err != nil {
			c.logger.Error("pending drops cleanup: could not delete the quarantine log record of a copy that is gone; it is deleted on a later pass",
				"database", target.Database,
				"environment", target.Environment,
				"quarantine_table", name,
				"error", err,
			)
			metrics.RecordPendingDropsCleanupError(ctx, target.Database, target.Environment, "log_error")
			return pruned, fmt.Errorf("delete log record of %s: %w", name, err)
		}
		pruned++
	}
	return pruned, nil
}

func targetLockName(target Target) string {
	sum := sha256.Sum256([]byte(target.Database + "\x00" + target.Environment))
	return lockNamePrefix + hex.EncodeToString(sum[:])[:lockHashLen]
}

func pendingDropsDatabaseExists(ctx context.Context, conn *sql.Conn) (bool, error) {
	var count int
	err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?",
		Database).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("query information_schema.schemata: %w", err)
	}
	return count > 0, nil
}

func listPendingDropTables(ctx context.Context, conn *sql.Conn) ([]string, error) {
	rows, err := conn.QueryContext(ctx,
		"SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
		Database)
	if err != nil {
		return nil, fmt.Errorf("query information_schema.tables: %w", err)
	}
	defer utils.CloseAndLog(rows)

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan table name: %w", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table names: %w", err)
	}
	return tables, nil
}
