package pendingdrops

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/metrics"
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
	db, err := sql.Open("mysql", target.DSN)
	if err != nil {
		metrics.RecordPendingDropsCleanupError(ctx, target.Database, target.Environment, "target_error")
		return fmt.Errorf("open target %s/%s: %w", target.Database, target.Environment, err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		metrics.RecordPendingDropsCleanupError(ctx, target.Database, target.Environment, "target_error")
		return fmt.Errorf("ping target %s/%s: %w", target.Database, target.Environment, err)
	}

	// GET_LOCK is session-scoped, so hold one connection for the whole pass.
	conn, err := db.Conn(ctx)
	if err != nil {
		metrics.RecordPendingDropsCleanupError(ctx, target.Database, target.Environment, "target_error")
		return fmt.Errorf("acquire connection for %s/%s: %w", target.Database, target.Environment, err)
	}
	defer utils.CloseAndLog(conn)

	lockName := targetLockName(target)
	var locked int
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", lockName).Scan(&locked); err != nil {
		metrics.RecordPendingDropsCleanupError(ctx, target.Database, target.Environment, "target_error")
		return fmt.Errorf("acquire advisory lock on %s/%s: %w", target.Database, target.Environment, err)
	}
	if locked != 1 {
		c.logger.Info("pending drops cleanup skipped: another instance holds the cleanup lock",
			"database", target.Database,
			"environment", target.Environment,
			"lock_name", lockName,
		)
		metrics.RecordPendingDropsCleanupLockSkipped(ctx, target.Database, target.Environment)
		return nil
	}
	defer func() {
		if _, err := conn.ExecContext(context.WithoutCancel(ctx), "SELECT RELEASE_LOCK(?)", lockName); err != nil {
			c.logger.Warn("failed to release pending drops cleanup lock; the lock releases when the session closes",
				"database", target.Database,
				"environment", target.Environment,
				"lock_name", lockName,
				"error", err,
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

	for _, tableName := range tables {
		quarantinedAt, ok := ParseTimestamp(tableName)
		if !ok {
			c.logger.Warn("pending drops cleanup: table name has no valid timestamp prefix, table will never be auto-dropped",
				"database", target.Database,
				"environment", target.Environment,
				"table", tableName,
			)
			metrics.RecordPendingDropsCleanupSkipped(ctx, target.Database, target.Environment)
			skipped++
			continue
		}
		if !quarantinedAt.Before(cutoff) {
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

	attrs := []any{
		"database", target.Database,
		"environment", target.Environment,
		"dry_run", c.dryRun,
		"kept", kept,
		"skipped_unparseable", skipped,
	}
	if c.dryRun {
		attrs = append(attrs, "would_drop", wouldDrop)
	} else {
		attrs = append(attrs, "dropped", dropped)
	}
	c.logger.Info("pending drops cleanup pass complete", attrs...)
	if dropErr != nil {
		return fmt.Errorf("drop expired pending drops tables on %s/%s: %w", target.Database, target.Environment, dropErr)
	}
	return nil
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
