// control.go implements runtime control operations for in-progress schema changes.
//
// While spirit.go handles the core lifecycle (Plan, Apply, Progress), this file
// provides operations that modify a running migration:
//   - Stop/Start: pause and resume copying (with checkpoint preservation)
//   - Cutover: trigger the final atomic table swap
//   - Revert/SkipRevert: roll back or skip rollback of completed changes
//   - Volume: adjust copier concurrency on the fly
package spirit

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/mysqlconn"
)

// Stop pauses a running schema change.
// Spirit uses a checkpoint table to track progress, so the change can be resumed later.
// We force a checkpoint before canceling to preserve progress (Spirit only checkpoints every 50s).
func (e *Engine) Stop(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	e.mu.Lock()
	rm := e.runningSchemaChange
	if rm == nil {
		// Spirit tracks the change in-process, so with nothing tracked there is
		// no change this instance could ever stop — retrying cannot make one
		// appear.
		e.mu.Unlock()
		return nil, engine.NewPermanentError("no active schema change to stop")
	}
	state := rm.state
	runners := rm.runners
	database := rm.database
	tables := rm.tables
	e.mu.Unlock()

	if state == engine.StateStopped {
		return &engine.ControlResult{
			Accepted: true,
			Message:  "Already stopped",
		}, nil
	}
	if state == engine.StateCompleted {
		// The change landed before the stop arrived. Recording it as stopped
		// would misrepresent the target; the typed rejection has the caller
		// reconcile to the completed outcome instead.
		return nil, engine.NewAlreadyCompletedError("stop rejected: the schema change on database %s completed before the stop arrived", database)
	}

	logger := e.schemaChangeLogger(rm)

	// Force a checkpoint BEFORE canceling the context.
	// Spirit only checkpoints every 50s, so without this we could lose progress.
	if len(runners) > 0 && runners[0] != nil {
		logger.Info("forcing checkpoint before stop",
			"database", database,
			"tables", tables,
		)
		if err := runners[0].DumpCheckpoint(ctx); err != nil {
			// Log but don't fail - checkpoint might not be ready yet (early in execution)
			logger.Warn("could not force checkpoint before stop",
				"error", err,
			)
		}
	}

	// Cancel the context to signal Spirit to stop
	e.mu.Lock()
	if rm.cancelFunc != nil {
		rm.cancelFunc()
	}
	rm.state = engine.StateStopped
	e.mu.Unlock()

	logger.Info("stop requested, waiting for goroutine",
		"database", database,
		"tables", tables,
	)

	// Wait for the goroutine to complete
	rm.wg.Wait()

	logger.Info("schema change stopped",
		"database", database,
		"tables", tables,
	)

	return &engine.ControlResult{
		Accepted: true,
		Message:  "Stopped - checkpoint saved for resume",
	}, nil
}

// Cancel terminates a running schema change without preserving a checkpoint for
// resume.
func (e *Engine) Cancel(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	e.mu.Lock()
	rm := e.runningSchemaChange
	if rm == nil {
		// Spirit tracks the change in-process, so with nothing tracked there is
		// no change this instance could ever cancel — retrying cannot make one
		// appear.
		e.mu.Unlock()
		return nil, engine.NewPermanentError("no active schema change to cancel")
	}
	if rm.state == engine.StateCompleted {
		// The change landed before the cancel arrived. Marking it cancelled and
		// dropping its artifacts would misrepresent the target; the typed
		// rejection has the caller reconcile to the completed outcome instead.
		database := rm.database
		e.mu.Unlock()
		return nil, engine.NewAlreadyCompletedError("cancel rejected: the schema change on database %s completed before the cancel arrived", database)
	}
	database := rm.database
	tables := rm.tables
	if rm.cancelFunc != nil {
		rm.cancelFunc()
	}
	rm.state = engine.StateCancelled
	e.mu.Unlock()

	logger := e.schemaChangeLogger(rm)
	logger.Info("cancel requested, waiting for goroutine",
		"database", database,
		"tables", tables,
	)
	rm.wg.Wait()

	logger.Info("schema change cancelled",
		"database", database,
		"tables", tables,
	)
	if err := e.dropCancelledArtifacts(context.WithoutCancel(ctx), rm); err != nil {
		return nil, err
	}
	e.mu.Lock()
	if e.runningSchemaChange == rm {
		e.runningSchemaChange = nil
	}
	e.mu.Unlock()

	return &engine.ControlResult{
		Accepted: true,
		Message:  "Cancelled",
	}, nil
}

func (e *Engine) dropCancelledArtifacts(ctx context.Context, rm *runningSchemaChange) error {
	if rm == nil || rm.host == "" {
		return fmt.Errorf("cancelled schema change cleanup missing connection details")
	}
	cfg := mysql.NewConfig()
	cfg.User = rm.username
	cfg.Passwd = rm.password
	cfg.Net = "tcp"
	cfg.Addr = rm.host
	cfg.DBName = rm.database
	cfg.InterpolateParams = true
	db, err := mysqlconn.Open(cfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("open database %s to clean up cancelled schema change artifacts: %w", rm.database, err)
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect to database %s to clean up cancelled schema change artifacts: %w", rm.database, err)
	}
	for _, tableName := range rm.tables {
		for _, artifact := range []string{
			utils.AuxTableName(tableName, "_new"),
			utils.AuxTableName(tableName, "_old"),
			utils.CheckpointTableName(tableName),
		} {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", quoteIdentifier(rm.database), quoteIdentifier(artifact))); err != nil {
				return fmt.Errorf("drop cancelled schema change artifact %s.%s: %w", rm.database, artifact, err)
			}
		}
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s._spirit_sentinel", quoteIdentifier(rm.database))); err != nil {
		return fmt.Errorf("drop cancelled schema change sentinel for database %s: %w", rm.database, err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s._spirit_checkpoint", quoteIdentifier(rm.database))); err != nil {
		return fmt.Errorf("drop cancelled schema change checkpoint for database %s: %w", rm.database, err)
	}
	return nil
}

// Start resumes a stopped schema change.
// Spirit automatically resumes from its checkpoint table, which stores:
// - copier watermark (where the copy was)
// - binlog position (for replication replay)
// - the DDL statement being executed
// When Run() is called, Spirit checks for a checkpoint and resumes if found.
func (e *Engine) Start(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	e.mu.Lock()
	rm := e.runningSchemaChange
	if rm == nil {
		e.mu.Unlock()
		return nil, fmt.Errorf("no schema change to resume - use Apply to start a new one")
	}
	state := rm.state
	host := rm.host
	username := rm.username
	password := rm.password
	database := rm.database
	tables := rm.tables
	originalDDLs := rm.originalDDLs
	combinedStatement := rm.combinedStatement
	deferCutover := rm.deferCutover
	directExecPolicy := rm.directPolicy
	e.mu.Unlock()

	if state == engine.StateRunning {
		return &engine.ControlResult{
			Accepted: true,
			Message:  "Already running",
		}, nil
	}

	if state != engine.StateStopped {
		return nil, fmt.Errorf("cannot resume schema change in state %s", state)
	}

	// Verify we have credentials for resume
	if host == "" || username == "" {
		return nil, fmt.Errorf("credentials not available for resume")
	}

	e.schemaChangeLogger(rm).Info("resuming schema change",
		"database", database,
		"tables", tables,
	)

	e.mu.Lock()
	rm.state = engine.StateRunning
	e.mu.Unlock()

	rm.wg.Go(func() {
		bgCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		defer cancel()
		e.mu.Lock()
		if e.runningSchemaChange != nil {
			e.runningSchemaChange.cancelFunc = cancel
		}
		e.mu.Unlock()
		e.resumeSchemaChange(bgCtx, host, username, password, database, originalDDLs, combinedStatement, deferCutover, directExecPolicy)
	})

	return &engine.ControlResult{
		Accepted: true,
		Message:  "Resumed from checkpoint",
	}, nil
}

// statelessControlDatabase resolves the schema a stateless control operation
// (cutover or sentinel lookup without an in-memory schema change) addresses.
// The DSN's database wins: it carries the physical schema name, which can
// differ from the request's logical database name when the target maps
// namespaces to per-deployment physical schemas. The request database is only
// a fallback for DSNs without a schema.
func statelessControlDatabase(dsn, requestDatabase string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	if cfg.DBName != "" {
		return cfg.DBName, nil
	}
	return requestDatabase, nil
}

// Cutover triggers the final table swap.
// When DeferCutOver was used, this triggers the deferred cutover by dropping
// Spirit's sentinel table (_spirit_sentinel).
func (e *Engine) Cutover(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	if req == nil {
		return nil, fmt.Errorf("cutover request is required")
	}
	if req.Credentials == nil || req.Credentials.DSN == "" {
		return nil, fmt.Errorf("DSN credentials required for cutover")
	}

	e.mu.Lock()
	rm := e.runningSchemaChange
	database := ""
	if rm != nil {
		if !rm.deferCutover {
			e.mu.Unlock()
			return &engine.ControlResult{
				Accepted: false,
				Message:  "schema change was not started with defer_cutover",
			}, nil
		}
		database = rm.database
	}
	e.mu.Unlock()

	// Stateless cutover — no in-memory schema change, or one without a
	// recorded database (e.g. after a restart) — addresses the schema the
	// DSN connects to.
	if database == "" {
		var err error
		database, err = statelessControlDatabase(req.Credentials.DSN, req.Database)
		if err != nil {
			return nil, fmt.Errorf("parse DSN for cutover database: %w", err)
		}
	}
	if database == "" {
		return nil, fmt.Errorf("database is required for cutover")
	}

	db, err := mysqlconn.Open(req.Credentials.DSN)
	if err != nil {
		return nil, fmt.Errorf("open connection for cutover: %w", err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping database for cutover: %w", err)
	}

	// Drop the sentinel table - Spirit will detect this and proceed with cutover.
	// Cutover is asynchronous — Spirit performs the table swap in its goroutine.
	// The caller should poll Progress() for state transitions.
	_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s._spirit_sentinel", quoteIdentifier(database)))
	if err != nil {
		return nil, fmt.Errorf("drop sentinel table: %w", err)
	}

	e.schemaChangeLogger(rm).Info("sentinel table dropped, cutover will proceed", "database", database, "stateless", rm == nil)

	return &engine.ControlResult{
		Accepted:    true,
		Message:     "Cutover triggered - schema change will complete shortly",
		ResumeState: req.ResumeState,
	}, nil
}

// DeferredCutoverSignalExists reports whether Spirit's deferred-cutover signal
// is still present in the target database.
func (e *Engine) DeferredCutoverSignalExists(ctx context.Context, req *engine.DeferredCutoverSignalRequest) (bool, error) {
	if req == nil {
		return false, fmt.Errorf("deferred cutover signal request is required")
	}
	if req.Credentials == nil || req.Credentials.DSN == "" {
		return false, fmt.Errorf("DSN credentials required for deferred cutover signal lookup")
	}

	// The sentinel lives in the schema the DSN connects to.
	database, err := statelessControlDatabase(req.Credentials.DSN, req.Database)
	if err != nil {
		return false, fmt.Errorf("parse DSN for deferred cutover signal database: %w", err)
	}
	if database == "" {
		return false, fmt.Errorf("database is required for deferred cutover signal lookup")
	}

	db, err := mysqlconn.Open(req.Credentials.DSN)
	if err != nil {
		return false, fmt.Errorf("open connection for deferred cutover signal lookup: %w", err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		return false, fmt.Errorf("ping database for deferred cutover signal lookup: %w", err)
	}

	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = '_spirit_sentinel'",
		database,
	).Scan(&count); err != nil {
		return false, fmt.Errorf("query deferred cutover signal for database %s: %w", database, err)
	}
	return count > 0, nil
}

func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// Revert rolls back a completed schema change.
// Spirit doesn't have built-in revert - this would need to be implemented separately.
func (e *Engine) Revert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("revert not supported for Spirit engine")
}

// SkipRevert ends the revert window early.
func (e *Engine) SkipRevert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("skip revert not supported for Spirit engine")
}

// Volume adjusts the schema change speed by stopping, reconfiguring, and restarting.
// Spirit doesn't support dynamic volume changes, so we stop the schema change,
// update its settings, and restart from checkpoint. The adjustment is scoped to
// the running schema change: the engine's configured defaults stay untouched,
// so the next schema change starts from the defaults again.
func (e *Engine) Volume(ctx context.Context, req *engine.VolumeRequest) (*engine.VolumeResult, error) {
	e.mu.Lock()
	rm := e.runningSchemaChange
	if rm == nil {
		e.mu.Unlock()
		return nil, fmt.Errorf("no active schema change to adjust volume")
	}
	cpuHint := e.cpuHint
	database := rm.database
	previousVolume := rm.volume
	if previousVolume == 0 {
		// No explicit volume was set for this schema change; report the
		// closest level for the settings it started with.
		previousVolume = settingsToVolume(rm.threads)
	}
	currentThreads := rm.threads
	e.mu.Unlock()

	// Calculate settings from volume level (1-11)
	newThreads := volumeToSpiritSettings(req.Volume, cpuHint)

	e.schemaChangeLogger(rm).Info("adjusting volume",
		"database", database,
		"volume", req.Volume,
		"previous_volume", previousVolume,
		"new_threads", newThreads,
	)

	// When the requested volume maps to the settings the change is already
	// running with, record the explicit volume and skip the restart.
	if newThreads == currentThreads {
		e.setSchemaChangeVolume(rm, req.Volume, newThreads)
		return &engine.VolumeResult{
			Accepted:       true,
			PreviousVolume: previousVolume,
			NewVolume:      req.Volume,
			Message:        "Volume unchanged - no restart needed",
		}, nil
	}

	// Log checkpoint state BEFORE stopping
	e.logCheckpointState(rm, "before_volume_change", map[string]any{
		"previous_volume": previousVolume,
		"new_volume":      req.Volume,
	})

	// Volume uses Stop to force a checkpoint before restarting with new settings.
	// Keep the stored stopped state available for Start while reporting the
	// adjustment as running to progress pollers.
	e.setVolumeRestartInProgress(rm, true)

	_, err := e.Stop(ctx, &engine.ControlRequest{
		Database:    req.Database,
		Credentials: req.Credentials,
	})
	if err != nil {
		e.setVolumeRestartInProgress(rm, false)
		return nil, fmt.Errorf("stop for volume change: %w", err)
	}

	// Log checkpoint state AFTER stopping (should be same as before)
	e.logCheckpointState(rm, "after_stop", nil)

	// Retune the running schema change; the engine's configured defaults stay
	// untouched so the next schema change starts from the defaults.
	e.setSchemaChangeVolume(rm, req.Volume, newThreads)

	// Restart the schema change
	_, err = e.Start(ctx, &engine.ControlRequest{
		Database:    req.Database,
		Credentials: req.Credentials,
	})
	if err != nil {
		e.setVolumeRestartInProgress(rm, false)
		return nil, fmt.Errorf("restart after volume change: %w", err)
	}
	e.setVolumeRestartInProgress(rm, false)

	// Log checkpoint state AFTER restart (should still be same - Spirit resumes from checkpoint)
	e.mu.Lock()
	rmAfter := e.runningSchemaChange
	e.mu.Unlock()
	if rmAfter != nil {
		e.logCheckpointState(rmAfter, "after_restart", nil)
	}

	return &engine.VolumeResult{
		Accepted:       true,
		PreviousVolume: previousVolume,
		NewVolume:      req.Volume,
		Message:        fmt.Sprintf("Volume changed: %d -> %d (%d threads)", previousVolume, req.Volume, newThreads),
	}, nil
}

// setSchemaChangeVolume records the explicit volume and its derived thread
// count on the tracked schema change. The setting ends with the change, so a
// volume set during one schema change never carries into a later one.
func (e *Engine) setSchemaChangeVolume(rm *runningSchemaChange, volume int32, threads int) {
	e.mu.Lock()
	tracked := e.runningSchemaChange == rm
	if tracked {
		rm.volume = volume
		rm.threads = threads
	}
	e.mu.Unlock()
	if !tracked {
		e.schemaChangeLogger(rm).Warn("volume adjustment target is no longer the tracked schema change; settings not applied",
			"database", rm.database,
			"volume", volume,
		)
	}
}

func (e *Engine) setVolumeRestartInProgress(rm *runningSchemaChange, inProgress bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runningSchemaChange == rm {
		rm.volumeRestartInProgress = inProgress
	}
}

// minThreads is the lower bound for CPU-scaled volumes (6-11).
// innodb_buffer_pool_instances often returns 1 on small instances, so a floor
// of 2 prevents CPU-scaled volumes from regressing below volume 2's thread count.
const minThreads = 2

// maxThreads is the upper bound on copier threads regardless of CPU hint.
// This prevents swarming the database even if innodb_buffer_pool_instances
// is set to a very high value.
const maxThreads = 16

// volumeToSpiritSettings converts a volume level (1-11) to a Spirit copier
// thread count. Volumes 1-5 use fixed thread counts. Volumes 6-11 use
// CPU-scaled formulas (ceil(cpus/N)) when cpuHint > 0, falling back to fixed
// thread counts when CPU info is unavailable. Thread counts are always capped
// at maxThreads.
//
// Thread count is the only lever volume adjusts: lock wait timeout is fixed
// engine-wide (see DefaultLockWaitTimeout) now that Spirit's ForceKill clears
// blockers well within it regardless of volume. Volumes 2 and 3, and 5-7
// without a CPU hint, consequently derive the same thread count and are
// indistinguishable in practice; when the engine's experimental autoscaling
// is enabled (the default) and Aurora is detected, autoscaling overrides
// these thread counts anyway on instances with more than a few vCPUs, so
// volume increasingly has nothing left to tune. A future change may collapse
// volume into fewer levels, or drop it, once autoscaling covers non-Aurora
// targets too.
func volumeToSpiritSettings(volume int32, cpuHint int) (threads int) {
	switch volume {
	case 1:
		return 1
	case 2:
		return 2
	case 4:
		return 4
	case 5:
		return 8
	case 6:
		return cpuScaledThreads(cpuHint, 16, 8)
	case 7:
		return cpuScaledThreads(cpuHint, 12, 8)
	case 8:
		return cpuScaledThreads(cpuHint, 8, 12)
	case 9:
		return cpuScaledThreads(cpuHint, 6, 12)
	case 10:
		return cpuScaledThreads(cpuHint, 4, maxThreads)
	case 11:
		return cpuScaledThreads(cpuHint, 2, maxThreads)
	default: // 3
		return 2
	}
}

// cpuScaledThreads computes ceil(cpuHint/divisor) when CPU info is available,
// falling back to fallback when cpuHint is 0. Result is clamped to [minThreads, maxThreads].
func cpuScaledThreads(cpuHint, divisor, fallback int) int {
	threads := fallback
	if cpuHint > 0 {
		threads = int(math.Ceil(float64(cpuHint) / float64(divisor)))
	}
	threads = max(threads, minThreads) // must be at least minThreads
	threads = min(threads, maxThreads) // can't be greater than maxThreads
	return threads
}

// settingsToVolume approximates the volume level for a schema change that was
// never given an explicit volume, mapping its starting thread count to the
// closest level. Volume levels that share a derived thread count map to the
// lowest such level — this includes volumes 2 and 3, which share a thread
// count now that lock wait timeout no longer varies by volume. Once an
// operator sets a volume, the explicit value is stored on the running schema
// change and this approximation is not used.
func settingsToVolume(threads int) int32 {
	switch {
	case threads <= 1:
		return 1
	case threads <= 2:
		return 2 // also covers vol 3 (same thread count)
	case threads <= 4:
		return 4
	case threads <= 8:
		return 5 // also covers vol 6, 7 (same settings without a CPU hint)
	case threads <= 12:
		return 8 // also covers vol 9 (same settings)
	default:
		return 10 // also covers vol 11 (same settings)
	}
}

// queryCPUHint queries innodb_buffer_pool_instances from the target database
// to infer the number of vCPUs. On RDS/Aurora, this variable is set by AWS to
// match the instance's vCPU count. On self-managed MySQL 8.4+, the default is
// dynamically calculated from available_logical_processors / 4.
// Returns 0 if the query fails or the value can't be determined.
// The logger is passed by the caller because the hint is probed while
// preparing a schema change, before it is tracked on the engine.
func (e *Engine) queryCPUHint(ctx context.Context, dsn string, logger *slog.Logger) int {
	db, err := mysqlconn.Open(dsn)
	if err != nil {
		logger.Debug("queryCPUHint: failed to open", "error", err)
		return 0
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		logger.Debug("queryCPUHint: failed to ping", "error", err)
		return 0
	}

	var instances int
	if err := db.QueryRowContext(ctx, "SELECT @@innodb_buffer_pool_instances").Scan(&instances); err != nil {
		logger.Debug("queryCPUHint: failed to query", "error", err)
		return 0
	}

	if instances <= 0 {
		return 0
	}

	logger.Info("detected CPU hint from innodb_buffer_pool_instances",
		"innodb_buffer_pool_instances", instances,
	)
	return instances
}

// logCheckpointState reads Spirit's checkpoint table and logs the checkpoint data.
// This is useful for debugging to understand what values change during volume adjustments.
// Spirit stores checkpoint data in _<table>_chkpnt tables with columns:
// - copier_watermark: position in the copy operation (e.g., "id:12345")
// - checksum_watermark: position in checksum verification
// - binlog_name: MySQL binlog file being replayed
// - binlog_pos: position within the binlog file
// - statement: the DDL being executed
func (e *Engine) logCheckpointState(rm *runningSchemaChange, phase string, extra map[string]any) {
	logger := e.schemaChangeLogger(rm)
	if rm == nil || rm.host == "" {
		logger.Debug("logCheckpointState: no running schema change or credentials")
		return
	}

	// Build DSN for connection.
	cfg := mysql.NewConfig()
	cfg.User = rm.username
	cfg.Passwd = rm.password
	cfg.Net = "tcp"
	cfg.Addr = rm.host
	cfg.DBName = rm.database
	cfg.InterpolateParams = true
	dsn := cfg.FormatDSN()

	db, err := mysqlconn.Open(dsn)
	if err != nil {
		logger.Warn("logCheckpointState: failed to open", "error", err)
		return
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(context.Background()); err != nil {
		logger.Warn("logCheckpointState: failed to connect", "error", err)
		return
	}

	// Query checkpoint table for each table being changed
	for _, tableName := range rm.tables {
		checkpointTable := fmt.Sprintf("_%s_chkpnt", tableName)

		// Check if checkpoint table exists
		var count int
		err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
			rm.database, checkpointTable).Scan(&count)
		if err != nil || count == 0 {
			logger.Debug("logCheckpointState: no checkpoint table found",
				"table", tableName,
				"checkpoint_table", checkpointTable,
				"phase", phase,
			)
			continue
		}

		// Read checkpoint data
		var copierWatermark, checksumWatermark, binlogName, statement sql.NullString
		var binlogPos sql.NullInt64

		query := fmt.Sprintf("SELECT copier_watermark, checksum_watermark, binlog_name, binlog_pos, statement FROM `%s`.`%s` LIMIT 1",
			rm.database, checkpointTable)
		err = db.QueryRowContext(context.Background(), query).Scan(&copierWatermark, &checksumWatermark, &binlogName, &binlogPos, &statement)
		if err != nil {
			logger.Warn("logCheckpointState: failed to read checkpoint",
				"table", tableName,
				"checkpoint_table", checkpointTable,
				"error", err,
			)
			continue
		}

		// Log the checkpoint data
		logFields := []any{
			"phase", phase,
			"table", tableName,
			"copier_watermark", copierWatermark.String,
			"checksum_watermark", checksumWatermark.String,
			"binlog_name", binlogName.String,
			"binlog_pos", binlogPos.Int64,
		}

		// Add extra context fields
		for k, v := range extra {
			logFields = append(logFields, k, v)
		}

		logger.Info("checkpoint_state", logFields...)
	}
}
