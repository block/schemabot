// control.go implements runtime control operations for in-progress schema changes.
//
// While spirit.go handles the core lifecycle (Plan, Apply, Progress), this file
// provides operations that modify a running migration:
//   - Stop/Start: pause and resume copying (with checkpoint preservation)
//   - Cutover: trigger the final atomic table swap
//   - Revert/SkipRevert: roll back or skip rollback of completed changes
//   - Volume: adjust concurrency and chunk timing on the fly
package spirit

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/engine"
)

// Stop pauses a running schema change.
// Spirit uses a checkpoint table to track progress, so the change can be resumed later.
// We force a checkpoint before canceling to preserve progress (Spirit only checkpoints every 50s).
func (e *Engine) Stop(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	e.mu.Lock()
	rm := e.runningMigration
	if rm == nil {
		e.mu.Unlock()
		return nil, fmt.Errorf("no active schema change to stop")
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

	// Force a checkpoint BEFORE canceling the context.
	// Spirit only checkpoints every 50s, so without this we could lose progress.
	if len(runners) > 0 && runners[0] != nil {
		e.logger.Info("forcing checkpoint before stop",
			"database", database,
			"tables", tables,
		)
		if err := runners[0].DumpCheckpoint(ctx); err != nil {
			// Log but don't fail - checkpoint might not be ready yet (early in execution)
			e.logger.Warn("could not force checkpoint before stop",
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

	e.logger.Info("stop requested, waiting for goroutine",
		"database", database,
		"tables", tables,
	)

	// Wait for the goroutine to complete
	rm.wg.Wait()

	e.logger.Info("schema change stopped",
		"database", database,
		"tables", tables,
	)

	return &engine.ControlResult{
		Accepted: true,
		Message:  "Stopped - checkpoint saved for resume",
	}, nil
}

// Start resumes a stopped schema change.
// Spirit automatically resumes from its checkpoint table, which stores:
// - copier watermark (where the copy was)
// - binlog position (for replication replay)
// - the DDL statement being executed
// When Run() is called, Spirit checks for a checkpoint and resumes if found.
func (e *Engine) Start(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	e.mu.Lock()
	rm := e.runningMigration
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
	ddls := rm.ddls
	combinedStatement := rm.combinedStatement
	deferCutover := rm.deferCutover
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

	e.logger.Info("resuming schema change",
		"database", database,
		"tables", tables,
	)

	// Update state under the lock before launching goroutine.
	// Use the stored combined statement directly (not executeMigration) to avoid
	// re-parsing DDLs through statement.New(), which can normalize formatting and
	// cause Spirit checkpoint mismatches ("alter statement does not match").
	e.mu.Lock()
	rm.state = engine.StateRunning
	e.mu.Unlock()

	rm.wg.Go(func() {
		bgCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		defer cancel()
		e.mu.Lock()
		if e.runningMigration != nil {
			e.runningMigration.cancelFunc = cancel
		}
		e.mu.Unlock()
		if combinedStatement != "" {
			_ = e.executeSpiritMigration(bgCtx, host, username, password, database, combinedStatement, deferCutover)
		} else {
			e.executeMigration(bgCtx, host, username, password, database, ddls, deferCutover)
		}
	})

	return &engine.ControlResult{
		Accepted: true,
		Message:  "Resumed from checkpoint",
	}, nil
}

// Cutover triggers the final table swap.
// When DeferCutOver was used, this triggers the deferred cutover by dropping
// Spirit's sentinel table (_spirit_sentinel).
func (e *Engine) Cutover(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	e.mu.Lock()
	rm := e.runningMigration
	e.mu.Unlock()

	if rm == nil {
		return nil, fmt.Errorf("no active schema change to cutover")
	}

	if !rm.deferCutover {
		return &engine.ControlResult{
			Accepted: false,
			Message:  "schema change was not started with defer_cutover",
		}, nil
	}

	// Drop the sentinel table to trigger cutover
	// Spirit waits for _spirit_sentinel to be dropped before proceeding
	if req.Credentials == nil || req.Credentials.DSN == "" {
		return nil, fmt.Errorf("DSN credentials required for cutover")
	}

	db, err := sql.Open("mysql", req.Credentials.DSN)
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
	_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`._spirit_sentinel", rm.database))
	if err != nil {
		return nil, fmt.Errorf("drop sentinel table: %w", err)
	}

	e.logger.Info("sentinel table dropped, cutover will proceed", "database", rm.database)

	return &engine.ControlResult{
		Accepted:    true,
		Message:     "Cutover triggered - schema change will complete shortly",
		ResumeState: req.ResumeState,
	}, nil
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
// update the settings, and restart from checkpoint.
func (e *Engine) Volume(ctx context.Context, req *engine.VolumeRequest) (*engine.VolumeResult, error) {
	e.mu.Lock()
	rm := e.runningMigration
	e.mu.Unlock()

	if rm == nil {
		return nil, fmt.Errorf("no active schema change to adjust volume")
	}

	// Calculate settings from volume level (1-11)
	previousVolume := settingsToVolume(e.threads, e.targetChunkTime)
	newThreads, newChunkTime, newLockTimeout := volumeToSpiritSettings(req.Volume, e.cpuHint)

	e.logger.Info("adjusting volume",
		"database", rm.database,
		"volume", req.Volume,
		"previous_volume", previousVolume,
		"new_threads", newThreads,
		"new_chunk_time", newChunkTime,
	)

	// If volume is the same, no need to restart
	if req.Volume == previousVolume {
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

	// Stop the schema change
	_, err := e.Stop(ctx, &engine.ControlRequest{
		Database:    req.Database,
		Credentials: req.Credentials,
	})
	if err != nil {
		return nil, fmt.Errorf("stop for volume change: %w", err)
	}

	// Log checkpoint state AFTER stopping (should be same as before)
	e.logCheckpointState(rm, "after_stop", nil)

	// Override the stopped state immediately. Stop() sets rm.state = StateStopped,
	// but during a volume change this is a transient internal state. If the poller
	// observes "stopped" it treats it as terminal and exits, leaving storage stuck.
	// Setting it back to "running" before Start() ensures Progress() never exposes
	// the intermediate stopped state.
	e.mu.Lock()
	rm.state = engine.StateRunning
	e.mu.Unlock()

	// Update engine configuration
	e.threads = newThreads
	e.targetChunkTime = newChunkTime
	e.lockWaitTimeout = newLockTimeout

	// Restart the schema change
	_, err = e.Start(ctx, &engine.ControlRequest{
		Database:    req.Database,
		Credentials: req.Credentials,
	})
	if err != nil {
		return nil, fmt.Errorf("restart after volume change: %w", err)
	}

	// Log checkpoint state AFTER restart (should still be same - Spirit resumes from checkpoint)
	e.mu.Lock()
	rmAfter := e.runningMigration
	e.mu.Unlock()
	if rmAfter != nil {
		e.logCheckpointState(rmAfter, "after_restart", nil)
	}

	return &engine.VolumeResult{
		Accepted:       true,
		PreviousVolume: previousVolume,
		NewVolume:      req.Volume,
		Message:        fmt.Sprintf("Volume changed: %d -> %d (%d threads, %v chunks)", previousVolume, req.Volume, newThreads, newChunkTime),
	}, nil
}

// minThreads is the lower bound for CPU-scaled volumes (6-11).
// innodb_buffer_pool_instances often returns 1 on small instances, so a floor
// of 2 prevents CPU-scaled volumes from regressing below volume 2's thread count.
const minThreads = 2

// maxThreads is the upper bound on copier threads regardless of CPU hint.
// This prevents swarming the database even if innodb_buffer_pool_instances
// is set to a very high value.
const maxThreads = 16

// volumeToSpiritSettings converts a volume level (1-11) to Spirit settings.
// Volumes 1-5 use fixed thread counts.
// Volumes 6-11 use CPU-scaled formulas (ceil(cpus/N)) when cpuHint > 0,
// falling back to fixed thread counts when CPU info is unavailable.
// Thread counts are always capped at maxThreads.
// Spirit requires TargetChunkTime to be in range 100ms-5s.
func volumeToSpiritSettings(volume int32, cpuHint int) (threads int, chunkTime time.Duration, lockTimeout time.Duration) {
	switch volume {
	case 1:
		return 1, 100 * time.Millisecond, 10 * time.Second
	case 2:
		return 2, 500 * time.Millisecond, 15 * time.Second
	case 4:
		return 4, 2 * time.Second, 60 * time.Second
	case 5:
		return 8, 2 * time.Second, 60 * time.Second
	case 6:
		return cpuScaledThreads(cpuHint, 16, 8), 5 * time.Second, 60 * time.Second
	case 7:
		return cpuScaledThreads(cpuHint, 12, 8), 5 * time.Second, 60 * time.Second
	case 8:
		return cpuScaledThreads(cpuHint, 8, 12), 5 * time.Second, 60 * time.Second
	case 9:
		return cpuScaledThreads(cpuHint, 6, 12), 5 * time.Second, 60 * time.Second
	case 10:
		return cpuScaledThreads(cpuHint, 4, maxThreads), 5 * time.Second, 600 * time.Second
	case 11:
		return cpuScaledThreads(cpuHint, 2, maxThreads), 5 * time.Second, 600 * time.Second
	default: // 3
		return 2, 2 * time.Second, 30 * time.Second
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

// settingsToVolume converts current Spirit settings back to approximate volume level.
func settingsToVolume(threads int, chunkTime time.Duration) int32 {
	switch {
	case threads <= 1:
		return 1
	case threads <= 2:
		if chunkTime <= 1*time.Second {
			return 2
		}
		return 3
	case threads <= 4:
		return 4
	case threads <= 8:
		if chunkTime <= 2*time.Second {
			return 5
		}
		return 6 // also covers vol 7 (same settings)
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
func (e *Engine) queryCPUHint(ctx context.Context, dsn string) int {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		e.logger.Debug("queryCPUHint: failed to open", "error", err)
		return 0
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		e.logger.Debug("queryCPUHint: failed to ping", "error", err)
		return 0
	}

	var instances int
	if err := db.QueryRowContext(ctx, "SELECT @@innodb_buffer_pool_instances").Scan(&instances); err != nil {
		e.logger.Debug("queryCPUHint: failed to query", "error", err)
		return 0
	}

	if instances <= 0 {
		return 0
	}

	e.logger.Info("detected CPU hint from innodb_buffer_pool_instances",
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
func (e *Engine) logCheckpointState(rm *runningMigration, phase string, extra map[string]any) {
	if rm == nil || rm.host == "" {
		e.logger.Debug("logCheckpointState: no running schema change or credentials")
		return
	}

	// Build DSN for connection
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?interpolateParams=true",
		rm.username, rm.password, rm.host, rm.database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		e.logger.Warn("logCheckpointState: failed to open", "error", err)
		return
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(context.Background()); err != nil {
		e.logger.Warn("logCheckpointState: failed to connect", "error", err)
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
			e.logger.Debug("logCheckpointState: no checkpoint table found",
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
			e.logger.Warn("logCheckpointState: failed to read checkpoint",
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

		e.logger.Info("checkpoint_state", logFields...)
	}
}
