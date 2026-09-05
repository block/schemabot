// control.go implements runtime control operations for in-progress schema changes.
//
// While spirit.go handles the core lifecycle (Plan, Apply, Progress), this file
// provides operations that modify a running migration:
//   - Stop/Start: pause and resume copying (with checkpoint preservation)
//   - Cutover: trigger the final atomic table swap
//   - Revert/SkipRevert: roll back or skip rollback of completed changes
package spirit

import (
	"context"
	"fmt"
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
		if d := e.drainedOutcome; d != nil && d.state == engine.StateCompleted {
			// The change landed and was drained before the stop arrived. The
			// retained outcome is still this engine's answer for that change,
			// so the typed rejection has the caller reconcile to the completed
			// outcome — exactly as it would had the stop raced a tracked
			// completion.
			database := d.database
			e.mu.Unlock()
			return nil, engine.NewAlreadyCompletedError("stop rejected: the schema change on database %s completed before the stop arrived", database)
		}
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
		if d := e.drainedOutcome; d != nil && d.state == engine.StateCompleted {
			// The change landed and was drained before the cancel arrived. The
			// retained outcome is still this engine's answer for that change,
			// so the typed rejection has the caller reconcile to the completed
			// outcome — exactly as it would had the cancel raced a tracked
			// completion.
			database := d.database
			e.mu.Unlock()
			return nil, engine.NewAlreadyCompletedError("cancel rejected: the schema change on database %s completed before the cancel arrived", database)
		}
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

// dropCancelledArtifacts releases the artifacts of a schema change cancelled
// while this instance was still running it, using the connection details the
// runner holds. It disposes of them through the same policy as a cancel that
// arrives with no runner alive, so how a copy is disposed of never depends on
// whether the instance that made it happened to survive.
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
	if _, err := e.releaseArtifacts(ctx, db, rm.database, rm.tables); err != nil {
		return err
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
	_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", quoteIdentifier(database), quoteIdentifier(deferredCutoverSentinelTable)))
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
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		database, deferredCutoverSentinelTable,
	).Scan(&count); err != nil {
		return false, fmt.Errorf("query deferred cutover signal for database %s: %w", database, err)
	}
	return count > 0, nil
}

func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// Revert rolls back a completed schema change. Spirit has no revert window: the
// change is applied by copying into a shadow table and swapping it in, and once
// that swap lands there is no engine phase left to undo it from. The decline is
// deterministic for every schema change on this engine, so it is typed rather
// than returned as a generic failure, and a caller holding a durable revert
// request resolves it terminally instead of retrying a rejection that can never
// succeed.
func (e *Engine) Revert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, engine.NewUnsupportedOperationError("revert is not supported for MySQL schema changes: the change is copied into a shadow table and swapped in, with no revert window to undo it from; undo it by planning and applying the inverse schema change")
}

// SkipRevert ends the revert window early. Spirit has no revert window to end,
// so the decline is deterministic and typed for the same reason as Revert.
func (e *Engine) SkipRevert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, engine.NewUnsupportedOperationError("skip-revert is not supported for MySQL schema changes: there is no revert window to close, since the change is already permanent once it cuts over")
}
