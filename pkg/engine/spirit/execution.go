// execution.go runs Spirit migrations: dispatching DDL statements to instant DDL,
// CREATE/DROP execution, or Spirit's online migration runner depending on type.
package spirit

import (
	"context"
	"fmt"
	"strings"

	spiritmigration "github.com/block/spirit/pkg/migration"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
)

// executeMigration runs the Spirit schema change synchronously.
// All DDL statements (CREATE, DROP, ALTER, RENAME) are passed through Spirit.
// Spirit requires non-ALTER statements to be executed individually (not combined).
// ALTER statements can be combined for atomic multi-table schema changes.
func (e *Engine) executeMigration(ctx context.Context, host, username, password, database string, ddlStatements []string, deferCutover bool) {
	phases, err := classifyDDLPhases(ddlStatements)
	if err != nil {
		e.logger.Error("failed to classify statement", "error", err)
		e.setMigrationFailed(err)
		return
	}

	// Execute CREATE TABLE statements first (each individually through Spirit).
	if !e.executeCreateStatements(ctx, host, username, password, database, phases.creates) {
		return
	}

	// Execute ALTER statements (combined for atomic multi-table execution).
	if len(phases.alters) > 0 {
		if !e.executeAlterPhase(ctx, host, username, password, database, phases.alters, deferCutover) {
			return
		}
	}

	// Execute DROP TABLE statements last. By default each table is quarantined
	// in the pending drops database instead of dropped; when pending drops is
	// disabled the DROP runs directly through Spirit.
	if !e.executeDropStatements(ctx, host, username, password, database, phases.drops) {
		return
	}

	// A cancelled context means the operator stopped the change; engine state
	// must remain Stopped, so do not overwrite it with StateCompleted.
	if ctx.Err() != nil {
		e.logger.Info("schema change stopped before completion",
			"database", database,
			"reason", ctx.Err(),
		)
		return
	}

	// Completion is signalled only after every phase has run, so a poller never
	// observes StateCompleted while a later DROP phase is still pending.
	e.setMigrationCompleted()
}

// resumeMigration continues a stopped schema change to completion.
//
// When an ALTER was in flight, its statements are resumed from Spirit's
// checkpoint using the stored verbatim combined statement (not the original DDL
// list) so the statement string matches the checkpoint exactly — re-parsing
// through statement.New() can normalize formatting and trip Spirit's "alter
// statement does not match" guard. CREATE statements always run before the ALTER
// phase, so once an ALTER has started they are already applied; only the DROP
// phase remains and must run after the resumed ALTER completes. When no ALTER was
// in flight, the whole plan is run from the start.
func (e *Engine) resumeMigration(ctx context.Context, host, username, password, database string, originalDDLs []string, combinedStatement string, deferCutover bool) {
	if combinedStatement == "" {
		e.executeMigration(ctx, host, username, password, database, originalDDLs, deferCutover)
		return
	}

	if err := e.executeSpiritMigration(ctx, host, username, password, database, combinedStatement, deferCutover); err != nil {
		return // Error already logged and state set; on stop the state stays Stopped.
	}

	// A stop during the resumed ALTER cancels the context; leave the state
	// Stopped and do not run the pending DROP phase.
	if ctx.Err() != nil {
		e.logger.Info("schema change stopped during resumed ALTER",
			"database", database,
			"reason", ctx.Err(),
		)
		return
	}

	phases, err := classifyDDLPhases(originalDDLs)
	if err != nil {
		e.logger.Error("failed to classify statements on resume", "database", database, "error", err)
		e.setMigrationFailed(err)
		return
	}

	if !e.executeDropStatements(ctx, host, username, password, database, phases.drops) {
		return
	}

	if ctx.Err() != nil {
		e.logger.Info("schema change stopped before completion",
			"database", database,
			"reason", ctx.Err(),
		)
		return
	}

	e.setMigrationCompleted()
}

// ddlPhases groups a plan's statements into the order Spirit requires:
// CREATE first, then ALTER (combined), then DROP.
type ddlPhases struct {
	creates []string
	alters  []string
	drops   []string
}

// classifyDDLPhases groups DDL statements into CREATE/ALTER/DROP phases using
// AST-based classification. RENAME and unrecognized statements run in the ALTER
// phase, matching Spirit's handling.
func classifyDDLPhases(ddlStatements []string) (ddlPhases, error) {
	var phases ddlPhases
	for _, stmt := range ddlStatements {
		stmtType, _, err := ddl.ClassifyStatement(stmt)
		if err != nil {
			return ddlPhases{}, fmt.Errorf("classify statement %q: %w", stmt, err)
		}
		switch stmtType {
		case statement.StatementCreateTable:
			phases.creates = append(phases.creates, stmt)
		case statement.StatementDropTable:
			phases.drops = append(phases.drops, stmt)
		default:
			phases.alters = append(phases.alters, stmt)
		}
	}
	return phases, nil
}

// executeCreateStatements runs each CREATE TABLE through Spirit. It returns false
// when execution should stop: a cancelled context leaves the state Stopped, a
// genuine failure transitions to StateFailed. The caller must not run later
// phases when this returns false.
func (e *Engine) executeCreateStatements(ctx context.Context, host, username, password, database string, creates []string) bool {
	for _, stmt := range creates {
		if err := e.executeSingleStatement(ctx, host, username, password, database, stmt); err != nil {
			if ctx.Err() != nil {
				e.logger.Info("schema change stopped during CREATE TABLE",
					"database", database,
					"reason", ctx.Err(),
				)
				return false
			}
			e.logger.Error("CREATE TABLE failed", "database", database, "error", err)
			e.setMigrationFailed(fmt.Errorf("CREATE TABLE failed: %w", err))
			return false
		}
	}
	return true
}

// executeAlterPhase combines the ALTER statements and runs them through Spirit.
// It returns true when the ALTER ran and the caller may proceed to the DROP
// phase, and false on a genuine failure (executeSpiritMigration has already set
// StateFailed). A stop cancels the context but returns true; the caller's later
// ctx.Err() checks then keep the state Stopped without running further phases.
func (e *Engine) executeAlterPhase(ctx context.Context, host, username, password, database string, alters []string, deferCutover bool) bool {
	combinedStatement := strings.Join(alters, "; ")

	var tables []string
	for _, stmt := range alters {
		parsed, err := statement.New(stmt)
		if err != nil {
			e.logger.Error("failed to parse statement for logging", "database", database, "error", err, "statement", stmt)
			e.setMigrationFailed(fmt.Errorf("parse ALTER statement %q: %w", stmt, err))
			return false
		}
		if len(parsed) > 0 {
			tables = append(tables, parsed[0].Table)
		}
	}

	e.logger.Info("executing ALTER via Spirit",
		"database", database,
		"tables", tables,
		"ddl_count", len(alters),
		"defer_cutover", deferCutover,
	)

	return e.executeSpiritMigration(ctx, host, username, password, database, combinedStatement, deferCutover) == nil
}

// executeDropStatements runs the DROP TABLE phase. By default each table is
// quarantined in the pending drops database so its data stays recoverable until
// the retention period expires; when pending drops is disabled the DROP runs
// directly through Spirit. Both the initial-apply DROP phase and the resume DROP
// phase call this helper, so a resumed DROP quarantines exactly like an initial
// one. It returns false when execution should stop: a cancelled context leaves
// the state Stopped, a genuine failure transitions to StateFailed.
func (e *Engine) executeDropStatements(ctx context.Context, host, username, password, database string, drops []string) bool {
	for _, stmt := range drops {
		if err := e.executeDropStatement(ctx, host, username, password, database, stmt); err != nil {
			if ctx.Err() != nil {
				e.logger.Info("schema change stopped during DROP TABLE",
					"database", database,
					"reason", ctx.Err(),
				)
				return false
			}
			e.logger.Error("DROP TABLE phase failed", "database", database, "error", err)
			e.setMigrationFailed(fmt.Errorf("DROP TABLE phase failed: %w", err))
			return false
		}
	}
	return true
}

// executeDropStatement runs a single DROP TABLE statement, quarantining the
// table by default and dropping it directly only when pending drops is disabled.
func (e *Engine) executeDropStatement(ctx context.Context, host, username, password, database, stmt string) error {
	if e.disablePendingDrops {
		if err := e.executeSingleStatement(ctx, host, username, password, database, stmt); err != nil {
			return fmt.Errorf("drop table directly: %w", err)
		}
		return nil
	}
	if err := e.quarantineDroppedTables(ctx, host, username, password, database, stmt); err != nil {
		return fmt.Errorf("quarantine dropped table: %w", err)
	}
	return nil
}

// executeSingleStatement runs a single DDL statement through Spirit.
// Used for CREATE/DROP TABLE which Spirit requires to be individual statements.
func (e *Engine) executeSingleStatement(ctx context.Context, host, username, password, database, stmt string) error {
	parsed, err := statement.New(stmt)
	if err != nil {
		return fmt.Errorf("parse statement: %w", err)
	}
	if len(parsed) == 0 {
		return fmt.Errorf("no statement parsed")
	}

	// Determine statement type from parsed result
	stmtType := "DDL"
	if parsed[0].IsCreateTable() {
		stmtType = "CREATE TABLE"
	} else if parsed[0].IsAlterTable() {
		stmtType = "ALTER TABLE"
	}
	// Note: Spirit doesn't expose IsDropTable/IsRenameTable but handles them

	e.logger.Info("executing single DDL through Spirit",
		"database", database,
		"table", parsed[0].Table,
		"type", stmtType,
	)

	migration := &spiritmigration.Migration{
		Host:              host,
		Username:          username,
		Password:          &password,
		Database:          database,
		Statement:         stmt,
		TargetChunkTime:   e.targetChunkTime,
		Threads:           e.threads,
		LockWaitTimeout:   e.lockWaitTimeout,
		InterpolateParams: true,
	}

	runner, err := spiritmigration.NewRunner(migration)
	if err != nil {
		return fmt.Errorf("create runner: %w", err)
	}
	// Run opens the runner's connection pool and background routines; they are
	// only released by Close. Close on every return path so each single DDL
	// statement leaves no leaked connections or goroutines behind.
	defer utils.CloseAndLog(runner)

	if err := runner.Run(ctx); err != nil {
		return fmt.Errorf("run Spirit: %w", err)
	}

	return nil
}

// executeSpiritMigration runs Spirit for ALTER statements.
func (e *Engine) executeSpiritMigration(ctx context.Context, host, username, password, database, combinedStatement string, deferCutover bool) error {
	// Parse statements to extract table names
	parsed, err := statement.New(combinedStatement)
	if err != nil {
		e.logger.Error("failed to parse combined statement", "error", err)
		e.setMigrationFailed(fmt.Errorf("failed to parse combined statement: %w", err))
		return err
	}
	var tables []string
	var ddls []string
	for _, p := range parsed {
		tables = append(tables, p.Table)
		ddls = append(ddls, p.Statement)
	}

	migration := &spiritmigration.Migration{
		Host:              host,
		Username:          username,
		Password:          &password,
		Database:          database,
		Statement:         combinedStatement,
		TargetChunkTime:   e.targetChunkTime,
		Threads:           e.threads,
		LockWaitTimeout:   e.lockWaitTimeout,
		InterpolateParams: true,
		DeferCutOver:      deferCutover,
		RespectSentinel:   deferCutover, // Only wait for sentinel when deferring cutover
	}

	runner, err := spiritmigration.NewRunner(migration)
	if err != nil {
		e.logger.Error("failed to create Spirit runner",
			"error", err,
			"statement", combinedStatement,
		)
		e.setMigrationFailed(fmt.Errorf("failed to create Spirit runner: %w", err))
		return err
	}

	// Use spiritLogger which filters noisy debug logs unless DebugLogs is enabled
	spiritLogger := e.spiritLogger.With("database", database)
	runner.SetLogger(spiritLogger)

	// Track schema change state
	e.mu.Lock()
	if e.runningMigration != nil {
		e.runningMigration.tables = tables
		e.runningMigration.ddls = ddls
		e.runningMigration.combinedStatement = combinedStatement
		e.runningMigration.runners = []*spiritmigration.Runner{runner}
		e.runningMigration.progressCallback = func() string {
			return runner.Progress().Summary
		}
	}
	e.mu.Unlock()

	e.logger.Info("starting Spirit runner",
		"database", database,
		"tables", tables,
		"defer_cutover", deferCutover,
		"statement_len", len(combinedStatement),
	)

	// On every exit path the tracked state must be terminal before the runner
	// is closed: Close() flips Spirit's status to close while teardown is
	// still in flight, and a progress poll must observe the recorded outcome,
	// never infer one from a runner mid-teardown.
	if err := runner.Run(ctx); err != nil {
		// Check if this was a cancellation (stop) vs a real failure
		if ctx.Err() != nil {
			e.logger.Info("schema change stopped",
				"reason", ctx.Err(),
			)
			// Don't change state - Stop() already set it to StateStopped
			utils.CloseAndLog(runner)
			return nil
		}
		e.logger.Error("schema change failed",
			"error", err,
		)
		e.setMigrationFailed(fmt.Errorf("schema change failed: %w", err))
		utils.CloseAndLog(runner)
		return err
	}

	utils.CloseAndLog(runner)
	e.logger.Info("ALTER phase completed", "database", database, "tables", tables)
	return nil
}

func (e *Engine) setMigrationCompleted() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runningMigration != nil {
		e.runningMigration.state = engine.StateCompleted
	}
}

// setMigrationFailed sets the state to failed with an error message.
func (e *Engine) setMigrationFailed(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runningMigration != nil {
		e.runningMigration.state = engine.StateFailed
		if err != nil {
			e.runningMigration.errorMessage = err.Error()
		}
	}
}
