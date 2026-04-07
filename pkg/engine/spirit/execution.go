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
	// Categorize DDL statements using AST-based classification
	// Spirit requires non-ALTER to be individual
	var createStatements []string
	var dropStatements []string
	var alterStatements []string

	for _, stmt := range ddlStatements {
		op, _, err := ddl.ClassifyStatementAST(stmt)
		if err != nil {
			e.logger.Error("failed to classify statement", "error", err, "statement", stmt)
			e.setMigrationFailed(err)
			return
		}
		switch op {
		case "create":
			createStatements = append(createStatements, stmt)
		case "drop":
			dropStatements = append(dropStatements, stmt)
		default:
			// ALTER TABLE, RENAME TABLE, and unknown go here
			alterStatements = append(alterStatements, stmt)
		}
	}

	// Execute CREATE TABLE statements first (each individually through Spirit)
	for _, stmt := range createStatements {
		if err := e.executeSingleStatement(ctx, host, username, password, database, stmt); err != nil {
			e.logger.Error("CREATE TABLE failed", "error", err)
			e.setMigrationFailed(fmt.Errorf("CREATE TABLE failed: %w", err))
			return
		}
	}

	// Execute ALTER statements (can be combined for atomic execution)
	if len(alterStatements) > 0 {
		combinedStatement := strings.Join(alterStatements, "; ")

		// Parse statements to extract table names for logging
		var tables []string
		for _, stmt := range alterStatements {
			parsed, err := statement.New(stmt)
			if err != nil {
				e.logger.Error("failed to parse statement for logging", "error", err, "statement", stmt)
				e.setMigrationFailed(fmt.Errorf("failed to parse statement: %w", err))
				return
			}
			if len(parsed) > 0 {
				tables = append(tables, parsed[0].Table)
			}
		}

		e.logger.Info("executing ALTER via Spirit",
			"database", database,
			"tables", tables,
			"ddl_count", len(alterStatements),
			"defer_cutover", deferCutover,
		)

		if err := e.executeSpiritMigration(ctx, host, username, password, database, combinedStatement, deferCutover); err != nil {
			return // Error already logged and state set
		}
	}

	// Execute DROP TABLE statements last (each individually through Spirit)
	for _, stmt := range dropStatements {
		if err := e.executeSingleStatement(ctx, host, username, password, database, stmt); err != nil {
			e.logger.Error("DROP TABLE failed", "error", err)
			e.setMigrationFailed(fmt.Errorf("DROP TABLE failed: %w", err))
			return
		}
	}

	// If we had no ALTER statements (only CREATE/DROP), mark as completed
	if len(alterStatements) == 0 {
		e.setMigrationState(engine.StateCompleted)
	}
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

	if err := runner.Run(ctx); err != nil {
		// Check if this was a cancellation (stop) vs a real failure
		if ctx.Err() != nil {
			e.logger.Info("schema change stopped",
				"reason", ctx.Err(),
			)
			utils.CloseAndLog(runner)
			// Don't change state - Stop() already set it to StateStopped
			return nil
		}
		utils.CloseAndLog(runner)
		e.logger.Error("schema change failed",
			"error", err,
		)
		e.setMigrationFailed(fmt.Errorf("schema change failed: %w", err))
		return err
	}

	utils.CloseAndLog(runner)
	e.setMigrationState(engine.StateCompleted)
	e.logger.Info("schema change completed", "database", database, "tables", tables)
	return nil
}

func (e *Engine) setMigrationState(state engine.State) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runningMigration != nil {
		e.runningMigration.state = state
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
