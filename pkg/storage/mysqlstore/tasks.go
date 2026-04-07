// tasks.go implements TaskStore for individual DDL operations within an apply.
// Each task represents one table's schema change.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// taskColumns lists all columns for SELECT queries.
const taskColumns = `id, task_identifier, apply_id, plan_id, database_name, database_type,
	namespace, table_name, ddl, ddl_action,
	engine, repository, pull_request, environment, state, error_message, options,
	rows_copied, rows_total, progress_percent, eta_seconds,
	is_instant, ready_to_complete, engine_migration_id,
	started_at, completed_at, created_at, updated_at`

// terminalTaskStatesSQL is formatted for SQL IN clause.
var terminalTaskStatesSQL = func() string {
	parts := make([]string, 0, len(state.TerminalTaskStates))
	for _, s := range state.TerminalTaskStates {
		parts = append(parts, "'"+s+"'")
	}
	return strings.Join(parts, ", ")
}()

// taskStore implements storage.TaskStore using MySQL.
type taskStore struct {
	db *sql.DB
}

// Create stores a new task.
func (s *taskStore) Create(ctx context.Context, task *storage.Task) (int64, error) {
	// Ensure options has valid JSON (empty object if nil)
	options := task.Options
	if len(options) == 0 {
		options = []byte("{}")
	}

	result, err := s.db.ExecContext(ctx, `
		INSERT INTO tasks (
			task_identifier, apply_id, plan_id, database_name, database_type,
			namespace, table_name, ddl, ddl_action,
			engine, repository, pull_request, environment, state, error_message, options,
			rows_copied, rows_total, progress_percent, eta_seconds,
			is_instant, ready_to_complete, engine_migration_id,
			started_at, completed_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		task.TaskIdentifier, task.ApplyID, task.PlanID, task.Database, task.DatabaseType,
		task.Namespace, nullString(task.TableName), nullString(task.DDL), nullString(task.DDLAction),
		task.Engine, task.Repository, task.PullRequest, task.Environment,
		task.State, nullString(task.ErrorMessage), string(options),
		task.RowsCopied, task.RowsTotal, task.ProgressPercent, task.ETASeconds,
		task.IsInstant, task.ReadyToComplete, nullString(task.EngineMigrationID),
		task.StartedAt, task.CompletedAt, task.CreatedAt, task.UpdatedAt,
	)
	if err != nil {
		return 0, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return id, nil
}

// Get returns a task by task_identifier (external identifier), or nil if not found.
func (s *taskStore) Get(ctx context.Context, taskIdentifier string) (*storage.Task, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+taskColumns+`
		FROM tasks
		WHERE task_identifier = ?
	`, taskIdentifier)

	return scanTask(row)
}

// Update updates an existing task.
func (s *taskStore) Update(ctx context.Context, task *storage.Task) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks SET
			state = ?, error_message = ?, options = ?,
			rows_copied = ?, rows_total = ?, progress_percent = ?, eta_seconds = ?,
			is_instant = ?, ready_to_complete = ?, engine_migration_id = ?,
			started_at = ?, completed_at = ?, updated_at = NOW()
		WHERE id = ?
	`, task.State, nullString(task.ErrorMessage), nullJSON(task.Options),
		task.RowsCopied, task.RowsTotal, task.ProgressPercent, task.ETASeconds,
		task.IsInstant, task.ReadyToComplete, nullString(task.EngineMigrationID),
		task.StartedAt, task.CompletedAt,
		task.ID)
	if err != nil {
		return err
	}

	return checkRowsAffected(result, storage.ErrTaskNotFound)
}

// GetByApplyID returns all tasks for an apply.
func (s *taskStore) GetByApplyID(ctx context.Context, applyID int64) ([]*storage.Task, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+taskColumns+`
		FROM tasks
		WHERE apply_id = ?
		ORDER BY created_at DESC
	`, applyID)
	if err != nil {
		return nil, fmt.Errorf("query tasks for apply %d: %w", applyID, err)
	}
	defer utils.CloseAndLog(rows)

	return scanTasks(rows)
}

// GetByDatabase returns all tasks for a database.
// Results are ordered by created_at DESC, then by id DESC as a tiebreaker
// (since created_at only has second precision).
func (s *taskStore) GetByDatabase(ctx context.Context, database string) ([]*storage.Task, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+taskColumns+`
		FROM tasks
		WHERE database_name = ?
		ORDER BY created_at DESC, id DESC
	`, database)
	if err != nil {
		return nil, fmt.Errorf("query tasks for database %s: %w", database, err)
	}
	defer utils.CloseAndLog(rows)

	return scanTasks(rows)
}

// GetActive returns all tasks in non-terminal states.
func (s *taskStore) GetActive(ctx context.Context) ([]*storage.Task, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+taskColumns+`
		FROM tasks
		WHERE state NOT IN (`+terminalTaskStatesSQL+`)
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	return scanTasks(rows)
}

// GetByPR returns all tasks for a repository and pull request.
func (s *taskStore) GetByPR(ctx context.Context, repo string, pr int) ([]*storage.Task, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+taskColumns+`
		FROM tasks
		WHERE repository = ? AND pull_request = ?
		ORDER BY created_at DESC
	`, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("query tasks for %s#%d: %w", repo, pr, err)
	}
	defer utils.CloseAndLog(rows)

	return scanTasks(rows)
}

// List returns tasks matching the filter criteria.
func (s *taskStore) List(ctx context.Context, filter storage.TaskFilter) ([]*storage.Task, error) {
	query := `
		SELECT ` + taskColumns + `
		FROM tasks
		WHERE 1=1`

	var args []any

	if filter.Repository != "" {
		query += " AND repository = ?"
		args = append(args, filter.Repository)

		if filter.PullRequest > 0 {
			query += " AND pull_request = ?"
			args = append(args, filter.PullRequest)
		}
	}

	if !filter.IncludeCompleted {
		query += " AND state NOT IN (" + terminalTaskStatesSQL + ")"
	}

	if !filter.Since.IsZero() {
		query += " AND started_at >= ?"
		args = append(args, filter.Since)
	}

	query += " ORDER BY created_at DESC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	return scanTasks(rows)
}

// scanTask scans a single task row, returning nil if not found.
func scanTask(row *sql.Row) (*storage.Task, error) {
	task, err := scanTaskInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return task, err
}

// scanTasks scans multiple task rows.
func scanTasks(rows *sql.Rows) ([]*storage.Task, error) {
	var tasks []*storage.Task
	for rows.Next() {
		task, err := scanTaskInto(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

// scanTaskInto scans task data from a scanner (works with both *sql.Row and *sql.Rows).
func scanTaskInto(s scanner) (*storage.Task, error) {
	var task storage.Task
	var tableName, ddl, ddlAction, errorMsg, engineMigrationID sql.NullString
	var options []byte
	var etaSeconds sql.NullInt64
	var startedAt, completedAt sql.NullTime

	err := s.Scan(
		&task.ID,
		&task.TaskIdentifier,
		&task.ApplyID,
		&task.PlanID,
		&task.Database,
		&task.DatabaseType,
		&task.Namespace,
		&tableName,
		&ddl,
		&ddlAction,
		&task.Engine,
		&task.Repository,
		&task.PullRequest,
		&task.Environment,
		&task.State,
		&errorMsg,
		&options,
		&task.RowsCopied,
		&task.RowsTotal,
		&task.ProgressPercent,
		&etaSeconds,
		&task.IsInstant,
		&task.ReadyToComplete,
		&engineMigrationID,
		&startedAt,
		&completedAt,
		&task.CreatedAt,
		&task.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	task.TableName = tableName.String
	task.DDL = ddl.String
	task.DDLAction = ddlAction.String
	task.ErrorMessage = errorMsg.String
	task.Options = options
	task.ETASeconds = int(etaSeconds.Int64)
	task.EngineMigrationID = engineMigrationID.String
	task.State = state.NormalizeTaskStatus(task.State)
	if startedAt.Valid {
		task.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		task.CompletedAt = &completedAt.Time
	}

	return &task, nil
}
