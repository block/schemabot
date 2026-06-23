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
const taskColumns = `id, task_identifier, apply_id, apply_operation_id, plan_id, database_name, database_type,
	namespace, table_name, shard, ddl, ddl_action,
	engine, repository, pull_request, environment, state, error_message, options, attempt,
	rows_copied, rows_total, progress_percent, eta_seconds, cutover_attempts,
	is_instant, ready_to_complete, engine_migration_id,
	started_at, completed_at, created_at, updated_at`

func prefixedTaskColumns(alias string) string {
	parts := strings.Split(taskColumns, ",")
	for i, part := range parts {
		parts[i] = alias + "." + strings.TrimSpace(part)
	}
	return strings.Join(parts, ", ")
}

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

type taskInserter interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Create stores a new task.
func (s *taskStore) Create(ctx context.Context, task *storage.Task) (int64, error) {
	return insertTask(ctx, s.db, task)
}

func insertTask(ctx context.Context, execer taskInserter, task *storage.Task) (int64, error) {
	// Ensure options has valid JSON (empty object if nil)
	options := task.Options
	if len(options) == 0 {
		options = []byte("{}")
	}

	result, err := execer.ExecContext(ctx, `
		INSERT INTO tasks (
			task_identifier, apply_id, apply_operation_id, plan_id, database_name, database_type,
			namespace, table_name, shard, ddl, ddl_action,
			engine, repository, pull_request, environment, state, error_message, options, attempt,
			rows_copied, rows_total, progress_percent, eta_seconds, cutover_attempts,
			is_instant, ready_to_complete, engine_migration_id,
			started_at, completed_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		task.TaskIdentifier, task.ApplyID, nullInt64Ptr(task.ApplyOperationID), task.PlanID, task.Database, task.DatabaseType,
		task.Namespace, nullString(task.TableName), task.Shard, nullString(task.DDL), nullString(task.DDLAction),
		task.Engine, task.Repository, task.PullRequest, task.Environment,
		task.State, nullString(task.ErrorMessage), string(options), task.Attempt,
		task.RowsCopied, task.RowsTotal, task.ProgressPercent, task.ETASeconds, task.CutoverAttempts,
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
//
// The write is guarded by whichever lease is on the context: an operation lease
// takes precedence over the parent apply lease so the operator can move to
// operation-scoped writes while callers that have not adopted operation leases
// keep falling back to the apply lease. An operation lease scopes the write to
// the task's own operation; the apply lease scopes it to the parent apply.
func (s *taskStore) Update(ctx context.Context, task *storage.Task) error {
	args := []any{
		task.State, nullString(task.ErrorMessage), nullJSON(task.Options), task.Attempt,
		task.RowsCopied, task.RowsTotal, task.ProgressPercent, task.ETASeconds, task.CutoverAttempts,
		task.IsInstant, task.ReadyToComplete, nullString(task.EngineMigrationID),
		task.StartedAt, task.CompletedAt,
		task.ID,
	}

	leasePredicate := ""
	var verifyLeaseStillOwned func() error
	if opLease, ok := storage.OperationLeaseFromContext(ctx); ok {
		if !opLease.Valid() {
			return fmt.Errorf("invalid operation lease for task %d: %w", task.ID, storage.ErrApplyLeaseLost)
		}
		leasePredicate = `
			AND tasks.apply_operation_id = ?
			AND EXISTS (
				SELECT 1 FROM apply_operations ao
				WHERE ao.id = ? AND ao.lease_token = ?
			)`
		args = append(args, opLease.OperationID, opLease.OperationID, opLease.Token)
		verifyLeaseStillOwned = func() error { return ensureOperationLeaseStillOwned(ctx, s.db, opLease) }
	} else if lease, hasLease, err := applyLeaseFromContext(ctx, task.ApplyID); err != nil {
		return err
	} else if hasLease {
		leasePredicate = `
			AND EXISTS (
				SELECT 1 FROM applies a
				WHERE a.id = tasks.apply_id AND a.lease_token = ?
			)`
		args = append(args, lease.Token)
		verifyLeaseStillOwned = func() error { return ensureApplyLeaseStillOwned(ctx, s.db, lease) }
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks SET
			state = ?, error_message = ?, options = ?, attempt = ?,
			rows_copied = ?, rows_total = ?, progress_percent = ?, eta_seconds = ?, cutover_attempts = ?,
			is_instant = ?, ready_to_complete = ?, engine_migration_id = ?,
			started_at = ?, completed_at = ?, updated_at = NOW()
		WHERE id = ?`+leasePredicate+`
	`, args...)
	if err != nil {
		return err
	}
	if verifyLeaseStillOwned == nil {
		return nil
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read task update rows affected for task %d: %w", task.ID, err)
	}
	if rows == 0 {
		return verifyLeaseStillOwned()
	}
	return nil
}

// UpsertShardProgress creates or updates the per-shard task row for
// (apply_operation_id, namespace, table_name, shard). It is the operator's
// write-through for reflected per-shard progress (e.g. PlanetScale shards from
// SHOW VITESS_MIGRATIONS).
//
// It requires the operation lease on the context: the single lease-holding
// operator is the only writer of an operation's per-shard rows, so the
// lookup-then-write is serialized by that lease without a database-level unique
// constraint. The insert is gated on the lease so a displaced operator that lost
// the lease fails closed (ErrApplyLeaseLost) instead of writing stale rows, and
// the update path reuses the lease-guarded Update. On conflict only the progress
// fields change; identity and DDL are preserved.
func (s *taskStore) UpsertShardProgress(ctx context.Context, task *storage.Task) error {
	opLease, ok := storage.OperationLeaseFromContext(ctx)
	if !ok {
		return fmt.Errorf("upsert shard progress for %s.%s shard %q requires an operation lease", task.Namespace, task.TableName, task.Shard)
	}
	if !opLease.Valid() {
		return fmt.Errorf("invalid operation lease for shard progress %s.%s shard %q: %w", task.Namespace, task.TableName, task.Shard, storage.ErrApplyLeaseLost)
	}
	if task.ApplyOperationID == nil {
		return fmt.Errorf("upsert shard progress for %s.%s shard %q requires apply_operation_id", task.Namespace, task.TableName, task.Shard)
	}
	// Fail closed if the row targets a different operation than the held lease:
	// the insert is gated on the leased operation, so without this a caller could
	// write a row pointing at another apply_operation under this lease.
	if *task.ApplyOperationID != opLease.OperationID {
		return fmt.Errorf("upsert shard progress targets operation %d but the held lease is for operation %d", *task.ApplyOperationID, opLease.OperationID)
	}
	// A per-shard row must identify its table and shard. An empty table_name
	// would store NULL and never match the lookup (re-inserting every pass), and
	// an empty shard would collide with the unsharded single-shard sentinel.
	if task.TableName == "" {
		return fmt.Errorf("upsert shard progress for operation %d shard %q requires a table name", *task.ApplyOperationID, task.Shard)
	}
	if task.Shard == "" {
		return fmt.Errorf("upsert shard progress for operation %d table %q requires a non-empty shard", *task.ApplyOperationID, task.TableName)
	}

	// Find the existing per-shard row under this operation. The lookup-then-write
	// is safe without a unique constraint because the operation lease makes the
	// caller the single writer of this operation's rows.
	var id int64
	err := s.db.QueryRowContext(ctx, `
		SELECT id FROM tasks
		WHERE apply_operation_id = ? AND namespace = ? AND table_name = ? AND shard = ?
	`, *task.ApplyOperationID, task.Namespace, nullString(task.TableName), task.Shard).Scan(&id)
	switch {
	case err == nil:
		// Existing shard row: update its progress fields under the lease guard.
		task.ID = id
		return s.Update(ctx, task)
	case errors.Is(err, sql.ErrNoRows):
		// New shard row: insert gated on the lease so a lost lease fails closed.
		return s.insertShardTaskGuarded(ctx, task, opLease)
	default:
		return fmt.Errorf("look up shard task for operation %d %s.%s shard %q: %w",
			*task.ApplyOperationID, task.Namespace, task.TableName, task.Shard, err)
	}
}

// insertShardTaskGuarded inserts a new per-shard task row only while the
// operation lease is still current. The INSERT ... SELECT ... WHERE the
// operation's lease_token matches means a displaced operator inserts zero rows
// and fails closed rather than writing a stale shard row.
func (s *taskStore) insertShardTaskGuarded(ctx context.Context, task *storage.Task, opLease storage.OperationLease) error {
	options := task.Options
	if len(options) == 0 {
		options = []byte("{}")
	}
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO tasks (
			task_identifier, apply_id, apply_operation_id, plan_id, database_name, database_type,
			namespace, table_name, shard, ddl, ddl_action,
			engine, repository, pull_request, environment, state, error_message, options, attempt,
			rows_copied, rows_total, progress_percent, eta_seconds, cutover_attempts,
			is_instant, ready_to_complete, engine_migration_id,
			started_at, completed_at, created_at, updated_at
		)
		SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		FROM apply_operations ao
		WHERE ao.id = ? AND ao.lease_token = ?
	`,
		task.TaskIdentifier, task.ApplyID, nullInt64Ptr(task.ApplyOperationID), task.PlanID, task.Database, task.DatabaseType,
		task.Namespace, nullString(task.TableName), task.Shard, nullString(task.DDL), nullString(task.DDLAction),
		task.Engine, task.Repository, task.PullRequest, task.Environment,
		task.State, nullString(task.ErrorMessage), string(options), task.Attempt,
		task.RowsCopied, task.RowsTotal, task.ProgressPercent, task.ETASeconds, task.CutoverAttempts,
		task.IsInstant, task.ReadyToComplete, nullString(task.EngineMigrationID),
		task.StartedAt, task.CompletedAt, task.CreatedAt, task.UpdatedAt,
		opLease.OperationID, opLease.Token,
	)
	if err != nil {
		return fmt.Errorf("insert shard task for operation %d %s.%s shard %q: %w",
			opLease.OperationID, task.Namespace, task.TableName, task.Shard, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read shard task insert rows affected for operation %d: %w", opLease.OperationID, err)
	}
	if rows == 0 {
		// Zero rows inserted means the operation lease is no longer current.
		return ensureOperationLeaseStillOwned(ctx, s.db, opLease)
	}
	if newID, err := result.LastInsertId(); err == nil {
		task.ID = newID
	}
	return nil
}

// GetByApplyID returns the per-table tasks for an apply. Per-shard detail rows
// (shard != "") are excluded: they are a reflected read-model written by the
// operator and must not re-enter the per-table drive/gating/progress pipeline on
// reload. Read those via GetShardProgressByApplyOperationID.
func (s *taskStore) GetByApplyID(ctx context.Context, applyID int64) ([]*storage.Task, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+taskColumns+`
		FROM tasks
		WHERE apply_id = ? AND shard = ''
		ORDER BY created_at DESC
	`, applyID)
	if err != nil {
		return nil, fmt.Errorf("query tasks for apply %d: %w", applyID, err)
	}
	defer utils.CloseAndLog(rows)

	return scanTasks(rows)
}

// GetByApplyOperationID returns the drive tasks for a single apply_operation.
// Unsharded operations load their per-table rows (shard = ""). Sharded work
// operations load the row whose namespace/shard/table matches the operation key,
// so TargetShards can be rebuilt from storage while reflected per-shard progress
// rows for unsharded operations stay out of the drive pipeline.
func (s *taskStore) GetByApplyOperationID(ctx context.Context, applyOperationID int64) ([]*storage.Task, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+prefixedTaskColumns("t")+`
		FROM tasks t
		JOIN apply_operations ao ON ao.id = t.apply_operation_id
		WHERE t.apply_operation_id = ?
			AND (
				t.shard = ''
				OR (
					ao.operation_kind = ?
					-- Keep this in sync with shardOperationKey's namespace/shard/table format.
					AND ao.operation_key = CONCAT(t.namespace, '/', t.shard, '/', t.table_name)
				)
			)
		ORDER BY t.created_at DESC, t.id DESC
	`, applyOperationID, storage.ApplyOperationKindWork)
	if err != nil {
		return nil, fmt.Errorf("query tasks for apply_operation %d: %w", applyOperationID, err)
	}
	defer utils.CloseAndLog(rows)

	tasks, err := scanTasks(rows)
	if err != nil {
		return nil, err
	}
	if tasks == nil {
		// Return a non-nil empty slice so callers can never confuse "operation
		// has no tasks" with nil and fall back to the parent apply's tasks.
		return []*storage.Task{}, nil
	}
	return tasks, nil
}

// GetShardProgressByApplyOperationID returns the per-shard task rows
// (shard != "") for an operation, ordered by namespace, table_name, shard. It is
// the read companion to UpsertShardProgress: the per-table loaders exclude these
// rows, so this is how the renderer (and tests) read the per-shard breakdown
// without the rows re-entering the per-table pipeline.
func (s *taskStore) GetShardProgressByApplyOperationID(ctx context.Context, applyOperationID int64) ([]*storage.Task, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+taskColumns+`
		FROM tasks
		WHERE apply_operation_id = ? AND shard != ''
		ORDER BY namespace, table_name, shard
	`, applyOperationID)
	if err != nil {
		return nil, fmt.Errorf("query shard progress tasks for apply_operation %d: %w", applyOperationID, err)
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
	var applyOperationID, etaSeconds sql.NullInt64
	var startedAt, completedAt sql.NullTime

	err := s.Scan(
		&task.ID,
		&task.TaskIdentifier,
		&task.ApplyID,
		&applyOperationID,
		&task.PlanID,
		&task.Database,
		&task.DatabaseType,
		&task.Namespace,
		&tableName,
		&task.Shard,
		&ddl,
		&ddlAction,
		&task.Engine,
		&task.Repository,
		&task.PullRequest,
		&task.Environment,
		&task.State,
		&errorMsg,
		&options,
		&task.Attempt,
		&task.RowsCopied,
		&task.RowsTotal,
		&task.ProgressPercent,
		&etaSeconds,
		&task.CutoverAttempts,
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
	if applyOperationID.Valid {
		v := applyOperationID.Int64
		task.ApplyOperationID = &v
	}
	if startedAt.Valid {
		task.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		task.CompletedAt = &completedAt.Time
	}

	return &task, nil
}
