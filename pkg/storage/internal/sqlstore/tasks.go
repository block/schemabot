// tasks.go implements TaskStore for individual DDL operations within an apply.
// Each task represents one table's schema change.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// taskColumns lists all columns for SELECT queries.
const taskColumns = `id, task_identifier, apply_id, apply_operation_id, plan_id, database_name, database_type,
	namespace, table_name, shard, ddl, ddl_action,
	engine, repository, pull_request, environment, state, error_message, options, attempt,
	rows_copied, rows_total, progress_percent, eta_seconds, checksum_rows_checked, checksum_rows_total, throttled, throttle_reason, cutover_attempts,
	is_instant, engine_migration_id,
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
	db       *rebindDB
	dialect  Dialect
	identity identityInserter
	locker   namedlock.Locker
}

func canonicalizeTaskIdentity(task *storage.Task) {
	task.Database = storage.CanonicalKey(task.Database)
	task.DatabaseType = storage.CanonicalKey(task.DatabaseType)
	task.Repository = storage.CanonicalKey(task.Repository)
	task.Environment = storage.CanonicalKey(task.Environment)
}

// Create stores a new task.
func (s *taskStore) Create(ctx context.Context, task *storage.Task) (int64, error) {
	return insertTask(ctx, s.db, s.identity, task)
}

func insertTask(ctx context.Context, exec queryExecer, identity identityInserter, task *storage.Task) (int64, error) {
	canonicalizeTaskIdentity(task)

	// Ensure options has valid JSON (empty object if nil)
	options := task.Options
	if len(options) == 0 {
		options = []byte("{}")
	}

	id, err := identity.InsertID(ctx, exec, `
		INSERT INTO tasks (
			task_identifier, apply_id, apply_operation_id, plan_id, database_name, database_type,
			namespace, table_name, shard, ddl, ddl_action,
			engine, repository, pull_request, environment, state, error_message, options, attempt,
			rows_copied, rows_total, progress_percent, eta_seconds, checksum_rows_checked, checksum_rows_total, throttled, throttle_reason, cutover_attempts,
			is_instant, engine_migration_id,
			started_at, completed_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		task.TaskIdentifier, task.ApplyID, nullInt64Ptr(task.ApplyOperationID), task.PlanID, task.Database, task.DatabaseType,
		task.Namespace, nullString(task.TableName), task.Shard, nullString(task.DDL), nullString(task.DDLAction),
		task.Engine, task.Repository, task.PullRequest, task.Environment,
		task.State, nullString(task.ErrorMessage), string(options), task.Attempt,
		task.RowsCopied, task.RowsTotal, task.ProgressPercent, task.ETASeconds, task.ChecksumRowsChecked, task.ChecksumRowsTotal, task.Throttled, task.ThrottleReason, task.CutoverAttempts,
		task.IsInstant, nullString(task.EngineMigrationID),
		task.StartedAt, task.CompletedAt, task.CreatedAt, task.UpdatedAt,
	)
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
		task.RowsCopied, task.RowsTotal, task.ProgressPercent, task.ETASeconds, task.ChecksumRowsChecked, task.ChecksumRowsTotal, task.Throttled, task.ThrottleReason, task.CutoverAttempts,
		task.IsInstant, nullString(task.EngineMigrationID),
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
			rows_copied = ?, rows_total = ?, progress_percent = ?, eta_seconds = ?, checksum_rows_checked = ?, checksum_rows_total = ?, throttled = ?, throttle_reason = ?, cutover_attempts = ?,
			is_instant = ?, engine_migration_id = ?,
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
// It requires the drive's ownership lease on the context. A multi-operation
// fan-out drive holds the operation lease; a single-operation (whole-apply)
// drive holds only the apply lease. Either is a sufficient single-writer
// guarantee for this operation's per-shard rows, so accept both, preferring the
// operation lease when present (it is the narrower claim). The lookup-then-write
// is serialized by that lease without a database-level unique constraint, and
// the insert is gated on the matching lease token so a displaced operator fails
// closed (ErrApplyLeaseLost) instead of writing stale rows. The update path
// reuses the lease-guarded Update, which applies the same lease precedence. On
// conflict only the progress fields change; identity and DDL are preserved.
func (s *taskStore) UpsertShardProgress(ctx context.Context, task *storage.Task) error {
	canonicalizeTaskIdentity(task)

	if task.ApplyOperationID == nil {
		return fmt.Errorf("upsert shard progress for %s.%s shard %q requires apply_operation_id", task.Namespace, task.TableName, task.Shard)
	}

	opLease, hasOpLease := storage.OperationLeaseFromContext(ctx)
	if hasOpLease {
		if !opLease.Valid() {
			return fmt.Errorf("invalid operation lease for shard progress %s.%s shard %q: %w", task.Namespace, task.TableName, task.Shard, storage.ErrApplyLeaseLost)
		}
		// Fail closed if the row targets a different operation than the held lease:
		// the insert is gated on the leased operation, so without this a caller could
		// write a row pointing at another apply_operation under this lease.
		if *task.ApplyOperationID != opLease.OperationID {
			return fmt.Errorf("upsert shard progress targets operation %d but the held lease is for operation %d", *task.ApplyOperationID, opLease.OperationID)
		}
	}
	applyLease, hasApplyLease, err := applyLeaseFromContext(ctx, task.ApplyID)
	if err != nil {
		return err
	}
	if !hasOpLease && !hasApplyLease {
		return fmt.Errorf("upsert shard progress for %s.%s shard %q requires an operation or apply lease", task.Namespace, task.TableName, task.Shard)
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
	// is safe without a unique constraint because the held lease makes the caller
	// the single writer of this operation's rows.
	var id int64
	err = s.db.QueryRowContext(ctx, `
		SELECT id FROM tasks
		WHERE apply_operation_id = ? AND namespace = ? AND table_name = ? AND shard = ?
	`, *task.ApplyOperationID, task.Namespace, nullString(task.TableName), task.Shard).Scan(&id)
	switch {
	case err == nil:
		// Existing shard row: update its progress fields under the lease guard.
		task.ID = id
		return s.Update(ctx, task)
	case errors.Is(err, sql.ErrNoRows):
		// New shard row: verify the operation belongs to the task's apply before
		// inserting. tasks has no foreign-key constraints, so neither the
		// operation-lease guard (which only matches the operation token) nor the
		// apply-lease guard (which only matches the apply token) would otherwise
		// catch an inconsistent (apply_id, apply_operation_id) pair, which would
		// corrupt the per-operation read-model. Fail closed on mismatch.
		if err := s.verifyOperationBelongsToApply(ctx, *task.ApplyOperationID, task.ApplyID); err != nil {
			return err
		}
		// Insert gated on the held lease so a displaced operator fails closed.
		if hasOpLease {
			return s.insertShardTaskGuarded(ctx, task, opLease)
		}
		return s.insertShardTaskGuardedByApply(ctx, task, applyLease)
	default:
		return fmt.Errorf("look up shard task for operation %d %s.%s shard %q: %w",
			*task.ApplyOperationID, task.Namespace, task.TableName, task.Shard, err)
	}
}

// verifyOperationBelongsToApply fails closed when apply_operation operationID
// does not belong to applyID. tasks has no foreign-key constraints, so a
// per-shard insert with an inconsistent (apply_id, apply_operation_id) pair
// would silently corrupt the per-operation read-model; this guard rejects it
// before the insert with an explicit error rather than a silent no-op.
func (s *taskStore) verifyOperationBelongsToApply(ctx context.Context, operationID, applyID int64) error {
	var opApplyID int64
	err := s.db.QueryRowContext(ctx, `SELECT apply_id FROM apply_operations WHERE id = ?`, operationID).Scan(&opApplyID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("shard progress references apply_operation %d that does not exist", operationID)
	case err != nil:
		return fmt.Errorf("verify apply_operation %d belongs to apply %d: %w", operationID, applyID, err)
	case opApplyID != applyID:
		return fmt.Errorf("shard progress apply_operation %d belongs to apply %d, not apply %d", operationID, opApplyID, applyID)
	default:
		return nil
	}
}

// shardTaskInsertColumns is the column list shared by the lease-guarded shard
// inserts. Keep its order in sync with shardTaskInsertValues.
const shardTaskInsertColumns = `
	task_identifier, apply_id, apply_operation_id, plan_id, database_name, database_type,
	namespace, table_name, shard, ddl, ddl_action,
	engine, repository, pull_request, environment, state, error_message, options, attempt,
	rows_copied, rows_total, progress_percent, eta_seconds, checksum_rows_checked, checksum_rows_total, throttled, throttle_reason, cutover_attempts,
	is_instant, engine_migration_id,
	started_at, completed_at, created_at, updated_at`

// shardTaskInsertValues returns the placeholder list and value args for a
// per-shard task INSERT ... SELECT, matching shardTaskInsertColumns. The caller
// appends its own lease-guard ("FROM <lease table> WHERE ... lease_token = ?")
// and the guard's args.
func shardTaskInsertValues(task *storage.Task) (string, []any) {
	options := task.Options
	if len(options) == 0 {
		options = []byte("{}")
	}
	return `?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?`,
		[]any{
			task.TaskIdentifier, task.ApplyID, nullInt64Ptr(task.ApplyOperationID), task.PlanID, task.Database, task.DatabaseType,
			task.Namespace, nullString(task.TableName), task.Shard, nullString(task.DDL), nullString(task.DDLAction),
			task.Engine, task.Repository, task.PullRequest, task.Environment,
			task.State, nullString(task.ErrorMessage), string(options), task.Attempt,
			task.RowsCopied, task.RowsTotal, task.ProgressPercent, task.ETASeconds, task.ChecksumRowsChecked, task.ChecksumRowsTotal, task.Throttled, task.ThrottleReason, task.CutoverAttempts,
			task.IsInstant, nullString(task.EngineMigrationID),
			task.StartedAt, task.CompletedAt, task.CreatedAt, task.UpdatedAt,
		}
}

// insertShardTaskGuarded inserts a new per-shard task row only while the
// operation lease is still current. The INSERT ... SELECT ... WHERE the
// operation's lease_token matches means a displaced operator inserts zero rows
// and fails closed rather than writing a stale shard row.
func (s *taskStore) insertShardTaskGuarded(ctx context.Context, task *storage.Task, opLease storage.OperationLease) error {
	values, args := shardTaskInsertValues(task)
	args = append(args, opLease.OperationID, opLease.Token)
	id, inserted, err := s.identity.InsertGuardedID(ctx, s.db, `
		INSERT INTO tasks (`+shardTaskInsertColumns+`)
		SELECT `+values+`
		FROM apply_operations ao
		WHERE ao.id = ? AND ao.lease_token = ?
	`, args...)
	if err != nil {
		return fmt.Errorf("insert shard task for operation %d %s.%s shard %q: %w",
			opLease.OperationID, task.Namespace, task.TableName, task.Shard, err)
	}
	if !inserted {
		// Zero rows inserted means the operation lease is no longer current.
		return ensureOperationLeaseStillOwned(ctx, s.db, opLease)
	}
	task.ID = id
	return nil
}

// insertShardTaskGuardedByApply is the whole-apply (single-operation drive)
// companion to insertShardTaskGuarded: that drive holds the apply lease rather
// than an operation lease, and that lease is the single-writer guarantee. The
// INSERT ... SELECT ... WHERE the apply's lease_token matches means a displaced
// operator inserts zero rows and fails closed rather than writing a stale row.
func (s *taskStore) insertShardTaskGuardedByApply(ctx context.Context, task *storage.Task, lease storage.ApplyLease) error {
	values, args := shardTaskInsertValues(task)
	args = append(args, lease.ApplyID, lease.Token)
	id, inserted, err := s.identity.InsertGuardedID(ctx, s.db, `
		INSERT INTO tasks (`+shardTaskInsertColumns+`)
		SELECT `+values+`
		FROM applies a
		WHERE a.id = ? AND a.lease_token = ?
	`, args...)
	if err != nil {
		return fmt.Errorf("insert shard task for apply %d %s.%s shard %q: %w",
			lease.ApplyID, task.Namespace, task.TableName, task.Shard, err)
	}
	if !inserted {
		// Zero rows inserted means the apply lease is no longer current.
		return ensureApplyLeaseStillOwned(ctx, s.db, lease)
	}
	task.ID = id
	return nil
}

// GetByApplyID returns the drive tasks for an apply. Unsharded rows (shard = "")
// always load. A shard-tagged row loads only when it is the drive task of a
// shard-scoped work operation — its operation belongs to the same apply and its
// operation's key matches the row's namespace/shard/table — the same
// discrimination GetByApplyOperationID applies. The join is constrained to the
// row's own apply so a mis-associated operation reference from another apply
// can never classify a row as drive work; tasks has no foreign-key constraint
// enforcing the association.
// Reflected per-shard progress rows (a read-model written by the operator under
// an operation whose key does not match) are excluded so they never re-enter the
// per-table drive/gating/progress pipeline on reload. Read those via
// GetShardProgressByApplyOperationID.
//
// Rows are returned in creation order — the plan's statement order — with the
// id tiebreaker since created_at has second precision. Apply-level drives
// (ResumeApply) execute tasks sequentially in the order this loader returns
// them, so the ordering carries the same stop-on-failure contract as
// GetByApplyOperationID.
func (s *taskStore) GetByApplyID(ctx context.Context, applyID int64) ([]*storage.Task, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+prefixedTaskColumns("t")+`
		FROM tasks t
		LEFT JOIN apply_operations ao
			ON ao.id = t.apply_operation_id
			AND ao.apply_id = t.apply_id
		WHERE t.apply_id = ?
			AND (
				t.shard = ''
				OR (
					ao.operation_kind = ?
					-- Keep this in sync with storage.ShardOperationKey's namespace/shard/table
					-- format. table_name is stored as NULL when empty, and MySQL's CONCAT
					-- returns NULL when any operand is NULL (Postgres's concat() ignores
					-- NULLs), so COALESCE keeps the key equal to the Go construction on
					-- both dialects.
					AND ao.operation_key = CONCAT(t.namespace, '/', t.shard, '/', COALESCE(t.table_name, ''))
				)
			)
		ORDER BY t.created_at, t.id
	`, applyID, storage.ApplyOperationKindWork)
	if err != nil {
		return nil, fmt.Errorf("query tasks for apply %d: %w", applyID, err)
	}
	defer utils.CloseAndLog(rows)

	return scanTasks(rows)
}

// CountByApplyID returns the number of task rows the apply owns, with no shard
// or operation filtering — the same "owns any task work" predicate the
// operator's claim gate uses, so drives can tell a genuinely task-less apply
// from one whose rows a filtered loader did not return.
func (s *taskStore) CountByApplyID(ctx context.Context, applyID int64) (int64, error) {
	var count int64
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM tasks
		WHERE apply_id = ?
	`, applyID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count task rows for apply %d: %w", applyID, err)
	}
	return count, nil
}

// GetByApplyOperationID returns the drive tasks for a single apply_operation.
// Unsharded operations load their per-table rows (shard = ""). Sharded work
// operations load the row whose namespace/shard/table matches the operation key,
// so TargetShards can be rebuilt from storage while reflected per-shard progress
// rows for unsharded operations stay out of the drive pipeline.
//
// Rows are returned in creation order — the plan's statement order — because
// the sequential drive executes tasks in the order this loader returns them.
// The planner orders destructive statements last (DROPs after CREATEs and
// ALTERs), and stop-on-failure semantics assume top-to-bottom execution:
// everything before a failing statement ran, everything after it did not.
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
					-- Keep this in sync with storage.ShardOperationKey's namespace/shard/table
					-- format. table_name is stored as NULL when empty, and MySQL's CONCAT
					-- returns NULL when any operand is NULL (Postgres's concat() ignores
					-- NULLs), so COALESCE keeps the key equal to the Go construction on
					-- both dialects.
					AND ao.operation_key = CONCAT(t.namespace, '/', t.shard, '/', COALESCE(t.table_name, ''))
				)
			)
		ORDER BY t.created_at, t.id
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
	database = storage.CanonicalKey(database)

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
	repo = storage.CanonicalKey(repo)

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

// tableOwnerLookupLimit bounds how many pull requests one ownership lookup
// returns. A long-lived table accumulates an owner for every pull request that
// ever changed it, and the caller resolves each one's state against GitHub
// until it finds an open one — so an unbounded result turns a routine plan into
// a serial walk of the table's entire history. Owners come back most recent
// first, and a pull request old enough to fall outside this window has long
// since been merged or closed, so the bound costs no attribution in practice.
const tableOwnerLookupLimit = 20

// FindTableOwners returns the pull requests stored tasks attribute a table
// to. The aggregate runs in SQL rather than over a loaded task list because it
// is on the plan path: one query per table the plan would drop, served by the
// tasks index on (database_name, database_type, environment, table_name).
func (s *taskStore) FindTableOwners(ctx context.Context, ref storage.TableRef) ([]storage.TableOwner, error) {
	ref.Database = storage.CanonicalKey(ref.Database)
	ref.DatabaseType = storage.CanonicalKey(ref.DatabaseType)
	ref.Environment = storage.CanonicalKey(ref.Environment)

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT repository, pull_request, MAX(created_at)
		FROM tasks
		WHERE database_name = ? AND database_type = ? AND environment = ? AND table_name = ?
		  AND repository != '' AND pull_request > 0
		GROUP BY repository, pull_request
		ORDER BY MAX(created_at) DESC
		LIMIT %d
	`, tableOwnerLookupLimit), ref.Database, ref.DatabaseType, ref.Environment, ref.TableName)
	if err != nil {
		return nil, fmt.Errorf("query table owners for %s.%s (%s, %s): %w",
			ref.Database, ref.TableName, ref.DatabaseType, ref.Environment, err)
	}
	defer utils.CloseAndLog(rows)

	var owners []storage.TableOwner
	for rows.Next() {
		var owner storage.TableOwner
		if err := rows.Scan(&owner.Repository, &owner.PullRequest, &owner.LastSeen); err != nil {
			return nil, fmt.Errorf("scan table owner for %s.%s (%s, %s): %w",
				ref.Database, ref.TableName, ref.DatabaseType, ref.Environment, err)
		}
		owners = append(owners, owner)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read table owners for %s.%s (%s, %s): %w",
			ref.Database, ref.TableName, ref.DatabaseType, ref.Environment, err)
	}
	return owners, nil
}

// List returns tasks matching the filter criteria.
func (s *taskStore) List(ctx context.Context, filter storage.TaskFilter) ([]*storage.Task, error) {
	filter.Repository = storage.CanonicalKey(filter.Repository)

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

// strandedRetryableQuiescence is how long a parent apply must have been settled
// before the reaper hardens a failed_retryable task under it to failed. It is
// deliberately longer than the retryable-recovery freshness window the claim
// paths key on the same applies.updated_at column: a retry admitted at the very
// edge of that window would refresh the parent's heartbeat the moment it is
// claimed, so a parent still quiet this long past the window cannot have a
// just-admitted retry in flight, and the reap can never race one at the
// boundary. The buffer only needs to cover the gap between a claim selecting
// the parent and its heartbeat write landing; all timestamps compared are
// database-side, so there is no clock skew to absorb.
const strandedRetryableQuiescence = retryableRecoveryFreshnessDays*24*time.Hour + 5*time.Minute

// strandedTaskReaperLockName is the advisory lock that elects one retryable-task
// reaper per pass. Instance-wide for the same reason as the stranded-operation
// reaper's: the pass scans every target's rows, so there is nothing to scope it
// to. It is a separate lock from the operation reaper's so the two maintenance
// sweeps never serialize on each other.
const strandedTaskReaperLockName = "schemabot_stranded_task_reaper"

// strandedRetryableTaskSweep identifies the retryable-task sweep to the shared
// election wrapper.
var strandedRetryableTaskSweep = strandedSweep{
	lockName: strandedTaskReaperLockName,
	busy:     storage.ErrStrandedTaskReaperBusy,
	subject:  "stranded retryable tasks",
}

// ReapStrandedRetryable elects one reaper per pass and reaps under the lock.
// See storage.TaskStore for the contract.
func (s *taskStore) ReapStrandedRetryable(ctx context.Context, limit int) ([]*storage.ReapedTask, error) {
	return reapUnderElection(ctx, s.db, s.locker, strandedRetryableTaskSweep,
		func(ctx context.Context) ([]*storage.ReapedTask, error) {
			return s.reapStrandedRetryable(ctx, limit)
		})
}

// reapStrandedRetryable hardens failed_retryable task rows under settled,
// quiescent parents to failed, without electing a reaper. ReapStrandedRetryable
// is the entry point that holds the lock; this is separate so the reaping
// itself can be exercised on its own.
//
// Each row is settled by its own committed write, so a mid-pass failure returns
// the settlements that already landed alongside the error: they are durable
// whatever the caller does next, and dropping them would leave real state
// changes with no log line and no count behind them.
func (s *taskStore) reapStrandedRetryable(ctx context.Context, limit int) ([]*storage.ReapedTask, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("reap stranded retryable tasks: limit must be positive, got %d", limit)
	}

	parentGate, parentGateArgs := strandedParentGate(s.dialect, "tasks.apply_id", strandedRetryableQuiescence)

	selectArgs := []any{state.Task.FailedRetryable}
	selectArgs = append(selectArgs, parentGateArgs...)
	selectArgs = append(selectArgs, limit)
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT %s
		FROM tasks
		WHERE state = ?
			AND %s
			AND %s
		ORDER BY created_at, id
		LIMIT ?
	`, taskColumns, parentGate, unleasedOperationGate(s.dialect)), selectArgs...)
	if err != nil {
		return nil, fmt.Errorf("query stranded retryable tasks: %w", err)
	}
	stranded, err := scanTasks(rows)
	utils.CloseAndLog(rows)
	if err != nil {
		return nil, fmt.Errorf("scan stranded retryable tasks: %w", err)
	}
	if len(stranded) == 0 {
		return nil, nil
	}

	applyIDs := make([]int64, 0, len(stranded))
	for _, task := range stranded {
		applyIDs = append(applyIDs, task.ApplyID)
	}
	parents, err := loadSettledParents(ctx, s.db, applyIDs, "stranded retryable tasks")
	if err != nil {
		return nil, err
	}

	reaped := make([]*storage.ReapedTask, 0, len(stranded))
	for _, task := range stranded {
		parent, ok := parents[task.ApplyID]
		if !ok {
			// The parent was deleted between the two reads (PR cleanup races the
			// reaper). Its rows go with it, so there is nothing left to settle.
			slog.WarnContext(ctx, "parent apply disappeared while reaping a stranded retryable task; the row is being deleted with it",
				task.LogAttrs()...)
			continue
		}
		settled, err := s.reapStrandedRetryableTask(ctx, task)
		if err != nil {
			return reaped, err
		}
		if !settled {
			// The row left failed_retryable, a driver took its operation's lease, or
			// the parent left the settled set, between the read and the guarded
			// write, so it belongs to whoever moved it.
			slog.DebugContext(ctx, "stranded retryable task changed before it could be reaped; skipping",
				task.LogAttrs()...)
			continue
		}
		reaped = append(reaped, &storage.ReapedTask{Task: task, Parent: parent})
	}
	return reaped, nil
}

// reapStrandedRetryableTask hardens one failed_retryable task row to failed,
// reporting whether the guarded write landed. The task's own error message is
// kept — its failure is real; only the dead retry promise is retired — and
// completed_at is stamped because failed is terminal.
//
// The write re-asserts the parent and lease gates rather than trusting the
// sweep's read: the guarded UPDATE re-verifies the parent it was chosen for and
// re-reads the operation lease, so a write never lands on the strength of a read
// that may be seconds old, and a driver that claims the operation in between
// keeps the row.
func (s *taskStore) reapStrandedRetryableTask(ctx context.Context, task *storage.Task) (bool, error) {
	parentGate, parentGateArgs := strandedParentGate(s.dialect, "tasks.apply_id", strandedRetryableQuiescence)
	args := []any{state.Task.Failed, task.ID, state.Task.FailedRetryable}
	args = append(args, parentGateArgs...)

	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE id = ? AND state = ?
			AND `+parentGate+`
			AND `+unleasedOperationGate(s.dialect)+`
	`, args...)
	if err != nil {
		return false, fmt.Errorf("reap stranded retryable task %s (table %q): %w",
			task.TaskIdentifier, task.TableName, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read reaped rows for stranded retryable task %s (table %q): %w",
			task.TaskIdentifier, task.TableName, err)
	}
	if changed == 0 {
		return false, nil
	}

	// Mirror the write onto the returned row so a caller reporting the
	// settlement reads what is now stored, not the pre-write values.
	now := time.Now()
	task.State = state.Task.Failed
	task.UpdatedAt = now
	if task.CompletedAt == nil {
		task.CompletedAt = &now
	}
	return true, nil
}

// strandedActiveTaskReaperLockName is the advisory lock that elects one
// active-task reaper per pass. It is separate from the retryable-task reaper's
// so the two never serialize: they select disjoint row sets on very different
// quiescence windows, and the retryable sweep frees a blocked remote drive.
const strandedActiveTaskReaperLockName = "schemabot_stranded_active_task_reaper"

// strandedActiveTaskQuiescence is how long a task row must have gone untouched
// before the sweep settles it. It is measured on the task row, not the parent,
// because the task row is the one a live drive writes: every tick mirrors its
// tasks unconditionally, so tasks.updated_at tracks a drive's activity closely
// enough to say when a row is worth looking at. The parent applies row is not a
// substitute — a drive holding only an operation lease is forbidden from bumping
// it, so it can sit quiet while that drive works.
//
// Three drive shapes make that unconditional write, and all three say so where
// they make it: syncAtomicTaskProgress (local grouped drives),
// pollTaskToCompletion (local sequential drives), and
// syncStoredTasksFromRemoteTasks (gRPC drives against remote Tern). A change
// that makes any of them write only on a field change breaks this window, which
// is why they are named here rather than left to be rediscovered.
//
// The window covers the operator's whole recovery budget rather than just its
// stall bound, because the two actions are not equally recoverable. The
// operator crossing that bound cancels the run so a peer can reclaim the
// operation and finish the work; the reaper crossing it writes a terminal
// verdict the operator cannot undo. So the reaper waits for that recovery to
// have played out and failed: the stall bound itself, plus a lease-staleness
// window for the cancelled drive's lease to become re-claimable, plus a poll
// interval for a peer to claim it. Half again the stall bound clears that
// budget, so a drive the operator would have cancelled is always cancelled
// first and the reaper cleans up after that path rather than racing it.
//
// The window is not what makes the sweep safe. The operation lease is
// (unleasedOperationGate), and it has to be, because this window rests on a
// premise no length can guarantee: the remote sync skips a stored task the
// remote stopped reporting, so a row can miss ticks while its drive is alive.
// The lease does not depend on the drive having mirrored anything. What the
// window adds on top is that a row is only reaped once the operator's own
// recovery has had its chance and not taken it.
//
// All timestamps compared are database-side, so there is no clock skew to
// absorb, and the guarded write re-asserts the window anyway: a drive that
// mirrors the row before the write lands keeps it whatever the scan concluded.
const strandedActiveTaskQuiescence = 3 * storage.ApplyDriveStallAfter / 2

// strandedActiveParentQuiescence is how long the parent apply must have been
// settled before the sweep considers its task rows. It is far shorter than the
// operation reaper's window because it is not what makes the sweep safe. What is
// left for the parent to prove is that its verdict is final and no driver holds
// the apply itself, which two lease-staleness windows establish — a lease that
// has not been heartbeated that long is already re-claimable, so an apply row
// quiet for twice that has no live driver by the claim path's own reckoning.
// That is the apply-level half of what unleasedOperationGate does per operation.
const strandedActiveParentQuiescence = 2 * storage.ApplyLeaseStaleAfter

// strandedActiveTaskGate renders the condition admitting only task rows no
// drive has touched for strandedActiveTaskQuiescence. Both the sweep's SELECT
// and its guarded per-row UPDATE assert it, so a drive that mirrors the row
// between the two loses the write rather than having its progress overwritten.
func strandedActiveTaskGate(d Dialect) string {
	return "tasks.updated_at < " + d.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime,
		LiteralIntervalAmount(uint64(strandedActiveTaskQuiescence.Microseconds())), IntervalMicrosecond)
}

// strandedActiveTaskSweep identifies the active-task sweep to the shared
// election wrapper.
var strandedActiveTaskSweep = strandedSweep{
	lockName: strandedActiveTaskReaperLockName,
	busy:     storage.ErrStrandedActiveTaskReaperBusy,
	subject:  "stranded active tasks",
}

// ReapStrandedActive elects one reaper per pass and reaps under the lock.
// See storage.TaskStore for the contract.
func (s *taskStore) ReapStrandedActive(ctx context.Context, limit int) ([]*storage.ReapedTask, error) {
	return reapUnderElection(ctx, s.db, s.locker, strandedActiveTaskSweep,
		func(ctx context.Context) ([]*storage.ReapedTask, error) {
			return s.reapStrandedActive(ctx, limit)
		})
}

// reapStrandedActive mirrors settled parents' outcomes onto their task rows that
// are still in an active state, without electing a reaper. ReapStrandedActive is
// the entry point that holds the lock; this is separate so the reaping itself
// can be exercised on its own.
//
// A driver that settles an apply and exits before closing its task rows leaves
// them describing work that will never resume: the parent's verdict is final, so
// no path revisits the children. The row then reads as live work forever, which
// is what makes a completed apply render a table still copying.
//
// A settled parent alone is not enough to call a row stranded, and neither is a
// quiescent one. Under a fan-out rollout a halt-policy deployment that fails
// projects the apply to failed immediately, by design, while its sibling
// deployments are still driving; and a sibling drive holding only an operation
// lease is forbidden from bumping the parent applies row, so the parent can look
// settled and quiescent while real work continues underneath it. No window on
// the parent detects that, because the parent is not the row the live drive
// touches.
//
// The operation the drive holds is what rules that out: the sweep takes only
// rows whose operation carries no live lease (unleasedOperationGate), reading
// that lease the way the claim path reads one it may take from a peer. On top
// of that it gates on the task's own quiescence, so a row is settled only after
// the operator's stalled-drive recovery has had its window and left it behind.
//
// failed_retryable is deliberately excluded. It is active by the task state
// machine, but it belongs to the retryable sweep, which waits out a far longer
// window because a retry may still be admitted against the parent. Reaping it
// here would harden a retry promise the recovery path could still dispatch.
//
// Each row is settled by its own committed write, so a mid-pass failure returns
// the settlements that already landed alongside the error: they are durable
// whatever the caller does next, and dropping them would leave real state
// changes with no log line and no count behind them.
func (s *taskStore) reapStrandedActive(ctx context.Context, limit int) ([]*storage.ReapedTask, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("reap stranded active tasks: limit must be positive, got %d", limit)
	}

	parentGate, parentGateArgs := strandedParentGate(s.dialect, "tasks.apply_id", strandedActiveParentQuiescence)

	excluded := append(append([]string{}, state.TerminalTaskStates...), state.Task.FailedRetryable)
	selectArgs := stringArgs(excluded)
	selectArgs = append(selectArgs, parentGateArgs...)
	selectArgs = append(selectArgs, limit)
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT %s
		FROM tasks
		WHERE state NOT IN (%s)
			AND %s
			AND %s
			AND %s
		ORDER BY created_at, id
		LIMIT ?
	`, taskColumns, placeholders(len(excluded)), strandedActiveTaskGate(s.dialect), parentGate,
		unleasedOperationGate(s.dialect)), selectArgs...)
	if err != nil {
		return nil, fmt.Errorf("query stranded active tasks: %w", err)
	}
	stranded, err := scanTasks(rows)
	utils.CloseAndLog(rows)
	if err != nil {
		return nil, fmt.Errorf("scan stranded active tasks: %w", err)
	}
	if len(stranded) == 0 {
		return nil, nil
	}

	applyIDs := make([]int64, 0, len(stranded))
	for _, task := range stranded {
		applyIDs = append(applyIDs, task.ApplyID)
	}
	parents, err := loadSettledParents(ctx, s.db, applyIDs, "stranded active tasks")
	if err != nil {
		return nil, err
	}

	reaped := make([]*storage.ReapedTask, 0, len(stranded))
	for _, task := range stranded {
		parent, ok := parents[task.ApplyID]
		if !ok {
			// The parent was deleted between the two reads (PR cleanup races the
			// reaper). Its rows go with it, so there is nothing left to settle.
			slog.WarnContext(ctx, "parent apply disappeared while reaping a stranded active task; the row is being deleted with it",
				task.LogAttrs()...)
			continue
		}
		settled, err := s.reapStrandedActiveTask(ctx, task, parent)
		if err != nil {
			return reaped, err
		}
		if !settled {
			// The row left the state it was read in, a drive mirrored it and
			// refreshed its liveness, a driver took its operation's lease, or the
			// parent left the settled set, between the read and the guarded write.
			// Any of the four means the row belongs to a live writer rather than to
			// the sweep.
			slog.DebugContext(ctx, "stranded active task changed before it could be reaped; skipping",
				task.LogAttrs()...)
			continue
		}
		reaped = append(reaped, &storage.ReapedTask{Task: task, Parent: parent})
	}
	return reaped, nil
}

// reapStrandedActiveTask writes one task row from its settled parent, reporting
// whether the guarded write landed. It takes the parent's verdict rather than
// deciding one: every settled parent state is a terminal task state, and a task
// whose outcome was never recorded is never assumed to have succeeded — under a
// failed parent it settles failed, carrying the parent's explanation, exactly as
// the operation reaper does.
//
// completed_at is stamped because every settled parent state is non-resumable.
//
// The write re-asserts the row's state, its quiescence, the parent gate and the
// operation lease rather than trusting the sweep's read, so it does not
// overwrite a row a driver moved, mirrored, or claimed after the scan selected
// it.
func (s *taskStore) reapStrandedActiveTask(ctx context.Context, task *storage.Task, parent *storage.Apply) (bool, error) {
	taskState := state.NormalizeState(parent.State)
	setClause := "state = ?, completed_at = COALESCE(completed_at, NOW())"
	args := []any{taskState}
	if state.IsState(parent.State, state.Apply.Failed) {
		setClause = "state = ?, error_message = COALESCE(NULLIF(error_message, ''), ?), completed_at = COALESCE(completed_at, NOW())"
		args = []any{taskState, nullString(parent.ErrorMessage)}
	}
	parentGate, parentGateArgs := strandedParentGate(s.dialect, "tasks.apply_id", strandedActiveParentQuiescence)
	args = append(args, task.ID, task.State)
	args = append(args, parentGateArgs...)

	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET `+setClause+`, updated_at = NOW()
		WHERE id = ? AND state = ?
			AND `+strandedActiveTaskGate(s.dialect)+`
			AND `+parentGate+`
			AND `+unleasedOperationGate(s.dialect)+`
	`, args...)
	if err != nil {
		return false, fmt.Errorf("reap stranded active task %s (table %q) from %s parent apply: %w",
			task.TaskIdentifier, task.TableName, parent.State, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read reaped rows for stranded active task %s (table %q): %w",
			task.TaskIdentifier, task.TableName, err)
	}
	if changed == 0 {
		return false, nil
	}

	// Mirror the write onto the returned row so a caller reporting the
	// settlement reads what is now stored, not the pre-write values.
	now := time.Now()
	task.State = taskState
	task.UpdatedAt = now
	if task.CompletedAt == nil {
		task.CompletedAt = &now
	}
	if state.IsState(parent.State, state.Apply.Failed) && task.ErrorMessage == "" {
		task.ErrorMessage = parent.ErrorMessage
	}
	return true, nil
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
		&task.ChecksumRowsChecked,
		&task.ChecksumRowsTotal,
		&task.Throttled,
		&task.ThrottleReason,
		&task.CutoverAttempts,
		&task.IsInstant,
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
