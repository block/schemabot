// applies.go implements ApplyStore for tracking schema change executions.
// Each apply is a top-level container that holds one or more tasks.
package mysqlstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// applyColumns lists all columns for SELECT queries.
const applyColumns = `id, apply_identifier, lock_id, plan_id, database_name, database_type,
	repository, pull_request, environment, deployment, caller, installation_id, external_id, engine,
	state, error_message, options, attempt,
	created_at, started_at, completed_at, updated_at`

const applyColumnsForApplyAlias = `a.id, a.apply_identifier, a.lock_id, a.plan_id, a.database_name, a.database_type,
	a.repository, a.pull_request, a.environment, a.deployment, a.caller, a.installation_id, a.external_id, a.engine,
	a.state, a.error_message, a.options, a.attempt,
	a.created_at, a.started_at, a.completed_at, a.updated_at`

// maxRecoveryAttempts is the retry budget for failed_retryable applies. The
// original apply attempt is separate; this counts scheduler redispatches.
const maxRecoveryAttempts = 10

const (
	applyTargetLockWait           = 10 * time.Second
	applyTargetLockReleaseTimeout = 5 * time.Second
)

// applyStore implements storage.ApplyStore using MySQL.
type applyStore struct {
	db *sql.DB
}

type applyWriteTx struct {
	tx             *sql.Tx
	targetLockConn *sql.Conn
	targetLockName string
}

type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type txBeginner interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// claimableApplyStates returns active apply states where scheduler recovery can
// safely resume work after the heartbeat becomes stale. Pending has a separate
// queue path and does not need to be stale before a worker claims it. Terminal
// states are already done, failed_retryable has its own retry path, stopped
// requires an explicit user start, and PlanetScale setup states are excluded
// until recovery can reload the persisted branch/deploy metadata needed to
// resume them without restarting setup.
func claimableApplyStates() []string {
	return []string{
		state.Apply.Running,
		state.Apply.WaitingForDeploy,
		state.Apply.WaitingForCutover,
		state.Apply.CuttingOver,
		state.Apply.RevertWindow,
	}
}

func terminalApplyStates() []string {
	return []string{
		state.Apply.Completed,
		state.Apply.Failed,
		state.Apply.Stopped,
		state.Apply.Cancelled,
		state.Apply.Reverted,
	}
}

func isActiveApplyState(applyState string) bool {
	return !state.IsTerminalApplyState(applyState)
}

func hasApplyTarget(database, dbType, environment string) bool {
	return database != "" && dbType != "" && environment != ""
}

func placeholders(count int) string {
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}

func stringArgs(values []string) []any {
	args := make([]any, len(values))
	for i, value := range values {
		args[i] = value
	}
	return args
}

func nonTerminalApplyStatePredicate(column string) (string, []any) {
	terminalStates := terminalApplyStates()
	return fmt.Sprintf("%s NOT IN (%s)", column, placeholders(len(terminalStates))), stringArgs(terminalStates)
}

func rollbackApplyTx(ctx context.Context, tx *sql.Tx, operation string) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		slog.WarnContext(ctx, "failed to roll back apply transaction", "operation", operation, "error", err)
	}
}

func beginApplyWriteTx(ctx context.Context, beginner txBeginner, operation string) (*applyWriteTx, error) {
	tx, err := beginner.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, fmt.Errorf("begin %s transaction: %w", operation, err)
	}

	return &applyWriteTx{tx: tx}, nil
}

func beginApplyTargetWriteTx(ctx context.Context, db *sql.DB, operation, database, dbType, environment string) (*applyWriteTx, error) {
	conn, lockName, err := acquireApplyTargetLockConn(ctx, db, database, dbType, environment)
	if err != nil {
		return nil, err
	}

	writeTx, err := beginApplyWriteTx(ctx, conn, operation)
	if err != nil {
		releaseApplyTargetLockConn(ctx, conn, lockName, operation)
		return nil, err
	}
	writeTx.targetLockConn = conn
	writeTx.targetLockName = lockName
	return writeTx, nil
}

func (w *applyWriteTx) close(ctx context.Context, operation string) {
	if w == nil {
		return
	}
	if w.tx != nil {
		rollbackApplyTx(ctx, w.tx, operation)
	}
	if w.targetLockConn != nil {
		releaseApplyTargetLockConn(ctx, w.targetLockConn, w.targetLockName, operation)
		w.targetLockConn = nil
	}
}

func (w *applyWriteTx) commit() error {
	err := w.tx.Commit()
	w.tx = nil
	return err
}

func applyTargetLockName(database, dbType, environment string) string {
	sum := sha256.Sum256([]byte(database + "\x00" + dbType + "\x00" + environment))
	return "schemabot_apply_" + hex.EncodeToString(sum[:16])
}

func acquireApplyTargetLockConn(ctx context.Context, db *sql.DB, database, dbType, environment string) (*sql.Conn, string, error) {
	if !hasApplyTarget(database, dbType, environment) {
		return nil, "", fmt.Errorf("active apply target is required for %s/%s/%s", database, dbType, environment)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("get apply target connection for %s/%s/%s: %w", database, dbType, environment, err)
	}

	lockName := applyTargetLockName(database, dbType, environment)
	var result sql.NullInt64
	err = conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", lockName, int(applyTargetLockWait.Seconds())).Scan(&result)
	if err != nil {
		slog.WarnContext(ctx, "failed to acquire apply target lock",
			"database", database,
			"database_type", dbType,
			"environment", environment,
			"lock", lockName,
			"wait", applyTargetLockWait,
			"error", err)
		closeApplyTargetLockConn(ctx, conn, lockName, "acquire apply target lock")
		return nil, "", fmt.Errorf("acquire apply target lock for %s/%s/%s: %w", database, dbType, environment, err)
	}
	if !result.Valid || result.Int64 != 1 {
		slog.WarnContext(ctx, "timed out waiting for apply target lock",
			"database", database,
			"database_type", dbType,
			"environment", environment,
			"lock", lockName,
			"wait", applyTargetLockWait,
			"result", result)
		closeApplyTargetLockConn(ctx, conn, lockName, "acquire apply target lock")
		return nil, "", fmt.Errorf("timed out waiting for apply target lock for %s/%s/%s", database, dbType, environment)
	}
	return conn, lockName, nil
}

func releaseApplyTargetLockConn(ctx context.Context, conn *sql.Conn, lockName, operation string) {
	if conn == nil {
		return
	}

	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), applyTargetLockReleaseTimeout)
	defer cancel()

	var result sql.NullInt64
	err := conn.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(?)", lockName).Scan(&result)
	if err != nil || !result.Valid || result.Int64 != 1 {
		slog.WarnContext(releaseCtx, "failed to release apply target lock; discarding connection",
			"operation", operation,
			"lock", lockName,
			"result", result,
			"error", err)
		if rawErr := conn.Raw(func(any) error { return driver.ErrBadConn }); rawErr != nil && !errors.Is(rawErr, driver.ErrBadConn) {
			slog.WarnContext(releaseCtx, "failed to discard apply target lock connection",
				"operation", operation,
				"lock", lockName,
				"error", rawErr)
		}
	}

	closeApplyTargetLockConn(releaseCtx, conn, lockName, operation)
}

func closeApplyTargetLockConn(ctx context.Context, conn *sql.Conn, lockName, operation string) {
	if err := conn.Close(); err != nil {
		slog.WarnContext(ctx, "failed to close apply target lock connection",
			"operation", operation,
			"lock", lockName,
			"error", err)
	}
}

func checkNoActiveApplyForTarget(ctx context.Context, tx *sql.Tx, database, dbType, environment string, excludeApplyID int64) error {
	statePredicate, stateArgs := nonTerminalApplyStatePredicate("state")
	query := fmt.Sprintf(`
		SELECT count(*) FROM applies FORCE INDEX (idx_database_env)
		WHERE database_name = ?
		AND database_type = ?
		AND environment = ?
		AND %s
	`, statePredicate)
	args := append([]any{database, dbType, environment}, stateArgs...)
	if excludeApplyID > 0 {
		query += " AND id != ?"
		args = append(args, excludeApplyID)
	}

	var activeCount int64
	err := tx.QueryRowContext(ctx, query, args...).Scan(&activeCount)
	if err != nil {
		return fmt.Errorf("check active applies for %s/%s/%s: %w", database, dbType, environment, err)
	}
	if activeCount > 0 {
		return fmt.Errorf("active apply exists for %s/%s/%s: %w", database, dbType, environment, storage.ErrActiveApplyExists)
	}
	return nil
}

func applyTargetForUpdate(ctx context.Context, db queryRower, apply *storage.Apply) (string, string, string, error) {
	if hasApplyTarget(apply.Database, apply.DatabaseType, apply.Environment) {
		return apply.Database, apply.DatabaseType, apply.Environment, nil
	}

	var database, dbType, environment string
	err := db.QueryRowContext(ctx, `
		SELECT database_name, database_type, environment
		FROM applies
		WHERE id = ?
	`, apply.ID).Scan(&database, &dbType, &environment)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", "", nil
	}
	if err != nil {
		return "", "", "", fmt.Errorf("load apply target for update %d: %w", apply.ID, err)
	}
	return database, dbType, environment, nil
}

// Create stores a new apply and returns its ID.
func (s *applyStore) Create(ctx context.Context, apply *storage.Apply) (int64, error) {
	// Ensure options has valid JSON (empty object if nil)
	options := apply.Options
	if len(options) == 0 {
		options = []byte("{}")
	}

	lockTarget := isActiveApplyState(apply.State)
	var writeTx *applyWriteTx
	var err error
	if lockTarget {
		writeTx, err = beginApplyTargetWriteTx(ctx, s.db, "create apply", apply.Database, apply.DatabaseType, apply.Environment)
		if err != nil {
			return 0, err
		}
	} else {
		writeTx, err = beginApplyWriteTx(ctx, s.db, "create apply")
		if err != nil {
			return 0, err
		}
	}
	defer writeTx.close(ctx, "create apply")

	if lockTarget {
		if err := checkNoActiveApplyForTarget(ctx, writeTx.tx, apply.Database, apply.DatabaseType, apply.Environment, 0); err != nil {
			return 0, err
		}
	}

	result, err := writeTx.tx.ExecContext(ctx, `
		INSERT INTO applies (
			apply_identifier, lock_id, plan_id, database_name, database_type,
			repository, pull_request, environment, deployment, caller, installation_id, external_id, engine,
			state, error_message, options, attempt
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		apply.ApplyIdentifier, apply.LockID, apply.PlanID, apply.Database, apply.DatabaseType,
		apply.Repository, apply.PullRequest, apply.Environment, apply.Deployment, apply.Caller, apply.InstallationID, apply.ExternalID, apply.Engine,
		apply.State, apply.ErrorMessage, string(options), apply.Attempt,
	)
	if err != nil {
		if isDuplicateKeyError(err) {
			return 0, fmt.Errorf("create apply %s: %w", apply.ApplyIdentifier, storage.ErrApplyIDExists)
		}
		return 0, fmt.Errorf("insert apply %s: %w", apply.ApplyIdentifier, err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("read inserted apply id for %s: %w", apply.ApplyIdentifier, err)
	}

	if err := writeTx.commit(); err != nil {
		return 0, fmt.Errorf("commit create apply: %w", err)
	}

	return id, nil
}

// CreateWithTasks stores an apply and its initial tasks in one transaction.
func (s *applyStore) CreateWithTasks(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) (int64, error) {
	// Ensure options has valid JSON (empty object if nil)
	options := apply.Options
	if len(options) == 0 {
		options = []byte("{}")
	}

	lockTarget := isActiveApplyState(apply.State)
	var writeTx *applyWriteTx
	var err error
	if lockTarget {
		writeTx, err = beginApplyTargetWriteTx(ctx, s.db, "create apply with tasks", apply.Database, apply.DatabaseType, apply.Environment)
		if err != nil {
			return 0, err
		}
	} else {
		writeTx, err = beginApplyWriteTx(ctx, s.db, "create apply with tasks")
		if err != nil {
			return 0, err
		}
	}
	defer writeTx.close(ctx, "create apply with tasks")

	if lockTarget {
		if err := checkNoActiveApplyForTarget(ctx, writeTx.tx, apply.Database, apply.DatabaseType, apply.Environment, 0); err != nil {
			return 0, err
		}
	}

	result, err := writeTx.tx.ExecContext(ctx, `
		INSERT INTO applies (
			apply_identifier, lock_id, plan_id, database_name, database_type,
			repository, pull_request, environment, deployment, caller, installation_id, external_id, engine,
			state, error_message, options, attempt
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		apply.ApplyIdentifier, apply.LockID, apply.PlanID, apply.Database, apply.DatabaseType,
		apply.Repository, apply.PullRequest, apply.Environment, apply.Deployment, apply.Caller, apply.InstallationID, apply.ExternalID, apply.Engine,
		apply.State, apply.ErrorMessage, string(options), apply.Attempt,
	)
	if err != nil {
		if isDuplicateKeyError(err) {
			return 0, fmt.Errorf("create apply %s: %w", apply.ApplyIdentifier, storage.ErrApplyIDExists)
		}
		return 0, fmt.Errorf("insert apply %s: %w", apply.ApplyIdentifier, err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("read inserted apply id for %s: %w", apply.ApplyIdentifier, err)
	}

	for _, task := range tasks {
		task.ApplyID = id
		taskID, err := insertTask(ctx, writeTx.tx, task)
		if err != nil {
			return 0, fmt.Errorf("insert task %s for apply %s: %w", task.TaskIdentifier, apply.ApplyIdentifier, err)
		}
		task.ID = taskID
	}

	if err := writeTx.commit(); err != nil {
		return 0, fmt.Errorf("commit create apply with tasks: %w", err)
	}

	return id, nil
}

// Get returns an apply by ID, or nil if not found.
func (s *applyStore) Get(ctx context.Context, id int64) (*storage.Apply, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE id = ?
	`, id)

	return scanApply(row)
}

// GetByApplyIdentifier returns an apply by apply_identifier, or nil if not found.
func (s *applyStore) GetByApplyIdentifier(ctx context.Context, applyIdentifier string) (*storage.Apply, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE apply_identifier = ?
	`, applyIdentifier)

	return scanApply(row)
}

// GetByPlan returns the apply for a plan_id, or nil if not found.
func (s *applyStore) GetByPlan(ctx context.Context, planID int64) (*storage.Apply, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE plan_id = ?
	`, planID)

	return scanApply(row)
}

// GetByLock returns applies for a lock (0-2: staging + production).
func (s *applyStore) GetByLock(ctx context.Context, lockID int64) ([]*storage.Apply, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE lock_id = ?
		ORDER BY created_at DESC
	`, lockID)
	if err != nil {
		return nil, fmt.Errorf("query applies for lock %d: %w", lockID, err)
	}
	defer utils.CloseAndLog(rows)

	return scanApplies(rows)
}

// Update updates apply state and fields.
func (s *applyStore) Update(ctx context.Context, apply *storage.Apply) error {
	lockTarget := isActiveApplyState(apply.State)
	database, dbType, environment := apply.Database, apply.DatabaseType, apply.Environment
	var err error
	if lockTarget && !hasApplyTarget(database, dbType, environment) {
		database, dbType, environment, err = applyTargetForUpdate(ctx, s.db, apply)
		if err != nil {
			return err
		}
	}

	shouldLockTarget := lockTarget && hasApplyTarget(database, dbType, environment)
	var writeTx *applyWriteTx
	if shouldLockTarget {
		writeTx, err = beginApplyTargetWriteTx(ctx, s.db, "update apply", database, dbType, environment)
		if err != nil {
			return err
		}
	} else {
		writeTx, err = beginApplyWriteTx(ctx, s.db, "update apply")
		if err != nil {
			return err
		}
	}
	defer writeTx.close(ctx, "update apply")

	if shouldLockTarget {
		if err := checkNoActiveApplyForTarget(ctx, writeTx.tx, database, dbType, environment, apply.ID); err != nil {
			return err
		}
	}

	_, err = writeTx.tx.ExecContext(ctx, `
		UPDATE applies
		SET state = ?, error_message = ?, attempt = ?,
		    external_id = ?, started_at = ?, completed_at = ?, updated_at = NOW()
		WHERE id = ?
	`, apply.State, apply.ErrorMessage, apply.Attempt,
		apply.ExternalID, apply.StartedAt, apply.CompletedAt, apply.ID)
	if err != nil {
		return fmt.Errorf("update apply %d: %w", apply.ID, err)
	}
	if err := writeTx.commit(); err != nil {
		return fmt.Errorf("commit update apply %d: %w", apply.ID, err)
	}
	return nil
}

// GetInProgress returns all applies in non-terminal states.
// Note: For recovery, use FindNextApply which handles locking and heartbeat staleness.
func (s *applyStore) GetInProgress(ctx context.Context) ([]*storage.Apply, error) {
	statePredicate, args := nonTerminalApplyStatePredicate("state")
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT `+applyColumns+`
		FROM applies
		WHERE %s
		ORDER BY created_at DESC
	`, statePredicate), args...)
	if err != nil {
		return nil, fmt.Errorf("query in-progress applies: %w", err)
	}
	defer utils.CloseAndLog(rows)

	return scanApplies(rows)
}

// GetRecent returns the most recent applies across all databases, ordered by start time desc.
func (s *applyStore) GetRecent(ctx context.Context, limit int) ([]*storage.Apply, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		ORDER BY COALESCE(started_at, created_at) DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	return scanApplies(rows)
}

// FindNextApply atomically claims the next apply that needs attention.
// A claim selects one stale apply and refreshes its heartbeat in the same
// transaction. That heartbeat is the scheduler's lease while it reloads state
// and resumes the apply.
// Returns the claimed apply, or nil if nothing needs work.
//
// Matches queued pending applies with persisted tasks, stale active applies
// where heartbeat expired > 1 minute, and failed_retryable applies that still
// have retry budget. Apply creation/update enforces one active apply per
// database/type/environment, so claims only need to lease one row and avoid
// worker races on that row.
func (s *applyStore) FindNextApply(ctx context.Context) (*storage.Apply, error) {
	// Read committed keeps concurrent SKIP LOCKED claims from taking next-key
	// range locks that can serialize workers across otherwise independent targets.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin claim apply transaction: %w", err)
	}
	defer rollbackApplyTx(ctx, tx, "claim apply")

	activeStates := claimableApplyStates()
	activeStatePlaceholders := placeholders(len(activeStates))
	queryArgs := []any{state.Apply.Pending}
	queryArgs = append(queryArgs, stringArgs(activeStates)...)
	queryArgs = append(queryArgs, state.Apply.FailedRetryable, maxRecoveryAttempts)

	// Apply creation/update enforces at most one active apply per
	// database/type/environment. The claim query only needs to find stale work;
	// FOR UPDATE SKIP LOCKED prevents concurrent workers from claiming the same row.
	row := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT %s
		FROM applies a
		WHERE (
			(a.state = ? AND EXISTS (SELECT 1 FROM tasks t WHERE t.apply_id = a.id))
			OR (a.state IN (%s) AND a.updated_at < NOW() - INTERVAL 1 MINUTE)
			OR (a.state = ? AND a.attempt < ?)
		)
		ORDER BY a.created_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, applyColumns, activeStatePlaceholders), queryArgs...)

	apply, err := scanApplyInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // No apply to claim
	}
	if err != nil {
		return nil, fmt.Errorf("query next claimable apply: %w", err)
	}

	// Refresh the heartbeat as part of the claim before releasing the row lock.
	// Pending and retryable applies become running while dispatch starts, so
	// the leased row remains in the stale-recovery state family if the worker
	// crashes before the engine starts.
	if apply.State == state.Apply.Pending || apply.State == state.Apply.FailedRetryable {
		var result sql.Result
		nextState := state.Apply.Running
		result, err = tx.ExecContext(ctx, `
			UPDATE applies
			SET state = ?, updated_at = NOW(),
			    attempt = CASE WHEN ? = ? THEN attempt + 1 ELSE attempt END,
			    completed_at = NULL,
			    error_message = CASE WHEN ? = ? THEN '' ELSE error_message END
			WHERE id = ? AND state = ?
		`, nextState, apply.State, state.Apply.FailedRetryable, apply.State, state.Apply.FailedRetryable, apply.ID, apply.State)
		if err != nil {
			return nil, fmt.Errorf("claim apply %d in state %s: %w", apply.ID, apply.State, err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("read claim rows affected for apply %d: %w", apply.ID, err)
		}
		if rows == 0 {
			return nil, nil
		}
		if apply.State == state.Apply.FailedRetryable {
			apply.Attempt++
			apply.ErrorMessage = ""
		}
	} else {
		_, err = tx.ExecContext(ctx, `
			UPDATE applies SET updated_at = NOW() WHERE id = ?
		`, apply.ID)
		if err != nil {
			return nil, fmt.Errorf("refresh heartbeat for claimed apply %d: %w", apply.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim apply %d: %w", apply.ID, err)
	}

	return apply, nil
}

// Heartbeat updates the apply's updated_at timestamp to maintain the lease.
// Should be called every 10 seconds while working on an apply.
// If not called for > 1 minute, another worker can claim the apply via FindNextApply.
// Does not check RowsAffected — if the apply was deleted, the UPDATE matches 0 rows
// and returns nil.
func (s *applyStore) Heartbeat(ctx context.Context, applyID int64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE applies SET updated_at = NOW() WHERE id = ?
	`, applyID)
	if err != nil {
		return fmt.Errorf("heartbeat apply %d: %w", applyID, err)
	}
	return nil
}

// ExpireRetryable transitions failed_retryable applies that exhausted their
// retry budget to permanent failed. Returns the applies updated.
func (s *applyStore) ExpireRetryable(ctx context.Context) ([]*storage.Apply, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin expire retryable applies transaction: %w", err)
	}
	defer rollbackApplyTx(ctx, tx, "expire retryable applies")

	rows, err := tx.QueryContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE state = ? AND attempt >= ?
		FOR UPDATE
	`, state.Apply.FailedRetryable, maxRecoveryAttempts)
	if err != nil {
		return nil, fmt.Errorf("query exhausted retryable applies: %w", err)
	}
	applies, err := scanApplies(rows)
	utils.CloseAndLog(rows)
	if err != nil {
		return nil, fmt.Errorf("scan exhausted retryable applies: %w", err)
	}
	if len(applies) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit empty expire retryable applies: %w", err)
		}
		return nil, nil
	}

	applyIDs := make([]any, 0, len(applies))
	for _, apply := range applies {
		applyIDs = append(applyIDs, apply.ID)
	}

	taskArgs := []any{state.Task.Failed}
	taskArgs = append(taskArgs, stringArgs(state.TerminalTaskStates)...)
	taskArgs = append(taskArgs, applyIDs...)
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE tasks t
		SET t.state = ?, t.completed_at = COALESCE(t.completed_at, NOW()), t.updated_at = NOW()
		WHERE t.state NOT IN (%s) AND t.apply_id IN (%s)
	`, placeholders(len(state.TerminalTaskStates)), placeholders(len(applyIDs))), taskArgs...)
	if err != nil {
		return nil, fmt.Errorf("expire retryable tasks: %w", err)
	}

	applyArgs := append([]any{state.Apply.Failed}, applyIDs...)
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE applies
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE id IN (%s)
	`, placeholders(len(applyIDs))), applyArgs...)
	if err != nil {
		return nil, fmt.Errorf("expire retryable applies: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit expire retryable applies: %w", err)
	}
	now := time.Now()
	for _, apply := range applies {
		apply.State = state.Apply.Failed
		apply.CompletedAt = &now
		apply.UpdatedAt = now
	}
	return applies, nil
}

// GetByDatabase returns applies for a specific database and optionally filtered by dbType and environment.
// If dbType or environment are empty strings, they are not used as filters.
func (s *applyStore) GetByDatabase(ctx context.Context, database, dbType, environment string) ([]*storage.Apply, error) {
	query := `
		SELECT ` + applyColumns + `
		FROM applies
		WHERE database_name = ?`
	args := []any{database}

	if dbType != "" {
		query += " AND database_type = ?"
		args = append(args, dbType)
	}
	if environment != "" {
		query += " AND environment = ?"
		args = append(args, environment)
	}
	query += " ORDER BY created_at DESC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query applies for database %s: %w", database, err)
	}
	defer utils.CloseAndLog(rows)

	return scanApplies(rows)
}

// FindMissingSummaryComment returns GitHub-backed applies that should have a
// terminal summary comment but only have a progress comment. Used to post missing
// summaries after restart.
func (s *applyStore) FindMissingSummaryComment(ctx context.Context) ([]*storage.Apply, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyColumnsForApplyAlias+`
		FROM applies a
		JOIN apply_comments acp ON acp.apply_id = a.id AND acp.comment_state = 'progress'
		LEFT JOIN apply_comments acs ON acs.apply_id = a.id AND acs.comment_state = 'summary'
		WHERE a.state IN (?, ?, ?, ?)
		  AND a.repository != ''
		  AND a.pull_request > 0
		  AND a.installation_id > 0
		  AND a.completed_at > NOW() - INTERVAL 1 HOUR
		  AND acs.id IS NULL
		ORDER BY a.completed_at DESC
	`, state.Apply.Completed, state.Apply.Failed, state.Apply.Reverted, state.Apply.Cancelled)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	var applies []*storage.Apply
	for rows.Next() {
		apply, err := scanApplyInto(rows)
		if err != nil {
			return nil, err
		}
		applies = append(applies, apply)
	}
	return applies, rows.Err()
}

// GetByPR returns all applies for a PR.
func (s *applyStore) GetByPR(ctx context.Context, repo string, pr int) ([]*storage.Apply, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE repository = ? AND pull_request = ?
		ORDER BY created_at DESC
	`, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("query applies for %s#%d: %w", repo, pr, err)
	}
	defer utils.CloseAndLog(rows)

	return scanApplies(rows)
}

// Delete removes an apply by ID.
func (s *applyStore) Delete(ctx context.Context, id int64) error {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM applies WHERE id = ?
	`, id)
	if err != nil {
		return err
	}

	return checkRowsAffected(result, storage.ErrApplyNotFound)
}

// DeleteByPR removes all applies for a PR.
func (s *applyStore) DeleteByPR(ctx context.Context, repo string, pr int) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM applies WHERE repository = ? AND pull_request = ?
	`, repo, pr)
	return err
}

// scanApply scans a single apply row, returning nil if not found.
func scanApply(row *sql.Row) (*storage.Apply, error) {
	apply, err := scanApplyInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return apply, err
}

// scanApplies scans multiple apply rows.
func scanApplies(rows *sql.Rows) ([]*storage.Apply, error) {
	var applies []*storage.Apply
	for rows.Next() {
		apply, err := scanApplyInto(rows)
		if err != nil {
			return nil, err
		}
		applies = append(applies, apply)
	}
	return applies, rows.Err()
}

// scanApplyInto scans apply data from any scanner (Row or Rows).
func scanApplyInto(s scanner) (*storage.Apply, error) {
	var apply storage.Apply
	var startedAt, completedAt sql.NullTime
	var options []byte

	err := s.Scan(
		&apply.ID, &apply.ApplyIdentifier, &apply.LockID, &apply.PlanID,
		&apply.Database, &apply.DatabaseType,
		&apply.Repository, &apply.PullRequest, &apply.Environment, &apply.Deployment,
		&apply.Caller, &apply.InstallationID, &apply.ExternalID, &apply.Engine,
		&apply.State, &apply.ErrorMessage, &options, &apply.Attempt,
		&apply.CreatedAt, &startedAt, &completedAt, &apply.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	apply.Options = options

	if startedAt.Valid {
		apply.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		apply.CompletedAt = &completedAt.Time
	}

	return &apply, nil
}
