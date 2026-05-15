// applies.go implements ApplyStore for tracking schema change executions.
// Each apply is a top-level container that holds one or more tasks.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// applyColumns lists all columns for SELECT queries.
const applyColumns = `id, apply_identifier, lock_id, plan_id, database_name, database_type,
	repository, pull_request, environment, deployment, caller, installation_id, external_id, engine,
	state, error_message, options,
	created_at, started_at, completed_at, updated_at`

const applyColumnsForApplyAlias = `a.id, a.apply_identifier, a.lock_id, a.plan_id, a.database_name, a.database_type,
	a.repository, a.pull_request, a.environment, a.deployment, a.caller, a.installation_id, a.external_id, a.engine,
	a.state, a.error_message, a.options,
	a.created_at, a.started_at, a.completed_at, a.updated_at`

// applyStore implements storage.ApplyStore using MySQL.
type applyStore struct {
	db *sql.DB
}

type applyWriteTx struct {
	tx *sql.Tx
}

type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// claimableApplyStates returns apply states where scheduler recovery can safely
// resume work after the heartbeat becomes stale. Pending and running states can
// be orphaned by a server crash, and deploy/cutover/revert-window states have
// already persisted enough engine metadata for recovery to continue from stored
// state. Terminal states are already done, stopped requires an explicit user
// start, and PlanetScale setup states are excluded until recovery can reload the
// persisted branch/deploy metadata needed to resume them without restarting setup.
func claimableApplyStates() []string {
	return []string{
		state.Apply.Pending,
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

func beginApplyWriteTx(ctx context.Context, db *sql.DB, operation string) (*applyWriteTx, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, fmt.Errorf("begin %s transaction: %w", operation, err)
	}

	return &applyWriteTx{tx: tx}, nil
}

func (w *applyWriteTx) close(ctx context.Context, operation string) {
	if w == nil {
		return
	}
	if w.tx != nil {
		rollbackApplyTx(ctx, w.tx, operation)
	}
}

func (w *applyWriteTx) commit() error {
	err := w.tx.Commit()
	w.tx = nil
	return err
}

// apply_target_locks rows are persistent mutex rows keyed by target. They are
// created lazily and intentionally survive apply completion; the apply lifecycle
// remains in applies, while this table gives first writers an exact row to lock.
func ensureApplyTargetLockRow(ctx context.Context, db *sql.DB, database, dbType, environment string) error {
	if !hasApplyTarget(database, dbType, environment) {
		return fmt.Errorf("active apply target is required for %s/%s/%s", database, dbType, environment)
	}

	var id int64
	err := db.QueryRowContext(ctx,
		"SELECT `id` FROM `apply_target_locks` "+
			"WHERE `database_name` = ? "+
			"AND `database_type` = ? "+
			"AND `environment` = ?",
		database, dbType, environment,
	).Scan(&id)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read apply target for %s/%s/%s: %w", database, dbType, environment, err)
	}

	_, err = db.ExecContext(ctx,
		"INSERT IGNORE INTO `apply_target_locks` (`database_name`, `database_type`, `environment`) "+
			"VALUES (?, ?, ?)",
		database, dbType, environment,
	)
	if err != nil {
		return fmt.Errorf("ensure apply target for %s/%s/%s: %w", database, dbType, environment, err)
	}
	return nil
}

func lockApplyTargetRow(ctx context.Context, tx *sql.Tx, database, dbType, environment string) error {
	var id int64
	err := tx.QueryRowContext(ctx,
		"SELECT `id` FROM `apply_target_locks` "+
			"WHERE `database_name` = ? "+
			"AND `database_type` = ? "+
			"AND `environment` = ? "+
			"FOR UPDATE",
		database, dbType, environment,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("apply target missing for %s/%s/%s", database, dbType, environment)
	}
	if err != nil {
		return fmt.Errorf("lock apply target for %s/%s/%s: %w", database, dbType, environment, err)
	}
	return nil
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
	if lockTarget {
		if err := ensureApplyTargetLockRow(ctx, s.db, apply.Database, apply.DatabaseType, apply.Environment); err != nil {
			return 0, err
		}
	}

	writeTx, err := beginApplyWriteTx(ctx, s.db, "create apply")
	if err != nil {
		return 0, err
	}
	defer writeTx.close(ctx, "create apply")

	if lockTarget {
		if err := lockApplyTargetRow(ctx, writeTx.tx, apply.Database, apply.DatabaseType, apply.Environment); err != nil {
			return 0, err
		}
		if err := checkNoActiveApplyForTarget(ctx, writeTx.tx, apply.Database, apply.DatabaseType, apply.Environment, 0); err != nil {
			return 0, err
		}
	}

	result, err := writeTx.tx.ExecContext(ctx, `
		INSERT INTO applies (
			apply_identifier, lock_id, plan_id, database_name, database_type,
			repository, pull_request, environment, deployment, caller, installation_id, external_id, engine,
			state, error_message, options
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		apply.ApplyIdentifier, apply.LockID, apply.PlanID, apply.Database, apply.DatabaseType,
		apply.Repository, apply.PullRequest, apply.Environment, apply.Deployment, apply.Caller, apply.InstallationID, apply.ExternalID, apply.Engine,
		apply.State, apply.ErrorMessage, string(options),
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
	if shouldLockTarget {
		if err := ensureApplyTargetLockRow(ctx, s.db, database, dbType, environment); err != nil {
			return err
		}
	}

	writeTx, err := beginApplyWriteTx(ctx, s.db, "update apply")
	if err != nil {
		return err
	}
	defer writeTx.close(ctx, "update apply")

	if shouldLockTarget {
		if err := lockApplyTargetRow(ctx, writeTx.tx, database, dbType, environment); err != nil {
			return err
		}
		if err := checkNoActiveApplyForTarget(ctx, writeTx.tx, database, dbType, environment, apply.ID); err != nil {
			return err
		}
	}

	_, err = writeTx.tx.ExecContext(ctx, `
		UPDATE applies
		SET state = ?, error_message = ?,
		    external_id = ?, started_at = ?, completed_at = ?, updated_at = NOW()
		WHERE id = ?
	`, apply.State, apply.ErrorMessage,
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
// Matches stale active applies where heartbeat expired > 1 minute. Apply
// creation/update enforces one active apply per database/type/environment, so
// claims only need to lease one stale row and avoid worker races on that row.
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
	queryArgs := stringArgs(activeStates)

	// Apply creation/update enforces at most one active apply per
	// database/type/environment. The claim query only needs to find stale work;
	// FOR UPDATE SKIP LOCKED prevents concurrent workers from claiming the same row.
	row := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT %s
		FROM applies a
		WHERE a.state IN (%s)
		AND a.updated_at < NOW() - INTERVAL 1 MINUTE
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
	_, err = tx.ExecContext(ctx, `
		UPDATE applies SET updated_at = NOW() WHERE id = ?
	`, apply.ID)
	if err != nil {
		return nil, fmt.Errorf("refresh heartbeat for claimed apply %d: %w", apply.ID, err)
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
		&apply.State, &apply.ErrorMessage, &options,
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
