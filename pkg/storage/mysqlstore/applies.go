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
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
	"github.com/google/uuid"
)

// applyColumns lists all columns for SELECT queries.
const applyColumns = `id, apply_identifier, lock_id, plan_id, database_name, database_type,
	repository, pull_request, environment, deployment, caller, installation_id, external_id, idempotency_key, engine,
	state, error_message, options, attempt,
	lease_owner, lease_token, lease_acquired_at,
	created_at, started_at, completed_at, updated_at, revert_skipped_at`

const applyColumnsForApplyAlias = `a.id, a.apply_identifier, a.lock_id, a.plan_id, a.database_name, a.database_type,
	a.repository, a.pull_request, a.environment, a.deployment, a.caller, a.installation_id, a.external_id, a.idempotency_key, a.engine,
	a.state, a.error_message, a.options, a.attempt,
	a.lease_owner, a.lease_token, a.lease_acquired_at,
	a.created_at, a.started_at, a.completed_at, a.updated_at, a.revert_skipped_at`

const (
	// maxRecoveryAttempts is the retry budget for failed_retryable applies,
	// shared with operator-facing progress rendering via the exported constant.
	maxRecoveryAttempts = storage.MaxRecoveryAttempts

	// retryableRecoveryFreshnessDays prevents old retryable failures from
	// being redispatched unexpectedly after retry policy or attempt budgets
	// change. Old failures require deliberate operator action instead of
	// automatic operator pickup.
	retryableRecoveryFreshnessDays = 1
)

const (
	applyTargetLockWait           = 10 * time.Second
	applyTargetLockReleaseTimeout = 5 * time.Second
)

// applyStore implements storage.ApplyStore using MySQL.
type applyStore struct {
	db      *sql.DB
	dialect Dialect
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

// claimableApplyStates returns active apply states where operator recovery can
// safely resume work after the heartbeat becomes stale. Pending has a separate
// queue path and does not need to be stale before a driver claims it. Terminal
// states are already done, failed_retryable has its own retry path, and stopped
// requires an explicit user start.
//
// The PlanetScale setup-phase states (preparing_branch through
// validating_deploy_request) are included: a stale heartbeat in any of them
// unambiguously means the driver driving engine setup died, and the persisted
// branch/deploy metadata lets recovery resume the apply from stored state. A
// healthy driver mid-setup keeps its heartbeat fresh, so it is never claimed out
// from under itself. Leaving these states out would strand a crashed setup-phase
// apply non-terminal and unclaimable, so no operator could ever recover it.
//
// Resuming is included for the same reason: it is a transient state a driver
// holds while waiting for the data plane to leave stopped after a start. A
// stale heartbeat there means the driver died mid-resume, and recovery must be
// able to reclaim and finish driving it to a terminal state.
func claimableApplyStates() []string {
	return []string{
		state.Apply.Running,
		state.Apply.RunningDegraded,
		state.Apply.Resuming,
		state.Apply.WaitingForDeploy,
		state.Apply.WaitingForCutover,
		state.Apply.Recovering,
		"recovering_cutover",
		state.Apply.CuttingOver,
		state.Apply.RevertWindow,
		state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest,
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
		rollbackTx(ctx, w.tx, operation)
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

// dedupeDeployments returns the input deployments as a sorted, de-duplicated
// slice. The empty string is a valid deployment value: it is the column default
// for single-deployment applies that predate fan-out, so it must be matched
// like any other key rather than dropped. An empty result means the caller
// passed no deployments at all, which the overlap check treats as a fail-closed
// error rather than skipping the check.
func dedupeDeployments(deployments []string) []string {
	seen := make(map[string]struct{}, len(deployments))
	out := make([]string, 0, len(deployments))
	for _, deployment := range deployments {
		if _, ok := seen[deployment]; ok {
			continue
		}
		seen[deployment] = struct{}{}
		out = append(out, deployment)
	}
	slices.Sort(out)
	return out
}

// operationDeploymentsForApply returns the deployments of an apply's
// apply_operations rows. It is used to build the full target set of an existing
// apply being started/resumed/reclaimed so the overlap check covers every
// deployment the apply owns, not just the parent's primary deployment.
func operationDeploymentsForApply(ctx context.Context, tx *sql.Tx, applyID int64) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT deployment FROM apply_operations WHERE apply_id = ?`, applyID)
	if err != nil {
		return nil, fmt.Errorf("list operation deployments for apply %d: %w", applyID, err)
	}
	defer utils.CloseAndLog(rows)

	var deployments []string
	for rows.Next() {
		var deployment string
		if err := rows.Scan(&deployment); err != nil {
			return nil, fmt.Errorf("scan operation deployment for apply %d: %w", applyID, err)
		}
		deployments = append(deployments, deployment)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operation deployments for apply %d: %w", applyID, err)
	}
	return deployments, nil
}

// checkNoActiveApplyForTargets enforces the "one active apply per physical
// deployment" invariant across the full target set an apply owns. Each
// deployment is a distinct physical target (its own Tern endpoint): two applies
// in the same environment that touch disjoint deployments may run concurrently,
// while a new apply is rejected if any of its deployments overlaps a deployment
// owned by a non-terminal apply.
//
// A non-terminal PARENT apply blocks every deployment in its operation set,
// even deployments whose own operation already completed under on_failure
// "continue": the apply is the unit of safety and reconciliation, so its whole
// target set stays reserved until the parent reaches a terminal state. Overlap
// is matched against both the parent applies.deployment (the primary) and the
// apply_operations.deployment rows, so single-operation applies (where the two
// are equal) behave exactly as before.
func checkNoActiveApplyForTargets(ctx context.Context, tx *sql.Tx, database, dbType, environment string, deployments []string, excludeApplyID int64) error {
	deployments = dedupeDeployments(deployments)
	if len(deployments) == 0 {
		return fmt.Errorf("active apply target requires at least one deployment for %s/%s/%s", database, dbType, environment)
	}

	statePredicate, stateArgs := nonTerminalApplyStatePredicate("a.state")
	deploymentPlaceholders := placeholders(len(deployments))
	query := fmt.Sprintf(`
		SELECT 1 FROM applies a FORCE INDEX (idx_database_env_deployment)
		WHERE a.database_name = ?
		AND a.database_type = ?
		AND a.environment = ?
		AND %s
		AND (
			a.deployment IN (%s)
			OR EXISTS (
				SELECT 1 FROM apply_operations o
				WHERE o.apply_id = a.id
				AND o.deployment IN (%s)
			)
		)
	`, statePredicate, deploymentPlaceholders, deploymentPlaceholders)
	args := []any{database, dbType, environment}
	args = append(args, stateArgs...)
	args = append(args, stringArgs(deployments)...)
	args = append(args, stringArgs(deployments)...)
	if excludeApplyID > 0 {
		query += " AND a.id != ?"
		args = append(args, excludeApplyID)
	}
	query += " LIMIT 1"

	var exists int
	err := tx.QueryRowContext(ctx, query, args...).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check active applies for %s/%s/%s deployments %v: %w", database, dbType, environment, deployments, err)
	}
	return fmt.Errorf("active apply exists for %s/%s/%s deployments %v: %w", database, dbType, environment, deployments, storage.ErrActiveApplyExists)
}

func applyLeaseFromContext(ctx context.Context, applyID int64) (storage.ApplyLease, bool, error) {
	lease, ok := storage.ApplyLeaseFromContext(ctx)
	if !ok {
		return storage.ApplyLease{}, false, nil
	}
	if !lease.Valid() {
		return storage.ApplyLease{}, true, fmt.Errorf("invalid apply lease for apply %d: %w", applyID, storage.ErrApplyLeaseLost)
	}
	if lease.ApplyID != applyID {
		return storage.ApplyLease{}, true, fmt.Errorf("apply lease for apply %d cannot write apply %d: %w", lease.ApplyID, applyID, storage.ErrApplyLeaseLost)
	}
	return lease, true, nil
}

func applyLeaseMatches(ctx context.Context, db queryRower, lease storage.ApplyLease) (bool, error) {
	var match int
	err := db.QueryRowContext(ctx, `
		SELECT 1
		FROM applies
		WHERE id = ? AND lease_token = ?
	`, lease.ApplyID, lease.Token).Scan(&match)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("verify apply lease for apply %d: %w", lease.ApplyID, err)
	}
	return true, nil
}

func ensureApplyLeaseStillOwned(ctx context.Context, db queryRower, lease storage.ApplyLease) error {
	matches, err := applyLeaseMatches(ctx, db, lease)
	if err != nil {
		return err
	}
	if !matches {
		return fmt.Errorf("apply lease for apply %d is no longer current: %w", lease.ApplyID, storage.ErrApplyLeaseLost)
	}
	return nil
}

// confirmLeaseOnZeroRows fails closed when a lease-scoped write changed no rows.
// Zero rows is ambiguous: either a legitimate idempotent no-op (the lease is
// still valid) or the lease token no longer matches because ownership was lost.
// It reloads the lease to distinguish the two so a displaced driver returns
// ErrApplyLeaseLost instead of silently treating a lost lease as a no-op. desc
// names the write and target identifies the row(s) in the rows-affected error
// ("read <desc> rows affected for <target>"). The affected row count is returned
// for callers that need it.
func confirmLeaseOnZeroRows(ctx context.Context, db queryRower, result sql.Result, lease storage.ApplyLease, desc, target string) (int64, error) {
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read %s rows affected for %s: %w", desc, target, err)
	}
	if rows == 0 {
		if err := ensureApplyLeaseStillOwned(ctx, db, lease); err != nil {
			return rows, err
		}
	}
	return rows, nil
}

// applyTargetForUpdate resolves the (database, type, environment, deployment)
// target an update should lock and check against. The stored row is
// authoritative, so it is reloaded whenever the in-memory apply is missing the
// 3-tuple target or carries an empty deployment (the Go zero value is
// indistinguishable from "not populated", and deployment is part of the active
// target identity).
func applyTargetForUpdate(ctx context.Context, db queryRower, apply *storage.Apply) (string, string, string, string, error) {
	if hasApplyTarget(apply.Database, apply.DatabaseType, apply.Environment) && apply.Deployment != "" {
		return apply.Database, apply.DatabaseType, apply.Environment, apply.Deployment, nil
	}

	var database, dbType, environment, deployment string
	err := db.QueryRowContext(ctx, `
		SELECT database_name, database_type, environment, deployment
		FROM applies
		WHERE id = ?
	`, apply.ID).Scan(&database, &dbType, &environment, &deployment)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", "", "", nil
	}
	if err != nil {
		return "", "", "", "", fmt.Errorf("load apply target for update %d: %w", apply.ID, err)
	}
	return database, dbType, environment, deployment, nil
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
	if err := verifyExpectedLockIntent(ctx, writeTx.tx, apply); err != nil {
		return 0, err
	}

	if lockTarget {
		if err := checkNoActiveApplyForTargets(ctx, writeTx.tx, apply.Database, apply.DatabaseType, apply.Environment, []string{apply.Deployment}, 0); err != nil {
			return 0, err
		}
	}

	result, err := writeTx.tx.ExecContext(ctx, `
		INSERT INTO applies (
			apply_identifier, lock_id, plan_id, database_name, database_type,
			repository, pull_request, environment, deployment, caller, installation_id, external_id, idempotency_key, engine,
			state, error_message, options, attempt
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		apply.ApplyIdentifier, apply.LockID, apply.PlanID, apply.Database, apply.DatabaseType,
		apply.Repository, apply.PullRequest, apply.Environment, apply.Deployment, apply.Caller, apply.InstallationID, apply.ExternalID, nullString(apply.IdempotencyKey), apply.Engine,
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
	return s.CreateWithTasksAndOperations(ctx, apply, tasks, nil)
}

type applyCreateWriter func(ctx context.Context, tx *sql.Tx, apply *storage.Apply, applyID int64) error

// CreateWithTasksAndOperations is the unified atomic apply-create path: it
// inserts the applies row, the initial tasks, and (optionally) the
// per-deployment apply_operations rows in a single transaction. Pending
// applies become operator-claimable only after every row commits, so no
// reader observes a partially-populated apply.
func (s *applyStore) CreateWithTasksAndOperations(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, operations []*storage.ApplyOperation) (int64, error) {
	const opName = "create apply with tasks"
	deployments := []string{apply.Deployment}
	for _, op := range operations {
		deployments = append(deployments, op.Deployment)
	}
	return s.createWithRows(ctx, apply, opName, deployments, func(ctx context.Context, tx *sql.Tx, apply *storage.Apply, applyID int64) error {
		return insertApplyTasksAndOperations(ctx, tx, apply, applyID, tasks, operations)
	})
}

// CreateWithGroupedOperations stores an apply with per-operation task groups in one transaction.
func (s *applyStore) CreateWithGroupedOperations(ctx context.Context, apply *storage.Apply, groups []*storage.ApplyOperationWithTasks) (int64, error) {
	const opName = "create apply with grouped operations"
	deployments := []string{apply.Deployment}
	for _, group := range groups {
		if group != nil && group.Operation != nil {
			deployments = append(deployments, group.Operation.Deployment)
		}
	}
	return s.createWithRows(ctx, apply, opName, deployments, func(ctx context.Context, tx *sql.Tx, apply *storage.Apply, applyID int64) error {
		return insertApplyGroupedOperations(ctx, tx, apply, applyID, groups)
	})
}

func (s *applyStore) createWithRows(ctx context.Context, apply *storage.Apply, opName string, newDeployments []string, writeRows applyCreateWriter) (int64, error) {
	// Ensure options has valid JSON (empty object if nil)
	options := apply.Options
	if len(options) == 0 {
		options = []byte("{}")
	}

	lockTarget := isActiveApplyState(apply.State)
	var writeTx *applyWriteTx
	var err error
	if lockTarget {
		writeTx, err = beginApplyTargetWriteTx(ctx, s.db, opName, apply.Database, apply.DatabaseType, apply.Environment)
		if err != nil {
			return 0, err
		}
	} else {
		writeTx, err = beginApplyWriteTx(ctx, s.db, opName)
		if err != nil {
			return 0, err
		}
	}
	defer writeTx.close(ctx, opName)
	if err := verifyExpectedLockIntent(ctx, writeTx.tx, apply); err != nil {
		return 0, err
	}

	if lockTarget {
		if err := checkNoActiveApplyForTargets(ctx, writeTx.tx, apply.Database, apply.DatabaseType, apply.Environment, newDeployments, 0); err != nil {
			return 0, err
		}
	}

	result, err := writeTx.tx.ExecContext(ctx, `
		INSERT INTO applies (
			apply_identifier, lock_id, plan_id, database_name, database_type,
			repository, pull_request, environment, deployment, caller, installation_id, external_id, idempotency_key, engine,
			state, error_message, options, attempt
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		apply.ApplyIdentifier, apply.LockID, apply.PlanID, apply.Database, apply.DatabaseType,
		apply.Repository, apply.PullRequest, apply.Environment, apply.Deployment, apply.Caller, apply.InstallationID, apply.ExternalID, nullString(apply.IdempotencyKey), apply.Engine,
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

	if err := writeRows(ctx, writeTx.tx, apply, id); err != nil {
		return 0, err
	}

	if err := writeTx.commit(); err != nil {
		return 0, fmt.Errorf("commit %s: %w", opName, err)
	}

	return id, nil
}

// verifyExpectedLockIntent locks and validates the webhook apply intent in the
// same transaction that inserts the apply. This closes the gap where a
// same-owner rollback can replace pending_plan_id after freshness validation
// but before the forward apply becomes durable.
func verifyExpectedLockIntent(ctx context.Context, tx *sql.Tx, apply *storage.Apply) error {
	if apply.ExpectedLockOwner == "" {
		if apply.ExpectedPendingPlanID != "" {
			return fmt.Errorf("verify lock intent for %s/%s: expected pending plan ID set without an expected lock owner", apply.Database, apply.DatabaseType)
		}
		return nil
	}

	// An empty ExpectedPendingPlanID is a real observed intent, not a missing
	// one: it matches only a lock whose pending_plan_id is unset (the column is
	// NOT NULL DEFAULT ''), so an unpinned lock that a rollback re-pins mid-flight
	// still fails this check.
	var lockID int64
	err := tx.QueryRowContext(ctx, `
		SELECT id
		FROM locks
		WHERE database_name = ? AND database_type = ? AND owner = ? AND pending_plan_id = ?
		FOR UPDATE
	`, apply.Database, apply.DatabaseType, apply.ExpectedLockOwner, apply.ExpectedPendingPlanID).Scan(&lockID)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrLockIntentChanged
	}
	if err != nil {
		return fmt.Errorf("verify lock intent for %s/%s: %w", apply.Database, apply.DatabaseType, err)
	}
	apply.LockID = lockID
	return nil
}

func insertApplyTasksAndOperations(ctx context.Context, tx *sql.Tx, apply *storage.Apply, applyID int64, tasks []*storage.Task, operations []*storage.ApplyOperation) error {
	// Operations are inserted BEFORE tasks so each task can be persisted
	// with the apply_operation_id of the operation it belongs to. Today
	// the config layer hard-blocks multi-entry deployments so there is
	// always exactly one operation per apply, and all tasks are linked
	// to it. When the multi-entry block is lifted and apply-create fans
	// tasks out per-operation, the per-task mapping needs to be encoded
	// by the caller (see the multi-op guard below).
	for _, op := range operations {
		op.ApplyID = applyID
		if _, err := insertApplyOperation(ctx, tx, op); err != nil {
			return fmt.Errorf("insert apply_operation (deployment=%s) for apply %s: %w", op.Deployment, apply.ApplyIdentifier, err)
		}
	}

	// tasks.apply_operation_id is indexed but not a foreign key, so the
	// store must validate the link itself: any non-nil ApplyOperationID
	// MUST point at one of the apply_operations rows just inserted above.
	// Without this check a caller could persist an arbitrary or zero id
	// and silently break per-operation task scoping once the operator
	// claim loop comes online.
	insertedOpIDs := make(map[int64]struct{}, len(operations))
	for _, op := range operations {
		insertedOpIDs[op.ID] = struct{}{}
	}

	switch {
	case len(tasks) == 0:
		// no tasks; nothing to link.
	case len(operations) == 0:
		// No operations supplied; any pre-populated ApplyOperationID is
		// invalid because there is no row it can reference for this apply.
		for _, task := range tasks {
			if task.ApplyOperationID != nil {
				return fmt.Errorf("create apply %s: task %s has apply_operation_id=%d but apply has no operations", apply.ApplyIdentifier, task.TaskIdentifier, *task.ApplyOperationID)
			}
		}
	case len(operations) == 1:
		// Single-operation apply: link every task to the lone operation
		// unless the caller already supplied an explicit value. An explicit
		// value still has to match the inserted operation.
		for _, task := range tasks {
			if task.ApplyOperationID == nil {
				task.ApplyOperationID = &operations[0].ID
				continue
			}
			if _, ok := insertedOpIDs[*task.ApplyOperationID]; !ok {
				return fmt.Errorf("create apply %s: task %s apply_operation_id=%d does not match any inserted operation for this apply", apply.ApplyIdentifier, task.TaskIdentifier, *task.ApplyOperationID)
			}
		}
	case len(operations) > 1:
		// Multi-operation apply: caller MUST decide which operation each
		// task belongs to and pre-populate task.ApplyOperationID. Silently
		// assigning every task to operations[0] would lock in a wrong
		// mapping the moment multi-entry deployments are unblocked.
		for _, task := range tasks {
			if task.ApplyOperationID == nil {
				return fmt.Errorf("create apply %s: task %s missing apply_operation_id (apply has %d operations; caller must encode the per-task mapping)", apply.ApplyIdentifier, task.TaskIdentifier, len(operations))
			}
			if _, ok := insertedOpIDs[*task.ApplyOperationID]; !ok {
				return fmt.Errorf("create apply %s: task %s apply_operation_id=%d does not match any inserted operation for this apply", apply.ApplyIdentifier, task.TaskIdentifier, *task.ApplyOperationID)
			}
		}
	}

	for _, task := range tasks {
		task.ApplyID = applyID
		taskID, err := insertTask(ctx, tx, task)
		if err != nil {
			return fmt.Errorf("insert task %s for apply %s: %w", task.TaskIdentifier, apply.ApplyIdentifier, err)
		}
		task.ID = taskID
	}
	return nil
}

func insertApplyGroupedOperations(ctx context.Context, tx *sql.Tx, apply *storage.Apply, applyID int64, groups []*storage.ApplyOperationWithTasks) error {
	if len(groups) == 0 {
		return fmt.Errorf("create apply %s: grouped operations are empty", apply.ApplyIdentifier)
	}
	for _, group := range groups {
		deployment := ""
		if group != nil && group.Operation != nil {
			deployment = group.Operation.Deployment
		}
		if group == nil {
			return fmt.Errorf("create apply %s deployment %s: grouped operation is nil", apply.ApplyIdentifier, deployment)
		}
		if group.Operation == nil {
			return fmt.Errorf("create apply %s deployment %s: grouped operation is missing its operation row", apply.ApplyIdentifier, deployment)
		}
		// A group_finalizer carries no tasks — it applies namespace-level work
		// reconstructed from the plan at drive time. A caller may also explicitly
		// allow a task-less work operation for a single grouped engine apply whose
		// only work is plan-level metadata. Every other work operation must have at
		// least one task so operation-scoped drives fail closed on bad scoping.
		if len(group.Tasks) == 0 && group.Operation.OperationKind != storage.ApplyOperationKindGroupFinalizer && !group.AllowTaskless {
			return fmt.Errorf("create apply %s deployment %s: grouped work operation has no tasks", apply.ApplyIdentifier, deployment)
		}

		group.Operation.ApplyID = applyID
		if _, err := insertApplyOperation(ctx, tx, group.Operation); err != nil {
			return fmt.Errorf("insert apply_operation (deployment=%s) for apply %s: %w", group.Operation.Deployment, apply.ApplyIdentifier, err)
		}
		for _, task := range group.Tasks {
			if task == nil {
				return fmt.Errorf("create apply %s deployment %s: grouped operation has a nil task", apply.ApplyIdentifier, group.Operation.Deployment)
			}
			task.ApplyID = applyID
			task.ApplyOperationID = &group.Operation.ID
			taskID, err := insertTask(ctx, tx, task)
			if err != nil {
				return fmt.Errorf("insert task %s for apply %s deployment %s: %w", task.TaskIdentifier, apply.ApplyIdentifier, group.Operation.Deployment, err)
			}
			task.ID = taskID
		}
	}
	return nil
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

// GetByIdempotencyKey returns the apply stamped with the given idempotency key,
// or nil if none exists. An empty key returns nil without querying: NULL keys
// are not deduplicated, so an empty lookup would never match a real dispatch.
func (s *applyStore) GetByIdempotencyKey(ctx context.Context, idempotencyKey string) (*storage.Apply, error) {
	if idempotencyKey == "" {
		return nil, nil
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE idempotency_key = ?
	`, idempotencyKey)

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
	// A drive that holds only an operation lease must never write the parent
	// applies row directly: under fan-out the parent state is owned solely by
	// the rollout projection (UpdateDerivedState). A single-operation drive
	// still carries the parent apply lease alongside the operation lease, so it
	// keeps the direct-write path; only an operation-lease-only context is
	// refused here.
	if _, hasOpLease := storage.OperationLeaseFromContext(ctx); hasOpLease {
		if _, hasApplyLease := storage.ApplyLeaseFromContext(ctx); !hasApplyLease {
			return fmt.Errorf("operation lease does not authorize a direct update of apply %d; parent state is owned by the rollout projection: %w", apply.ID, storage.ErrApplyLeaseLost)
		}
	}

	lease, hasLease, err := applyLeaseFromContext(ctx, apply.ID)
	if err != nil {
		return err
	}
	lockTarget := isActiveApplyState(apply.State)
	database, dbType, environment, deployment := apply.Database, apply.DatabaseType, apply.Environment, apply.Deployment
	if lockTarget && (!hasApplyTarget(database, dbType, environment) || deployment == "") {
		database, dbType, environment, deployment, err = applyTargetForUpdate(ctx, s.db, apply)
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
		deployments, err := operationDeploymentsForApply(ctx, writeTx.tx, apply.ID)
		if err != nil {
			return err
		}
		deployments = append(deployments, deployment)
		if err := checkNoActiveApplyForTargets(ctx, writeTx.tx, database, dbType, environment, deployments, apply.ID); err != nil {
			return err
		}
	}

	optionsUpdate := ""
	args := []any{
		apply.State, apply.ErrorMessage, apply.Attempt,
		apply.ExternalID,
	}
	if len(apply.Options) > 0 {
		optionsUpdate = ", options = ?"
		args = append(args, string(apply.Options))
	}
	args = append(args, apply.StartedAt, apply.CompletedAt, apply.ID)
	leasePredicate := ""
	if hasLease {
		leasePredicate = " AND lease_token = ?"
		args = append(args, lease.Token)
	}

	result, err := writeTx.tx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE applies
		SET state = ?, error_message = ?, attempt = ?,
		    external_id = ?%s, started_at = ?, completed_at = ?, updated_at = NOW()
		WHERE id = ?%s
	`, optionsUpdate, leasePredicate), args...)
	if err != nil {
		return fmt.Errorf("update apply %d: %w", apply.ID, err)
	}
	if hasLease {
		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("read apply update rows affected for apply %d: %w", apply.ID, err)
		}
		if rows == 0 {
			if err := ensureApplyLeaseStillOwned(ctx, writeTx.tx, lease); err != nil {
				return err
			}
		}
	}
	if err := writeTx.commit(); err != nil {
		return fmt.Errorf("commit update apply %d: %w", apply.ID, err)
	}
	return nil
}

// derivedStateGuard is the lease authorization for a rollout-projection write.
// predicate is appended to the CAS UPDATE's WHERE clause and predicateArgs holds
// its bind values; ensureStillOwned re-checks ownership on a zero-rows result so
// the caller can fail closed on a lost lease (nil for an unguarded write).
type derivedStateGuard struct {
	predicate        string
	predicateArgs    []any
	ensureStillOwned func(ctx context.Context, db queryRower) error
}

// derivedStateGuardForContext selects how a UpdateDerivedState write is
// authorized. An operation lease takes precedence over the parent apply lease,
// mirroring taskStore.Update: a multi-operation drive advances the parent only
// through the projection, scoped to an operation that still holds its token and
// belongs to applyID. The apply-lease path keeps the single-operation behavior.
func derivedStateGuardForContext(ctx context.Context, applyID int64) (derivedStateGuard, error) {
	if opLease, ok := storage.OperationLeaseFromContext(ctx); ok {
		if !opLease.Valid() {
			return derivedStateGuard{}, fmt.Errorf("invalid operation lease for apply %d: %w", applyID, storage.ErrApplyLeaseLost)
		}
		if opLease.ApplyID != applyID {
			return derivedStateGuard{}, fmt.Errorf("operation lease for apply %d cannot write derived state for apply %d: %w", opLease.ApplyID, applyID, storage.ErrApplyLeaseLost)
		}
		return derivedStateGuard{
			predicate: ` AND EXISTS (
				SELECT 1 FROM apply_operations ao
				WHERE ao.id = ? AND ao.apply_id = ? AND ao.lease_token = ?
			)`,
			predicateArgs: []any{opLease.OperationID, applyID, opLease.Token},
			ensureStillOwned: func(ctx context.Context, db queryRower) error {
				return ensureOperationLeaseOwnsApply(ctx, db, opLease, applyID)
			},
		}, nil
	}

	lease, hasLease, err := applyLeaseFromContext(ctx, applyID)
	if err != nil {
		return derivedStateGuard{}, err
	}
	if hasLease {
		return derivedStateGuard{
			predicate:     " AND lease_token = ?",
			predicateArgs: []any{lease.Token},
			ensureStillOwned: func(ctx context.Context, db queryRower) error {
				return ensureApplyLeaseStillOwned(ctx, db, lease)
			},
		}, nil
	}

	return derivedStateGuard{}, nil
}

// ensureOperationLeaseOwnsApply returns ErrApplyLeaseLost unless the operation
// row still holds the lease's token and still belongs to applyID, so a CAS that
// missed because the operation was reassigned (or rebound to another apply)
// fails closed instead of being read as a benign miss.
func ensureOperationLeaseOwnsApply(ctx context.Context, db queryRower, lease storage.OperationLease, applyID int64) error {
	var match int
	err := db.QueryRowContext(ctx, `
		SELECT 1
		FROM apply_operations
		WHERE id = ? AND apply_id = ? AND lease_token = ?
	`, lease.OperationID, applyID, lease.Token).Scan(&match)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("operation lease for apply_operation %d (apply %d) is no longer current: %w", lease.OperationID, applyID, storage.ErrApplyLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("verify operation lease for apply_operation %d (apply %d): %w", lease.OperationID, applyID, err)
	}
	return nil
}

// UpdateDerivedState compare-and-swaps the rollout-projected applies.state.
// It writes only the fields owned by the projection and only when the row still
// holds expectedState, so a stale projection cannot clobber a newer state a
// sibling drive already wrote. A lost lease fails closed with an error; a CAS
// miss (state no longer matches) returns swapped=false so the caller can skip
// side-effects and reconcile on the next poll.
func (s *applyStore) UpdateDerivedState(ctx context.Context, applyID int64, expectedState, newState, errorMessage string, startedAt, completedAt *time.Time) (bool, error) {
	guard, err := derivedStateGuardForContext(ctx, applyID)
	if err != nil {
		return false, err
	}

	// started_at is stamped only when it is still NULL so the projection can move
	// the parent into an active state without ever rewinding a recorded start.
	args := []any{newState, errorMessage, startedAt, completedAt, applyID, expectedState}
	args = append(args, guard.predicateArgs...)

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE applies
		SET state = ?, error_message = ?, started_at = COALESCE(started_at, ?), completed_at = ?, updated_at = NOW()
		WHERE id = ? AND state = ?%s
	`, guard.predicate), args...)
	if err != nil {
		return false, fmt.Errorf("compare-and-swap derived apply state for apply %d (%q -> %q): %w", applyID, expectedState, newState, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read derived apply state rows affected for apply %d: %w", applyID, err)
	}
	if rows > 0 {
		return true, nil
	}

	// Zero rows. A leased caller must distinguish a lost lease (fail closed) from
	// a benign CAS outcome: the former is an ownership change the caller must
	// surface, the latter is reconciled on the next poll.
	if guard.ensureStillOwned != nil {
		if err := guard.ensureStillOwned(ctx, s.db); err != nil {
			return false, err
		}
	}

	// Under production changed-rows semantics an UPDATE that touches a matching
	// row but leaves every column unchanged reports zero rows. That only happens
	// when the projection is a no-op (newState == expectedState and the other
	// projected fields already hold their target), so a re-read distinguishes
	// "row still holds expectedState" (idempotent swap) from a genuine CAS miss
	// where another drive already advanced the state. When newState differs from
	// expectedState a matching row would have changed, so zero rows is always a
	// miss and no re-read is needed.
	if !state.IsState(newState, expectedState) {
		return false, nil
	}
	var currentState string
	err = s.db.QueryRowContext(ctx, `SELECT state FROM applies WHERE id = ?`, applyID).Scan(&currentState)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("re-read derived apply state for apply %d: %w", applyID, err)
	}
	return state.IsState(currentState, expectedState), nil
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

// GetRecent returns the most recent applies across all databases, ordered by creation time desc.
func (s *applyStore) GetRecent(ctx context.Context, filter storage.RecentAppliesFilter) ([]*storage.Apply, error) {
	query := `
		SELECT ` + applyColumns + `
		FROM applies
	`
	var args []any
	var where []string
	if filter.Environment != "" {
		where = append(where, "environment = ?")
		args = append(args, filter.Environment)
	}
	if filter.Deployment != "" {
		where = append(where, `(deployment = ? OR EXISTS (
			SELECT 1
			FROM apply_operations ao
			WHERE ao.apply_id = applies.id AND ao.deployment = ?
		))`)
		args = append(args, filter.Deployment, filter.Deployment)
	}
	if len(filter.States) > 0 {
		where = append(where, fmt.Sprintf("state IN (%s)", placeholders(len(filter.States))))
		args = append(args, stringArgs(filter.States)...)
	}
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY created_at DESC, id DESC LIMIT ?"
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	return scanApplies(rows)
}

// FindNextApply atomically claims the next apply that needs attention.
// A claim selects one stale apply and refreshes its heartbeat in the same
// transaction. That heartbeat is the operator's lease while it reloads state
// and resumes the apply.
// Returns the claimed apply, or nil if nothing needs work.
//
// Matches queued pending applies with persisted tasks, pending, stopped, or
// waiting-for-deploy applies with a pending start control request, stale active
// applies whose heartbeat expired beyond the lease staleness window, and
// recently failed_retryable applies that still have retry budget.
// Apply creation/update enforces one active apply per database/type/environment,
// so claims only need to lease one row and avoid driver races on that row.
func (s *applyStore) FindNextApply(ctx context.Context, owner string) (*storage.Apply, error) {
	if owner == "" {
		return nil, fmt.Errorf("operator owner is required to claim apply: %w", storage.ErrApplyLeaseLost)
	}
	// Read committed keeps concurrent SKIP LOCKED claims from taking next-key
	// range locks that can serialize drivers across otherwise independent targets.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin claim apply transaction: %w", err)
	}
	defer rollbackTx(ctx, tx, "claim apply")

	activeStates := claimableApplyStates()
	activeStatePlaceholders := placeholders(len(activeStates))
	queryArgs := []any{state.Apply.Pending}
	queryArgs = append(queryArgs, stringArgs(activeStates)...)
	queryArgs = append(queryArgs, state.Apply.FailedRetryable, maxRecoveryAttempts, retryableRecoveryFreshnessDays)
	queryArgs = append(queryArgs,
		state.Apply.Pending,
		storage.ControlOperationStart, storage.ControlRequestPending)
	queryArgs = append(queryArgs,
		state.Apply.Stopped,
		storage.ControlOperationStart, storage.ControlRequestPending)
	queryArgs = append(queryArgs,
		state.Apply.WaitingForDeploy,
		storage.ControlOperationStart, storage.ControlRequestPending)

	staleClaimCutoff := s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, LiteralIntervalAmount(uint64(storage.ApplyLeaseStaleAfter.Microseconds())), IntervalMicrosecond)
	retryFreshnessCutoff := s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, ParameterIntervalAmount(), IntervalDay)

	// Apply creation/update enforces at most one active apply per
	// database/type/environment. The claim query only needs to find stale work;
	// FOR UPDATE SKIP LOCKED prevents concurrent drivers from claiming the same row.
	//
	// The pending clause requires child rows so a half-created apply is never
	// claimed. Creation dual-writes tasks and the apply_operations row in one
	// transaction, so either proves the create committed fully; a VSchema-only
	// apply carries an operation row but no tasks, so tasks alone would strand it.
	row := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT %s
		FROM applies a
		WHERE (
				(a.state = ? AND (
					EXISTS (SELECT 1 FROM tasks t WHERE t.apply_id = a.id)
					OR EXISTS (SELECT 1 FROM apply_operations ao WHERE ao.apply_id = a.id)
				))
				OR (a.state IN (%s) AND a.updated_at < %s)
				OR (a.state = ? AND a.attempt < ? AND a.updated_at >= %s)
				OR (
					a.state = ?
					AND EXISTS (
						SELECT 1
						FROM apply_control_requests cr
						WHERE cr.apply_id = a.id AND cr.operation = ? AND cr.status = ?
					)
				)
				OR (
					a.state = ?
					AND EXISTS (
						SELECT 1
						FROM apply_control_requests cr
						WHERE cr.apply_id = a.id AND cr.operation = ? AND cr.status = ?
					)
				)
				OR (
					a.state = ?
					AND EXISTS (
						SELECT 1
						FROM apply_control_requests cr
						WHERE cr.apply_id = a.id AND cr.operation = ? AND cr.status = ?
						AND (
							a.lease_acquired_at IS NULL
							OR a.lease_acquired_at < cr.updated_at
							OR a.updated_at < %s
						)
					)
				)
			)
		ORDER BY a.created_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, applyColumns, activeStatePlaceholders, staleClaimCutoff, retryFreshnessCutoff, staleClaimCutoff), queryArgs...)

	apply, err := scanApplyInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // No apply to claim
	}
	if err != nil {
		return nil, fmt.Errorf("query next claimable apply: %w", err)
	}

	outcome, err := persistApplyClaim(ctx, s.db, tx, apply, owner)
	if err != nil {
		return nil, err
	}
	if outcome.claimedAndComplete() {
		return apply, nil
	}
	if outcome != claimAcquired {
		return nil, nil
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim apply %d (%s): %w", apply.ID, apply.ApplyIdentifier, err)
	}

	return apply, nil
}

// ClaimApplyByID atomically claims one specific apply by ID using the same
// claimability rules as FindNextApply, scoped to a single row. The operation-
// level claim loop calls this after claiming an apply_operations row to acquire
// the parent apply lease that lease-guarded writes (ResumeApply, MarkCompleted,
// Heartbeat) require. Returns nil when the apply does not exist, is locked by a
// peer (SKIP LOCKED), is not currently claimable, or — for a stopped apply with
// a pending start request — the claim was refused because another active apply
// owns the target (the start request is failed in that case; see
// persistApplyClaim).
func (s *applyStore) ClaimApplyByID(ctx context.Context, applyID int64, owner string) (*storage.Apply, error) {
	if owner == "" {
		return nil, fmt.Errorf("operator owner is required to claim apply %d: %w", applyID, storage.ErrApplyLeaseLost)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin claim apply %d transaction: %w", applyID, err)
	}
	defer rollbackTx(ctx, tx, "claim apply by id")

	activeStates := claimableApplyStates()
	activeStatePlaceholders := placeholders(len(activeStates))
	queryArgs := []any{applyID, state.Apply.Pending}
	queryArgs = append(queryArgs, stringArgs(activeStates)...)
	queryArgs = append(queryArgs, state.Apply.FailedRetryable, maxRecoveryAttempts, retryableRecoveryFreshnessDays)
	queryArgs = append(queryArgs,
		state.Apply.Pending,
		storage.ControlOperationStart, storage.ControlRequestPending)
	queryArgs = append(queryArgs,
		state.Apply.Stopped,
		storage.ControlOperationStart, storage.ControlRequestPending)
	queryArgs = append(queryArgs,
		state.Apply.WaitingForDeploy,
		storage.ControlOperationStart, storage.ControlRequestPending)

	staleClaimCutoff := s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, LiteralIntervalAmount(uint64(storage.ApplyLeaseStaleAfter.Microseconds())), IntervalMicrosecond)
	retryFreshnessCutoff := s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, ParameterIntervalAmount(), IntervalDay)

	row := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT %s
		FROM applies a
		WHERE a.id = ?
			AND (
				(a.state = ? AND (
					EXISTS (SELECT 1 FROM tasks t WHERE t.apply_id = a.id)
					OR EXISTS (SELECT 1 FROM apply_operations ao WHERE ao.apply_id = a.id)
				))
				OR (a.state IN (%s) AND a.updated_at < %s)
				OR (a.state = ? AND a.attempt < ? AND a.updated_at >= %s)
				OR (
					a.state = ?
					AND EXISTS (
						SELECT 1
						FROM apply_control_requests cr
						WHERE cr.apply_id = a.id AND cr.operation = ? AND cr.status = ?
					)
				)
				OR (
					a.state = ?
					AND EXISTS (
						SELECT 1
						FROM apply_control_requests cr
						WHERE cr.apply_id = a.id AND cr.operation = ? AND cr.status = ?
					)
				)
				OR (
					a.state = ?
					AND EXISTS (
						SELECT 1
						FROM apply_control_requests cr
						WHERE cr.apply_id = a.id AND cr.operation = ? AND cr.status = ?
						AND (
							a.lease_acquired_at IS NULL
							OR a.lease_acquired_at < cr.updated_at
							OR a.updated_at < %s
						)
					)
				)
			)
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, applyColumns, activeStatePlaceholders, staleClaimCutoff, retryFreshnessCutoff, staleClaimCutoff), queryArgs...)

	apply, err := scanApplyInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // apply does not exist, is locked, or is not claimable
	}
	if err != nil {
		return nil, fmt.Errorf("query claimable apply %d: %w", applyID, err)
	}

	outcome, err := persistApplyClaim(ctx, s.db, tx, apply, owner)
	if err != nil {
		return nil, err
	}
	if outcome.claimedAndComplete() {
		return apply, nil
	}
	if outcome != claimAcquired {
		return nil, nil
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim apply %d (%s): %w", apply.ID, apply.ApplyIdentifier, err)
	}

	return apply, nil
}

// FindNextApplyForStopReconciliation claims one apply eligible for stop
// reconciliation (pending or an active recovery-claimable state; see
// claimableApplyStates) that has a
// pending stop control request, at least one pending operation, and no operation
// currently being driven, so the operator can terminalize the pending siblings
// and let the apply settle. See the interface doc for why this trigger is needed
// under on_failure "continue".
func (s *applyStore) FindNextApplyForStopReconciliation(ctx context.Context, owner string) (*storage.Apply, error) {
	if owner == "" {
		return nil, fmt.Errorf("operator owner is required to claim apply for stop reconciliation: %w", storage.ErrApplyLeaseLost)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin claim apply for stop reconciliation transaction: %w", err)
	}
	defer rollbackTx(ctx, tx, "claim apply for stop reconciliation")

	// Parent eligibility: pending and paused plus the active recovery-claimable
	// states (claimableApplyStates); the resumable failed_retryable and stopped
	// states are excluded because they have their own resume paths. pending is
	// included so a stop requested before the first operation is ever claimed is
	// not stranded — the claim gate refuses the pending ops, so reconciliation
	// must own them; persistApplyClaim transitions a pending apply to running for
	// the claim. paused is included so an on_failure='pause' rollout that an
	// operator stops (rather than releases) is terminalized: paused holds the
	// later siblings pending behind the failed one, and paused is deliberately
	// absent from claimableApplyStates (it needs an explicit human decision, not
	// stale-heartbeat recovery), so without it here a stopped paused apply would
	// strand its pending siblings forever.
	parentStates := append([]string{state.Apply.Pending, state.Apply.Paused}, claimableApplyStates()...)
	parentStatePlaceholders := placeholders(len(parentStates))

	// A child operation in any of these states is being driven, or is a crashed
	// driver awaiting stale recovery. Either way the operation-claim path owns
	// settling it through the engine (its drive observes the stop), so this path
	// skips the apply whenever any operation is active — stale or fresh — and
	// handles only applies with nothing active left to drive. recoverApplyPendingStop
	// runs before the operation claim, so an active operation falls through to be
	// recovered there, then a later tick reconciles the remaining pending siblings.
	activeOpStates := claimableApplyStates()
	activeOpStatePlaceholders := placeholders(len(activeOpStates))

	queryArgs := stringArgs(parentStates)
	queryArgs = append(queryArgs, storage.ControlOperationStop, storage.ControlRequestPending)
	queryArgs = append(queryArgs, state.ApplyOperation.Pending)
	queryArgs = append(queryArgs, stringArgs(activeOpStates)...)

	row := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT %s
		FROM applies a
		WHERE a.state IN (%s)
			AND EXISTS (
				SELECT 1
				FROM apply_control_requests cr
				WHERE cr.apply_id = a.id AND cr.operation = ? AND cr.status = ?
			)
			AND EXISTS (
				SELECT 1
				FROM apply_operations pending
				WHERE pending.apply_id = a.id AND pending.state = ?
			)
			AND NOT EXISTS (
				SELECT 1
				FROM apply_operations active
				WHERE active.apply_id = a.id
					AND active.state IN (%s)
			)
		ORDER BY a.created_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, applyColumns, parentStatePlaceholders, activeOpStatePlaceholders), queryArgs...)

	apply, err := scanApplyInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // nothing to reconcile
	}
	if err != nil {
		return nil, fmt.Errorf("query next apply for stop reconciliation: %w", err)
	}

	// persistApplyClaim rotates the lease: an active apply just refreshes its
	// heartbeat, while a pending apply transitions to running for the claim (no
	// stopped apply reaches this path — terminal states are excluded above — so
	// claimAcquiredCommitted, the stopped-only outcome, cannot occur and the
	// caller always commits an acquired claim).
	outcome, err := persistApplyClaim(ctx, s.db, tx, apply, owner)
	if err != nil {
		return nil, err
	}
	if outcome != claimAcquired {
		return nil, nil
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim apply %d (%s) for stop reconciliation: %w", apply.ID, apply.ApplyIdentifier, err)
	}

	return apply, nil
}

// claimOutcome describes how persistApplyClaim resolved a claim attempt and
// who owns committing the transaction.
type claimOutcome int

const (
	// claimLostRace means a concurrent writer moved the row between the SELECT
	// and the guarded UPDATE; the caller must roll back and back off.
	claimLostRace claimOutcome = iota
	// claimAcquired means the claim succeeded and the caller must commit tx.
	claimAcquired
	// claimAcquiredCommitted means a stopped→running claim succeeded and was
	// already committed inside persistApplyClaim under the apply-target lock, so
	// the caller must return the apply without committing again.
	claimAcquiredCommitted
	// claimRefusedActiveTarget means a stopped→running claim was refused because
	// another active apply already owns the target. The pending start control
	// request was failed and committed inside persistApplyClaim, so the caller
	// must not claim and must not commit again.
	claimRefusedActiveTarget
)

// claimedAndComplete reports whether the outcome both acquired the claim and
// requires no further commit from the caller — i.e. the stopped path committed
// under the apply-target lock.
func (o claimOutcome) claimedAndComplete() bool {
	return o == claimAcquiredCommitted
}

// persistApplyClaim rotates the lease (owner, token, acquired_at) onto apply in
// memory and persists it inside tx, refreshing the heartbeat in the same write.
// Pending, stopped-start, and retryable applies move into running as part of the
// claim so another driver cannot immediately re-claim the same durable request
// after the transaction releases its row lock; active applies just refresh the
// heartbeat.
//
// A stopped→running claim re-checks the one-active-apply-per-target invariant
// under the apply-target lock before transitioning, because a stopped apply is
// not "active" and a newer apply may have been created for the same target while
// it sat stopped. The lock is held across the transition and the commit so a
// concurrent create cannot slip a second active apply onto the target in the
// window between the re-check and the commit. When another active apply owns the
// target the claim is refused and the pending start control request is failed so
// the operator sees why.
func persistApplyClaim(ctx context.Context, db *sql.DB, tx *sql.Tx, apply *storage.Apply, owner string) (claimOutcome, error) {
	leaseToken := uuid.NewString()
	leaseAcquiredAt := time.Now()
	apply.LeaseOwner = owner
	apply.LeaseToken = leaseToken
	apply.LeaseAcquiredAt = &leaseAcquiredAt

	if state.IsState(apply.State, state.Apply.Stopped) {
		return claimStoppedApplyUnderTargetLock(ctx, db, tx, apply, owner, leaseToken)
	}

	if isStartingClaim(apply.State) {
		claimed, err := transitionClaimToState(ctx, tx, apply, state.Apply.Running, owner, leaseToken)
		if err != nil {
			return claimLostRace, err
		}
		if !claimed {
			return claimLostRace, nil
		}
		return claimAcquired, nil
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE applies
		SET updated_at = NOW(),
		    lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
		WHERE id = ?
	`, owner, leaseToken, apply.ID); err != nil {
		return claimLostRace, fmt.Errorf("refresh heartbeat for claimed apply %d (%s): %w", apply.ID, apply.ApplyIdentifier, err)
	}
	return claimAcquired, nil
}

// claimStoppedApplyUnderTargetLock drives the full stopped→resuming claim while
// holding the apply-target lock: re-check the one-active-apply-per-target
// invariant, then either transition+commit or refuse+commit. It commits inside
// the lock so a concurrent create cannot add a second active apply between the
// re-check and the commit. The lock is released only after the commit.
func claimStoppedApplyUnderTargetLock(ctx context.Context, db *sql.DB, tx *sql.Tx, apply *storage.Apply, owner, leaseToken string) (claimOutcome, error) {
	database, dbType, environment, deployment, err := applyTargetForUpdate(ctx, tx, apply)
	if err != nil {
		return claimLostRace, err
	}
	if !hasApplyTarget(database, dbType, environment) {
		return claimLostRace, fmt.Errorf("stopped apply %d (%s) is missing target metadata for claim re-check", apply.ID, apply.ApplyIdentifier)
	}

	conn, lockName, err := acquireApplyTargetLockConn(ctx, db, database, dbType, environment)
	if err != nil {
		return claimLostRace, err
	}
	defer releaseApplyTargetLockConn(ctx, conn, lockName, "claim stopped apply")

	deployments, err := operationDeploymentsForApply(ctx, tx, apply.ID)
	if err != nil {
		return claimLostRace, err
	}
	deployments = append(deployments, deployment)
	if err := checkNoActiveApplyForTargets(ctx, tx, database, dbType, environment, deployments, apply.ID); err != nil {
		if !errors.Is(err, storage.ErrActiveApplyExists) {
			return claimLostRace, err
		}
		return refuseStoppedClaimForActiveTarget(ctx, tx, apply, owner, database, dbType, environment)
	}

	claimed, err := transitionClaimToState(ctx, tx, apply, state.Apply.Resuming, owner, leaseToken)
	if err != nil {
		return claimLostRace, err
	}
	if !claimed {
		return claimLostRace, nil
	}
	if err := tx.Commit(); err != nil {
		return claimLostRace, fmt.Errorf("commit claim stopped apply %d (%s) on %s/%s/%s: %w", apply.ID, apply.ApplyIdentifier, database, dbType, environment, err)
	}
	return claimAcquiredCommitted, nil
}

// isStartingClaim reports whether a claim of an apply in this state transitions
// it into running (as opposed to only refreshing the heartbeat of an already
// active apply).
func isStartingClaim(applyState string) bool {
	return state.IsState(applyState, state.Apply.Pending, state.Apply.Stopped, state.Apply.FailedRetryable)
}

// transitionClaimToState performs the guarded claim transition that rotates the
// lease and moves the apply into targetState. A pending or failed_retryable
// claim transitions to running; a stopped claim transitions to resuming, the
// transient state held while the driver waits for the data plane to leave
// stopped after a start. WHERE state = ? guards against a concurrent transition
// landing between the SELECT and this UPDATE; a zero rows-affected result means
// another driver already moved the row, so the caller backs off cleanly. Reports
// false on that lost race.
func transitionClaimToState(ctx context.Context, tx *sql.Tx, apply *storage.Apply, targetState, owner, leaseToken string) (bool, error) {
	result, err := tx.ExecContext(ctx, `
		UPDATE applies
		SET state = ?, updated_at = NOW(),
		    lease_owner = ?, lease_token = ?, lease_acquired_at = NOW(),
		    attempt = CASE WHEN ? = ? THEN attempt + 1 ELSE attempt END,
		    completed_at = NULL,
		    error_message = CASE WHEN ? = ? THEN '' ELSE error_message END
		WHERE id = ? AND state = ?
	`, targetState, owner, leaseToken, apply.State, state.Apply.FailedRetryable, apply.State, state.Apply.FailedRetryable, apply.ID, apply.State)
	if err != nil {
		return false, fmt.Errorf("claim apply %d (%s) in state %s: %w", apply.ID, apply.ApplyIdentifier, apply.State, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read claim rows affected for apply %d (%s): %w", apply.ID, apply.ApplyIdentifier, err)
	}
	if rows == 0 {
		return false, nil
	}
	if state.IsState(apply.State, state.Apply.FailedRetryable) {
		apply.Attempt++
		apply.ErrorMessage = ""
	}
	return true, nil
}

// refuseStoppedClaimForActiveTarget fails the pending start control request for
// a stopped apply that cannot be resumed because another active apply owns the
// target, and commits that failure inside tx. The start was accepted but cannot
// be honored, so leaving the request pending would silently strand it; failing
// it surfaces the reason to the operator. Returns claimRefusedActiveTarget.
func refuseStoppedClaimForActiveTarget(ctx context.Context, tx *sql.Tx, apply *storage.Apply, owner, database, dbType, environment string) (claimOutcome, error) {
	reason := fmt.Sprintf("start refused: another active apply exists for %s/%s/%s", database, dbType, environment)
	if err := failPendingStartControlRequestTx(ctx, tx, apply.ID, reason); err != nil {
		return claimLostRace, fmt.Errorf("fail pending start control request for apply %d (%s) on %s/%s/%s: %w", apply.ID, apply.ApplyIdentifier, database, dbType, environment, err)
	}
	if err := tx.Commit(); err != nil {
		return claimLostRace, fmt.Errorf("commit refused stopped claim for apply %d (%s) on %s/%s/%s: %w", apply.ID, apply.ApplyIdentifier, database, dbType, environment, err)
	}
	slog.WarnContext(ctx, "claim refused: another active apply exists for target; failing pending start control request",
		"apply_id", apply.ApplyIdentifier,
		"database", database,
		"database_type", dbType,
		"environment", environment,
		"lease_owner", owner,
		"reason", reason)
	return claimRefusedActiveTarget, nil
}

// failPendingStartControlRequestTx marks the pending 'start' control request for
// an apply failed inside the supplied claim transaction so the refusal and the
// failed request commit atomically. Mirrors controlRequestStore.FailPending's
// SQL; this path holds no apply lease, so it is unguarded by lease token.
func failPendingStartControlRequestTx(ctx context.Context, tx *sql.Tx, applyID int64, reason string) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE apply_control_requests
		SET status = ?, error_message = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE apply_id = ? AND operation = ? AND status = ?
	`, storage.ControlRequestFailed, reason, applyID, storage.ControlOperationStart, storage.ControlRequestPending)
	if err != nil {
		return fmt.Errorf("update apply_control_requests for apply %d: %w", applyID, err)
	}
	return nil
}

// Heartbeat updates the apply's updated_at timestamp to maintain the lease.
// Should be called every 10 seconds while working on an apply.
// If not called for > 1 minute, another driver can claim the apply via FindNextApply.
// When ctx has an apply lease, a stale token returns ErrApplyLeaseLost so the
// old operator owner stops before writing state or external side effects.
// SetRevertSkipped records when skip-revert was dispatched for an apply. It is a
// targeted update of revert_skipped_at only — it touches no other apply fields
// and is not lease-guarded — so both the control-plane skip-revert handler (no
// lease) and the data-plane finalizer can set this visibility flag.
//
// updated_at is pinned to its current value so this write does not trip the
// column's ON UPDATE CURRENT_TIMESTAMP. updated_at is the apply's lease
// heartbeat (the staleness gate in FindNextApply); bumping it here would renew
// the heartbeat from a non-lease caller and could delay another driver's
// recovery claim.
func (s *applyStore) SetRevertSkipped(ctx context.Context, applyID int64, at time.Time) error {
	if _, err := s.db.ExecContext(ctx,
		`UPDATE applies SET revert_skipped_at = ?, updated_at = updated_at WHERE id = ?`, at, applyID); err != nil {
		return fmt.Errorf("set revert_skipped_at for apply %d: %w", applyID, err)
	}
	return nil
}

func (s *applyStore) Heartbeat(ctx context.Context, applyID int64) error {
	// A drive that holds only an operation lease must never bump the parent
	// applies row: under fan-out the parent's liveness is owned by the parent
	// lease and the rollout projection, not a per-operation drive. A
	// single-operation drive still carries the parent apply lease alongside the
	// operation lease, so it keeps the heartbeat; only an operation-lease-only
	// context is refused here. Mirrors the guard in Update.
	if _, hasOpLease := storage.OperationLeaseFromContext(ctx); hasOpLease {
		if _, hasApplyLease := storage.ApplyLeaseFromContext(ctx); !hasApplyLease {
			return fmt.Errorf("operation lease does not authorize a heartbeat of apply %d; parent liveness is owned by the parent lease: %w", applyID, storage.ErrApplyLeaseLost)
		}
	}

	lease, hasLease, err := applyLeaseFromContext(ctx, applyID)
	if err != nil {
		return err
	}
	args := []any{applyID}
	leasePredicate := ""
	if hasLease {
		leasePredicate = " AND lease_token = ?"
		args = append(args, lease.Token)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE applies SET updated_at = NOW() WHERE id = ?`+leasePredicate+`
	`, args...)
	if err != nil {
		return fmt.Errorf("heartbeat apply %d: %w", applyID, err)
	}
	if hasLease {
		if _, err := confirmLeaseOnZeroRows(ctx, s.db, result, lease, "heartbeat", fmt.Sprintf("apply %d", applyID)); err != nil {
			return err
		}
	}
	return nil
}

func (s *applyStore) CheckLease(ctx context.Context, lease storage.ApplyLease) error {
	if !lease.Valid() {
		return fmt.Errorf("invalid apply lease for apply %d: %w", lease.ApplyID, storage.ErrApplyLeaseLost)
	}
	return ensureApplyLeaseStillOwned(ctx, s.db, lease)
}

// ExpireRetryable transitions failed_retryable applies that exhausted their
// retry budget or recovery freshness window to permanent failed. Returns the
// applies updated.
func (s *applyStore) ExpireRetryable(ctx context.Context) ([]*storage.RetryableApplyExpiration, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin expire retryable applies transaction: %w", err)
	}
	defer rollbackTx(ctx, tx, "expire retryable applies")

	rows, err := tx.QueryContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE state = ? AND (
			attempt >= ?
			OR updated_at < `+s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, ParameterIntervalAmount(), IntervalDay)+`
		)
		FOR UPDATE
	`, state.Apply.FailedRetryable, maxRecoveryAttempts, retryableRecoveryFreshnessDays)
	if err != nil {
		return nil, fmt.Errorf("query expired retryable applies: %w", err)
	}
	applies, err := scanApplies(rows)
	utils.CloseAndLog(rows)
	if err != nil {
		return nil, fmt.Errorf("scan expired retryable applies: %w", err)
	}
	if len(applies) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit empty expire retryable applies: %w", err)
		}
		return nil, nil
	}

	applyIDs := make([]any, 0, len(applies))
	expirations := make([]*storage.RetryableApplyExpiration, 0, len(applies))
	for _, apply := range applies {
		applyIDs = append(applyIDs, apply.ID)
		expirations = append(expirations, &storage.RetryableApplyExpiration{
			Apply:  apply,
			Reason: retryableExpirationReason(apply),
		})
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

	// Terminalize the apply's retryable operation rows alongside the apply: once
	// the apply is being expired — whether its retry budget is spent or its
	// recovery freshness window has elapsed — the rollout's verdict is final, so
	// a per-deployment operation that was still retryable is permanently failed,
	// not retryable. The deployment-order claim gates read earlier.state from
	// apply_operations, so a row left failed_retryable would keep blocking a
	// healthy later deployment under on_failure "continue" even though the
	// rollout has already failed. Only failed_retryable rows are flipped — a
	// successor parked at waiting_for_cutover is a healthy deployment that must
	// still be allowed to cut over, so it is left untouched.
	opArgs := append([]any{state.ApplyOperation.Failed, state.ApplyOperation.FailedRetryable}, applyIDs...)
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE apply_operations
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE state = ? AND apply_id IN (%s)
	`, placeholders(len(applyIDs))), opArgs...)
	if err != nil {
		return nil, fmt.Errorf("expire retryable apply_operations: %w", err)
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
	return expirations, nil
}

// ReapplyFailed transitions a recent permanently failed apply back onto the
// retryable recovery path so operator drivers can claim and drive the
// remaining failed work.
func (s *applyStore) ReapplyFailed(ctx context.Context, applyID int64) (*storage.Apply, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin reapply failed apply %d transaction: %w", applyID, err)
	}
	defer rollbackTx(ctx, tx, "reapply failed apply")

	row := tx.QueryRowContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE id = ?
		FOR UPDATE
	`, applyID)
	apply, err := scanApply(row)
	if err != nil {
		return nil, fmt.Errorf("load apply %d for reapply: %w", applyID, err)
	}
	if apply == nil {
		return nil, storage.ErrApplyNotFound
	}
	if !state.IsState(apply.State, state.Apply.Failed) {
		return nil, fmt.Errorf("apply %s is %s; only failed applies can be reapplied: %w", apply.ApplyIdentifier, apply.State, storage.ErrApplyNotReappliable)
	}
	if apply.CompletedAt == nil {
		return nil, fmt.Errorf("apply %s has no failure completion time; create a new apply instead: %w", apply.ApplyIdentifier, storage.ErrApplyNotReappliable)
	}
	if apply.CompletedAt.Before(time.Now().AddDate(0, 0, -storage.ReapplyFailureFreshnessDays)) {
		return nil, fmt.Errorf("apply %s failed more than %d day(s) ago; create a new apply instead: %w", apply.ApplyIdentifier, storage.ReapplyFailureFreshnessDays, storage.ErrApplyNotReappliable)
	}

	database, dbType, environment, deployment := apply.Database, apply.DatabaseType, apply.Environment, apply.Deployment
	if !hasApplyTarget(database, dbType, environment) || deployment == "" {
		database, dbType, environment, deployment, err = applyTargetForUpdate(ctx, tx, apply)
		if err != nil {
			return nil, err
		}
	}
	if !hasApplyTarget(database, dbType, environment) || deployment == "" {
		return nil, fmt.Errorf("apply %s is missing target metadata for reapply: %w", apply.ApplyIdentifier, storage.ErrApplyNotReappliable)
	}

	conn, lockName, err := acquireApplyTargetLockConn(ctx, s.db, database, dbType, environment)
	if err != nil {
		return nil, err
	}
	defer releaseApplyTargetLockConn(ctx, conn, lockName, "reapply failed apply")

	deployments, err := operationDeploymentsForApply(ctx, tx, apply.ID)
	if err != nil {
		return nil, err
	}
	deployments = append(deployments, deployment)
	if err := checkNoActiveApplyForTargets(ctx, tx, database, dbType, environment, deployments, apply.ID); err != nil {
		return nil, err
	}
	if err := rejectNonReappliableOperations(ctx, tx, apply); err != nil {
		return nil, err
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE tasks t
		LEFT JOIN apply_operations ao ON ao.id = t.apply_operation_id AND ao.apply_id = t.apply_id
		SET t.state = ?, t.error_message = NULL, t.completed_at = NULL, t.updated_at = NOW()
		WHERE t.apply_id = ?
			AND t.state IN (?, ?)
			AND (t.apply_operation_id IS NULL OR ao.state = ?)
	`, state.Task.FailedRetryable, apply.ID, state.Task.Failed, state.Task.Cancelled, state.ApplyOperation.Failed); err != nil {
		return nil, fmt.Errorf("mark failed tasks retryable for apply %s: %w", apply.ApplyIdentifier, err)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE apply_operations
		SET state = ?, error_message = NULL, completed_at = NULL, updated_at = NOW(),
		    lease_owner = '', lease_token = '', lease_acquired_at = NULL
		WHERE apply_id = ? AND state = ?
	`, state.ApplyOperation.FailedRetryable, apply.ID, state.ApplyOperation.Failed); err != nil {
		return nil, fmt.Errorf("mark failed operations retryable for apply %s: %w", apply.ApplyIdentifier, err)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE applies
		SET state = ?, error_message = '', attempt = 0, completed_at = NULL, updated_at = NOW(),
		    lease_owner = '', lease_token = '', lease_acquired_at = NULL
		WHERE id = ? AND state = ?
	`, state.Apply.FailedRetryable, apply.ID, state.Apply.Failed)
	if err != nil {
		return nil, fmt.Errorf("mark apply %s retryable for reapply: %w", apply.ApplyIdentifier, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("read reapply rows affected for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if rows == 0 {
		return nil, fmt.Errorf("apply %s changed before reapply: %w", apply.ApplyIdentifier, storage.ErrApplyNotReappliable)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit reapply failed apply %s: %w", apply.ApplyIdentifier, err)
	}

	apply.State = state.Apply.FailedRetryable
	apply.ErrorMessage = ""
	apply.Attempt = 0
	apply.CompletedAt = nil
	apply.UpdatedAt = time.Now()
	apply.LeaseOwner = ""
	apply.LeaseToken = ""
	apply.LeaseAcquiredAt = nil
	return apply, nil
}

func rejectNonReappliableOperations(ctx context.Context, tx *sql.Tx, apply *storage.Apply) error {
	var count int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM apply_operations
		WHERE apply_id = ? AND state IN (?, ?, ?)
	`, apply.ID, state.ApplyOperation.Stopped, state.ApplyOperation.Cancelled, state.ApplyOperation.Reverted).Scan(&count); err != nil {
		return fmt.Errorf("check non-reappliable operations for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if count > 0 {
		return fmt.Errorf("apply %s has %d stopped, cancelled, or reverted operation(s); create a new apply instead: %w", apply.ApplyIdentifier, count, storage.ErrApplyNotReappliable)
	}
	return nil
}

func retryableExpirationReason(apply *storage.Apply) storage.RetryableExpirationReason {
	if apply.Attempt >= maxRecoveryAttempts {
		return storage.RetryableExpirationAttemptBudget
	}
	return storage.RetryableExpirationRecoveryWindow
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

// FindMissingSummaryComment returns GitHub-backed applies that reached a
// terminal state recently but whose progress comment was never followed by a
// summary comment. Used to post missing summaries after restart.
//
// Two forms of "missing" qualify: no summary marker at all (the publisher never
// ran), and a summary claim sentinel (github_comment_id = 0) stale for longer
// than storage.SummaryClaimStaleAfter (the publisher claimed, then crashed
// before posting). A fresh sentinel is an in-flight publish and is left alone.
//
// Recency is judged per state family: most terminal states stamp completed_at,
// but a stopped apply is resumable and keeps completed_at NULL, so it qualifies
// by updated_at instead.
func (s *applyStore) FindMissingSummaryComment(ctx context.Context) ([]*storage.Apply, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyColumnsForApplyAlias+`
		FROM applies a
		JOIN apply_comments acp ON acp.apply_id = a.id AND acp.comment_state = 'progress'
		LEFT JOIN apply_comments acs ON acs.apply_id = a.id AND acs.comment_state = 'summary'
		WHERE a.repository != ''
		  AND a.pull_request > 0
		  AND a.installation_id > 0
		  AND (
			(a.state IN (?, ?, ?, ?) AND a.completed_at > `+s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, LiteralIntervalAmount(1), IntervalHour)+`)
			OR (a.state = ? AND a.updated_at > `+s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, LiteralIntervalAmount(1), IntervalHour)+`)
		  )
		  AND (
			acs.id IS NULL
			OR (acs.github_comment_id = 0 AND acs.updated_at < `+s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, ParameterIntervalAmount(), IntervalSecond)+`)
		  )
		ORDER BY a.updated_at DESC
	`, state.Apply.Completed, state.Apply.Failed, state.Apply.Reverted, state.Apply.Cancelled,
		state.Apply.Stopped, int64(storage.SummaryClaimStaleAfter.Seconds()))
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

// ExistsForDatabaseHead reports whether any apply for the PR and database was
// created from a plan for headSHA. The LEFT JOIN keeps applies whose plan row
// was deleted: without the plan there is no proof of which head the apply came
// from, so it must count as matching any head rather than silently dropping
// out of the result.
func (s *applyStore) ExistsForDatabaseHead(ctx context.Context, repo string, pr int, database, databaseType, headSHA string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM applies a
			LEFT JOIN plans p ON p.id = a.plan_id
			WHERE a.repository = ? AND a.pull_request = ?
				AND a.database_name = ? AND a.database_type = ?
				AND (p.id IS NULL OR p.head_sha = ?)
		)
	`, repo, pr, database, databaseType, headSHA).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check applies for %s#%d database %s (%s) head %s: %w", repo, pr, database, databaseType, headSHA, err)
	}
	return exists, nil
}

// Delete removes an apply by ID, along with its per-deployment
// apply_operations rows, in a single transaction. Deleting the children
// transactionally prevents orphan operation rows that the operator claim loop
// would otherwise re-claim forever (their parent lookup returns nil).
func (s *applyStore) Delete(ctx context.Context, id int64) error {
	const opName = "delete apply"
	writeTx, err := beginApplyWriteTx(ctx, s.db, opName)
	if err != nil {
		return err
	}
	defer writeTx.close(ctx, opName)

	if _, err := writeTx.tx.ExecContext(ctx, `
		DELETE FROM apply_operations WHERE apply_id = ?
	`, id); err != nil {
		return fmt.Errorf("delete apply_operations for apply %d: %w", id, err)
	}

	result, err := writeTx.tx.ExecContext(ctx, `
		DELETE FROM applies WHERE id = ?
	`, id)
	if err != nil {
		return fmt.Errorf("delete apply %d: %w", id, err)
	}
	if err := checkRowsAffected(result, storage.ErrApplyNotFound); err != nil {
		return err
	}

	if err := writeTx.commit(); err != nil {
		return fmt.Errorf("commit delete apply %d: %w", id, err)
	}
	return nil
}

// DeleteByPR removes all applies for a PR, along with their per-deployment
// apply_operations rows, in a single transaction. Deleting the children
// transactionally prevents orphan operation rows that the operator claim loop
// would otherwise re-claim forever (their parent lookup returns nil).
func (s *applyStore) DeleteByPR(ctx context.Context, repo string, pr int) error {
	const opName = "delete applies by PR"
	writeTx, err := beginApplyWriteTx(ctx, s.db, opName)
	if err != nil {
		return err
	}
	defer writeTx.close(ctx, opName)

	if _, err := writeTx.tx.ExecContext(ctx, `
		DELETE ao FROM apply_operations ao
		JOIN applies a ON a.id = ao.apply_id
		WHERE a.repository = ? AND a.pull_request = ?
	`, repo, pr); err != nil {
		return fmt.Errorf("delete apply_operations for PR %s#%d: %w", repo, pr, err)
	}

	if _, err := writeTx.tx.ExecContext(ctx, `
		DELETE FROM applies WHERE repository = ? AND pull_request = ?
	`, repo, pr); err != nil {
		return fmt.Errorf("delete applies for PR %s#%d: %w", repo, pr, err)
	}

	if err := writeTx.commit(); err != nil {
		return fmt.Errorf("commit delete applies for PR %s#%d: %w", repo, pr, err)
	}
	return nil
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
	var leaseAcquiredAt, startedAt, completedAt, revertSkippedAt sql.NullTime
	var idempotencyKey sql.NullString
	var options []byte

	err := s.Scan(
		&apply.ID, &apply.ApplyIdentifier, &apply.LockID, &apply.PlanID,
		&apply.Database, &apply.DatabaseType,
		&apply.Repository, &apply.PullRequest, &apply.Environment, &apply.Deployment,
		&apply.Caller, &apply.InstallationID, &apply.ExternalID, &idempotencyKey, &apply.Engine,
		&apply.State, &apply.ErrorMessage, &options, &apply.Attempt,
		&apply.LeaseOwner, &apply.LeaseToken, &leaseAcquiredAt,
		&apply.CreatedAt, &startedAt, &completedAt, &apply.UpdatedAt, &revertSkippedAt,
	)
	if err != nil {
		return nil, err
	}

	apply.IdempotencyKey = idempotencyKey.String
	apply.Options = options

	if leaseAcquiredAt.Valid {
		apply.LeaseAcquiredAt = &leaseAcquiredAt.Time
	}

	if startedAt.Valid {
		apply.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		apply.CompletedAt = &completedAt.Time
	}
	if revertSkippedAt.Valid {
		apply.RevertSkippedAt = &revertSkippedAt.Time
	}

	return &apply, nil
}
