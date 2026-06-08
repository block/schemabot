// apply_operations.go implements ApplyOperationStore for per-(apply,
// deployment) child rows under a multi-deployment apply — the unit of work
// the operator claims.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// applyOperationColumns lists all columns for SELECT queries.
const applyOperationColumns = `id, apply_id, deployment, target, state, error_message,
	started_at, completed_at, engine_resume_context, engine_resume_metadata,
	created_at, updated_at`

// mysqlErrDupEntry is MySQL's error number for a duplicate-key violation.
// Used to translate unique-index conflicts into typed storage errors.
const mysqlErrDupEntry = 1062

// applyOperationStore implements storage.ApplyOperationStore using MySQL.
type applyOperationStore struct {
	db *sql.DB
}

// Insert stores a new apply_operations row and returns its ID.
// Translates a unique-key conflict on (apply_id, deployment) into
// storage.ErrApplyOperationExists so callers can branch cleanly.
func (s *applyOperationStore) Insert(ctx context.Context, ad *storage.ApplyOperation) (int64, error) {
	return insertApplyOperation(ctx, s.db, ad)
}

// sqlExecer is the subset of *sql.DB / *sql.Tx used by insertApplyOperation.
// Defined locally so the helper can run against either the pool or an
// in-flight transaction (for atomic apply-create dual-writes).
type sqlExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// insertApplyOperation inserts one apply_operations row using the supplied
// executer (pool or transaction). On success the row's ID and State fields
// are set. A duplicate-key violation on (apply_id, deployment) is translated
// to storage.ErrApplyOperationExists for callers to branch on.
func insertApplyOperation(ctx context.Context, exec sqlExecer, ad *storage.ApplyOperation) (int64, error) {
	stateVal := ad.State
	if stateVal == "" {
		stateVal = state.ApplyOperation.Pending
	}

	result, err := exec.ExecContext(ctx, `
		INSERT INTO apply_operations (
			apply_id, deployment, target, state, error_message,
			started_at, completed_at, engine_resume_context, engine_resume_metadata
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		ad.ApplyID, ad.Deployment, ad.Target, stateVal, nullString(ad.ErrorMessage),
		ad.StartedAt, ad.CompletedAt, nullString(ad.EngineResumeContext), nullString(ad.EngineResumeMetadata),
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDupEntry {
			return 0, storage.ErrApplyOperationExists
		}
		return 0, fmt.Errorf("insert apply_operations (apply=%d, deployment=%s): %w", ad.ApplyID, ad.Deployment, err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("last insert id: %w", err)
	}
	ad.ID = id
	ad.State = stateVal
	return id, nil
}

// Get returns a child row by ID, or nil if not found.
func (s *applyOperationStore) Get(ctx context.Context, id int64) (*storage.ApplyOperation, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+applyOperationColumns+`
		FROM apply_operations
		WHERE id = ?
	`, id)
	return scanApplyOperation(row)
}

// GetByApplyAndDeployment returns the child row for (apply_id, deployment), or nil if not found.
func (s *applyOperationStore) GetByApplyAndDeployment(ctx context.Context, applyID int64, deployment string) (*storage.ApplyOperation, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+applyOperationColumns+`
		FROM apply_operations
		WHERE apply_id = ? AND deployment = ?
	`, applyID, deployment)
	return scanApplyOperation(row)
}

// ListByApply returns all child rows for an apply, ordered by id ascending.
func (s *applyOperationStore) ListByApply(ctx context.Context, applyID int64) ([]*storage.ApplyOperation, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyOperationColumns+`
		FROM apply_operations
		WHERE apply_id = ?
		ORDER BY id
	`, applyID)
	if err != nil {
		return nil, fmt.Errorf("query apply_operations for apply %d: %w", applyID, err)
	}
	defer utils.CloseAndLog(rows)

	return scanApplyOperations(rows)
}

// UpdateState transitions a child row to the given state.
// Returns storage.ErrApplyOperationNotFound if no row matches the ID.
//
// Idempotent: re-applying the same state to a row is a no-op and returns nil.
// MySQL's RowsAffected reports rows *changed* (not matched) by default, so a
// repeat call would report 0; we disambiguate with an existence check.
func (s *applyOperationStore) UpdateState(ctx context.Context, id int64, newState string) error {
	lease, hasLease, err := applyOperationLeaseFromContext(ctx)
	if err != nil {
		return err
	}
	leaseJoin, leasePredicate, leaseArgs := applyOperationLeaseSQL(lease, hasLease)
	args := append([]any{newState, id}, leaseArgs...)
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_operations ao
		`+leaseJoin+`
		SET ao.state = ?
		WHERE ao.id = ?`+leasePredicate+`
	`, args...)
	if err != nil {
		return fmt.Errorf("update apply_operations state (id=%d): %w", id, err)
	}
	return s.checkUpdatedOrExists(ctx, result, id, lease, hasLease, false)
}

// MarkStarted sets state=running and stamps started_at=NOW().
// Returns storage.ErrApplyOperationNotFound if no row matches the ID.
//
// Idempotent: COALESCE preserves started_at on repeat calls, so a re-issue
// against an already-started row is a no-op and returns nil.
func (s *applyOperationStore) MarkStarted(ctx context.Context, id int64) error {
	lease, hasLease, err := applyOperationLeaseFromContext(ctx)
	if err != nil {
		return err
	}
	leaseJoin, leasePredicate, leaseArgs := applyOperationLeaseSQL(lease, hasLease)
	args := append([]any{state.ApplyOperation.Running, id}, leaseArgs...)
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_operations ao
		`+leaseJoin+`
		SET ao.state = ?, ao.started_at = COALESCE(ao.started_at, NOW())
		WHERE ao.id = ?`+leasePredicate+`
	`, args...)
	if err != nil {
		return fmt.Errorf("mark apply_operation started (id=%d): %w", id, err)
	}
	return s.checkUpdatedOrExists(ctx, result, id, lease, hasLease, false)
}

// checkUpdatedOrExists returns nil if the UPDATE affected at least one row,
// nil if it affected zero rows but the row exists (idempotent no-op), or
// ErrApplyOperationNotFound if the row truly does not exist.
//
// Needed for idempotent UPDATEs where MySQL's default RowsAffected ("changed"
// rather than "matched") can return 0 for a successful no-op write.
func (s *applyOperationStore) checkUpdatedOrExists(ctx context.Context, result sql.Result, id int64, lease storage.ApplyLease, hasLease bool, missingOK bool) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read apply_operation update rows affected (id=%d): %w", id, err)
	}
	if rows > 0 {
		return nil
	}
	if hasLease {
		if err := ensureApplyLeaseStillOwned(ctx, s.db, lease); err != nil {
			return err
		}
		match, err := s.applyOperationLeaseMatch(ctx, id, lease)
		if err != nil {
			return err
		}
		if !match.Exists {
			return applyOperationMissingResult(id, missingOK)
		}
		if !match.BelongsToLease {
			return fmt.Errorf("apply_operation %d is not owned by apply lease %d: %w", id, lease.ApplyID, storage.ErrApplyLeaseLost)
		}
		return nil
	}
	var exists bool
	if err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM apply_operations WHERE id = ?)`, id,
	).Scan(&exists); err != nil {
		return fmt.Errorf("verify apply_operation exists (id=%d): %w", id, err)
	}
	if !exists {
		return applyOperationMissingResult(id, missingOK)
	}
	return nil
}

func applyOperationMissingResult(id int64, missingOK bool) error {
	if missingOK {
		return nil
	}
	return fmt.Errorf("apply_operation %d not found: %w", id, storage.ErrApplyOperationNotFound)
}

type applyOperationLeaseMatch struct {
	Exists         bool
	BelongsToLease bool
}

func (s *applyOperationStore) applyOperationLeaseMatch(ctx context.Context, id int64, lease storage.ApplyLease) (applyOperationLeaseMatch, error) {
	var applyID int64
	err := s.db.QueryRowContext(ctx, `
		SELECT apply_id
		FROM apply_operations
		WHERE id = ?
	`, id).Scan(&applyID)
	if errors.Is(err, sql.ErrNoRows) {
		return applyOperationLeaseMatch{}, nil
	}
	if err != nil {
		return applyOperationLeaseMatch{}, fmt.Errorf("verify apply_operation lease ownership (id=%d): %w", id, err)
	}
	return applyOperationLeaseMatch{
		Exists:         true,
		BelongsToLease: applyID == lease.ApplyID,
	}, nil
}

func applyOperationLeaseFromContext(ctx context.Context) (storage.ApplyLease, bool, error) {
	lease, ok := storage.ApplyLeaseFromContext(ctx)
	if !ok {
		return storage.ApplyLease{}, false, nil
	}
	if !lease.Valid() {
		return storage.ApplyLease{}, true, fmt.Errorf("invalid apply_operation lease: %w", storage.ErrApplyLeaseLost)
	}
	return lease, true, nil
}

func applyOperationLeaseSQL(lease storage.ApplyLease, hasLease bool) (string, string, []any) {
	if !hasLease {
		return "", "", nil
	}
	return " JOIN applies a ON a.id = ao.apply_id", " AND ao.apply_id = ? AND a.lease_token = ?", []any{lease.ApplyID, lease.Token}
}

// MarkCompleted sets state=completed and stamps completed_at=NOW().
// Returns storage.ErrApplyOperationNotFound if no row matches the ID.
//
// Idempotent: a retry within the same MySQL DATETIME second on an already-
// completed row may leave every column unchanged, producing RowsAffected=0.
// checkUpdatedOrExists disambiguates that no-op from a missing row so we
// don't spuriously return ErrApplyOperationNotFound.
func (s *applyOperationStore) MarkCompleted(ctx context.Context, id int64) error {
	lease, hasLease, err := applyOperationLeaseFromContext(ctx)
	if err != nil {
		return err
	}
	leaseJoin, leasePredicate, leaseArgs := applyOperationLeaseSQL(lease, hasLease)
	args := append([]any{state.ApplyOperation.Completed, id}, leaseArgs...)
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_operations ao
		`+leaseJoin+`
		SET ao.state = ?, ao.completed_at = COALESCE(ao.completed_at, NOW())
		WHERE ao.id = ?`+leasePredicate+`
	`, args...)
	if err != nil {
		return fmt.Errorf("mark apply_operation completed (id=%d): %w", id, err)
	}
	return s.checkUpdatedOrExists(ctx, result, id, lease, hasLease, false)
}

// MarkFailed sets state=failed, error_message, and stamps completed_at=NOW().
// Returns storage.ErrApplyOperationNotFound if no row matches the ID.
//
// Idempotent: same rationale as MarkCompleted — a retry within the same
// DATETIME second on an already-failed row with the same error_message can
// produce RowsAffected=0, which checkUpdatedOrExists disambiguates from
// a missing row.
func (s *applyOperationStore) MarkFailed(ctx context.Context, id int64, errMsg string) error {
	lease, hasLease, err := applyOperationLeaseFromContext(ctx)
	if err != nil {
		return err
	}
	leaseJoin, leasePredicate, leaseArgs := applyOperationLeaseSQL(lease, hasLease)
	args := append([]any{state.ApplyOperation.Failed, nullString(errMsg), id}, leaseArgs...)
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_operations ao
		`+leaseJoin+`
		SET ao.state = ?, ao.error_message = ?, ao.completed_at = COALESCE(ao.completed_at, NOW())
		WHERE ao.id = ?`+leasePredicate+`
	`, args...)
	if err != nil {
		return fmt.Errorf("mark apply_operation failed (id=%d): %w", id, err)
	}
	return s.checkUpdatedOrExists(ctx, result, id, lease, hasLease, false)
}

// SaveEngineResumeState stores opaque engine state on the operation that owns
// the execution. It updates only resume-state columns so callers can persist
// engine progress without changing operation lifecycle state.
func (s *applyOperationStore) SaveEngineResumeState(ctx context.Context, operationID int64, resumeState *storage.EngineResumeState) error {
	if resumeState == nil {
		return fmt.Errorf("save engine resume state for apply_operation %d: resume state is nil", operationID)
	}
	if resumeState.ApplyOperationID != 0 && resumeState.ApplyOperationID != operationID {
		return fmt.Errorf("save engine resume state for apply_operation %d: resume state belongs to apply_operation %d", operationID, resumeState.ApplyOperationID)
	}
	lease, hasLease, err := applyOperationLeaseFromContext(ctx)
	if err != nil {
		return err
	}
	metadata := resumeState.Metadata
	if metadata == "" {
		metadata = "{}"
	}
	leaseJoin, leasePredicate, leaseArgs := applyOperationLeaseSQL(lease, hasLease)
	args := append([]any{nullString(resumeState.MigrationContext), metadata, operationID}, leaseArgs...)
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_operations ao
		`+leaseJoin+`
		SET ao.engine_resume_context = ?, ao.engine_resume_metadata = ?
		WHERE ao.id = ?`+leasePredicate+`
	`, args...)
	if err != nil {
		return fmt.Errorf("save engine resume state for apply_operation %d: %w", operationID, err)
	}
	return s.checkUpdatedOrExists(ctx, result, operationID, lease, hasLease, false)
}

// GetEngineResumeState returns opaque engine state for an operation. Missing
// state is distinct from a missing operation so control/progress callers can
// surface the storage invariant violation clearly.
func (s *applyOperationStore) GetEngineResumeState(ctx context.Context, operationID int64) (*storage.EngineResumeState, error) {
	var contextVal sql.NullString
	var metadata sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT engine_resume_context, engine_resume_metadata
		FROM apply_operations
		WHERE id = ?
	`, operationID).Scan(&contextVal, &metadata)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storage.ErrApplyOperationNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get engine resume state for apply_operation %d: %w", operationID, err)
	}
	if !metadata.Valid || metadata.String == "" {
		return nil, storage.ErrEngineResumeStateNotFound
	}
	return &storage.EngineResumeState{
		ApplyOperationID: operationID,
		MigrationContext: contextVal.String,
		Metadata:         metadata.String,
	}, nil
}

// applyOperationHeartbeatStaleness is the lease window after which a claimed
// apply_operations row may be re-claimed by another worker. Mirrors the apply
// heartbeat staleness in applies.go.
const applyOperationHeartbeatStaleness = "1 MINUTE"

// FindNextApplyOperation atomically claims the next apply_operations row that
// needs attention and refreshes its heartbeat in the same transaction.
//
// Pending rows are transitioned to running and stamped with started_at;
// already-active rows with a stale heartbeat (updated_at older than the
// staleness window) are re-leased without changing their state. Terminal
// rows are never claimed.
//
// Mirrors ApplyStore.FindNextApply: SELECT ... FOR UPDATE SKIP LOCKED to
// avoid worker races, READ COMMITTED isolation to prevent next-key range
// locks from serializing claims across otherwise independent rows.
//
// Pure storage primitive: no caller wires this in yet. The per-deployment
// claim loop arrives in a subsequent PR in the multi-deployment apply
// workstream.
func (s *applyOperationStore) FindNextApplyOperation(ctx context.Context) (*storage.ApplyOperation, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin claim apply_operation transaction: %w", err)
	}
	defer rollbackApplyTx(ctx, tx, "claim apply_operation")

	activeStates := claimableApplyStates()
	activeStatePlaceholders := placeholders(len(activeStates))

	queryArgs := []any{state.ApplyOperation.Pending}
	queryArgs = append(queryArgs, stringArgs(activeStates)...)

	row := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT %s
		FROM apply_operations
		WHERE (
			state = ?
			OR (state IN (%s) AND updated_at < NOW() - INTERVAL %s)
		)
		ORDER BY created_at, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, applyOperationColumns, activeStatePlaceholders, applyOperationHeartbeatStaleness), queryArgs...)

	ad, err := scanApplyOperationInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // nothing to claim
	}
	if err != nil {
		return nil, fmt.Errorf("query next claimable apply_operation: %w", err)
	}

	if ad.State == state.ApplyOperation.Pending {
		// Pending → running: stamp started_at and update the heartbeat in the
		// same write. WHERE state = ? guards against a concurrent transition
		// landing between the SELECT and this UPDATE; RowsAffected == 0 means
		// another writer already moved the row, so we back off cleanly.
		result, err := tx.ExecContext(ctx, `
			UPDATE apply_operations
			SET state = ?, started_at = COALESCE(started_at, NOW()), updated_at = NOW()
			WHERE id = ? AND state = ?
		`, state.ApplyOperation.Running, ad.ID, state.ApplyOperation.Pending)
		if err != nil {
			return nil, fmt.Errorf("claim pending apply_operation %d: %w", ad.ID, err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("read claim rows affected for apply_operation %d: %w", ad.ID, err)
		}
		if rows == 0 {
			return nil, nil
		}
	} else {
		// Already-active stale row: just refresh the heartbeat as our lease.
		_, err = tx.ExecContext(ctx, `
			UPDATE apply_operations SET updated_at = NOW() WHERE id = ?
		`, ad.ID)
		if err != nil {
			return nil, fmt.Errorf("refresh heartbeat for claimed apply_operation %d: %w", ad.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim apply_operation %d: %w", ad.ID, err)
	}

	return ad, nil
}

// Heartbeat refreshes updated_at to maintain the claim's lease. Should be
// called periodically by a worker holding the lease. Silent no-op when the
// row no longer exists (mirrors ApplyStore.Heartbeat).
func (s *applyOperationStore) Heartbeat(ctx context.Context, id int64) error {
	lease, hasLease, err := applyOperationLeaseFromContext(ctx)
	if err != nil {
		return err
	}
	leaseJoin, leasePredicate, leaseArgs := applyOperationLeaseSQL(lease, hasLease)
	args := append([]any{id}, leaseArgs...)
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_operations ao
		`+leaseJoin+`
		SET ao.updated_at = NOW()
		WHERE ao.id = ?`+leasePredicate+`
	`, args...)
	if err != nil {
		return fmt.Errorf("heartbeat apply_operation %d: %w", id, err)
	}
	return s.checkUpdatedOrExists(ctx, result, id, lease, hasLease, true)
}

// DeleteByApply removes all child rows for an apply.
func (s *applyOperationStore) DeleteByApply(ctx context.Context, applyID int64) error {
	lease, hasLease, err := applyLeaseFromContext(ctx, applyID)
	if err != nil {
		return err
	}
	if !hasLease {
		_, err := s.db.ExecContext(ctx, `DELETE FROM apply_operations WHERE apply_id = ?`, applyID)
		if err != nil {
			return fmt.Errorf("delete apply_operations for apply %d: %w", applyID, err)
		}
		return nil
	}
	result, err := s.db.ExecContext(ctx, `
		DELETE ao
		FROM apply_operations ao
		JOIN applies a ON a.id = ao.apply_id
		WHERE ao.apply_id = ? AND a.lease_token = ?
	`, applyID, lease.Token)
	if err != nil {
		return fmt.Errorf("delete apply_operations for apply %d: %w", applyID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read deleted apply_operations rows affected for apply %d: %w", applyID, err)
	}
	if rows == 0 {
		if err := ensureApplyLeaseStillOwned(ctx, s.db, lease); err != nil {
			return err
		}
	}
	return nil
}

// scanApplyOperation scans a single apply_operations row, returning nil if not found.
func scanApplyOperation(row *sql.Row) (*storage.ApplyOperation, error) {
	ad, err := scanApplyOperationInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return ad, err
}

// scanApplyOperations scans multiple apply_operations rows.
func scanApplyOperations(rows *sql.Rows) ([]*storage.ApplyOperation, error) {
	var out []*storage.ApplyOperation
	for rows.Next() {
		ad, err := scanApplyOperationInto(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ad)
	}
	return out, rows.Err()
}

// scanApplyOperationInto scans apply_operations data from any scanner.
func scanApplyOperationInto(s scanner) (*storage.ApplyOperation, error) {
	var ad storage.ApplyOperation
	var errMsg sql.NullString
	var engineResumeContext, engineResumeMetadata sql.NullString
	var startedAt, completedAt sql.NullTime

	if err := s.Scan(
		&ad.ID, &ad.ApplyID, &ad.Deployment, &ad.Target, &ad.State, &errMsg,
		&startedAt, &completedAt, &engineResumeContext, &engineResumeMetadata,
		&ad.CreatedAt, &ad.UpdatedAt,
	); err != nil {
		return nil, err
	}

	if errMsg.Valid {
		ad.ErrorMessage = errMsg.String
	}
	if startedAt.Valid {
		t := startedAt.Time
		ad.StartedAt = &t
	}
	if completedAt.Valid {
		t := completedAt.Time
		ad.CompletedAt = &t
	}
	if engineResumeContext.Valid {
		ad.EngineResumeContext = engineResumeContext.String
	}
	if engineResumeMetadata.Valid {
		ad.EngineResumeMetadata = engineResumeMetadata.String
	}
	return &ad, nil
}
