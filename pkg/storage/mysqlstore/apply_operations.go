// apply_operations.go implements ApplyOperationStore for per-(apply,
// deployment) child rows under a multi-deployment apply — the unit of work
// the operator claims.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// applyOperationColumns lists all columns for SELECT queries.
const applyOperationColumns = `id, apply_id, deployment, target, state, error_message,
	started_at, completed_at, lease_owner, lease_token, lease_acquired_at,
	engine_resume_context, engine_resume_metadata, created_at, updated_at`

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
	guard, err := operationWriteGuardFromContext(ctx)
	if err != nil {
		return err
	}
	args := append([]any{newState, id}, guard.args()...)
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_operations ao
		`+guard.join()+`
		SET ao.state = ?
		WHERE ao.id = ?`+guard.predicate()+`
	`, args...)
	if err != nil {
		return fmt.Errorf("update apply_operations state (id=%d): %w", id, err)
	}
	return s.checkUpdatedOrExists(ctx, result, id, guard, false)
}

// MarkStarted sets state=running and stamps started_at=NOW().
// Returns storage.ErrApplyOperationNotFound if no row matches the ID.
//
// Idempotent: COALESCE preserves started_at on repeat calls, so a re-issue
// against an already-started row is a no-op and returns nil.
func (s *applyOperationStore) MarkStarted(ctx context.Context, id int64) error {
	guard, err := operationWriteGuardFromContext(ctx)
	if err != nil {
		return err
	}
	args := append([]any{state.ApplyOperation.Running, id}, guard.args()...)
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_operations ao
		`+guard.join()+`
		SET ao.state = ?, ao.started_at = COALESCE(ao.started_at, NOW())
		WHERE ao.id = ?`+guard.predicate()+`
	`, args...)
	if err != nil {
		return fmt.Errorf("mark apply_operation started (id=%d): %w", id, err)
	}
	return s.checkUpdatedOrExists(ctx, result, id, guard, false)
}

// checkUpdatedOrExists returns nil if the UPDATE affected at least one row,
// nil if it affected zero rows but the row exists (idempotent no-op), or
// ErrApplyOperationNotFound if the row truly does not exist.
//
// Needed for idempotent UPDATEs where MySQL's default RowsAffected ("changed"
// rather than "matched") can return 0 for a successful no-op write.
func (s *applyOperationStore) checkUpdatedOrExists(ctx context.Context, result sql.Result, id int64, guard operationWriteGuard, missingOK bool) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read apply_operation update rows affected (id=%d): %w", id, err)
	}
	if rows > 0 {
		return nil
	}
	switch guard.kind {
	case operationGuardOperation:
		// Zero rows under an operation lease is either an idempotent no-op (the
		// row is still ours but no column changed) or a lost lease. Re-checking
		// the token disambiguates the two.
		ownsRow, err := operationLeaseOwnsRow(ctx, s.db, id, guard.opLease.Token)
		if err != nil {
			return err
		}
		if ownsRow {
			return nil
		}
		exists, err := s.applyOperationExists(ctx, id)
		if err != nil {
			return err
		}
		if !exists {
			return applyOperationMissingResult(id, missingOK)
		}
		return fmt.Errorf("apply_operation %d is no longer owned by its operation lease: %w", id, storage.ErrApplyLeaseLost)
	case operationGuardApply:
		if err := ensureApplyLeaseStillOwned(ctx, s.db, guard.applyLease); err != nil {
			return err
		}
		match, err := s.applyOperationLeaseMatch(ctx, id, guard.applyLease)
		if err != nil {
			return err
		}
		if !match.Exists {
			return applyOperationMissingResult(id, missingOK)
		}
		if !match.BelongsToLease {
			return fmt.Errorf("apply_operation %d is not owned by apply lease %d: %w", id, guard.applyLease.ApplyID, storage.ErrApplyLeaseLost)
		}
		return nil
	default:
		exists, err := s.applyOperationExists(ctx, id)
		if err != nil {
			return err
		}
		if !exists {
			return applyOperationMissingResult(id, missingOK)
		}
		return nil
	}
}

// applyOperationExists reports whether an apply_operations row exists by id.
func (s *applyOperationStore) applyOperationExists(ctx context.Context, id int64) (bool, error) {
	var exists bool
	if err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM apply_operations WHERE id = ?)`, id,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("verify apply_operation exists (id=%d): %w", id, err)
	}
	return exists, nil
}

// operationLeaseOwnsRow reports whether the apply_operations row still carries
// the given lease token, i.e. the operation lease is current.
func operationLeaseOwnsRow(ctx context.Context, db queryRower, id int64, token string) (bool, error) {
	var x int
	err := db.QueryRowContext(ctx,
		`SELECT 1 FROM apply_operations WHERE id = ? AND lease_token = ?`,
		id, token,
	).Scan(&x)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("verify operation lease for apply_operation %d: %w", id, err)
	}
	return true, nil
}

// ensureOperationLeaseStillOwned returns ErrApplyLeaseLost if the operation row
// no longer carries the lease's token, mirroring ensureApplyLeaseStillOwned.
func ensureOperationLeaseStillOwned(ctx context.Context, db queryRower, lease storage.OperationLease) error {
	owns, err := operationLeaseOwnsRow(ctx, db, lease.OperationID, lease.Token)
	if err != nil {
		return err
	}
	if !owns {
		return fmt.Errorf("operation lease for apply_operation %d is no longer current: %w", lease.OperationID, storage.ErrApplyLeaseLost)
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

// operationGuardKind selects which lease (if any) guards an operation write.
type operationGuardKind int

const (
	// operationGuardNone performs an unguarded write (no lease in context).
	operationGuardNone operationGuardKind = iota
	// operationGuardApply guards on the parent apply's lease token.
	operationGuardApply
	// operationGuardOperation guards on the operation row's own lease token.
	operationGuardOperation
)

// operationWriteGuard carries the lease that authorizes a write to an
// apply_operations row, plus the SQL fragments needed to enforce it. An
// operation lease takes precedence over the parent apply lease so the operator
// can move to operation-scoped writes while still falling back to the apply
// lease for callers that have not adopted operation leases yet.
type operationWriteGuard struct {
	kind       operationGuardKind
	applyLease storage.ApplyLease
	opLease    storage.OperationLease
}

// join returns the extra FROM/JOIN fragment the guard needs in an UPDATE.
func (g operationWriteGuard) join() string {
	if g.kind == operationGuardApply {
		return " JOIN applies a ON a.id = ao.apply_id"
	}
	return ""
}

// predicate returns the extra WHERE fragment that enforces lease ownership.
func (g operationWriteGuard) predicate() string {
	switch g.kind {
	case operationGuardOperation:
		return " AND ao.lease_token = ?"
	case operationGuardApply:
		return " AND ao.apply_id = ? AND a.lease_token = ?"
	default:
		return ""
	}
}

// args returns the bind args matching predicate(), to append after the
// statement's own arguments.
func (g operationWriteGuard) args() []any {
	switch g.kind {
	case operationGuardOperation:
		return []any{g.opLease.Token}
	case operationGuardApply:
		return []any{g.applyLease.ApplyID, g.applyLease.Token}
	default:
		return nil
	}
}

// operationWriteGuardFromContext resolves the guard for an operation write,
// preferring an operation lease over the parent apply lease. An invalid lease
// is fail-closed: it returns an error rather than degrading to an unguarded
// write.
func operationWriteGuardFromContext(ctx context.Context) (operationWriteGuard, error) {
	if lease, ok := storage.OperationLeaseFromContext(ctx); ok {
		if !lease.Valid() {
			return operationWriteGuard{}, fmt.Errorf("invalid operation lease: %w", storage.ErrApplyLeaseLost)
		}
		return operationWriteGuard{kind: operationGuardOperation, opLease: lease}, nil
	}
	if lease, ok := storage.ApplyLeaseFromContext(ctx); ok {
		if !lease.Valid() {
			return operationWriteGuard{}, fmt.Errorf("invalid apply_operation lease: %w", storage.ErrApplyLeaseLost)
		}
		return operationWriteGuard{kind: operationGuardApply, applyLease: lease}, nil
	}
	return operationWriteGuard{kind: operationGuardNone}, nil
}

// MarkCompleted sets state=completed and stamps completed_at=NOW().
// Returns storage.ErrApplyOperationNotFound if no row matches the ID.
//
// Idempotent: a retry within the same MySQL DATETIME second on an already-
// completed row may leave every column unchanged, producing RowsAffected=0.
// checkUpdatedOrExists disambiguates that no-op from a missing row so we
// don't spuriously return ErrApplyOperationNotFound.
func (s *applyOperationStore) MarkCompleted(ctx context.Context, id int64) error {
	guard, err := operationWriteGuardFromContext(ctx)
	if err != nil {
		return err
	}
	args := append([]any{state.ApplyOperation.Completed, id}, guard.args()...)
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_operations ao
		`+guard.join()+`
		SET ao.state = ?, ao.completed_at = COALESCE(ao.completed_at, NOW())
		WHERE ao.id = ?`+guard.predicate()+`
	`, args...)
	if err != nil {
		return fmt.Errorf("mark apply_operation completed (id=%d): %w", id, err)
	}
	return s.checkUpdatedOrExists(ctx, result, id, guard, false)
}

// MarkFailed sets state=failed, error_message, and stamps completed_at=NOW().
// Returns storage.ErrApplyOperationNotFound if no row matches the ID.
//
// Idempotent: same rationale as MarkCompleted — a retry within the same
// DATETIME second on an already-failed row with the same error_message can
// produce RowsAffected=0, which checkUpdatedOrExists disambiguates from
// a missing row.
func (s *applyOperationStore) MarkFailed(ctx context.Context, id int64, errMsg string) error {
	guard, err := operationWriteGuardFromContext(ctx)
	if err != nil {
		return err
	}
	args := append([]any{state.ApplyOperation.Failed, nullString(errMsg), id}, guard.args()...)
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_operations ao
		`+guard.join()+`
		SET ao.state = ?, ao.error_message = ?, ao.completed_at = COALESCE(ao.completed_at, NOW())
		WHERE ao.id = ?`+guard.predicate()+`
	`, args...)
	if err != nil {
		return fmt.Errorf("mark apply_operation failed (id=%d): %w", id, err)
	}
	return s.checkUpdatedOrExists(ctx, result, id, guard, false)
}

// MarkTerminal sets the given terminal state and stamps completed_at=NOW().
// Returns storage.ErrApplyOperationNotFound if no row matches the ID.
//
// For terminal states that record a reconciliation time (cancelled, reverted).
// stopped is resumable and must keep completed_at nil — use UpdateState for it.
//
// Idempotent: COALESCE preserves completed_at, and re-applying the same state
// is a no-op, so a re-issue against an already-terminal row returns nil.
func (s *applyOperationStore) MarkTerminal(ctx context.Context, id int64, newState string) error {
	guard, err := operationWriteGuardFromContext(ctx)
	if err != nil {
		return err
	}
	args := append([]any{newState, id}, guard.args()...)
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_operations ao
		`+guard.join()+`
		SET ao.state = ?, ao.completed_at = COALESCE(ao.completed_at, NOW())
		WHERE ao.id = ?`+guard.predicate()+`
	`, args...)
	if err != nil {
		return fmt.Errorf("mark apply_operation terminal (id=%d, state=%s): %w", id, newState, err)
	}
	return s.checkUpdatedOrExists(ctx, result, id, guard, false)
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
	guard, err := operationWriteGuardFromContext(ctx)
	if err != nil {
		return err
	}
	metadata := resumeState.Metadata
	if metadata == "" {
		metadata = "{}"
	}
	args := append([]any{nullString(resumeState.MigrationContext), metadata, operationID}, guard.args()...)
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_operations ao
		`+guard.join()+`
		SET ao.engine_resume_context = ?, ao.engine_resume_metadata = ?
		WHERE ao.id = ?`+guard.predicate()+`
	`, args...)
	if err != nil {
		return fmt.Errorf("save engine resume state for apply_operation %d: %w", operationID, err)
	}
	return s.checkUpdatedOrExists(ctx, result, operationID, guard, false)
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
// Sibling ordering: a pending row is only claimable once every earlier
// sibling of the same apply (lower created_at, id) has reached completed.
// This serializes a multi-deployment apply along its deployment_order — the
// order materialized by the apply-create dual-write into row insertion order
// — and halts the rollout on the first non-completed sibling (e.g. a failed
// deployment), since any earlier non-completed row blocks every later one.
// The gate applies only to starting a pending row; an already-active row
// re-leasing a stale heartbeat is recovering work it already started, so it
// is never re-gated.
//
// Mirrors ApplyStore.FindNextApply: SELECT ... FOR UPDATE SKIP LOCKED to
// avoid worker races, READ COMMITTED isolation to prevent next-key range
// locks from serializing claims across otherwise independent rows.
//
// Caller: the operator's per-poll recovery (Service.recoverApplyOperation)
// claims one operation per tick through this primitive when operation-level
// claiming is enabled. The per-deployment fan-out loop — driving multiple
// sibling operations of the same apply concurrently — is deferred to the
// multi-deployment apply workstream.
func (s *applyOperationStore) FindNextApplyOperation(ctx context.Context, owner string) (*storage.ApplyOperation, error) {
	if owner == "" {
		return nil, fmt.Errorf("operator owner is required to claim apply_operation: %w", storage.ErrApplyLeaseLost)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin claim apply_operation transaction: %w", err)
	}
	defer rollbackApplyTx(ctx, tx, "claim apply_operation")

	activeStates := claimableApplyStates()
	activeStatePlaceholders := placeholders(len(activeStates))

	queryArgs := []any{state.ApplyOperation.Pending, state.ApplyOperation.Completed}
	queryArgs = append(queryArgs, stringArgs(activeStates)...)
	queryArgs = append(queryArgs,
		state.ApplyOperation.Stopped,
		storage.ControlOperationStart, storage.ControlRequestPending)
	queryArgs = append(queryArgs, state.ApplyOperation.FailedRetryable)
	queryArgs = append(queryArgs, state.Apply.FailedRetryable, maxRecoveryAttempts, retryableRecoveryFreshnessDays)
	queryArgs = append(queryArgs, stringArgs(activeStates)...)

	// The stopped-row and failed_retryable clauses mirror ApplyStore.FindNextApply:
	// neither carries a deployment-order gate, because both rows already ran —
	// resuming them is recovering work they started, not starting a new deployment.
	//
	//   - A stopped operation whose parent apply has a pending start request is
	//     reclaimable so the operator can resume it.
	//
	//   - A failed_retryable operation is reclaimable only while its PARENT apply
	//     is itself claimable for that operation's recovery. The operator claim
	//     path drives the parent apply, so the operation row is a shadow of the
	//     parent: gating on the parent's claimability (not just its retry budget)
	//     is what keeps a healthy retry from being re-claimed every poll. The two
	//     sub-conditions mirror the parent clauses in ApplyStore.FindNextApply:
	//       * parent still failed_retryable, within recovery budget (attempt < max)
	//         and recent — a fresh bounded retry; and
	//       * parent already claimed into an active state but its lease has gone
	//         stale — crash recovery, with no budget gate (the attempt was already
	//         admitted and counted when the parent was claimed).
	//     Claiming a failed_retryable parent transitions it to running and refreshes
	//     applies.updated_at (see persistApplyClaim), so once a worker owns the
	//     retry neither sub-condition matches and peers back off instead of
	//     churning on a row another worker is actively driving.
	//
	// There is intentionally no "pending + pending start request" clause to
	// match ApplyStore.FindNextApply's pending-start clause. That apply-level
	// clause only matters because apply-level pending claimability is
	// task-gated (state = pending AND EXISTS tasks); a start request lets a
	// no-task pending apply be claimed. Operation-level pending claimability is
	// instead deployment-order-gated (the clause below), so a pending operation
	// is already claimable the moment it is legal to start — once every earlier
	// sibling has completed. A parent start request must not relax that gate:
	// adding an ungated pending-start clause would let a later deployment be
	// claimed out of order while an earlier sibling is still non-completed, and
	// a gated one would be redundant with the pending clause below. Start
	// requests resume eligible work; they do not reorder the rollout.
	row := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT %s
		FROM apply_operations
		WHERE (
			(
				state = ?
				AND NOT EXISTS (
					SELECT 1
					FROM apply_operations AS earlier
					WHERE earlier.apply_id = apply_operations.apply_id
						AND (earlier.created_at, earlier.id) < (apply_operations.created_at, apply_operations.id)
						AND earlier.state <> ?
				)
			)
			OR (state IN (%s) AND updated_at < NOW() - INTERVAL %s)
			OR (
				state = ?
				AND EXISTS (
					SELECT 1
					FROM apply_control_requests cr
					WHERE cr.apply_id = apply_operations.apply_id
						AND cr.operation = ?
						AND cr.status = ?
				)
			)
			OR (
				state = ?
				AND EXISTS (
					SELECT 1
					FROM applies a
					WHERE a.id = apply_operations.apply_id
						AND (
							(
								a.state = ?
								AND a.attempt < ?
								AND a.updated_at >= NOW() - INTERVAL ? DAY
							)
							OR (
								a.state IN (%s)
								AND a.updated_at < NOW() - INTERVAL %s
							)
						)
				)
			)
		)
		ORDER BY created_at, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, applyOperationColumns, activeStatePlaceholders, applyOperationHeartbeatStaleness, activeStatePlaceholders, applyOperationHeartbeatStaleness), queryArgs...)

	ad, err := scanApplyOperationInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // nothing to claim
	}
	if err != nil {
		return nil, fmt.Errorf("query next claimable apply_operation: %w", err)
	}

	// Rotate a fresh operation lease onto the claimed row so the claiming worker
	// can guard operation-scoped writes on this token. Mirrors persistApplyClaim
	// at the apply level.
	leaseToken := uuid.NewString()
	leaseAcquiredAt := time.Now()

	if ad.State == state.ApplyOperation.Pending {
		// Pending → running: stamp started_at, rotate the lease, and update the
		// heartbeat in the same write. WHERE state = ? guards against a concurrent
		// transition landing between the SELECT and this UPDATE; RowsAffected == 0
		// means another writer already moved the row, so we back off cleanly.
		result, err := tx.ExecContext(ctx, `
			UPDATE apply_operations
			SET state = ?, started_at = COALESCE(started_at, NOW()), updated_at = NOW(),
			    lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
			WHERE id = ? AND state = ?
		`, state.ApplyOperation.Running, owner, leaseToken, ad.ID, state.ApplyOperation.Pending)
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
		// Re-leasing a row that already started: a stale active heartbeat, or a
		// stopped operation whose parent apply has a pending start request. Both
		// keep their current state and are driven by the caller, so rotate the
		// lease onto this worker and refresh the heartbeat.
		_, err = tx.ExecContext(ctx, `
			UPDATE apply_operations
			SET updated_at = NOW(),
			    lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
			WHERE id = ?
		`, owner, leaseToken, ad.ID)
		if err != nil {
			return nil, fmt.Errorf("refresh heartbeat for claimed apply_operation %d: %w", ad.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim apply_operation %d: %w", ad.ID, err)
	}

	ad.LeaseOwner = owner
	ad.LeaseToken = leaseToken
	ad.LeaseAcquiredAt = &leaseAcquiredAt

	return ad, nil
}

// Heartbeat refreshes updated_at to maintain the claim's lease. Should be
// called periodically by a worker holding the lease. Silent no-op when the
// row no longer exists (mirrors ApplyStore.Heartbeat).
func (s *applyOperationStore) Heartbeat(ctx context.Context, id int64) error {
	guard, err := operationWriteGuardFromContext(ctx)
	if err != nil {
		return err
	}
	args := append([]any{id}, guard.args()...)
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_operations ao
		`+guard.join()+`
		SET ao.updated_at = NOW()
		WHERE ao.id = ?`+guard.predicate()+`
	`, args...)
	if err != nil {
		return fmt.Errorf("heartbeat apply_operation %d: %w", id, err)
	}
	return s.checkUpdatedOrExists(ctx, result, id, guard, true)
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
	var startedAt, completedAt, leaseAcquiredAt sql.NullTime

	if err := s.Scan(
		&ad.ID, &ad.ApplyID, &ad.Deployment, &ad.Target, &ad.State, &errMsg,
		&startedAt, &completedAt, &ad.LeaseOwner, &ad.LeaseToken, &leaseAcquiredAt,
		&engineResumeContext, &engineResumeMetadata, &ad.CreatedAt, &ad.UpdatedAt,
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
	if leaseAcquiredAt.Valid {
		t := leaseAcquiredAt.Time
		ad.LeaseAcquiredAt = &t
	}
	if engineResumeContext.Valid {
		ad.EngineResumeContext = engineResumeContext.String
	}
	if engineResumeMetadata.Valid {
		ad.EngineResumeMetadata = engineResumeMetadata.String
	}
	return &ad, nil
}
