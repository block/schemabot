// apply_operations.go implements ApplyOperationStore for per-(apply,
// deployment, operation_key) child rows under a multi-operation apply — the
// unit of work the driver claims.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// applyOperationColumns lists all columns for SELECT queries.
const applyOperationColumns = `id, apply_id, deployment, operation_key, operation_kind, target, external_id, external_operation_id, state, error_message,
	cutover_policy, on_failure, attempt, started_at, completed_at, lease_owner, lease_token, lease_acquired_at,
	engine_resume_context, engine_resume_metadata, created_at, updated_at`

// applyOperationStore implements storage.ApplyOperationStore using MySQL.
type applyOperationStore struct {
	db         *rebindDB
	dialect    Dialect
	identity   identityInserter
	locker     namedlock.Locker
	classifier ErrorClassifier

	// maxDriversPerApply caps how many operation leases one apply may hold at
	// once; see freshLeaseCountSQL. Always positive — NewWithDependencies
	// substitutes storage.DefaultMaxDriversPerApply for an unset value, so the
	// claim path has no uncapped mode.
	maxDriversPerApply int
}

// Insert stores a new apply_operations row and returns its ID.
// Translates a unique-key conflict on (apply_id, deployment, operation_key) into
// storage.ErrApplyOperationExists so callers can branch cleanly.
func (s *applyOperationStore) Insert(ctx context.Context, ad *storage.ApplyOperation) (int64, error) {
	return insertApplyOperation(ctx, s.db, s.identity, s.classifier, ad)
}

// insertApplyOperation inserts one apply_operations row using the supplied
// executer (pool or transaction). On success the row's ID and State fields
// are set. A duplicate-key violation on (apply_id, deployment, operation_key)
// is translated to storage.ErrApplyOperationExists for callers to branch on.
func insertApplyOperation(ctx context.Context, exec queryExecer, identity identityInserter, classifier ErrorClassifier, ad *storage.ApplyOperation) (int64, error) {
	stateVal := ad.State
	if stateVal == "" {
		stateVal = state.ApplyOperation.Pending
	}

	// An empty policy means the caller did not resolve a cutover_policy, so fall
	// back to rolling — the serial default that matches the column's NOT NULL
	// DEFAULT 'rolling'.
	cutoverPolicy := ad.CutoverPolicy
	if cutoverPolicy == "" {
		cutoverPolicy = storage.CutoverPolicyRolling
	}

	// An empty policy means the caller did not resolve an on_failure preference,
	// so fall back to halting — the safe default that matches the column's
	// NOT NULL DEFAULT 'halt' and never silently degrades to non-halting
	// behaviour.
	onFailure := ad.OnFailure
	if onFailure == "" {
		onFailure = storage.OnFailureHalt
	}

	operationKind := ad.OperationKind
	if operationKind == "" {
		operationKind = storage.ApplyOperationKindWork
	}

	id, err := identity.InsertID(ctx, exec, `
		INSERT INTO apply_operations (
			apply_id, deployment, operation_key, operation_kind, target, external_id, external_operation_id, state, error_message, cutover_policy, on_failure,
			started_at, completed_at, engine_resume_context, engine_resume_metadata
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		ad.ApplyID, ad.Deployment, ad.OperationKey, operationKind, ad.Target, nullString(ad.ExternalID), nullString(ad.ExternalOperationID), stateVal, nullString(ad.ErrorMessage), cutoverPolicy, onFailure,
		ad.StartedAt, ad.CompletedAt, nullString(ad.EngineResumeContext), nullString(ad.EngineResumeMetadata),
	)
	if err != nil {
		if classifier.IsDuplicateKey(err) {
			return 0, storage.ErrApplyOperationExists
		}
		return 0, fmt.Errorf("insert apply_operations (apply=%d, deployment=%s, operation_key=%s): %w", ad.ApplyID, ad.Deployment, ad.OperationKey, err)
	}
	ad.ID = id
	ad.State = stateVal
	ad.OperationKind = operationKind
	ad.CutoverPolicy = cutoverPolicy
	ad.OnFailure = onFailure
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

// GetByApplyAndDeployment returns the legacy unkeyed child row for
// (apply_id, deployment), or nil if not found.
func (s *applyOperationStore) GetByApplyAndDeployment(ctx context.Context, applyID int64, deployment string) (*storage.ApplyOperation, error) {
	return s.GetByApplyDeploymentAndOperationKey(ctx, applyID, deployment, "")
}

// GetByApplyDeploymentAndOperationKey returns the child row for
// (apply_id, deployment, operation_key), or nil if not found.
func (s *applyOperationStore) GetByApplyDeploymentAndOperationKey(ctx context.Context, applyID int64, deployment, operationKey string) (*storage.ApplyOperation, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+applyOperationColumns+`
		FROM apply_operations
		WHERE apply_id = ? AND deployment = ? AND operation_key = ?
	`, applyID, deployment, operationKey)
	return scanApplyOperation(row)
}

// ListByApply returns all child rows for an apply, ordered by (created_at, id)
// ascending — the same deployment order the claim gate enforces. Keeping the
// projection and "first failed deployment" derivation on this order means the
// aggregate apply state and its surfaced failure reason agree with the order the
// rollout actually drives deployments in.
func (s *applyOperationStore) ListByApply(ctx context.Context, applyID int64) ([]*storage.ApplyOperation, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyOperationColumns+`
		FROM apply_operations
		WHERE apply_id = ?
		ORDER BY created_at, id
	`, applyID)
	if err != nil {
		return nil, fmt.Errorf("query apply_operations for apply %d: %w", applyID, err)
	}
	defer utils.CloseAndLog(rows)

	return scanApplyOperations(rows)
}

// ListByApplies returns all child rows for the requested applies in
// (apply_id, created_at, id) order.
func (s *applyOperationStore) ListByApplies(ctx context.Context, applyIDs []int64) ([]*storage.ApplyOperation, error) {
	if len(applyIDs) == 0 {
		return nil, nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(applyIDs)), ",")
	args := make([]any, 0, len(applyIDs))
	for _, applyID := range applyIDs {
		args = append(args, applyID)
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyOperationColumns+`
		FROM apply_operations
		WHERE apply_id IN (`+placeholders+`)
		ORDER BY apply_id, created_at, id
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("query apply_operations for %d applies: %w", len(applyIDs), err)
	}
	defer utils.CloseAndLog(rows)

	return scanApplyOperations(rows)
}

// execStateUpdate runs a guarded UPDATE against a single apply_operations row,
// applying the given SET assignments with their placeholder args and the write
// guard's statement rendering. It centralizes the guard load, argument
// ordering, error wrapping, and idempotent row-existence check shared by the
// state-transition helpers. updateStatement stamps updated_at on every write.
func (s *applyOperationStore) execStateUpdate(ctx context.Context, id int64, errContext string, assignments []JoinedUpdateAssignment, setArgs ...any) error {
	guard, err := operationWriteGuardFromContext(ctx)
	if err != nil {
		return err
	}
	guardArgs := guard.args()
	args := make([]any, 0, len(setArgs)+1+len(guardArgs))
	args = append(args, setArgs...)
	args = append(args, id)
	args = append(args, guardArgs...)
	result, err := s.db.ExecContext(ctx, guard.updateStatement(s.dialect, assignments), args...)
	if err != nil {
		return fmt.Errorf("%s (id=%d): %w", errContext, id, err)
	}
	return s.checkUpdatedOrExists(ctx, result, id, guard, false)
}

// UpdateState transitions a child row to the given state.
// Returns storage.ErrApplyOperationNotFound if no row matches the ID.
//
// Idempotent: re-applying the same state to a row is a no-op and returns nil.
// MySQL's RowsAffected reports rows *changed* (not matched) by default, so a
// repeat call would report 0; we disambiguate with an existence check.
func (s *applyOperationStore) UpdateState(ctx context.Context, id int64, newState string) error {
	return s.execStateUpdate(ctx, id, "update apply_operations state",
		[]JoinedUpdateAssignment{{Column: "state", Expr: "?"}}, newState)
}

// MarkStarted sets state=running and stamps started_at=NOW().
// Returns storage.ErrApplyOperationNotFound if no row matches the ID.
//
// Idempotent: COALESCE preserves started_at on repeat calls, so a re-issue
// against an already-started row is a no-op and returns nil.
func (s *applyOperationStore) MarkStarted(ctx context.Context, id int64) error {
	return s.execStateUpdate(ctx, id, "mark apply_operation started",
		[]JoinedUpdateAssignment{
			{Column: "state", Expr: "?"},
			{Column: "started_at", Expr: "COALESCE(ao.started_at, NOW())"},
		}, state.ApplyOperation.Running)
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

// updateStatement renders the guarded single-row UPDATE against
// apply_operations for the store's dialect. The apply-lease guard joins the
// parent applies row, so its statement goes through the dialect's joined-UPDATE
// rendering; the other guards produce a portable single-table UPDATE. In both
// shapes the WHERE placeholders follow the SET placeholders, so callers append
// the row ID and then args() to their SET arguments.
//
// The apply-lease guard's token check is built with the dialect's
// LeaseTokenFence, so it serializes against a concurrent lease steal on every
// dialect: MySQL's joined rendering locks the applies row it scans, and
// PostgreSQL's fence locks it explicitly. The operation-lease guard checks the
// token on the updated row itself, which every dialect locks.
//
// Assignment columns are unqualified (the dialect qualifies them where its
// syntax requires); expressions that read current row values qualify them with
// the "ao" alias, which is valid in every rendering. Every guarded write stamps
// updated_at: the column is the claim's lease heartbeat read by the staleness
// predicates, and stamping is the application's responsibility on every
// dialect, so the stamp is appended here rather than left to each caller.
func (g operationWriteGuard) updateStatement(d Dialect, assignments []JoinedUpdateAssignment) string {
	stamped := make([]JoinedUpdateAssignment, 0, len(assignments)+1)
	stamped = append(stamped, assignments...)
	stamped = append(stamped, JoinedUpdateAssignment{Column: "updated_at", Expr: "NOW()"})
	if g.kind == operationGuardApply {
		return d.JoinedUpdate(
			"apply_operations", "ao", "applies", "a", "a.id = ao.apply_id",
			stamped,
			"ao.id = ? AND ao.apply_id = ? AND "+d.LeaseTokenFence("applies", "a", "id", "lease_token"),
		)
	}
	sets := make([]string, len(stamped))
	for i, assignment := range stamped {
		sets[i] = assignment.Column + " = " + assignment.Expr
	}
	predicate := "ao.id = ?"
	if g.kind == operationGuardOperation {
		predicate += " AND ao.lease_token = ?"
	}
	return "UPDATE apply_operations ao SET " + strings.Join(sets, ", ") + " WHERE " + predicate
}

// args returns the bind args matching updateStatement's guard predicate, to
// append after the statement's own arguments and the row ID.
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
	return s.execStateUpdate(ctx, id, "mark apply_operation completed",
		[]JoinedUpdateAssignment{
			{Column: "state", Expr: "?"},
			{Column: "completed_at", Expr: "COALESCE(ao.completed_at, NOW())"},
		}, state.ApplyOperation.Completed)
}

// MarkFailed sets state=failed, error_message, and stamps completed_at=NOW().
// Returns storage.ErrApplyOperationNotFound if no row matches the ID.
//
// Idempotent: same rationale as MarkCompleted — a retry within the same
// DATETIME second on an already-failed row with the same error_message can
// produce RowsAffected=0, which checkUpdatedOrExists disambiguates from
// a missing row.
func (s *applyOperationStore) MarkFailed(ctx context.Context, id int64, errMsg string) error {
	return s.execStateUpdate(ctx, id, "mark apply_operation failed",
		[]JoinedUpdateAssignment{
			{Column: "state", Expr: "?"},
			{Column: "error_message", Expr: "?"},
			{Column: "completed_at", Expr: "COALESCE(ao.completed_at, NOW())"},
		},
		state.ApplyOperation.Failed, nullString(errMsg))
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
	return s.execStateUpdate(ctx, id, fmt.Sprintf("mark apply_operation terminal (state=%s)", newState),
		[]JoinedUpdateAssignment{
			{Column: "state", Expr: "?"},
			{Column: "completed_at", Expr: "COALESCE(ao.completed_at, NOW())"},
		}, newState)
}

// SaveExternalOperationID stores the remote data plane's apply_operation_id on
// the operation row. It refuses empty IDs so callers do not convert a missing
// remote field into an apparent successful correlation.
func (s *applyOperationStore) SaveExternalOperationID(ctx context.Context, operationID int64, externalOperationID string) error {
	if externalOperationID == "" {
		return fmt.Errorf("save external operation id for apply_operation %d: external operation id is empty", operationID)
	}
	guard, err := operationWriteGuardFromContext(ctx)
	if err != nil {
		return err
	}
	args := append([]any{externalOperationID, operationID}, guard.args()...)
	query := guard.updateStatement(s.dialect, []JoinedUpdateAssignment{
		{Column: "external_operation_id", Expr: "?"},
	})
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("save external operation id for apply_operation %d: %w", operationID, err)
	}
	return s.checkUpdatedOrExists(ctx, result, operationID, guard, false)
}

// ApplyIdentifierForRemoteApply returns the identifier of the apply this control
// plane dispatched as externalID, or "" when it dispatched no such thing.
//
// An empty externalID correlates to nothing by definition, and is answered
// without a query so that stays true however the column spells "none". The
// answer happens to be the same today, since an unrecorded id is stored as NULL
// and NULL matches nothing; making it a property of the method instead means a
// caller with no identifier can never be correlated to every operation that has
// none, which would name an unrelated schema change as the one to go look at.
//
// Distinct parents are counted rather than taking the first row: sibling
// operations of one deployment share a remote apply, so many rows for one
// parent is the normal shape, but two parents for one remote apply means the
// one-remote-apply-per-deployment invariant has already been violated. Naming
// either one would send an operator to the wrong schema change, so the store
// reports the ambiguity instead.
func (s *applyOperationStore) ApplyIdentifierForRemoteApply(ctx context.Context, externalID string) (string, error) {
	if externalID == "" {
		return "", nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT a.apply_identifier
		FROM apply_operations o
		JOIN applies a ON a.id = o.apply_id
		WHERE o.external_id = ?
	`, externalID)
	if err != nil {
		return "", fmt.Errorf("query apply identifier for remote apply %q: %w", externalID, err)
	}
	defer utils.CloseAndLog(rows)

	var identifiers []string
	for rows.Next() {
		var identifier string
		if err := rows.Scan(&identifier); err != nil {
			return "", fmt.Errorf("scan apply identifier for remote apply %q: %w", externalID, err)
		}
		identifiers = append(identifiers, identifier)
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate apply identifiers for remote apply %q: %w", externalID, err)
	}
	switch len(identifiers) {
	case 0:
		return "", nil
	case 1:
		return identifiers[0], nil
	default:
		return "", fmt.Errorf("remote apply %q correlates to %d applies (%s): %w",
			externalID, len(identifiers), strings.Join(identifiers, ", "), storage.ErrRemoteApplyDeploymentIDConflict)
	}
}

// SaveExternalID stores the remote data plane's apply_id on the operation row.
// It refuses empty IDs so callers do not convert a missing remote field into an
// apparent successful correlation.
//
// The write is atomic with the deployment's one-remote-apply invariant: in one
// transaction it locks the apply's operation rows, verifies the operation's
// deployment records no remote apply id other than the one being stored, and
// only then writes. Sibling operations of one deployment persist concurrently
// across the driver pool, so a check outside the writing transaction cannot
// stop two of them from each seeing "no id recorded yet" and committing
// divergent ids. Divergence — among the siblings themselves or between the
// siblings and this id — returns an error wrapping
// storage.ErrRemoteApplyDeploymentIDConflict.
func (s *applyOperationStore) SaveExternalID(ctx context.Context, applyID, operationID int64, externalID string) error {
	if externalID == "" {
		return fmt.Errorf("save external id for apply_operation %d: external id is empty", operationID)
	}
	guard, err := operationWriteGuardFromContext(ctx)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin save external id transaction for apply_operation %d: %w", operationID, err)
	}
	defer rollbackTx(ctx, tx, "save apply_operation external id")

	ops, err := lockApplyOperationsForUpdate(ctx, tx, applyID)
	if err != nil {
		return fmt.Errorf("lock apply_operations of apply %d before storing remote apply id for apply_operation %d: %w", applyID, operationID, err)
	}
	var current *storage.ApplyOperation
	for _, op := range ops {
		if op.ID == operationID {
			current = op
			break
		}
	}
	if current == nil {
		return fmt.Errorf("apply_operation %d does not belong to apply %d: %w", operationID, applyID, storage.ErrApplyOperationNotFound)
	}
	sharedID, err := storage.DeploymentRemoteApplyID(ops, current.Deployment)
	if err != nil {
		return fmt.Errorf("refusing to store remote apply id %q for apply_operation %d: %w: %w", externalID, operationID, err, storage.ErrRemoteApplyDeploymentIDConflict)
	}
	if sharedID != "" && sharedID != externalID {
		return fmt.Errorf("deployment %q of apply %d already correlates to remote apply %q; refusing to store %q for apply_operation %d: %w", current.Deployment, applyID, sharedID, externalID, operationID, storage.ErrRemoteApplyDeploymentIDConflict)
	}

	args := append([]any{externalID, operationID}, guard.args()...)
	query := guard.updateStatement(s.dialect, []JoinedUpdateAssignment{
		{Column: "external_id", Expr: "?"},
	})
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("save external id for apply_operation %d: %w", operationID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit save external id transaction for apply_operation %d: %w", operationID, err)
	}
	return s.checkUpdatedOrExists(ctx, result, operationID, guard, false)
}

// lockApplyOperationsForUpdate returns all child rows for an apply, locked
// FOR UPDATE inside the given transaction so concurrent writers that must
// agree across sibling rows serialize against each other.
func lockApplyOperationsForUpdate(ctx context.Context, tx *rebindTx, applyID int64) ([]*storage.ApplyOperation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT `+applyOperationColumns+`
		FROM apply_operations
		WHERE apply_id = ?
		ORDER BY created_at, id
		FOR UPDATE
	`, applyID)
	if err != nil {
		return nil, fmt.Errorf("lock apply_operations for apply %d: %w", applyID, err)
	}
	defer utils.CloseAndLog(rows)

	return scanApplyOperations(rows)
}

// SaveEngineResumeState stores opaque engine state on the operation that owns
// the execution. It updates only resume-state columns (plus the updated_at
// heartbeat) so callers can persist engine progress without changing operation
// lifecycle state.
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
	query := guard.updateStatement(s.dialect, []JoinedUpdateAssignment{
		{Column: "engine_resume_context", Expr: "?"},
		{Column: "engine_resume_metadata", Expr: "?"},
	})
	result, err := s.db.ExecContext(ctx, query, args...)
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

// releasedFailureExemptionSQL stops a terminal-failed earlier sibling from
// blocking a later deployment's claim once the rollout policy says to keep
// going: on_failure='continue', or on_failure='pause' after a release control
// request latches the rollout open. A release latches while pending or
// completed; a failed release does not (fail-closed), mirroring
// storage.ApplyControlRequest.ReleasesPausedRollout. Only terminal `failed` is
// exempted — in-flight or recoverable earlier siblings still block. The
// fragment references the apply_operations and earlier aliases, so the copy
// claim (FindNextApplyOperation) and the cutover claim
// (FindNextApplyOperationCutover) embed it identically. Placeholders, in order:
// earlier-failed state, continue, pause, release operation, pending, completed
// (see releasedFailureExemptionArgs).
const releasedFailureExemptionSQL = `NOT (
	earlier.state = ?
	AND (
		apply_operations.on_failure = ?
		OR (
			apply_operations.on_failure = ?
			AND EXISTS (
				SELECT 1
				FROM apply_control_requests AS release_req
				WHERE release_req.apply_id = apply_operations.apply_id
					AND release_req.operation = ?
					AND release_req.status IN (?, ?)
			)
		)
	)
)`

// freshLeaseCountSQL counts the operation leases the candidate row's parent
// apply already holds: sibling operations in an active state, owned by some
// driver, whose heartbeat is still inside the staleness window. It is the
// measure behind maxDriversPerApply: it is how many drivers the
// apply occupies right now.
//
// Neither a stale-lease row nor a released one is counted. A stale row's driver
// is gone and the row is claimable again by the recovery clauses, so counting it
// would let a dead driver keep an apply capped out of the pool; a row released
// at the cutover barrier clears its lease and backdates its heartbeat, so it is
// parked, not occupying anything.
//
// The cap is a filter on the pending arm only, never a sort key. Sorting by the
// count would replace the indexed (created_at, id) walk with a full sort that
// evaluates a correlated subquery per row, which is the claim cost the ordering
// index exists to avoid. Filtering keeps the index walk: a driver steps past a
// capped apply's rows to the next apply's oldest claimable row, which is the
// fairness the cap is for.
//
// It takes two format arguments in order: the occupying-state placeholder list
// and the dialect's staleness cutoff expression, and one positional argument per
// occupying state.
const freshLeaseCountSQL = `(
			SELECT COUNT(*)
			FROM apply_operations AS busy
			WHERE busy.apply_id = apply_operations.apply_id
				AND busy.lease_owner <> ''
				AND busy.state IN (%s)
				AND busy.updated_at >= %s
		)`

// driverOccupyingOperationStates lists the operation states in which a driver
// is holding the row and doing work on it. It is the question the fan-out cap
// asks, and it is deliberately not claimableApplyStates(): "states an apply can
// be claimed in" and "states in which a driver occupies an operation" are
// different questions that only mostly overlap.
//
// failed_retryable is where they part. A redispatched operation keeps that
// state for its whole drive — the claim rotates the lease and leaves the state
// alone, and the row is not rewritten until the drive settles it — so a driver
// really is occupying it the entire time. Counting it is what keeps a retry
// sweep from running the apply at twice its cap.
func driverOccupyingOperationStates() []string {
	return append(claimableApplyStates(), state.ApplyOperation.FailedRetryable)
}

// driverCapClause renders the fan-out cap predicate and returns it with its
// positional arguments, so an arm can be gated by appending both together.
//
// The clause is rendered here rather than spliced into the claim query as `%s`
// verbs on purpose. The claim is a long hand-ordered query with many positional
// arguments; adding the cap's arguments to that list at three separate sites
// would put them in the middle of it, where one misplaced entry shifts every
// later binding and silently corrupts eligibility for unrelated arms. Rendering
// the count up front keeps each site's arguments to one named unit.
func (s *applyOperationStore) driverCapClause(staleClaimCutoff string) (string, []any) {
	occupyingStates := driverOccupyingOperationStates()
	clause := fmt.Sprintf(freshLeaseCountSQL, placeholders(len(occupyingStates)), staleClaimCutoff) + " < ?"
	return clause, append(stringArgs(occupyingStates), s.maxDriversPerApply)
}

// releasedFailureExemptionArgs returns the positional arguments for
// releasedFailureExemptionSQL, in placeholder order.
func releasedFailureExemptionArgs() []any {
	return []any{
		state.ApplyOperation.Failed,
		storage.OnFailureContinue,
		storage.OnFailurePause,
		storage.ControlOperationRelease,
		storage.ControlRequestPending,
		storage.ControlRequestCompleted,
	}
}

// FindNextApplyOperation atomically claims the next apply_operations row that
// needs attention and refreshes its heartbeat in the same transaction.
//
// Pending rows are transitioned to running and stamped with started_at;
// already-active rows with a stale heartbeat (updated_at older than the
// staleness window) are re-leased without changing their state. Terminal
// rows are never claimed.
//
// Sibling ordering: a pending row's claimability is gated only on its earlier
// siblings in an EARLIER deployment (rows of the same apply with a different
// deployment and a lower created_at, id) along deployment_order — the order
// materialized by the apply-create dual-write into row insertion order. Work
// rows in the SAME deployment (the per-shard, per-namespace fan-out of a
// sharded apply) do not gate each other, so a deployment's shard work drives
// in parallel; the group_finalizer clause below still holds each namespace's
// finalizer until that namespace's work siblings complete — and a namespace
// whose only change is its VSchema (no shard work siblings) has its finalizer
// claimable immediately, since there is no incomplete sibling to wait on. The
// gate is
// cutover_policy-aware (the policy is captured per row at apply-create):
//
//   - rolling (the default, and any non-barrier value — which fails closed to
//     the serial gate): a pending row is claimable only once every earlier
//     sibling has reached completed. This serializes the rollout and halts it
//     on the first non-completed sibling (e.g. a failed deployment).
//   - barrier: an earlier sibling stops blocking once it reaches the cutover
//     barrier or succeeds (waiting_for_cutover, cutting_over, revert_window,
//     completed), so a later deployment may start its copy phase while earlier
//     siblings sit at the barrier. Earlier siblings that are still in-flight or
//     not yet at the barrier (pending, running, failed_retryable, stopped) — and
//     terminal non-success states (failed, cancelled, reverted) — still block,
//     so a failed earlier deployment still halts the rollout.
//
// on_failure (per-apply policy, also captured on each row at create)
// layers on top of both policies: "halt" (the default) keeps a terminal-failed
// earlier sibling blocking every later sibling, so the rollout halts on the
// first failure. "continue" treats a terminal `failed` earlier sibling as
// settled so it no longer blocks: later deployments are still claimed and
// attempted. "pause" holds the rollout like "halt" until an operator releases
// it; once a release control request latches the apply open (pending or
// completed), a terminal-failed earlier sibling stops blocking and the rollout
// proceeds like "continue". Only terminal `failed` is exempted — pending,
// running, failed_retryable, and stopped earlier siblings still block under all
// policies (work is in-flight or recoverable). The policy governs only rollout
// continuation; the apply's pass/fail verdict and the merge gate stay
// fail-closed on any failed deployment. See releasedFailureExemptionSQL.
//
// A pending stop control request layers a hard halt on top: while a stop is
// pending for the apply, no pending sibling is claimable for start, regardless
// of cutover_policy or on_failure. This is what makes `stop` halt remaining
// siblings under "continue" — without it a continue-exempted pending sibling
// would still be started after the user asked to stop.
//
// The gate applies only to starting a pending row; an already-active row
// re-leasing a stale heartbeat is recovering work it already started, so it
// is never re-gated. A single-operation apply has no earlier-deployment
// sibling, so the gate is a no-op for it regardless of policy.
//
// Mirrors ApplyStore.ClaimApplyByID: SELECT ... FOR UPDATE SKIP LOCKED to
// avoid driver races, READ COMMITTED isolation to prevent next-key range
// locks from serializing claims across otherwise independent rows.
//
// Caller: the operator's per-poll recovery (Service.recoverApplyOperation)
// claims one operation per tick through this primitive when operation-level
// claiming is enabled. Multiple drivers each claim a different claimable row
// (FOR UPDATE SKIP LOCKED), so same-deployment shard work siblings of one
// apply drive concurrently across the driver pool.
//
// The ORDER BY must be able to walk an index on (created_at, id) rather than
// sort. The eligibility filter ORs across several states, so no state-prefixed
// index serves the ordering; without one on the ordering pair the planner
// collects every claimable row and sorts it. On InnoDB that sort runs under
// FOR UPDATE and locks the whole candidate set before LIMIT 1 applies, giving
// SKIP LOCKED the opposite of its intended effect — each driver skips
// everything a peer locked instead of taking the next free row, so concurrent
// claims serialize. PostgreSQL locks rows only after the sort, so a single row
// is ever locked; there the index spares the sort itself, which otherwise
// reads and spills the full candidate set. Each dialect's schema file names
// that index by its own convention.
//
// The index bounds the sort, not the walk: nothing prunes terminal rows from
// an ascending scan, so a claim still steps over dead history before reaching
// claimable work as the table grows. A filtered or partial index is not an
// option — the schema parity tests pin both dialects to the same index shape,
// and MySQL has no partial indexes — so retention, not indexing, is the lever
// for that remaining bound.
//
// Oldest-first is fair only between applies of comparable width. On its own it
// lets one wide fan-out hold every driver on the plane for as long as its
// slowest operation runs, so the pending arm is additionally capped at
// maxDriversPerApply fresh leases per apply (see freshLeaseCountSQL).
func (s *applyOperationStore) FindNextApplyOperation(ctx context.Context, owner string) (*storage.ApplyOperation, error) {
	if owner == "" {
		return nil, fmt.Errorf("operator owner is required to claim apply_operation: %w", storage.ErrApplyLeaseLost)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin claim apply_operation transaction: %w", err)
	}
	defer rollbackTx(ctx, tx, "claim apply_operation")

	activeStates := claimableApplyStates()
	activeStatePlaceholders := placeholders(len(activeStates))
	terminalStates := terminalApplyStates()
	terminalStatePlaceholders := placeholders(len(terminalStates))
	staleClaimCutoff := s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, LiteralIntervalAmount(uint64(storage.ApplyLeaseStaleAfter.Microseconds())), IntervalMicrosecond)
	driverCap, driverCapArgs := s.driverCapClause(staleClaimCutoff)

	queryArgs := []any{state.ApplyOperation.Pending}
	queryArgs = append(queryArgs, storage.ApplyOperationKindGroupFinalizer)
	// Sibling-gate args for the pending claim, cutover_policy-aware (see the
	// gate SQL below). Under barrier, an earlier sibling stops blocking once it
	// reaches the cutover barrier or succeeds (waiting_for_cutover, cutting_over,
	// revert_window, completed). Under parallel there is intentionally no arm: a
	// parallel operation matches neither branch, so no earlier sibling can make
	// the blocking EXISTS true and its copy starts immediately (concurrent copy).
	// Under rolling — and any unrecognized value, which fails closed to the
	// serial gate via NOT IN (barrier, parallel) — only a completed earlier
	// sibling stops blocking. The trailing releasedFailureExemptionArgs drive the
	// exemption: a terminal-failed earlier sibling no longer blocks later ones
	// under "continue", or under "pause" once a release latches the rollout open.
	queryArgs = append(queryArgs,
		storage.CutoverPolicyBarrier,
		state.ApplyOperation.WaitingForCutover,
		state.ApplyOperation.CuttingOver,
		state.ApplyOperation.RevertWindow,
		state.ApplyOperation.Completed,
		storage.CutoverPolicyBarrier,
		storage.CutoverPolicyParallel,
		state.ApplyOperation.Completed,
	)
	queryArgs = append(queryArgs, releasedFailureExemptionArgs()...)
	queryArgs = append(queryArgs,
		storage.ApplyOperationKindGroupFinalizer,
		storage.ApplyOperationKindWork,
		state.ApplyOperation.Completed,
	)
	// Pending stop gate: a pending operation is not claimable for start while
	// its apply has a pending stop control request. This is what makes `stop`
	// halt remaining siblings under on_failure "continue" — without it, a
	// continue-exempted pending sibling would still be claimed and started even
	// though the user asked to stop the rollout. The gate covers only pending
	// (not-yet-started) rows; an already-active row re-leasing a stale heartbeat
	// is recovering work it started and is handled by the staleness clause below.
	queryArgs = append(queryArgs,
		storage.ControlOperationStop, storage.ControlRequestPending)
	// Claimable-parent gate: a pending operation is claimable exactly while its
	// parent apply is itself claimable. Starting a deployment belongs to a
	// rollout a driver can still take somewhere; a terminal parent means the
	// apply's outcome is already settled (completed, failed, cancelled,
	// reverted) and its never-started rows must not be claimed or shadow live
	// work in the claim order.
	//
	// The one terminal state that stays claimable is stopped with a pending
	// start request — a stopped rollout is resumable, and ClaimApplyByID admits
	// exactly that pair. Mirroring it here keeps the two claims from
	// disagreeing: a stopped apply whose deployments had not started yet has
	// only pending rows, so gating them off would leave an accepted start
	// request with nothing able to service it, and the rollout could never
	// resume. Without a start request those same rows stay gated off, so a
	// stopped-and-abandoned apply still costs the queue nothing.
	//
	// The non-stopped half is written NOT IN terminal rather than IN claimable
	// so a future non-terminal parent state keeps its pending operations
	// claimable. It gates only this pending arm: the stale-active,
	// stopped+start, waiting_for_deploy+start, and failed_retryable arms
	// recover or resume work that already started. The window where the parent
	// terminalizes after this SELECT is handled by the driver-side parent claim
	// refusing and reconcileUnclaimableParent settling the claimed row from the
	// parent's state.
	queryArgs = append(queryArgs, stringArgs(terminalStates)...)
	queryArgs = append(queryArgs,
		state.Apply.Stopped,
		storage.ControlOperationStart, storage.ControlRequestPending)
	// Fan-out cap: a pending row is claimable only while its apply holds fewer
	// than maxDriversPerApply fresh operation leases.
	//
	// The cap gates every arm that *starts* work — this pending arm, the
	// stopped+start resume arm, and the deferred-deploy arm — because all three
	// admit an apply's siblings onto drivers, and an uncapped one is a way around
	// the bound rather than an exception to it. A stop/start cycle is the shape
	// that matters: stop bulk-moves every pending row to stopped, so an uncapped
	// resume arm would make all of them claimable at once and hand one apply the
	// whole pool durably, which is the starvation this exists to prevent.
	//
	// The recovery and control-request arms stay uncapped: a capped-out apply
	// still has to be able to re-lease a crashed operation and to consume a
	// pending stop or cancel, and gating those on the same count would make the
	// cap itself the thing that wedges the apply it is bounding.
	//
	// Capping a start arm cannot wedge either, because the states it admits work
	// into (resuming, running) are themselves counted: each resumed operation
	// drops out of the count as it settles, so the rest drain behind it exactly
	// as the pending arm's do.
	queryArgs = append(queryArgs, driverCapArgs...)
	queryArgs = append(queryArgs, stringArgs(activeStates)...)
	// Stale-active barrier-park exemption (see the staleness clause below): a
	// multi-deployment operation parked at the cutover barrier under an
	// ordered-cutover policy (barrier or parallel) is reserved for the
	// deployment-ordered cutover claim, so the copy claim must not re-lease it as
	// stale-active work. A single-operation apply has no sibling and is never
	// exempted, so manual --defer-cutover behaviour is unchanged.
	queryArgs = append(queryArgs,
		state.ApplyOperation.WaitingForCutover,
		storage.CutoverPolicyBarrier, storage.CutoverPolicyParallel)
	// A stopped operation is reclaimable for start and for cancel, but the two
	// arms are separate because only one of them terminates itself. The start
	// claim always resolves — it either moves the row to resuming or the parent
	// claim fails the request — so it needs no rematch guard. The cancel claim
	// leaves the row stopped when the parent apply refuses to be claimed (a
	// stopped apply is terminal), so it carries the rematch guard to keep an
	// undeliverable cancel from re-claiming a driver on every poll.
	queryArgs = append(queryArgs,
		state.ApplyOperation.Stopped,
		storage.ControlOperationStart, storage.ControlRequestPending)
	queryArgs = append(queryArgs, driverCapArgs...)
	queryArgs = append(queryArgs,
		state.ApplyOperation.Stopped,
		storage.ControlOperationCancel, storage.ControlRequestPending)
	// Deferred-deploy start gate keys on the PARENT apply state, not the
	// operation state: the operation row stays active (running) while the apply
	// parks at waiting_for_deploy, because the copy phase finished and only the
	// deploy is deferred. The operation row is never persisted to
	// waiting_for_deploy, so an operation-state predicate would never match.
	// Gating on the parent's waiting_for_deploy state mirrors
	// ApplyStore.ClaimApplyByID's deferred-deploy clause.
	queryArgs = append(queryArgs, stringArgs(activeStates)...)
	queryArgs = append(queryArgs, state.Apply.WaitingForDeploy)
	queryArgs = append(queryArgs,
		storage.ControlOperationStart, storage.ControlRequestPending)
	queryArgs = append(queryArgs, driverCapArgs...)
	queryArgs = append(queryArgs, state.ApplyOperation.FailedRetryable)
	queryArgs = append(queryArgs, state.Apply.FailedRetryable, maxRecoveryAttempts, retryableRecoveryFreshnessDays)
	queryArgs = append(queryArgs, stringArgs(activeStates)...)

	// The stopped-row and failed_retryable clauses mirror ApplyStore.ClaimApplyByID:
	// neither carries a deployment-order gate, because both rows already ran —
	// resuming them is recovering work they started, not starting a new deployment.
	//
	//   - A stopped operation whose parent apply has a pending start request is
	//     reclaimable so the operator can resume it, and one whose parent has a
	//     pending cancel request is reclaimable so the drive can terminalize it.
	//     The cancel arm carries the rematch guard because its claim can leave
	//     the row stopped; see requestRematchGuardSQL.
	//
	//   - An active operation whose PARENT apply is waiting_for_deploy and has a
	//     pending start request is reclaimable so the operator can trigger the
	//     deferred deploy. The operation row stays running while the apply parks
	//     at waiting_for_deploy (only the deploy is deferred), so this gate keys
	//     on the parent apply state, mirroring ApplyStore.ClaimApplyByID.
	//
	//   - A failed_retryable operation is reclaimable only while its PARENT apply
	//     is itself claimable for that operation's recovery. The operator claim
	//     path drives the parent apply, so the operation row is a shadow of the
	//     parent: gating on the parent's claimability (not just its retry budget)
	//     is what keeps a healthy retry from being re-claimed every poll. The two
	//     sub-conditions mirror the parent clauses in ApplyStore.ClaimApplyByID:
	//       * parent still failed_retryable, within recovery budget (attempt < max)
	//         and recent — a fresh bounded retry; and
	//       * parent already claimed into an active state but its lease has gone
	//         stale — crash recovery, with no budget gate (the attempt was already
	//         admitted and counted when the parent was claimed).
	//     Claiming a failed_retryable parent transitions it to running and refreshes
	//     applies.updated_at (see persistApplyClaim), so once a driver owns the
	//     retry neither sub-condition matches and peers back off instead of
	//     churning on a row another driver is actively driving.
	//
	// There is intentionally no "pending + pending start request" clause to
	// match ApplyStore.ClaimApplyByID's pending-start clause. That apply-level
	// clause only matters because apply-level pending claimability is
	// task-gated (state = pending AND EXISTS tasks); a start request lets a
	// no-task pending apply be claimed. Operation-level pending claimability is
	// instead deployment-order-gated (the clause below), so a pending operation
	// is already claimable the moment it is legal to start — once every
	// earlier-deployment sibling has completed. A parent start request must not
	// relax that gate: adding an ungated pending-start clause would let a later
	// deployment be claimed out of order while an earlier one is still
	// non-completed, and
	// a gated one would be redundant with the pending clause below. Start
	// requests resume eligible work; they do not reorder the rollout.
	retryFreshnessCutoff := s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, ParameterIntervalAmount(), IntervalDay)
	// The two guarded arms differ in the equal-timestamp case for the same reason
	// they do in ApplyStore.ClaimApplyByID: the stopped+cancel claim moves the row
	// out of stopped atomically with its lease rotation, so no peer can match it
	// in the same second and it can admit an equal timestamp; the
	// waiting_for_deploy claim leaves the row where it is and must refuse one.
	// See leaseRematchComparison.
	deferredDeployGuard := requestRematchGuardSQL("apply_operations", rematchOnlyAfterLease, staleClaimCutoff)
	stoppedCancelGuard := requestRematchGuardSQL("apply_operations", rematchAtOrAfterLease, staleClaimCutoff)
	row := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT %s
		FROM apply_operations
		WHERE (
			(
				state = ?
				AND (
					(
						apply_operations.operation_kind <> ?
						AND NOT EXISTS (
							SELECT 1
							FROM apply_operations AS earlier
							WHERE earlier.apply_id = apply_operations.apply_id
								AND earlier.deployment <> apply_operations.deployment
								AND (earlier.created_at, earlier.id) < (apply_operations.created_at, apply_operations.id)
								AND (
									(
										apply_operations.cutover_policy = ?
										AND earlier.state NOT IN (?, ?, ?, ?)
									)
									OR (
										apply_operations.cutover_policy NOT IN (?, ?)
										AND earlier.state <> ?
									)
								)
								AND `+releasedFailureExemptionSQL+`
						)
					)
					OR (
						apply_operations.operation_kind = ?
						AND NOT EXISTS (
							SELECT 1
							FROM apply_operations AS sibling
							WHERE sibling.apply_id = apply_operations.apply_id
								AND sibling.deployment = apply_operations.deployment
								AND sibling.operation_kind = ?
								AND CASE
									WHEN POSITION('/' IN sibling.operation_key) = 0 THEN sibling.operation_key
									ELSE SUBSTRING(sibling.operation_key FROM 1 FOR POSITION('/' IN sibling.operation_key) - 1)
								END = CASE
									WHEN POSITION('/' IN apply_operations.operation_key) = 0 THEN apply_operations.operation_key
									ELSE SUBSTRING(apply_operations.operation_key FROM 1 FOR POSITION('/' IN apply_operations.operation_key) - 1)
								END
								AND sibling.state <> ?
							)
						)
				)
				AND NOT EXISTS (
					SELECT 1
					FROM apply_control_requests cr
					WHERE cr.apply_id = apply_operations.apply_id
						AND cr.operation = ?
						AND cr.status = ?
				)
				AND EXISTS (
					SELECT 1
					FROM applies a
					WHERE a.id = apply_operations.apply_id
						AND (
							a.state NOT IN (%s)
							OR (
								a.state = ?
								AND EXISTS (
									SELECT 1
									FROM apply_control_requests cr
									WHERE cr.apply_id = a.id
										AND cr.operation = ?
										AND cr.status = ?
								)
							)
						)
				)
				AND `+driverCap+`
			)
			OR (
				state IN (%s)
				AND updated_at < %s
				AND NOT (
					state = ?
					AND cutover_policy IN (?, ?)
					AND EXISTS (
						SELECT 1
						FROM apply_operations AS sibling
						WHERE sibling.apply_id = apply_operations.apply_id
							AND sibling.id <> apply_operations.id
					)
				)
			)
			OR (
				state = ?
				AND EXISTS (
					SELECT 1
					FROM apply_control_requests cr
					WHERE cr.apply_id = apply_operations.apply_id
						AND cr.operation = ?
						AND cr.status = ?
				)
				AND `+driverCap+`
			)
			OR (
				state = ?
				AND EXISTS (
					SELECT 1
					FROM apply_control_requests cr
					WHERE cr.apply_id = apply_operations.apply_id
						AND cr.operation = ?
						AND cr.status = ?
						AND `+stoppedCancelGuard+`
				)
			)
			OR (
				state IN (%s)
				AND EXISTS (
					SELECT 1
					FROM applies a
					WHERE a.id = apply_operations.apply_id
						AND a.state = ?
				)
				AND EXISTS (
					SELECT 1
					FROM apply_control_requests cr
					WHERE cr.apply_id = apply_operations.apply_id
						AND cr.operation = ?
						AND cr.status = ?
						AND `+deferredDeployGuard+`
				)
				AND `+driverCap+`
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
								AND a.updated_at >= %s
							)
							OR (
								a.state IN (%s)
								AND a.updated_at < %s
							)
						)
				)
			)
		)
		ORDER BY created_at, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, applyOperationColumns, terminalStatePlaceholders, activeStatePlaceholders, staleClaimCutoff, activeStatePlaceholders, retryFreshnessCutoff, activeStatePlaceholders, staleClaimCutoff), queryArgs...)

	ad, err := scanApplyOperationInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // nothing to claim
	}
	if err != nil {
		return nil, fmt.Errorf("query next claimable apply_operation: %w", err)
	}

	// Rotate a fresh operation lease onto the claimed row so the claiming driver
	// can guard operation-scoped writes on this token. Mirrors persistApplyClaim
	// at the apply level.
	leaseToken := uuid.NewString()
	leaseAcquiredAt := time.Now()

	switch ad.State {
	case state.ApplyOperation.Pending:
		// Pending → running: stamp started_at, rotate the lease, and update the
		// heartbeat in the same write. WHERE state = ? guards against a concurrent
		// transition landing between the SELECT and this UPDATE; RowsAffected == 0
		// means another writer already moved the row, so we back off cleanly. The
		// NOT EXISTS stop guard mirrors the SELECT's pending-stop gate so a stop
		// request committed between the SELECT and this UPDATE still wins — the
		// pending sibling is not started once a stop is pending.
		result, err := tx.ExecContext(ctx, `
			UPDATE apply_operations
			SET state = ?, started_at = COALESCE(started_at, NOW()), updated_at = NOW(),
			    lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
			WHERE id = ? AND state = ?
				AND NOT EXISTS (
					SELECT 1
					FROM apply_control_requests cr
					WHERE cr.apply_id = ? AND cr.operation = ? AND cr.status = ?
				)
		`, state.ApplyOperation.Running, owner, leaseToken, ad.ID, state.ApplyOperation.Pending,
			ad.ApplyID, storage.ControlOperationStop, storage.ControlRequestPending)
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
	case state.ApplyOperation.Stopped:
		// Stopped → resuming: a stopped operation whose parent apply has a
		// pending start or cancel request is claimable, mirroring the
		// apply-level stopped claim. The claim must move the row out of stopped
		// atomically with the lease rotation because the stopped-row predicate
		// is request-gated, not heartbeat-gated: while the row stays stopped it
		// keeps matching that predicate and a peer could re-lease it mid-drive.
		// As resuming (an active state) it is re-leasable only by the
		// heartbeat-guarded stale-active clause, which recovers a crashed drive.
		// A cancel drive holds resuming only transiently: after the drive the
		// row is re-derived from its own tasks — cancelled tasks terminalize
		// it, and a drive that delivered nothing settles it back to stopped.
		// A crashed drive's leftover resuming row is recovered by the
		// stale-active clause, whose next drive re-derives it the same way or,
		// when the parent already terminalized, mirrors the terminal parent
		// down (reconcileUnclaimableParent).
		// WHERE state = ? guards a concurrent transition landing between the
		// SELECT and this UPDATE; RowsAffected == 0 means another writer moved it.
		result, err := tx.ExecContext(ctx, `
			UPDATE apply_operations
			SET state = ?, updated_at = NOW(),
			    lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
			WHERE id = ? AND state = ?
		`, state.ApplyOperation.Resuming, owner, leaseToken, ad.ID, state.ApplyOperation.Stopped)
		if err != nil {
			return nil, fmt.Errorf("claim stopped apply_operation %d: %w", ad.ID, err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("read claim rows affected for stopped apply_operation %d: %w", ad.ID, err)
		}
		if rows == 0 {
			return nil, nil
		}
	default:
		// Re-leasing a row that already started with a stale active heartbeat:
		// crash recovery. The row keeps its current (active) state and is driven
		// by the caller, so rotate the lease onto this driver and refresh the
		// heartbeat.
		_, err = tx.ExecContext(ctx, `
			UPDATE apply_operations
			SET updated_at = NOW(),
			    lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
			WHERE id = ?
		`, owner, leaseToken, ad.ID)
		if err != nil {
			return nil, fmt.Errorf("refresh heartbeat for claimed apply_operation %d: %w", ad.ID, err)
		}

		// Re-claiming a failed_retryable operation is a redispatch, so consume one
		// unit of the parent apply's retry budget. Only multi-operation applies are
		// charged here: a single-operation apply's parent attempt is incremented by
		// the ClaimApplyByID its drive performs, so bumping here too would
		// double-count. Without this, an operation-only multi-deployment retry never
		// advances applies.attempt, so ExpireRetryable's budget path never fires and
		// a permanently failing deployment retries forever — stranding a healthy
		// sibling parked at the cutover barrier under on_failure "continue". The
		// EXISTS-sibling guard selects multi-op, and the failed_retryable parent
		// guard keeps this to genuine redispatches (a terminal-failed parent never
		// re-leases its operations). The attempt < budget guard makes the
		// increment idempotent once the budget is spent: two operators can claim
		// different failed_retryable operations of the same apply concurrently, and
		// the row lock this UPDATE takes serializes them, so the second sees the
		// already-incremented attempt and does not overshoot maxRecoveryAttempts.
		if ad.State == state.ApplyOperation.FailedRetryable {
			// Advance the operation's own attempt only on a genuine deliberate
			// redispatch — the parent apply is still failed_retryable. A
			// failed_retryable child is also re-claimed for crash recovery when
			// the parent apply is already active (running) with a stale heartbeat
			// (its retry was admitted, then the driver crashed); that re-lease
			// must leave attempt untouched so the orphaned dispatch's idempotency
			// key stays stable and is reused, not duplicated. The parent-state
			// gate mirrors the budget bump below; the join lets one statement both
			// test the parent state and write the child row. Unlike the budget
			// bump this is not gated on multi-operation: attempt is
			// operation-local and a sibling's retry must never rotate it.
			attemptBump := s.dialect.JoinedUpdate(
				"apply_operations", "o", "applies", "a", "a.id = o.apply_id",
				[]JoinedUpdateAssignment{
					{Column: "attempt", Expr: "o.attempt + 1"},
					{Column: "updated_at", Expr: "NOW()"},
				},
				"o.id = ? AND a.state = ?",
			)
			res, err := tx.ExecContext(ctx, attemptBump, ad.ID, state.Apply.FailedRetryable)
			if err != nil {
				return nil, fmt.Errorf("advance attempt for apply_operation %d redispatch: %w", ad.ID, err)
			}
			bumped, err := res.RowsAffected()
			if err != nil {
				return nil, fmt.Errorf("read attempt bump rows affected for apply_operation %d: %w", ad.ID, err)
			}
			if bumped > 0 {
				ad.Attempt++
			}

			if _, err := tx.ExecContext(ctx, `
				UPDATE applies
				SET attempt = attempt + 1, updated_at = NOW()
				WHERE id = ?
					AND state = ?
					AND attempt < ?
					AND EXISTS (
						SELECT 1 FROM apply_operations o
						WHERE o.apply_id = applies.id AND o.id <> ?
					)
			`, ad.ApplyID, state.Apply.FailedRetryable, maxRecoveryAttempts, ad.ID); err != nil {
				return nil, fmt.Errorf("consume retry budget for apply_operation %d redispatch: %w", ad.ID, err)
			}
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

// FindNextApplyOperationCutover atomically claims the next operation parked at
// the cutover barrier whose turn it is, in deployment order, and rotates a fresh
// operation lease onto it in the same transaction. It is the cutover counterpart
// to FindNextApplyOperation: that primitive gates the copy phase (pending →
// running); this one gates the cutover phase (waiting_for_cutover → cutting_over).
//
// Both claim paths are bound by a shared eligibility gate — barrier policy, a
// multi-operation apply, and a parent apply not started with manual
// --defer-cutover — so the worker only ever drives the operations OC-2
// parks-and-releases for it (see the gate comment in the body and
// shouldReleaseAtCutoverBarrier in pkg/tern/cutover_barrier.go). Anything else
// (single-op, rolling, or manual-defer) stays on the copy/manual cutover path.
//
// Two claim paths, mirroring FindNextApplyOperation:
//
//   - Start a parked cutover. A waiting_for_cutover row is claimed and
//     transitioned to cutting_over only when every earlier deployment_order
//     sibling has reached completed and no pending stop control request exists.
//     Unlike the copy gate's barrier relaxation, the cutover gate's "done" set is
//     completed-only, so the high-risk swaps never overlap and run strictly in
//     order. The on_failure "continue" exemption lets a terminal-failed earlier
//     sibling stop blocking, matching the copy gate.
//   - Recover a stale in-flight cutover. A row already in cutting_over or
//     revert_window whose heartbeat has gone stale is re-leased without changing
//     its state — its driver died mid-cutover and another driver resumes it. This
//     path carries no ordering gate: the row already started its cutover, so
//     resuming is recovering work, not starting a new swap.
//
// As with FindNextApplyOperation, the returned row carries its pre-claim state:
// a returned waiting_for_cutover row means a parked cutover was just started (the
// row is now cutting_over), while a returned cutting_over/revert_window row means
// an in-flight cutover was recovered. owner is required and recorded as the lease
// owner. Returns nil when nothing is ready to cut over.
//
// This path shares FindNextApplyOperation's claim ordering and its dependence
// on an index over (created_at, id): the same OR'd state filter defeats every
// state-prefixed index, so without that index the claim sorts the full
// candidate set before LIMIT 1 — locking all of it on InnoDB, where the sort
// runs under FOR UPDATE and turns SKIP LOCKED into a serializer, and paying
// the sort itself on every dialect. Both claim queries draw from the same
// driver pool, so a change to one claim's ordering or eligibility belongs in
// the other.
//
// It carries no maxDriversPerApply cap, unlike the copy claim's
// pending arm, because the start gate above already bounds an apply to one
// concurrent cutover: an earlier sibling that is itself parked is not completed,
// so it blocks every later cutover. A cap could only bind here by exceeding what
// the ordering gate already allows. The recovery arm is uncapped for the same
// reason it is there — a stale mid-cutover row must always be re-leasable.
func (s *applyOperationStore) FindNextApplyOperationCutover(ctx context.Context, owner string) (*storage.ApplyOperation, error) {
	if owner == "" {
		return nil, fmt.Errorf("operator owner is required to claim apply_operation cutover: %w", storage.ErrApplyLeaseLost)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin claim apply_operation cutover transaction: %w", err)
	}
	defer rollbackTx(ctx, tx, "claim apply_operation cutover")

	// Eligibility gate (the three leading conditions in the SQL below). The
	// cutover worker may only claim operations it is actually responsible for
	// driving — exactly the population OC-2 parks-and-releases. This mirrors
	// shouldReleaseAtCutoverBarrier in pkg/tern/cutover_barrier.go:
	//
	//   1. cutover_policy is ordered (barrier or parallel) — rolling rows are
	//      never deployment-ordered at the cutover phase. Barrier and parallel
	//      both park at the barrier and cut over in deployment order; they differ
	//      only in the copy-start gate, so both are eligible here.
	//   2. multi-operation apply (EXISTS sibling) — a single-op apply has no
	//      siblings to order, so it never auto-defers; its cutover stays on the
	//      copy/manual path. Mirrors shouldAutoDeferCutover's multiOperation arg.
	//   3. parent apply NOT started with manual --defer-cutover — that contract
	//      keeps the operator holding the claim and polling for a manual cutover,
	//      so the cutover worker must not steal it. defer_cutover lives in the
	//      applies.options JSON (boolean, omitted when false), not a column; the
	//      predicate matches exactly JSON boolean true per Dialect.JSONBooleanIsTrue,
	//      so absent keys, JSON null, and non-boolean values do not match.
	//
	// The gate wraps BOTH claim branches (start and stale recovery): a stale
	// cutting_over/revert_window row outside this population reached that state
	// via the copy/manual path, so recovering it here would steal legitimate work.
	queryArgs := []any{storage.CutoverPolicyBarrier, storage.CutoverPolicyParallel}
	// Start-a-parked-cutover gate (see SQL below). A waiting_for_cutover row is
	// claimable only when no earlier deployment_order sibling is still
	// non-completed; releasedFailureExemptionArgs exempt a terminal-failed earlier
	// sibling so it no longer blocks later cutovers under "continue", or under
	// "pause" once a release latches the rollout open; and the pending-stop NOT
	// EXISTS makes `stop` halt remaining cutovers even under those exemptions.
	queryArgs = append(queryArgs,
		state.ApplyOperation.WaitingForCutover,
		state.ApplyOperation.Completed,
	)
	queryArgs = append(queryArgs, releasedFailureExemptionArgs()...)
	queryArgs = append(queryArgs,
		storage.ControlOperationStop, storage.ControlRequestPending,
	)
	// Recovery clause: a row already mid-cutover whose heartbeat has gone stale is
	// re-leased without changing state, so it carries no ordering gate (but is
	// still bound by the eligibility gate above).
	queryArgs = append(queryArgs,
		state.ApplyOperation.CuttingOver,
		state.ApplyOperation.RevertWindow,
	)

	staleClaimCutoff := s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, LiteralIntervalAmount(uint64(storage.ApplyLeaseStaleAfter.Microseconds())), IntervalMicrosecond)
	deferCutoverIsTrue := s.dialect.JSONBooleanIsTrue("a.options", []string{"defer_cutover"})
	row := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT %s
		FROM apply_operations
		WHERE apply_operations.cutover_policy IN (?, ?)
		AND EXISTS (
			SELECT 1
			FROM apply_operations AS sibling
			WHERE sibling.apply_id = apply_operations.apply_id
				AND sibling.id <> apply_operations.id
		)
		AND EXISTS (
			SELECT 1
			FROM applies AS a
			WHERE a.id = apply_operations.apply_id
				AND NOT %s
		)
		AND (
			(
				state = ?
				AND NOT EXISTS (
					SELECT 1
					FROM apply_operations AS earlier
					WHERE earlier.apply_id = apply_operations.apply_id
						AND (earlier.created_at, earlier.id) < (apply_operations.created_at, apply_operations.id)
						AND earlier.state <> ?
						AND `+releasedFailureExemptionSQL+`
				)
				AND NOT EXISTS (
					SELECT 1
					FROM apply_control_requests cr
					WHERE cr.apply_id = apply_operations.apply_id
						AND cr.operation = ?
						AND cr.status = ?
				)
			)
			OR (state IN (?, ?) AND updated_at < %s)
		)
		ORDER BY created_at, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, applyOperationColumns, deferCutoverIsTrue, staleClaimCutoff), queryArgs...)

	ad, err := scanApplyOperationInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // nothing ready to cut over
	}
	if err != nil {
		return nil, fmt.Errorf("query next claimable apply_operation cutover: %w", err)
	}

	// Rotate a fresh operation lease onto the claimed row, mirroring
	// FindNextApplyOperation.
	leaseToken := uuid.NewString()
	leaseAcquiredAt := time.Now()

	if ad.State == state.ApplyOperation.WaitingForCutover {
		// waiting_for_cutover → cutting_over: rotate the lease and refresh the
		// heartbeat in the same write, leaving started_at untouched (it was
		// stamped when the copy phase started). WHERE state = ? guards against a
		// concurrent transition between the SELECT and this UPDATE; the mirrored
		// pending-stop NOT EXISTS lets a stop committed in that window still win.
		// RowsAffected == 0 means another writer moved the row or a stop landed,
		// so we back off cleanly.
		result, err := tx.ExecContext(ctx, `
			UPDATE apply_operations
			SET state = ?, updated_at = NOW(),
			    lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
			WHERE id = ? AND state = ?
				AND NOT EXISTS (
					SELECT 1
					FROM apply_control_requests cr
					WHERE cr.apply_id = ? AND cr.operation = ? AND cr.status = ?
				)
		`, state.ApplyOperation.CuttingOver, owner, leaseToken, ad.ID, state.ApplyOperation.WaitingForCutover,
			ad.ApplyID, storage.ControlOperationStop, storage.ControlRequestPending)
		if err != nil {
			return nil, fmt.Errorf("claim waiting_for_cutover apply_operation %d: %w", ad.ID, err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("read cutover claim rows affected for apply_operation %d: %w", ad.ID, err)
		}
		if rows == 0 {
			return nil, nil
		}
	} else {
		// Recovering a stale in-flight cutover (cutting_over or revert_window):
		// keep the current state and rotate the lease onto this driver.
		_, err = tx.ExecContext(ctx, `
			UPDATE apply_operations
			SET updated_at = NOW(),
			    lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
			WHERE id = ?
		`, owner, leaseToken, ad.ID)
		if err != nil {
			return nil, fmt.Errorf("refresh heartbeat for recovered cutover apply_operation %d: %w", ad.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim apply_operation cutover %d: %w", ad.ID, err)
	}

	ad.LeaseOwner = owner
	ad.LeaseToken = leaseToken
	ad.LeaseAcquiredAt = &leaseAcquiredAt

	return ad, nil
}

// ReleaseClaim clears the lease fields and backdates the heartbeat past the
// staleness window, guarded on the lease token, so the row is re-claimable on
// the next poll through the stale-active recovery arm of
// FindNextApplyOperation. State is intentionally untouched: a released row is
// indistinguishable from a crashed driver's work, and the recovery arm already
// re-leases active rows without changing their state. Reports false when the
// lease no longer matches, meaning another writer already rotated or cleared
// it and the row has moved on.
func (s *applyOperationStore) ReleaseClaim(ctx context.Context, lease storage.OperationLease) (bool, error) {
	if !lease.Valid() {
		return false, fmt.Errorf("release claim for apply_operation %d: %w", lease.OperationID, storage.ErrApplyLeaseLost)
	}
	// Backdate one second past the staleness window: updated_at has second
	// precision, so a backdate of exactly the window can round onto the cutoff
	// boundary and fail the claim's strict comparison.
	staleBackdate := storage.ApplyLeaseStaleAfter + time.Second
	staleHeartbeat := s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, LiteralIntervalAmount(uint64(staleBackdate.Microseconds())), IntervalMicrosecond)
	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE apply_operations
		SET lease_owner = '', lease_token = '', lease_acquired_at = NULL,
		    updated_at = %s
		WHERE id = ? AND lease_token = ?
	`, staleHeartbeat), lease.OperationID, lease.Token)
	if err != nil {
		return false, fmt.Errorf("release claim for apply_operation %d: %w", lease.OperationID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read release claim rows affected for apply_operation %d: %w", lease.OperationID, err)
	}
	return rows > 0, nil
}

// Heartbeat refreshes updated_at to maintain the claim's lease. Should be
// called periodically by a driver holding the lease. Silent no-op when the
// row no longer exists (mirrors ApplyStore.Heartbeat).
func (s *applyOperationStore) Heartbeat(ctx context.Context, id int64) error {
	guard, err := operationWriteGuardFromContext(ctx)
	if err != nil {
		return err
	}
	args := append([]any{id}, guard.args()...)
	result, err := s.db.ExecContext(ctx, guard.updateStatement(s.dialect, nil), args...)
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
	query := s.dialect.JoinedDelete(
		"apply_operations", "ao", "applies", "a", "a.id = ao.apply_id",
		"ao.apply_id = ? AND "+s.dialect.LeaseTokenFence("applies", "a", "id", "lease_token"),
	)
	result, err := s.db.ExecContext(ctx, query, applyID, lease.Token)
	if err != nil {
		return fmt.Errorf("delete apply_operations for apply %d: %w", applyID, err)
	}
	if _, err := confirmLeaseOnZeroRows(ctx, s.db, result, lease, "deleted apply_operations", fmt.Sprintf("apply %d", applyID)); err != nil {
		return err
	}
	return nil
}

// MarkPendingStoppedByApply transitions every still-pending operation of an
// apply to stopped, returning the number of rows changed. It is the operator's
// stop-reconciliation primitive: once a stop is pending the claim gate keeps
// pending siblings from ever starting, so they must be terminalized here or the
// apply would strand non-terminal under on_failure "continue" — a failed sibling
// holds the projection running while the pending siblings never settle.
//
// Only pending rows are touched, never running or terminal: an in-flight
// operation is left for its own driver (which observes the stop through the
// engine) and an already-terminal operation keeps its recorded result. stopped
// is resumable, so completed_at is left nil to match the apply-level convention.
// Writes are apply-lease guarded when a lease is present in ctx, mirroring
// DeleteByApply.
func (s *applyOperationStore) MarkPendingStoppedByApply(ctx context.Context, applyID int64) (int64, error) {
	// An operation-lease-only drive (fan-out multi-operation mode) holds no
	// parent apply lease, but it is still an authorized driver of the apply's
	// rollout and may terminalize the apply's still-pending sibling operations
	// under a pending stop. Authorize the bulk stop by joining the owning
	// operation row and requiring it still carries the lease token, so a
	// displaced driver that lost its lease fails closed instead of stopping
	// siblings it no longer owns. Operation lease takes precedence over the
	// parent apply lease, matching operationWriteGuardFromContext.
	if opLease, ok := storage.OperationLeaseFromContext(ctx); ok {
		if !opLease.Valid() {
			return 0, fmt.Errorf("invalid operation lease for stopping pending apply_operations of apply %d: %w", applyID, storage.ErrApplyLeaseLost)
		}
		if opLease.ApplyID != applyID {
			return 0, fmt.Errorf("operation lease for apply %d does not authorize stopping pending operations of apply %d: %w", opLease.ApplyID, applyID, storage.ErrApplyLeaseLost)
		}
		query := s.dialect.JoinedUpdate(
			"apply_operations", "ao", "apply_operations", "owner_op", "owner_op.apply_id = ao.apply_id",
			[]JoinedUpdateAssignment{
				{Column: "state", Expr: "?"},
				{Column: "updated_at", Expr: "NOW()"},
			},
			"ao.apply_id = ? AND ao.state = ? AND owner_op.id = ? AND "+s.dialect.LeaseTokenFence("apply_operations", "owner_op", "id", "lease_token"),
		)
		result, err := s.db.ExecContext(ctx, query, state.ApplyOperation.Stopped, applyID, state.ApplyOperation.Pending, opLease.OperationID, opLease.Token)
		if err != nil {
			return 0, fmt.Errorf("stop pending apply_operations for apply %d under operation lease: %w", applyID, err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("read stopped pending apply_operations rows affected for apply %d: %w", applyID, err)
		}
		if rows == 0 {
			// No pending rows changed: either there were none (lease still
			// valid, a legitimate no-op) or the operation lease token no longer
			// matches (ownership lost). Distinguish so a displaced driver fails
			// closed.
			if err := ensureOperationLeaseStillOwned(ctx, s.db, opLease); err != nil {
				return 0, err
			}
		}
		return rows, nil
	}

	lease, hasLease, err := applyLeaseFromContext(ctx, applyID)
	if err != nil {
		return 0, err
	}
	if !hasLease {
		result, err := s.db.ExecContext(ctx, `
			UPDATE apply_operations
			SET state = ?, updated_at = NOW()
			WHERE apply_id = ? AND state = ?
		`, state.ApplyOperation.Stopped, applyID, state.ApplyOperation.Pending)
		if err != nil {
			return 0, fmt.Errorf("stop pending apply_operations for apply %d: %w", applyID, err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("read stopped pending apply_operations rows affected for apply %d: %w", applyID, err)
		}
		return rows, nil
	}
	query := s.dialect.JoinedUpdate(
		"apply_operations", "ao", "applies", "a", "a.id = ao.apply_id",
		[]JoinedUpdateAssignment{
			{Column: "state", Expr: "?"},
			{Column: "updated_at", Expr: "NOW()"},
		},
		"ao.apply_id = ? AND ao.state = ? AND "+s.dialect.LeaseTokenFence("applies", "a", "id", "lease_token"),
	)
	result, err := s.db.ExecContext(ctx, query, state.ApplyOperation.Stopped, applyID, state.ApplyOperation.Pending, lease.Token)
	if err != nil {
		return 0, fmt.Errorf("stop pending apply_operations for apply %d: %w", applyID, err)
	}
	rows, err := confirmLeaseOnZeroRows(ctx, s.db, result, lease, "stopped pending apply_operations", fmt.Sprintf("apply %d", applyID))
	if err != nil {
		return 0, err
	}
	return rows, nil
}

// strandedParentQuiescence is how long a parent apply must have been settled
// before the reaper mirrors its outcome onto a pending operation row. Stranded
// rows keep indefinitely, so the wait costs nothing, and it keeps the reaper
// clear of an apply that only just terminalized and whose own paths — stop
// reconciliation, terminal derivation — may still be writing sibling rows.
const strandedParentQuiescence = 10 * time.Minute

// strandedReaperLockName is the advisory lock that elects one reaper per pass.
// It is instance-wide rather than per-target: the reaper scans every deployment's
// rows in one pass, so there is nothing to scope it to.
const strandedReaperLockName = "schemabot_stranded_reaper"

// strandedOperationSweep identifies the stranded-operation sweep to the shared
// election wrapper.
var strandedOperationSweep = strandedSweep{
	lockName: strandedReaperLockName,
	busy:     storage.ErrStrandedReaperBusy,
	subject:  "stranded apply_operations",
}

// strandedParentGate renders the EXISTS admitting only parents whose outcome can
// no longer change, correlated on the given apply_id column. Both a reaper
// sweep's SELECT and its guarded per-row UPDATE assert it, so the write
// re-verifies the parent it was chosen for rather than trusting a read that may
// be seconds old.
//
// Two conditions, each for its own reason:
//   - settled state: the rollout has a verdict, so a child row describing
//     future work (a pending operation, a promised task retry) is dead.
//   - quiescent for the given window: the apply's own paths — stop
//     reconciliation, terminal derivation — may still be writing child rows
//     just after it terminalized. Each reaper picks the window matching what
//     could still race it.
func strandedParentGate(d Dialect, correlation string, quiescence time.Duration) (string, []any) {
	parentStates := settledApplyStates()
	quiescentBefore := d.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime,
		LiteralIntervalAmount(uint64(quiescence.Microseconds())), IntervalMicrosecond)

	args := stringArgs(parentStates)
	return fmt.Sprintf(`EXISTS (
			SELECT 1
			FROM applies a
			WHERE a.id = %s
				AND a.state IN (%s)
				AND a.updated_at < %s
		)`, correlation, placeholders(len(parentStates)), quiescentBefore), args
}

// unleasedOperationGate renders the NOT EXISTS admitting only task rows whose
// operation no driver currently holds.
//
// This is what separates a reaper from a driver by the same mechanism drivers
// are separated from each other, rather than by a timing argument. A driver
// takes an operation lease to work an operation and heartbeats it for as long as
// it lives, and the claim path treats a lease whose heartbeat is older than
// storage.ApplyLeaseStaleAfter as re-claimable. Reading the lease the same way
// means a reaper writes a row only where a driver would be allowed to take it.
//
// What that buys is a window, not an impossibility. The gate is a plain
// NOT EXISTS with no row lock, and the claim path locks apply_operations rather
// than tasks, so a driver claiming just after the reaper's UPDATE commits is not
// excluded by anything here. The residual race is the width of one autocommit
// statement; without the gate it was the width of the whole scan-to-write gap,
// which is seconds. Both sweeps assert the gate in the guarded write as well as
// the scan so that gap is not what the write rests on.
//
// It reads the lease slightly more strictly than the claim path does, in both
// directions that matter. It requires an owner, which the claim path's steal arm
// does not test, so an operation the operation reaper released with a fresh
// stamp is correctly unowned rather than mistaken for held. It omits the
// occupying-state filter that freshLeaseCountSQL carries, so a heartbeated lease
// holds the row whatever state its operation is in. Both departures make the
// reaper more reluctant to write than a driver is to claim, which is the safe
// direction when the write is a terminal verdict.
//
// It matters most under fan-out, where a live sibling drive holds only an
// operation lease: the parent apply can be settled and quiet while that drive
// works, so no gate on the parent sees it, and the lease is the only signal
// that does not depend on the drive having recently mirrored the row.
//
// A task with no operation is admitted. Nothing holds a lease over it, so there
// is nothing here to exclude it by, and it is left to the caller's other gates.
func unleasedOperationGate(d Dialect) string {
	freshLeaseAfter := d.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime,
		LiteralIntervalAmount(uint64(storage.ApplyLeaseStaleAfter.Microseconds())), IntervalMicrosecond)

	return fmt.Sprintf(`NOT EXISTS (
			SELECT 1
			FROM apply_operations lease_holder
			WHERE lease_holder.id = tasks.apply_operation_id
				AND lease_holder.lease_owner <> ''
				AND lease_holder.updated_at >= %s
		)`, freshLeaseAfter)
}

// ReapStranded elects one reaper per pass and reaps under the lock. See
// storage.ApplyOperationStore for the contract.
func (s *applyOperationStore) ReapStranded(ctx context.Context, limit int) ([]*storage.ReapedOperation, error) {
	return reapUnderElection(ctx, s.db, s.locker, strandedOperationSweep,
		func(ctx context.Context) ([]*storage.ReapedOperation, error) {
			return s.reapStranded(ctx, limit)
		})
}

// reapStranded mirrors settled parents' outcomes onto their pending, unleased
// operation rows, without electing a reaper. ReapStranded is the entry point that
// holds the lock; this is separate so the reaping itself can be exercised on its
// own.
//
// Each row is settled by its own committed write, so a mid-pass failure returns
// the settlements that already landed alongside the error: they are durable
// whatever the caller does next, and dropping them would leave real state
// changes with no log line and no count behind them.
func (s *applyOperationStore) reapStranded(ctx context.Context, limit int) ([]*storage.ReapedOperation, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("reap stranded apply_operations: limit must be positive, got %d", limit)
	}

	parentGate, parentGateArgs := strandedParentGate(s.dialect, "apply_operations.apply_id", strandedParentQuiescence)

	selectArgs := []any{state.ApplyOperation.Pending}
	selectArgs = append(selectArgs, parentGateArgs...)
	selectArgs = append(selectArgs, limit)
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT %s
		FROM apply_operations
		WHERE state = ?
			AND (lease_owner IS NULL OR lease_owner = '')
			AND %s
		ORDER BY created_at, id
		LIMIT ?
	`, applyOperationColumns, parentGate), selectArgs...)
	if err != nil {
		return nil, fmt.Errorf("query stranded apply_operations: %w", err)
	}
	stranded, err := scanApplyOperations(rows)
	utils.CloseAndLog(rows)
	if err != nil {
		return nil, fmt.Errorf("scan stranded apply_operations: %w", err)
	}
	if len(stranded) == 0 {
		return nil, nil
	}

	applyIDs := make([]int64, 0, len(stranded))
	for _, op := range stranded {
		applyIDs = append(applyIDs, op.ApplyID)
	}
	parents, err := loadSettledParents(ctx, s.db, applyIDs, "stranded apply_operations")
	if err != nil {
		return nil, err
	}

	reaped := make([]*storage.ReapedOperation, 0, len(stranded))
	for _, op := range stranded {
		parent, ok := parents[op.ApplyID]
		if !ok {
			// The parent was deleted between the two reads (PR cleanup races the
			// reaper). Its rows go with it, so there is nothing left to settle.
			slog.WarnContext(ctx, "parent apply disappeared while reaping a stranded apply_operation; the row is being deleted with it",
				op.LogAttrs()...)
			continue
		}
		settled, err := s.reapStrandedOperation(ctx, op, parent)
		if err != nil {
			return reaped, err
		}
		if !settled {
			// The row left pending (or was claimed) between the read and the
			// guarded write, so it is live again and belongs to whoever moved it.
			slog.DebugContext(ctx, "stranded apply_operation changed before it could be reaped; skipping",
				op.LogAttrs()...)
			continue
		}
		reaped = append(reaped, &storage.ReapedOperation{Operation: op, Parent: parent})
	}
	return reaped, nil
}

// loadSettledParents returns the settled parent applies of the rows a reaper
// sweep selected, keyed by apply id. subject names the reaped row kind for
// error context.
func loadSettledParents(ctx context.Context, db *rebindDB, applyIDs []int64, subject string) (map[int64]*storage.Apply, error) {
	args := make([]any, 0, len(applyIDs))
	seen := make(map[int64]bool, len(applyIDs))
	for _, applyID := range applyIDs {
		if seen[applyID] {
			continue
		}
		seen[applyID] = true
		args = append(args, applyID)
	}

	rows, err := db.QueryContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE id IN (`+placeholders(len(args))+`)
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("query settled parents of %d %s: %w", len(applyIDs), subject, err)
	}
	applies, err := scanApplies(rows)
	utils.CloseAndLog(rows)
	if err != nil {
		return nil, fmt.Errorf("scan settled parents of %d %s: %w", len(applyIDs), subject, err)
	}

	parents := make(map[int64]*storage.Apply, len(applies))
	for _, apply := range applies {
		parents[apply.ID] = apply
	}
	return parents, nil
}

// reapStrandedOperation writes one operation row from its settled parent,
// reporting whether the guarded write landed. The row keeps the same field
// stamping the claim-path settle uses: every settled parent state is
// non-resumable, so completed_at is stamped, and error_message is mirrored only
// for a failed parent — the state that owns an explanation.
//
// The write re-asserts the parent gate rather than trusting the sweep's read:
// the guarded UPDATE re-verifies the parent it was chosen for, so a write never
// lands on the strength of a read that may be seconds old.
func (s *applyOperationStore) reapStrandedOperation(ctx context.Context, op *storage.ApplyOperation, parent *storage.Apply) (bool, error) {
	setClause := "state = ?, completed_at = COALESCE(completed_at, NOW())"
	args := []any{parent.State}
	if state.IsState(parent.State, state.Apply.Failed) {
		setClause = "state = ?, error_message = ?, completed_at = COALESCE(completed_at, NOW())"
		args = []any{parent.State, nullString(parent.ErrorMessage)}
	}
	parentGate, parentGateArgs := strandedParentGate(s.dialect, "apply_operations.apply_id", strandedParentQuiescence)
	args = append(args, op.ID, state.ApplyOperation.Pending)
	args = append(args, parentGateArgs...)

	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_operations
		SET `+setClause+`, updated_at = NOW()
		WHERE id = ? AND state = ? AND (lease_owner IS NULL OR lease_owner = '')
			AND `+parentGate+`
	`, args...)
	if err != nil {
		return false, fmt.Errorf("reap stranded apply_operation %d (deployment %q) from %s parent apply: %w",
			op.ID, op.Deployment, parent.State, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read reaped rows for stranded apply_operation %d (deployment %q): %w",
			op.ID, op.Deployment, err)
	}
	if changed == 0 {
		return false, nil
	}

	// Mirror the write onto the returned row so a caller reporting the
	// settlement reads what is now stored, not the pre-write values.
	now := time.Now()
	op.State = parent.State
	op.UpdatedAt = now
	if op.CompletedAt == nil {
		op.CompletedAt = &now
	}
	if state.IsState(parent.State, state.Apply.Failed) {
		op.ErrorMessage = parent.ErrorMessage
	}
	return true, nil
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
	var externalID sql.NullString
	var externalOperationID sql.NullString
	var engineResumeContext, engineResumeMetadata sql.NullString
	var startedAt, completedAt, leaseAcquiredAt sql.NullTime

	if err := s.Scan(
		&ad.ID, &ad.ApplyID, &ad.Deployment, &ad.OperationKey, &ad.OperationKind, &ad.Target, &externalID, &externalOperationID, &ad.State, &errMsg,
		&ad.CutoverPolicy, &ad.OnFailure, &ad.Attempt, &startedAt, &completedAt, &ad.LeaseOwner, &ad.LeaseToken, &leaseAcquiredAt,
		&engineResumeContext, &engineResumeMetadata, &ad.CreatedAt, &ad.UpdatedAt,
	); err != nil {
		return nil, err
	}

	if errMsg.Valid {
		ad.ErrorMessage = errMsg.String
	}
	if externalID.Valid {
		ad.ExternalID = externalID.String
	}
	if externalOperationID.Valid {
		ad.ExternalOperationID = externalOperationID.String
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
