// merge_gate_requests.go implements MergeGateRequestStore for the durable
// merge gate fan-out.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

const mergeGateColumns = `id, apply_id, kind, apply_identifier, environment, database_type, database_name,
	provider, repository, change_key, requested_by, state, attempts,
	lease_owner, lease_token, lease_expires_at, retry_after, last_error,
	holds_recorded_at, completed_at, created_at, updated_at`

type mergeGateRequestStore struct {
	db       *sql.DB
	dialect  Dialect
	identity identityInserter
}

func (s *mergeGateRequestStore) Record(ctx context.Context, req *storage.MergeGateRequest) (bool, error) {
	if req.ApplyID == 0 {
		return false, fmt.Errorf("merge gate request requires the originating apply row id")
	}
	if req.ApplyIdentifier == "" {
		return false, fmt.Errorf("merge gate request for apply row %d requires the apply identifier", req.ApplyID)
	}
	if req.Kind != storage.MergeGateKindPreflight && req.Kind != storage.MergeGateKindSettle {
		return false, fmt.Errorf("merge gate request for apply %s has unknown kind %q", req.ApplyIdentifier, req.Kind)
	}
	if req.Environment == "" || req.DatabaseType == "" || req.DatabaseName == "" {
		return false, fmt.Errorf("merge gate request for apply %s requires environment, database type, and database name", req.ApplyIdentifier)
	}

	provider := req.Provider
	if provider == "" {
		provider = storage.ProviderGitHub
	}

	id, err := s.identity.InsertID(ctx, s.db, `
		INSERT INTO merge_gate_requests (
			apply_id, kind, apply_identifier, environment, database_type, database_name,
			provider, repository, change_key, requested_by, state
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, req.ApplyID, req.Kind, req.ApplyIdentifier, req.Environment, req.DatabaseType, req.DatabaseName,
		provider, req.Repository, req.ChangeKey, req.RequestedBy, storage.MergeGatePending)
	if err != nil {
		// The unique key on (apply_id, kind) is the idempotency guard: the
		// drive tails, the operator preflight gate, and the backfill sweeps may
		// all record the same apply and kind, and a request already in any
		// state means the fan-out is recorded or done.
		if isDuplicateKeyError(err) {
			return false, nil
		}
		return false, fmt.Errorf("record %s merge gate request for apply %s (%s/%s in %s): %w",
			req.Kind, req.ApplyIdentifier, req.DatabaseType, req.DatabaseName, req.Environment, err)
	}
	req.ID = id
	req.Provider = provider
	req.State = storage.MergeGatePending
	return true, nil
}

func (s *mergeGateRequestStore) GetByApplyAndKind(ctx context.Context, applyID int64, kind string) (*storage.MergeGateRequest, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+mergeGateColumns+`
		FROM merge_gate_requests
		WHERE apply_id = ? AND kind = ?
	`, applyID, kind)
	req, err := scanMergeGateRequest(row)
	if err != nil {
		return nil, fmt.Errorf("get %s merge gate request for apply row %d: %w", kind, applyID, err)
	}
	return req, nil
}

// ReopenForRetry re-arms a terminally failed request back to pending with a
// fresh attempt budget, so the operator preflight gate can recover an apply
// whose preflight exhausted its retries during an outage. Conditional on the
// failed state: pending, processing, and completed rows are left untouched.
func (s *mergeGateRequestStore) ReopenForRetry(ctx context.Context, id int64) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE merge_gate_requests
		SET state = ?, attempts = 0, lease_owner = NULL, lease_token = NULL,
			lease_expires_at = NULL, retry_after = NULL, completed_at = NULL,
			updated_at = NOW()
		WHERE id = ? AND state = ?
	`, storage.MergeGatePending, id, storage.MergeGateFailed)
	if err != nil {
		return false, fmt.Errorf("reopen merge gate request %d for retry: %w", id, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read reopened merge gate request %d rows affected: %w", id, err)
	}
	return rows > 0, nil
}

// ReopenTerminalPreflightsForActiveApplies re-arms terminally failed
// preflight requests whose apply is still non-terminal, so a code-host
// rendering that exhausted its retries (the stored holds themselves are
// storage-only and cannot fail on the code host) keeps retrying for as long
// as the apply runs. Terminal means unclaimable: no retry window, or attempts
// at the cap. Requests for terminal applies are left alone — the apply's
// settle re-plan supersedes the render.
func (s *mergeGateRequestStore) ReopenTerminalPreflightsForActiveApplies(ctx context.Context) (int64, error) {
	nonTerminal, nonTerminalArgs := nonTerminalApplyStatePredicate("a.state")
	args := []any{
		storage.MergeGatePending,
		storage.MergeGateKindPreflight, storage.MergeGateFailed, storage.MaxMergeGateAttempts,
	}
	args = append(args, nonTerminalArgs...)
	result, err := s.db.ExecContext(ctx, `
		UPDATE merge_gate_requests r
		JOIN applies a ON a.id = r.apply_id
		SET r.state = ?, r.attempts = 0, r.lease_owner = NULL, r.lease_token = NULL,
			r.lease_expires_at = NULL, r.retry_after = NULL, r.completed_at = NULL,
			r.updated_at = NOW()
		WHERE r.kind = ? AND r.state = ?
		  AND (r.retry_after IS NULL OR r.attempts >= ?)
		  AND `+nonTerminal+`
	`, args...)
	if err != nil {
		return 0, fmt.Errorf("reopen terminal preflight merge gate requests for active applies: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read reopened terminal preflight merge gate rows affected: %w", err)
	}
	return rows, nil
}

// mergeGateClaimablePredicate matches exactly the rows a processor would
// claim: a pending row, a retryable row whose retry window has elapsed and is
// under the attempt cap, or a processing row whose lease has expired and is
// under the attempt cap. Bind its placeholders with mergeGateClaimableArgs.
func (s *mergeGateRequestStore) mergeGateClaimablePredicate() string {
	return `(
			state = ?
			OR (state = ? AND retry_after IS NOT NULL AND retry_after <= ` + s.dialect.CurrentTimestamp(TimestampPrecisionDefault) + ` AND attempts < ?)
			OR (state = ? AND lease_expires_at <= ` + s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond) + ` AND attempts < ?)
		)`
}

// mergeGateClaimableArgs returns the placeholder bindings for
// mergeGateClaimablePredicate, in order.
func mergeGateClaimableArgs() []any {
	return []any{
		storage.MergeGatePending,
		storage.MergeGateFailed, storage.MaxMergeGateAttempts,
		storage.MergeGateProcessing, storage.MaxMergeGateAttempts,
	}
}

func (s *mergeGateRequestStore) ClaimNext(ctx context.Context, owner string, leaseDuration time.Duration) (*storage.MergeGateRequest, error) {
	if owner == "" {
		return nil, fmt.Errorf("merge gate claim owner is required")
	}
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("merge gate lease duration must be positive")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin claim merge gate request transaction: %w", err)
	}
	defer rollbackTx(ctx, tx, "claim merge gate request")

	var id int64
	err = tx.QueryRowContext(ctx, `
		SELECT id
		FROM merge_gate_requests
		WHERE `+s.mergeGateClaimablePredicate()+`
		ORDER BY created_at, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, mergeGateClaimableArgs()...).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query next claimable merge gate request: %w", err)
	}

	row := tx.QueryRowContext(ctx, `
		SELECT `+mergeGateColumns+`
		FROM merge_gate_requests
		WHERE id = ?
	`, id)
	req, err := scanMergeGateRequestInto(row)
	if err != nil {
		return nil, fmt.Errorf("load claimed merge gate request %d: %w", id, err)
	}

	leaseToken := uuid.NewString()
	now := time.Now()
	leaseExpiresAt := now.Add(leaseDuration)
	_, err = tx.ExecContext(ctx, `
		UPDATE merge_gate_requests
		SET state = ?, attempts = attempts + 1, lease_owner = ?, lease_token = ?,
			lease_expires_at = `+s.dialect.RelativeTime(TimestampPrecisionMicrosecond, AfterCurrentTime, ParameterIntervalAmount(), IntervalMicrosecond)+`,
			retry_after = NULL, updated_at = NOW()
		WHERE id = ?
	`, storage.MergeGateProcessing, owner, leaseToken, leaseDuration.Microseconds(), req.ID)
	if err != nil {
		return nil, fmt.Errorf("claim merge gate request %d: %w", req.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim merge gate request %d: %w", req.ID, err)
	}

	// Reflect the committed claim on the scanned row instead of reloading it.
	// The row was held under FOR UPDATE for the whole transaction, so no other
	// writer could have changed it. lease_expires_at is the application-clock
	// estimate of the database's NOW(6)+leaseDuration.
	req.State = storage.MergeGateProcessing
	req.Attempts++
	req.LeaseOwner = owner
	req.LeaseToken = leaseToken
	req.LeaseExpiresAt = &leaseExpiresAt
	req.RetryAfter = nil
	req.UpdatedAt = now

	return req, nil
}

func (s *mergeGateRequestStore) PendingForTarget(ctx context.Context, environment, databaseType, databaseName, kind string, excludeID int64) ([]*storage.MergeGateRequest, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+mergeGateColumns+`
		FROM merge_gate_requests
		WHERE environment = ? AND database_type = ? AND database_name = ?
		  AND kind = ? AND state = ? AND id != ?
		ORDER BY created_at, id
	`, environment, databaseType, databaseName, kind, storage.MergeGatePending, excludeID)
	if err != nil {
		return nil, fmt.Errorf("query pending %s merge gate requests for %s/%s in %s: %w",
			kind, databaseType, databaseName, environment, err)
	}
	defer utils.CloseAndLog(rows)

	var reqs []*storage.MergeGateRequest
	for rows.Next() {
		req, err := scanMergeGateRequestInto(rows)
		if err != nil {
			return nil, fmt.Errorf("scan pending merge gate request for %s/%s in %s: %w",
				databaseType, databaseName, environment, err)
		}
		reqs = append(reqs, req)
	}
	return reqs, rows.Err()
}

// Heartbeat extends the lease on a claimed request. Returns
// ErrMergeGateLeaseLost when the lease token is stale.
func (s *mergeGateRequestStore) Heartbeat(ctx context.Context, id int64, leaseToken string, leaseDuration time.Duration) error {
	if leaseToken == "" {
		return fmt.Errorf("merge gate lease token is required")
	}
	if leaseDuration <= 0 {
		return fmt.Errorf("merge gate lease duration must be positive")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE merge_gate_requests
		SET lease_expires_at = `+s.dialect.RelativeTime(TimestampPrecisionMicrosecond, AfterCurrentTime, ParameterIntervalAmount(), IntervalMicrosecond)+`, updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, leaseDuration.Microseconds(), id, leaseToken)
	if err != nil {
		return fmt.Errorf("heartbeat merge gate request %d: %w", id, err)
	}
	return s.mergeGateLeaseResult(ctx, result, id, leaseToken)
}

// MarkCompleted marks a claimed request terminal-successful.
//
// Idempotent: the lease token is retained (not cleared) and completed_at is
// COALESCE-preserved, so a retry after a committed-but-unacknowledged first
// attempt still matches the row and is a no-op, rather than misreporting the
// completion as a lost lease. A genuine reclaim rotates the token, so a write
// with a stale token still returns ErrMergeGateLeaseLost.
func (s *mergeGateRequestStore) MarkCompleted(ctx context.Context, id int64, leaseToken string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE merge_gate_requests
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, storage.MergeGateCompleted, id, leaseToken)
	if err != nil {
		return fmt.Errorf("mark merge gate request %d completed: %w", id, err)
	}
	return s.mergeGateLeaseResult(ctx, result, id, leaseToken)
}

// MarkPreflightHoldsRecorded stamps holds_recorded_at on a claimed preflight
// request once its storage-only hold phase has flipped every sibling change's
// stored check. Set-once (COALESCE-preserved), so retries after a partial
// render keep the original stamp. Idempotent for the same lease token, on the
// same rationale as MarkCompleted.
func (s *mergeGateRequestStore) MarkPreflightHoldsRecorded(ctx context.Context, id int64, leaseToken string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE merge_gate_requests
		SET holds_recorded_at = COALESCE(holds_recorded_at, NOW()), updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, id, leaseToken)
	if err != nil {
		return fmt.Errorf("mark merge gate request %d preflight holds recorded: %w", id, err)
	}
	return s.mergeGateLeaseResult(ctx, result, id, leaseToken)
}

func (s *mergeGateRequestStore) CompletePendingCoalesced(ctx context.Context, id int64) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE merge_gate_requests
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE id = ? AND state = ?
	`, storage.MergeGateCompleted, id, storage.MergeGatePending)
	if err != nil {
		return false, fmt.Errorf("complete coalesced merge gate request %d: %w", id, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read coalesced merge gate request %d rows affected: %w", id, err)
	}
	return rows > 0, nil
}

// MarkFailed marks a claimed request failed. A non-nil retryAfter keeps it
// retryable after that time; nil makes the failure terminal. Idempotent for
// the same lease token, on the same rationale as MarkCompleted.
func (s *mergeGateRequestStore) MarkFailed(ctx context.Context, id int64, leaseToken string, errMsg string, retryAfter *time.Time) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE merge_gate_requests
		SET state = ?, last_error = ?, retry_after = ?,
			completed_at = CASE WHEN ? THEN COALESCE(completed_at, NOW()) ELSE completed_at END,
			updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, storage.MergeGateFailed, nullString(errMsg), retryAfter, retryAfter == nil, id, leaseToken)
	if err != nil {
		return fmt.Errorf("mark merge gate request %d failed: %w", id, err)
	}
	return s.mergeGateLeaseResult(ctx, result, id, leaseToken)
}

func (s *mergeGateRequestStore) FindCompletedAppliesMissingRequest(ctx context.Context, lookback time.Duration) ([]*storage.Apply, error) {
	if lookback <= 0 {
		return nil, fmt.Errorf("merge gate sweep lookback must be positive")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyColumnsForApplyAlias+`
		FROM applies a
		LEFT JOIN merge_gate_requests r ON r.apply_id = a.id AND r.kind = ?
		WHERE a.state = ?
		  AND a.completed_at > `+s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, ParameterIntervalAmount(), IntervalSecond)+`
		  AND r.id IS NULL
		ORDER BY a.completed_at, a.id
	`, storage.MergeGateKindSettle, state.Apply.Completed, int64(lookback.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("query completed applies missing settle merge gate requests: %w", err)
	}
	defer utils.CloseAndLog(rows)

	var applies []*storage.Apply
	for rows.Next() {
		apply, err := scanApplyInto(rows)
		if err != nil {
			return nil, fmt.Errorf("scan completed apply missing settle merge gate request: %w", err)
		}
		applies = append(applies, apply)
	}
	return applies, rows.Err()
}

// FindTerminalAppliesWithPreflightMissingSettle returns applies that settled
// terminally within the lookback window with a preflight request but no
// settle. Their preflight fan-out held sibling change checks action-required, and
// only a settle fan-out re-plans those checks back to a live verdict — so the
// settle must exist for every terminal outcome, including failed and
// cancelled applies that never changed the schema.
func (s *mergeGateRequestStore) FindTerminalAppliesWithPreflightMissingSettle(ctx context.Context, lookback time.Duration) ([]*storage.Apply, error) {
	if lookback <= 0 {
		return nil, fmt.Errorf("merge gate sweep lookback must be positive")
	}
	terminalStates := terminalApplyStates()
	args := []any{storage.MergeGateKindPreflight, storage.MergeGateKindSettle}
	args = append(args, stringArgs(terminalStates)...)
	args = append(args, int64(lookback.Seconds()))
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyColumnsForApplyAlias+`
		FROM applies a
		JOIN merge_gate_requests pre ON pre.apply_id = a.id AND pre.kind = ?
		LEFT JOIN merge_gate_requests settle ON settle.apply_id = a.id AND settle.kind = ?
		WHERE a.state IN (`+placeholders(len(terminalStates))+`)
		  AND a.updated_at > `+s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, ParameterIntervalAmount(), IntervalSecond)+`
		  AND settle.id IS NULL
		ORDER BY a.updated_at, a.id
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("query terminal applies with preflight missing settle merge gate requests: %w", err)
	}
	defer utils.CloseAndLog(rows)

	var applies []*storage.Apply
	for rows.Next() {
		apply, err := scanApplyInto(rows)
		if err != nil {
			return nil, fmt.Errorf("scan terminal apply with preflight missing settle merge gate request: %w", err)
		}
		applies = append(applies, apply)
	}
	return applies, rows.Err()
}

// HasActivePreflightedApplyOnTarget reports whether any non-terminal apply on
// the target has a recorded preflight request. Such an apply has held (or is
// holding) sibling change checks and is guaranteed a settle of its own once it
// settles terminally, so a settle fan-out for an earlier apply defers to it
// rather than re-planning holds away mid-apply. Applies without a preflight
// (queued, never started) do not count: they have invalidated nothing yet.
func (s *mergeGateRequestStore) HasActivePreflightedApplyOnTarget(ctx context.Context, environment, databaseType, databaseName string) (bool, error) {
	nonTerminal, nonTerminalArgs := nonTerminalApplyStatePredicate("a.state")
	args := []any{storage.MergeGateKindPreflight, environment, databaseType, databaseName}
	args = append(args, nonTerminalArgs...)
	var exists bool
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM merge_gate_requests pre
			JOIN applies a ON a.id = pre.apply_id
			WHERE pre.kind = ?
			  AND pre.environment = ? AND pre.database_type = ? AND pre.database_name = ?
			  AND `+nonTerminal+`
		)
	`, args...).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("query active preflighted applies for %s/%s in %s: %w",
			databaseType, databaseName, environment, err)
	}
	return exists, nil
}

func (s *mergeGateRequestStore) TerminateStuckProcessing(ctx context.Context, reason string) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE merge_gate_requests
		SET state = ?, last_error = ?, completed_at = COALESCE(completed_at, NOW()),
			lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			retry_after = NULL, updated_at = NOW()
		WHERE state = ? AND lease_expires_at <= `+s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)+` AND attempts >= ?
	`, storage.MergeGateFailed, nullString(reason),
		storage.MergeGateProcessing, storage.MaxMergeGateAttempts)
	if err != nil {
		return 0, fmt.Errorf("terminate stuck processing merge gate requests: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read terminate stuck processing merge gate rows affected: %w", err)
	}
	return rows, nil
}

func (s *mergeGateRequestStore) mergeGateLeaseResult(ctx context.Context, result sql.Result, id int64, leaseToken string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read merge gate request %d lease write rows affected: %w", id, err)
	}
	if rows > 0 {
		return nil
	}
	var currentToken sql.NullString
	err = s.db.QueryRowContext(ctx, `SELECT lease_token FROM merge_gate_requests WHERE id = ?`, id).Scan(&currentToken)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrMergeGateNotFound
	}
	if err != nil {
		return fmt.Errorf("verify merge gate request %d lease token: %w", id, err)
	}
	if currentToken.Valid && currentToken.String == leaseToken {
		return nil
	}
	return storage.ErrMergeGateLeaseLost
}

func scanMergeGateRequest(row *sql.Row) (*storage.MergeGateRequest, error) {
	req, err := scanMergeGateRequestInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return req, err
}

func scanMergeGateRequestInto(row scanner) (*storage.MergeGateRequest, error) {
	var req storage.MergeGateRequest
	var leaseOwner, leaseToken, lastError sql.NullString
	var leaseExpiresAt, retryAfter, holdsRecordedAt, completedAt sql.NullTime
	err := row.Scan(
		&req.ID, &req.ApplyID, &req.Kind, &req.ApplyIdentifier, &req.Environment, &req.DatabaseType, &req.DatabaseName,
		&req.Provider, &req.Repository, &req.ChangeKey, &req.RequestedBy, &req.State, &req.Attempts,
		&leaseOwner, &leaseToken, &leaseExpiresAt, &retryAfter, &lastError,
		&holdsRecordedAt, &completedAt, &req.CreatedAt, &req.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	req.LeaseOwner = leaseOwner.String
	req.LeaseToken = leaseToken.String
	req.LastError = lastError.String
	if leaseExpiresAt.Valid {
		req.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if retryAfter.Valid {
		req.RetryAfter = &retryAfter.Time
	}
	if holdsRecordedAt.Valid {
		req.HoldsRecordedAt = &holdsRecordedAt.Time
	}
	if completedAt.Valid {
		req.CompletedAt = &completedAt.Time
	}
	return &req, nil
}
