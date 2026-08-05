// check_refresh_requests.go implements CheckRefreshRequestStore for the durable
// check refresh fan-out.
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

const checkRefreshColumns = `id, apply_id, apply_identifier, environment, database_type, database_name,
	repository, pull_request, requested_by, state, attempts,
	lease_owner, lease_token, lease_expires_at, retry_after, last_error,
	completed_at, created_at, updated_at`

type checkRefreshRequestStore struct {
	db       *sql.DB
	dialect  Dialect
	identity identityInserter
}

func (s *checkRefreshRequestStore) Record(ctx context.Context, req *storage.CheckRefreshRequest) (bool, error) {
	if req.ApplyID == 0 {
		return false, fmt.Errorf("check refresh request requires the originating apply row id")
	}
	if req.ApplyIdentifier == "" {
		return false, fmt.Errorf("check refresh request for apply row %d requires the apply identifier", req.ApplyID)
	}
	if req.Environment == "" || req.DatabaseType == "" || req.DatabaseName == "" {
		return false, fmt.Errorf("check refresh request for apply %s requires environment, database type, and database name", req.ApplyIdentifier)
	}

	id, err := s.identity.InsertID(ctx, s.db, `
		INSERT INTO check_refresh_requests (
			apply_id, apply_identifier, environment, database_type, database_name,
			repository, pull_request, requested_by, state
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, req.ApplyID, req.ApplyIdentifier, req.Environment, req.DatabaseType, req.DatabaseName,
		req.Repository, req.PullRequest, req.RequestedBy, storage.CheckRefreshPending)
	if err != nil {
		// The unique key on apply_id is the idempotency guard: the drive tail
		// and the backfill sweep may both record the same apply, and a request
		// already in any state means the fan-out is recorded or done.
		if isDuplicateKeyError(err) {
			return false, nil
		}
		return false, fmt.Errorf("record check refresh request for apply %s (%s/%s in %s): %w",
			req.ApplyIdentifier, req.DatabaseType, req.DatabaseName, req.Environment, err)
	}
	req.ID = id
	req.State = storage.CheckRefreshPending
	return true, nil
}

func (s *checkRefreshRequestStore) GetByApplyID(ctx context.Context, applyID int64) (*storage.CheckRefreshRequest, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+checkRefreshColumns+`
		FROM check_refresh_requests
		WHERE apply_id = ?
	`, applyID)
	req, err := scanCheckRefreshRequest(row)
	if err != nil {
		return nil, fmt.Errorf("get check refresh request for apply row %d: %w", applyID, err)
	}
	return req, nil
}

// checkRefreshClaimablePredicate matches exactly the rows a processor would
// claim: a pending row, a retryable row whose retry window has elapsed and is
// under the attempt cap, or a processing row whose lease has expired and is
// under the attempt cap. Bind its placeholders with checkRefreshClaimableArgs.
func (s *checkRefreshRequestStore) checkRefreshClaimablePredicate() string {
	return `(
			state = ?
			OR (state = ? AND retry_after IS NOT NULL AND retry_after <= ` + s.dialect.CurrentTimestamp(TimestampPrecisionDefault) + ` AND attempts < ?)
			OR (state = ? AND lease_expires_at <= ` + s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond) + ` AND attempts < ?)
		)`
}

// checkRefreshClaimableArgs returns the placeholder bindings for
// checkRefreshClaimablePredicate, in order.
func checkRefreshClaimableArgs() []any {
	return []any{
		storage.CheckRefreshPending,
		storage.CheckRefreshFailed, storage.MaxCheckRefreshAttempts,
		storage.CheckRefreshProcessing, storage.MaxCheckRefreshAttempts,
	}
}

func (s *checkRefreshRequestStore) ClaimNext(ctx context.Context, owner string, leaseDuration time.Duration) (*storage.CheckRefreshRequest, error) {
	if owner == "" {
		return nil, fmt.Errorf("check refresh claim owner is required")
	}
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("check refresh lease duration must be positive")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin claim check refresh request transaction: %w", err)
	}
	defer rollbackTx(ctx, tx, "claim check refresh request")

	var id int64
	err = tx.QueryRowContext(ctx, `
		SELECT id
		FROM check_refresh_requests
		WHERE `+s.checkRefreshClaimablePredicate()+`
		ORDER BY created_at, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, checkRefreshClaimableArgs()...).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query next claimable check refresh request: %w", err)
	}

	row := tx.QueryRowContext(ctx, `
		SELECT `+checkRefreshColumns+`
		FROM check_refresh_requests
		WHERE id = ?
	`, id)
	req, err := scanCheckRefreshRequestInto(row)
	if err != nil {
		return nil, fmt.Errorf("load claimed check refresh request %d: %w", id, err)
	}

	leaseToken := uuid.NewString()
	now := time.Now()
	leaseExpiresAt := now.Add(leaseDuration)
	_, err = tx.ExecContext(ctx, `
		UPDATE check_refresh_requests
		SET state = ?, attempts = attempts + 1, lease_owner = ?, lease_token = ?,
			lease_expires_at = `+s.dialect.RelativeTime(TimestampPrecisionMicrosecond, AfterCurrentTime, ParameterIntervalAmount(), IntervalMicrosecond)+`,
			retry_after = NULL, updated_at = NOW()
		WHERE id = ?
	`, storage.CheckRefreshProcessing, owner, leaseToken, leaseDuration.Microseconds(), req.ID)
	if err != nil {
		return nil, fmt.Errorf("claim check refresh request %d: %w", req.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim check refresh request %d: %w", req.ID, err)
	}

	// Reflect the committed claim on the scanned row instead of reloading it.
	// The row was held under FOR UPDATE for the whole transaction, so no other
	// writer could have changed it. lease_expires_at is the application-clock
	// estimate of the database's NOW(6)+leaseDuration.
	req.State = storage.CheckRefreshProcessing
	req.Attempts++
	req.LeaseOwner = owner
	req.LeaseToken = leaseToken
	req.LeaseExpiresAt = &leaseExpiresAt
	req.RetryAfter = nil
	req.UpdatedAt = now

	return req, nil
}

func (s *checkRefreshRequestStore) PendingForTarget(ctx context.Context, environment, databaseType, databaseName string, excludeID int64) ([]*storage.CheckRefreshRequest, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+checkRefreshColumns+`
		FROM check_refresh_requests
		WHERE environment = ? AND database_type = ? AND database_name = ?
		  AND state = ? AND id != ?
		ORDER BY created_at, id
	`, environment, databaseType, databaseName, storage.CheckRefreshPending, excludeID)
	if err != nil {
		return nil, fmt.Errorf("query pending check refresh requests for %s/%s in %s: %w",
			databaseType, databaseName, environment, err)
	}
	defer utils.CloseAndLog(rows)

	var reqs []*storage.CheckRefreshRequest
	for rows.Next() {
		req, err := scanCheckRefreshRequestInto(rows)
		if err != nil {
			return nil, fmt.Errorf("scan pending check refresh request for %s/%s in %s: %w",
				databaseType, databaseName, environment, err)
		}
		reqs = append(reqs, req)
	}
	return reqs, rows.Err()
}

// Heartbeat extends the lease on a claimed request. Returns
// ErrCheckRefreshLeaseLost when the lease token is stale.
func (s *checkRefreshRequestStore) Heartbeat(ctx context.Context, id int64, leaseToken string, leaseDuration time.Duration) error {
	if leaseToken == "" {
		return fmt.Errorf("check refresh lease token is required")
	}
	if leaseDuration <= 0 {
		return fmt.Errorf("check refresh lease duration must be positive")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE check_refresh_requests
		SET lease_expires_at = `+s.dialect.RelativeTime(TimestampPrecisionMicrosecond, AfterCurrentTime, ParameterIntervalAmount(), IntervalMicrosecond)+`, updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, leaseDuration.Microseconds(), id, leaseToken)
	if err != nil {
		return fmt.Errorf("heartbeat check refresh request %d: %w", id, err)
	}
	return s.checkRefreshLeaseResult(ctx, result, id, leaseToken)
}

// MarkCompleted marks a claimed request terminal-successful.
//
// Idempotent: the lease token is retained (not cleared) and completed_at is
// COALESCE-preserved, so a retry after a committed-but-unacknowledged first
// attempt still matches the row and is a no-op, rather than misreporting the
// completion as a lost lease. A genuine reclaim rotates the token, so a write
// with a stale token still returns ErrCheckRefreshLeaseLost.
func (s *checkRefreshRequestStore) MarkCompleted(ctx context.Context, id int64, leaseToken string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE check_refresh_requests
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, storage.CheckRefreshCompleted, id, leaseToken)
	if err != nil {
		return fmt.Errorf("mark check refresh request %d completed: %w", id, err)
	}
	return s.checkRefreshLeaseResult(ctx, result, id, leaseToken)
}

func (s *checkRefreshRequestStore) CompletePendingCoalesced(ctx context.Context, id int64) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE check_refresh_requests
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE id = ? AND state = ?
	`, storage.CheckRefreshCompleted, id, storage.CheckRefreshPending)
	if err != nil {
		return false, fmt.Errorf("complete coalesced check refresh request %d: %w", id, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read coalesced check refresh request %d rows affected: %w", id, err)
	}
	return rows > 0, nil
}

// MarkFailed marks a claimed request failed. A non-nil retryAfter keeps it
// retryable after that time; nil makes the failure terminal. Idempotent for
// the same lease token, on the same rationale as MarkCompleted.
func (s *checkRefreshRequestStore) MarkFailed(ctx context.Context, id int64, leaseToken string, errMsg string, retryAfter *time.Time) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE check_refresh_requests
		SET state = ?, last_error = ?, retry_after = ?,
			completed_at = CASE WHEN ? THEN COALESCE(completed_at, NOW()) ELSE completed_at END,
			updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, storage.CheckRefreshFailed, nullString(errMsg), retryAfter, retryAfter == nil, id, leaseToken)
	if err != nil {
		return fmt.Errorf("mark check refresh request %d failed: %w", id, err)
	}
	return s.checkRefreshLeaseResult(ctx, result, id, leaseToken)
}

func (s *checkRefreshRequestStore) FindCompletedAppliesMissingRequest(ctx context.Context, lookback time.Duration) ([]*storage.Apply, error) {
	if lookback <= 0 {
		return nil, fmt.Errorf("check refresh sweep lookback must be positive")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyColumnsForApplyAlias+`
		FROM applies a
		LEFT JOIN check_refresh_requests r ON r.apply_id = a.id
		WHERE a.state = ?
		  AND a.completed_at > `+s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, ParameterIntervalAmount(), IntervalSecond)+`
		  AND r.id IS NULL
		ORDER BY a.completed_at, a.id
	`, state.Apply.Completed, int64(lookback.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("query completed applies missing check refresh requests: %w", err)
	}
	defer utils.CloseAndLog(rows)

	var applies []*storage.Apply
	for rows.Next() {
		apply, err := scanApplyInto(rows)
		if err != nil {
			return nil, fmt.Errorf("scan completed apply missing check refresh request: %w", err)
		}
		applies = append(applies, apply)
	}
	return applies, rows.Err()
}

func (s *checkRefreshRequestStore) TerminateStuckProcessing(ctx context.Context, reason string) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE check_refresh_requests
		SET state = ?, last_error = ?, completed_at = COALESCE(completed_at, NOW()),
			lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			retry_after = NULL, updated_at = NOW()
		WHERE state = ? AND lease_expires_at <= `+s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)+` AND attempts >= ?
	`, storage.CheckRefreshFailed, nullString(reason),
		storage.CheckRefreshProcessing, storage.MaxCheckRefreshAttempts)
	if err != nil {
		return 0, fmt.Errorf("terminate stuck processing check refresh requests: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read terminate stuck processing check refresh rows affected: %w", err)
	}
	return rows, nil
}

func (s *checkRefreshRequestStore) checkRefreshLeaseResult(ctx context.Context, result sql.Result, id int64, leaseToken string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read check refresh request %d lease write rows affected: %w", id, err)
	}
	if rows > 0 {
		return nil
	}
	var currentToken sql.NullString
	err = s.db.QueryRowContext(ctx, `SELECT lease_token FROM check_refresh_requests WHERE id = ?`, id).Scan(&currentToken)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrCheckRefreshNotFound
	}
	if err != nil {
		return fmt.Errorf("verify check refresh request %d lease token: %w", id, err)
	}
	if currentToken.Valid && currentToken.String == leaseToken {
		return nil
	}
	return storage.ErrCheckRefreshLeaseLost
}

func scanCheckRefreshRequest(row *sql.Row) (*storage.CheckRefreshRequest, error) {
	req, err := scanCheckRefreshRequestInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return req, err
}

func scanCheckRefreshRequestInto(row scanner) (*storage.CheckRefreshRequest, error) {
	var req storage.CheckRefreshRequest
	var leaseOwner, leaseToken, lastError sql.NullString
	var leaseExpiresAt, retryAfter, completedAt sql.NullTime
	err := row.Scan(
		&req.ID, &req.ApplyID, &req.ApplyIdentifier, &req.Environment, &req.DatabaseType, &req.DatabaseName,
		&req.Repository, &req.PullRequest, &req.RequestedBy, &req.State, &req.Attempts,
		&leaseOwner, &leaseToken, &leaseExpiresAt, &retryAfter, &lastError,
		&completedAt, &req.CreatedAt, &req.UpdatedAt,
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
	if completedAt.Valid {
		req.CompletedAt = &completedAt.Time
	}
	return &req, nil
}
