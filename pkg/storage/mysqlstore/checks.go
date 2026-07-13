// checks.go implements CheckStore for SchemaBot's stored check state.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// checkColumns lists all columns for SELECT queries.
const checkColumns = `id, repository, pull_request, head_sha,
	environment, database_type, database_name,
	check_run_id, apply_id, has_changes, status, conclusion,
	blocking_reason, error_message, change_summary, created_at, updated_at`

const (
	checkStatusInProgress  = "in_progress"
	checkConclusionSuccess = "success"
)

// checkStore implements storage.CheckStore using MySQL.
type checkStore struct {
	db *sql.DB
}

// Upsert creates or updates stored check state.
func (s *checkStore) Upsert(ctx context.Context, check *storage.Check) error {
	// Convert CheckRunID=0 to NULL (0 is Go's zero value, not a valid check run ID)
	var checkRunID any
	if check.CheckRunID != 0 {
		checkRunID = check.CheckRunID
	}
	var applyID any
	if check.ApplyID != 0 {
		applyID = check.ApplyID
	}

	op := fmt.Sprintf("upsert check result for %s#%d %s/%s/%s",
		check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName)
	return withLockRetry(ctx, op, func() error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO checks (
				repository, pull_request, head_sha,
				environment, database_type, database_name,
				check_run_id, apply_id, has_changes, status, conclusion, blocking_reason, error_message, change_summary
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE
				head_sha = VALUES(head_sha),
				check_run_id = VALUES(check_run_id),
				apply_id = VALUES(apply_id),
				has_changes = VALUES(has_changes),
				status = VALUES(status),
				conclusion = VALUES(conclusion),
				blocking_reason = VALUES(blocking_reason),
				error_message = VALUES(error_message),
				change_summary = COALESCE(NULLIF(VALUES(change_summary), ''), change_summary)
		`, check.Repository, check.PullRequest, check.HeadSHA,
			check.Environment, check.DatabaseType, check.DatabaseName,
			checkRunID, applyID, check.HasChanges, check.Status, check.Conclusion, check.BlockingReason, check.ErrorMessage, nullString(check.ChangeSummary))
		return err
	})
}

// UpsertPlanResult stores plan-derived check state without overwriting
// in-progress apply-owned state for the same PR/environment/database.
//
// drift declares whether this write evaluated review-time deployment drift. A
// not-evaluated write (an apply-time plan) must not silently clear a stored
// drift block: the block depends on live deployment state, not PR content, so
// only a write that re-ran the rollup and found the deployments clean may clear
// it. See storage.PlanDriftState.
func (s *checkStore) UpsertPlanResult(ctx context.Context, check *storage.Check, drift storage.PlanDriftState) error {
	var checkRunID any
	if check.CheckRunID != 0 {
		checkRunID = check.CheckRunID
	}

	op := fmt.Sprintf("upsert plan check result for %s#%d %s/%s/%s",
		check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName)
	return withLockRetry(ctx, op, func() error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO checks (
				repository, pull_request, head_sha,
				environment, database_type, database_name,
				check_run_id, apply_id, has_changes, status, conclusion, blocking_reason, error_message, change_summary
			) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)
		`, check.Repository, check.PullRequest, check.HeadSHA,
			check.Environment, check.DatabaseType, check.DatabaseName,
			checkRunID, check.HasChanges, check.Status, check.Conclusion, check.BlockingReason, check.ErrorMessage, nullString(check.ChangeSummary))
		// Fast path: no existing check state for this PR/environment/database, so
		// the insert is the complete write. Any non-duplicate error is a real
		// storage failure; duplicate key means the row exists and needs the
		// guarded update below.
		if err == nil {
			return nil
		}
		if !isDuplicateKeyError(err) {
			return err
		}

		// Preserve in-progress apply-owned state regardless of the plan's head SHA.
		// Once an apply has started, the stored row is authoritative until the apply
		// completes (CompleteForApply, MarkActionRequiredForApply) or an explicit
		// recovery path releases it. A plan result — even from a newer PR commit that
		// diffs cleanly against the mid-apply database — must not take ownership or
		// convert the row into a passing check.
		//
		// A not-evaluated write additionally preserves the full gating state of an
		// existing review-time drift block: it may refresh the head SHA and check
		// run id so the current-head aggregate stays aligned, but it must not clear
		// the block's conclusion, blocking reason, or summary. Only a write that
		// re-ran the rollup (clean or blocked) rewrites those columns.
		if drift == storage.PlanDriftNotEvaluated {
			_, err = s.db.ExecContext(ctx, `
				UPDATE checks
				SET head_sha = ?,
				    check_run_id = ?,
				    apply_id     = CASE WHEN COALESCE(blocking_reason, '') = ? THEN apply_id     ELSE NULL END,
				    has_changes  = CASE WHEN COALESCE(blocking_reason, '') = ? THEN has_changes  ELSE ?    END,
				    status       = CASE WHEN COALESCE(blocking_reason, '') = ? THEN status       ELSE ?    END,
				    conclusion   = CASE WHEN COALESCE(blocking_reason, '') = ? THEN conclusion   ELSE ?    END,
				    blocking_reason = CASE WHEN COALESCE(blocking_reason, '') = ? THEN blocking_reason ELSE ? END,
				    error_message   = CASE WHEN COALESCE(blocking_reason, '') = ? THEN error_message   ELSE ? END,
				    change_summary  = CASE WHEN COALESCE(blocking_reason, '') = ? THEN change_summary  ELSE ? END
				WHERE repository = ? AND pull_request = ?
				  AND environment = ? AND database_type = ? AND database_name = ?
				  AND NOT (status = ? AND apply_id IS NOT NULL)
			`, check.HeadSHA, checkRunID,
				storage.ReviewTimeDeploymentDriftBlockingReason,
				storage.ReviewTimeDeploymentDriftBlockingReason, check.HasChanges,
				storage.ReviewTimeDeploymentDriftBlockingReason, check.Status,
				storage.ReviewTimeDeploymentDriftBlockingReason, check.Conclusion,
				storage.ReviewTimeDeploymentDriftBlockingReason, check.BlockingReason,
				storage.ReviewTimeDeploymentDriftBlockingReason, check.ErrorMessage,
				storage.ReviewTimeDeploymentDriftBlockingReason, nullString(check.ChangeSummary),
				check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName,
				checkStatusInProgress)
			return err
		}

		_, err = s.db.ExecContext(ctx, `
			UPDATE checks
			SET head_sha = ?,
			    check_run_id = ?,
			    apply_id = NULL,
			    has_changes = ?,
			    status = ?,
			    conclusion = ?,
			    blocking_reason = ?,
			    error_message = ?,
			    change_summary = ?
			WHERE repository = ? AND pull_request = ?
			  AND environment = ? AND database_type = ? AND database_name = ?
			  AND NOT (status = ? AND apply_id IS NOT NULL)
		`, check.HeadSHA, checkRunID, check.HasChanges, check.Status, check.Conclusion, check.BlockingReason, check.ErrorMessage, nullString(check.ChangeSummary),
			check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName,
			checkStatusInProgress)
		return err
	})
}

// RecoverApplyOwnedCheckWithNoOpPlan updates same-head apply-owned stored check
// state when a successful no-op plan proves the target already matches the PR schema.
func (s *checkStore) RecoverApplyOwnedCheckWithNoOpPlan(ctx context.Context, check *storage.Check) (bool, error) {
	if !successfulNoOpPlanResult(check) {
		return false, nil
	}

	var checkRunID any
	if check.CheckRunID != 0 {
		checkRunID = check.CheckRunID
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE checks
		SET head_sha = ?,
		    check_run_id = ?,
		    apply_id = NULL,
		    has_changes = ?,
		    status = ?,
		    conclusion = ?,
		    blocking_reason = ?,
		    error_message = ?,
		    change_summary = COALESCE(NULLIF(?, ''), change_summary)
		WHERE repository = ? AND pull_request = ?
		  AND environment = ? AND database_type = ? AND database_name = ?
		  AND status = ? AND head_sha = ? AND apply_id IS NOT NULL
	`, check.HeadSHA, checkRunID, check.HasChanges, check.Status, check.Conclusion, check.BlockingReason, check.ErrorMessage, check.ChangeSummary,
		check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName,
		checkStatusInProgress, check.HeadSHA)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func successfulNoOpPlanResult(check *storage.Check) bool {
	return check != nil &&
		check.Status == "completed" &&
		check.Conclusion == checkConclusionSuccess &&
		!check.HasChanges
}

// MarkStalePlanSuccessful marks plan-only stored check state successful when its
// database is no longer in the PR. The update is guarded so a started apply that
// claimed the row after stale cleanup read it keeps blocking: a row that is
// in_progress or owns an apply ID is left untouched, because a passing check must
// never be derived from cleanup alone while an apply may have reached the live
// database. Returns true when the row is in the plan-only successful state after
// this call (whether this call wrote it or it already was), and false only when a
// started apply still owns it.
//
// This deliberately clears a review-time deployment drift block too: once a later
// commit removes the database from the PR and no apply has started, the reviewed
// plan is no longer part of the merge gate, so the plan-only drift block should
// stop blocking. A started apply still owns the row and is left untouched.
func (s *checkStore) MarkStalePlanSuccessful(ctx context.Context, check *storage.Check) (bool, error) {
	var checkRunID any
	if check.CheckRunID != 0 {
		checkRunID = check.CheckRunID
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE checks
		SET head_sha = ?,
		    check_run_id = ?,
		    apply_id = NULL,
		    has_changes = ?,
		    status = ?,
		    conclusion = ?,
		    blocking_reason = ?,
		    error_message = ?,
		    change_summary = COALESCE(NULLIF(?, ''), change_summary)
		WHERE repository = ? AND pull_request = ?
		  AND environment = ? AND database_type = ? AND database_name = ?
		  AND status != ? AND apply_id IS NULL
	`, check.HeadSHA, checkRunID, check.HasChanges, check.Status, check.Conclusion, check.BlockingReason, check.ErrorMessage, check.ChangeSummary,
		check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName,
		checkStatusInProgress)
	if err != nil {
		return false, fmt.Errorf("mark stale plan check successful for %s#%d %s/%s/%s: %w",
			check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected marking stale plan check successful for %s#%d %s/%s/%s: %w",
			check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName, err)
	}
	if rows > 0 {
		return true, nil
	}

	// Under changed-rows semantics, RowsAffected is 0 both when the guard
	// excluded the row (an apply claimed it) and when the row already held the
	// exact plan-only success values this call would have written. Re-read the
	// row to tell these apart so an already-successful row is treated as success
	// rather than left blocking.
	current, err := s.Get(ctx, check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName)
	if err != nil {
		return false, fmt.Errorf("re-read stale plan check after no-op update for %s#%d %s/%s/%s: %w",
			check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName, err)
	}
	if current == nil {
		return false, fmt.Errorf("stale plan check vanished after no-op update for %s#%d %s/%s/%s",
			check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName)
	}
	return isPlanOnlySuccessful(current), nil
}

// ClearAggregateBlock clears the blocking reason on stored aggregate check
// state. The WHERE clause pins the head SHA and blocking reason the caller
// read, making the clear an optimistic-concurrency write: a block recorded
// concurrently (a different reason, or the same reason re-recorded on a newer
// commit) does not match and is preserved. Returns true when the row was
// cleared.
func (s *checkStore) ClearAggregateBlock(ctx context.Context, check *storage.Check) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE checks
		SET blocking_reason = '',
		    error_message = ''
		WHERE repository = ? AND pull_request = ?
		  AND environment = ? AND database_type = ? AND database_name = ?
		  AND head_sha = ? AND blocking_reason = ?
	`, check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName,
		check.HeadSHA, check.BlockingReason)
	if err != nil {
		return false, fmt.Errorf("clear aggregate blocking reason %q for %s#%d %s (head %s): %w",
			check.BlockingReason, check.Repository, check.PullRequest, check.Environment, check.HeadSHA, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected clearing aggregate blocking reason %q for %s#%d %s: %w",
			check.BlockingReason, check.Repository, check.PullRequest, check.Environment, err)
	}
	return rows > 0, nil
}

// isPlanOnlySuccessful reports whether stored check state is already in the
// plan-only successful state stale cleanup converges to: a completed, successful
// check with no started apply and no pending schema change.
func isPlanOnlySuccessful(check *storage.Check) bool {
	return check.Status == "completed" &&
		check.Conclusion == checkConclusionSuccess &&
		check.ApplyID == 0 &&
		!check.HasChanges
}

// CompleteForApply updates stored check state to a terminal state only if it
// still belongs to the apply being completed.
func (s *checkStore) CompleteForApply(ctx context.Context, check *storage.Check, apply *storage.Apply) (bool, error) {
	var checkRunID any
	if check.CheckRunID != 0 {
		checkRunID = check.CheckRunID
	}
	leasePredicate := ""
	args := []any{check.HeadSHA, checkRunID, apply.ID, check.HasChanges, check.Status, check.Conclusion, check.BlockingReason, check.ErrorMessage, check.ChangeSummary,
		check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName,
		checkStatusInProgress, apply.ID, apply.ID}
	lease := apply.Lease()
	if lease.Valid() {
		leasePredicate = `
		  AND EXISTS (
		    SELECT 1
		    FROM applies lease_apply
		    WHERE lease_apply.id = ? AND lease_apply.lease_token = ?
		  )`
		args = append(args, lease.ApplyID, lease.Token)
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE checks
		SET head_sha = ?,
		    check_run_id = ?,
		    apply_id = ?,
		    has_changes = ?,
		    status = ?,
		    conclusion = ?,
		    blocking_reason = ?,
		    error_message = ?,
		    change_summary = COALESCE(NULLIF(?, ''), change_summary)
		WHERE repository = ? AND pull_request = ?
		  AND environment = ? AND database_type = ? AND database_name = ?
		  AND status = ?
		  AND apply_id = ?
		  AND NOT EXISTS (
		    SELECT 1
		    FROM applies newer
		    WHERE newer.repository = checks.repository
		      AND newer.pull_request = checks.pull_request
		      AND newer.environment = checks.environment
		      AND newer.database_type = checks.database_type
		      AND newer.database_name = checks.database_name
		      AND newer.id > ?
		  )`+leasePredicate+`
	`, args...)
	if err != nil {
		return false, err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if rows == 0 && lease.Valid() {
		if err := ensureApplyLeaseStillOwned(ctx, s.db, lease); err != nil {
			return false, err
		}
	}
	return rows > 0, nil
}

// MarkActionRequiredForApply marks stored check state action_required after a
// rollback only if no apply newer than the rollback exists for the same
// PR/environment/database. Unlike CompleteForApply, the write does not require
// the row to be owned by the rollback apply: a rollback that never claimed the
// row (its claim failed or the driver crashed before it landed) must still be
// able to block a stale successful check left over from the apply it reverted.
// Rows owned by an older apply or with no owner qualify; the newer-apply guard
// is what protects a re-apply that started after the rollback.
func (s *checkStore) MarkActionRequiredForApply(ctx context.Context, check *storage.Check, apply *storage.Apply) (bool, error) {
	var checkRunID any
	if check.CheckRunID != 0 {
		checkRunID = check.CheckRunID
	}
	leasePredicate := ""
	args := []any{check.HeadSHA, checkRunID, check.HasChanges, check.Status, check.Conclusion, check.BlockingReason, check.ErrorMessage, check.ChangeSummary,
		check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName,
		apply.ID, apply.ID}
	lease := apply.Lease()
	if lease.Valid() {
		leasePredicate = `
		  AND EXISTS (
		    SELECT 1
		    FROM applies lease_apply
		    WHERE lease_apply.id = ? AND lease_apply.lease_token = ?
		  )`
		args = append(args, lease.ApplyID, lease.Token)
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE checks
		SET head_sha = ?,
		    check_run_id = ?,
		    apply_id = NULL,
		    has_changes = ?,
		    status = ?,
		    conclusion = ?,
		    blocking_reason = ?,
		    error_message = ?,
		    change_summary = COALESCE(NULLIF(?, ''), change_summary)
		WHERE repository = ? AND pull_request = ?
		  AND environment = ? AND database_type = ? AND database_name = ?
		  AND (apply_id IS NULL OR apply_id <= ?)
		  AND NOT EXISTS (
		    SELECT 1
		    FROM applies newer
		    WHERE newer.repository = checks.repository
		      AND newer.pull_request = checks.pull_request
		      AND newer.environment = checks.environment
		      AND newer.database_type = checks.database_type
		      AND newer.database_name = checks.database_name
		      AND newer.id > ?
		  )`+leasePredicate+`
	`, args...)
	if err != nil {
		return false, err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if rows == 0 && lease.Valid() {
		if err := ensureApplyLeaseStillOwned(ctx, s.db, lease); err != nil {
			return false, err
		}
	}
	return rows > 0, nil
}

// Get returns a check by its unique key (PR + env + database), or nil if not found.
func (s *checkStore) Get(ctx context.Context, repo string, pr int, environment, dbType, database string) (*storage.Check, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+checkColumns+`
		FROM checks
		WHERE repository = ? AND pull_request = ?
		  AND environment = ? AND database_type = ? AND database_name = ?
	`, repo, pr, environment, dbType, database)

	return scanCheck(row)
}

// GetByCheckRunID returns a check by GitHub's check run ID, or nil if not found.
func (s *checkStore) GetByCheckRunID(ctx context.Context, checkRunID int64) (*storage.Check, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+checkColumns+`
		FROM checks
		WHERE check_run_id = ?
	`, checkRunID)

	return scanCheck(row)
}

// GetByPR returns all checks for a PR.
func (s *checkStore) GetByPR(ctx context.Context, repo string, pr int) ([]*storage.Check, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+checkColumns+`
		FROM checks
		WHERE repository = ? AND pull_request = ?
		ORDER BY environment, database_type, database_name
	`, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("query checks for %s#%d: %w", repo, pr, err)
	}
	defer utils.CloseAndLog(rows)

	return scanChecks(rows)
}

// GetByDatabase returns all checks for a database across all PRs.
// Used for cross-PR coordination (blocking other PRs when one is applying).
func (s *checkStore) GetByDatabase(ctx context.Context, repo, environment, dbType, database string) ([]*storage.Check, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+checkColumns+`
		FROM checks
		WHERE repository = ? AND environment = ?
		  AND database_type = ? AND database_name = ?
		ORDER BY pull_request
	`, repo, environment, dbType, database)
	if err != nil {
		return nil, fmt.Errorf("query checks for database %s: %w", database, err)
	}
	defer utils.CloseAndLog(rows)

	return scanChecks(rows)
}

// Delete removes stored check state by ID.
func (s *checkStore) Delete(ctx context.Context, id int64) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM checks WHERE id = ?`, id)
	if err != nil {
		return err
	}

	return checkRowsAffected(result, storage.ErrCheckNotFound)
}

// DeleteByPRRetainingBlockingApplyOwned removes stored check state for a
// closed PR, retaining apply-owned rows the close must not unblock. Once an
// apply has started, its stored check state is authoritative until an
// operator reconciles the target environment — closing and reopening the PR
// must not convert it into a passing aggregate.
//
// Plan-only rows (apply_id unset) are always deleted: no apply ever reached
// the live database for them, so a reopened PR starts clean.
//
// On a merged close, an apply-owned row is retained when it is either:
//
//   - in_progress: the apply may still be changing the live database, even
//     when PR-close cleanup found no non-terminal apply in the applies
//     table; or
//   - concluded as anything but success (action_required, failure, or
//     unset): the apply reached the live database and the row records that
//     operator attention is still required, e.g. the schema change was
//     removed from the PR after the apply started or a rollback completed.
//
// Apply-owned rows whose conclusion is success are deleted on a merged
// close: the apply finished cleanly and the merged PR carries the applied
// schema, so nothing remains for the row to block.
//
// On an unmerged close, every apply-owned row is retained, including rows
// whose conclusion is success. A stored success only proves the database
// matched the PR when the row was last written — a commit that removed the
// applied change may close the PR before stale cleanup converts the row to
// action_required, and the unmerged branch means the change never landed.
// Reopen-time stale cleanup converges the retained row: it converts it to
// action_required when the schema change is gone from the PR, or a fresh
// plan result replaces it when the change is still present.
func (s *checkStore) DeleteByPRRetainingBlockingApplyOwned(ctx context.Context, repo string, pr int, merged bool) error {
	if merged {
		_, err := s.db.ExecContext(ctx, `
			DELETE FROM checks
			WHERE repository = ? AND pull_request = ?
			  AND (apply_id IS NULL OR (status != ? AND conclusion = ?))
		`, repo, pr, checkStatusInProgress, checkConclusionSuccess)
		if err != nil {
			return fmt.Errorf("delete checks for merged closed PR %s#%d: %w", repo, pr, err)
		}
		return nil
	}

	_, err := s.db.ExecContext(ctx, `
		DELETE FROM checks
		WHERE repository = ? AND pull_request = ?
		  AND apply_id IS NULL
	`, repo, pr)
	if err != nil {
		return fmt.Errorf("delete plan-only checks for unmerged closed PR %s#%d: %w", repo, pr, err)
	}
	return nil
}

// scanCheck scans a single check row, returning nil if not found.
func scanCheck(row *sql.Row) (*storage.Check, error) {
	check, err := scanCheckInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return check, err
}

// scanChecks scans multiple check rows.
func scanChecks(rows *sql.Rows) ([]*storage.Check, error) {
	var checks []*storage.Check
	for rows.Next() {
		check, err := scanCheckInto(rows)
		if err != nil {
			return nil, err
		}
		checks = append(checks, check)
	}
	return checks, rows.Err()
}

// scanCheckInto scans check data from any scanner (Row or Rows).
func scanCheckInto(s scanner) (*storage.Check, error) {
	var check storage.Check
	var checkRunID, applyID sql.NullInt64
	var conclusion, blockingReason, errorMessage, changeSummary sql.NullString

	err := s.Scan(
		&check.ID, &check.Repository, &check.PullRequest, &check.HeadSHA,
		&check.Environment, &check.DatabaseType, &check.DatabaseName,
		&checkRunID, &applyID, &check.HasChanges, &check.Status, &conclusion,
		&blockingReason, &errorMessage, &changeSummary, &check.CreatedAt, &check.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	if checkRunID.Valid {
		check.CheckRunID = checkRunID.Int64
	}
	if applyID.Valid {
		check.ApplyID = applyID.Int64
	}
	if conclusion.Valid {
		check.Conclusion = conclusion.String
	}
	if blockingReason.Valid {
		check.BlockingReason = blockingReason.String
	}
	if errorMessage.Valid {
		check.ErrorMessage = errorMessage.String
	}
	if changeSummary.Valid {
		check.ChangeSummary = changeSummary.String
	}

	return &check, nil
}
