// apply_comments.go implements ApplyCommentStore for tracking GitHub PR comment IDs.
// One comment per (apply_id, comment_state) combination.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// applyCommentColumns lists all columns for SELECT queries.
const applyCommentColumns = `id, apply_id, comment_state, github_comment_id, posted_volume, pending_freeze_github_comment_id, edit_count, last_edited_at, superseded_at, created_at, updated_at`

// applyCommentStore implements storage.ApplyCommentStore using MySQL.
type applyCommentStore struct {
	db *sql.DB
}

// Upsert creates or updates a comment record.
// On conflict (same apply_id + comment_state), updates the github_comment_id,
// posted_volume, and pending_freeze_github_comment_id so the row always
// describes the currently tracked comment and the freeze it may still owe.
func (s *applyCommentStore) Upsert(ctx context.Context, comment *storage.ApplyComment) error {
	lease, hasLease, err := applyLeaseFromContext(ctx, comment.ApplyID)
	if err != nil {
		return err
	}
	if !hasLease {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO apply_comments (apply_id, comment_state, github_comment_id, posted_volume, pending_freeze_github_comment_id)
			VALUES (?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE
				github_comment_id = VALUES(github_comment_id),
				posted_volume = VALUES(posted_volume),
				pending_freeze_github_comment_id = VALUES(pending_freeze_github_comment_id),
				superseded_at = NULL
		`, comment.ApplyID, comment.CommentState, comment.GitHubCommentID, comment.PostedVolume, comment.PendingFreezeCommentID)
		return err
	}

	result, err := s.db.ExecContext(ctx, `
		INSERT INTO apply_comments (apply_id, comment_state, github_comment_id, posted_volume, pending_freeze_github_comment_id)
		SELECT ?, ?, ?, ?, ? FROM applies a
		WHERE a.id = ? AND a.lease_token = ?
		ON DUPLICATE KEY UPDATE
			github_comment_id = VALUES(github_comment_id),
			posted_volume = VALUES(posted_volume),
			pending_freeze_github_comment_id = VALUES(pending_freeze_github_comment_id),
			superseded_at = NULL
	`, comment.ApplyID, comment.CommentState, comment.GitHubCommentID, comment.PostedVolume, comment.PendingFreezeCommentID, comment.ApplyID, lease.Token)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read apply comment upsert rows affected for apply %d state %s: %w", comment.ApplyID, comment.CommentState, err)
	}
	if rows == 0 {
		if err := ensureApplyLeaseStillOwned(ctx, s.db, lease); err != nil {
			return err
		}
	}
	return err
}

// Get returns a comment by (apply_id, comment_state), or nil if not found.
func (s *applyCommentStore) Get(ctx context.Context, applyID int64, commentState string) (*storage.ApplyComment, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+applyCommentColumns+`
		FROM apply_comments
		WHERE apply_id = ? AND comment_state = ?
	`, applyID, commentState)

	return scanApplyComment(row)
}

// ListByApply returns all comments for an apply.
func (s *applyCommentStore) ListByApply(ctx context.Context, applyID int64) ([]*storage.ApplyComment, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyCommentColumns+`
		FROM apply_comments
		WHERE apply_id = ?
		ORDER BY id
	`, applyID)
	if err != nil {
		return nil, fmt.Errorf("query apply comments for apply %d: %w", applyID, err)
	}
	defer utils.CloseAndLog(rows)

	return scanApplyComments(rows)
}

// IncrementEditCount atomically increments the edit count and updates last_edited_at.
func (s *applyCommentStore) IncrementEditCount(ctx context.Context, applyID int64, commentState string) error {
	lease, hasLease, err := applyLeaseFromContext(ctx, applyID)
	if err != nil {
		return err
	}
	leaseJoin := ""
	leasePredicate := ""
	args := []any{applyID, commentState}
	if hasLease {
		leaseJoin = " JOIN applies a ON a.id = c.apply_id"
		leasePredicate = " AND a.lease_token = ?"
		args = append(args, lease.Token)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_comments c
		`+leaseJoin+`
		SET c.edit_count = c.edit_count + 1, c.last_edited_at = NOW()
		WHERE c.apply_id = ? AND c.comment_state = ?`+leasePredicate+`
	`, args...)
	if err != nil {
		return err
	}
	if hasLease {
		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("read apply comment edit rows affected for apply %d state %s: %w", applyID, commentState, err)
		}
		if rows == 0 {
			if err := ensureApplyLeaseStillOwned(ctx, s.db, lease); err != nil {
				return err
			}
		}
	}
	return err
}

// DeleteByApply removes all comment records for an apply.
func (s *applyCommentStore) DeleteByApply(ctx context.Context, applyID int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM apply_comments WHERE apply_id = ?`, applyID)
	return err
}

// Supersede retires the tracked comment for a single (apply_id, comment_state) by
// stamping superseded_at, without deleting the row or the GitHub comment. The
// comment stays on the PR as a record; SchemaBot just no longer treats it as the
// active comment for its state (so it is not edited again and does not re-trigger
// resume detection). A missing or already-superseded row is not an error. A later
// Upsert for the same state clears superseded_at, making the row active again.
func (s *applyCommentStore) Supersede(ctx context.Context, applyID int64, commentState string) error {
	lease, hasLease, err := applyLeaseFromContext(ctx, applyID)
	if err != nil {
		return err
	}
	if !hasLease {
		_, err := s.db.ExecContext(ctx, `
			UPDATE apply_comments SET superseded_at = NOW()
			WHERE apply_id = ? AND comment_state = ? AND superseded_at IS NULL
		`, applyID, commentState)
		return err
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_comments c
		JOIN applies a ON a.id = c.apply_id
		SET c.superseded_at = NOW()
		WHERE c.apply_id = ? AND c.comment_state = ? AND c.superseded_at IS NULL AND a.lease_token = ?
	`, applyID, commentState, lease.Token)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read apply comment supersede rows affected for apply %d state %s: %w", applyID, commentState, err)
	}
	if rows == 0 {
		// Zero rows can mean the row was missing, already superseded, or the lease
		// was lost. The first two are benign; surface only a lost lease.
		if err := ensureApplyLeaseStillOwned(ctx, s.db, lease); err != nil {
			return err
		}
	}
	return nil
}

// ClearPendingFreeze removes the pending-freeze marker for a single
// (apply_id, comment_state) once the superseded comment's frozen rendering has
// landed on GitHub. A missing row or an already-clear marker is not an error;
// with a lease in context, a zero-row update surfaces a lost lease.
func (s *applyCommentStore) ClearPendingFreeze(ctx context.Context, applyID int64, commentState string) error {
	lease, hasLease, err := applyLeaseFromContext(ctx, applyID)
	if err != nil {
		return err
	}
	if !hasLease {
		_, err := s.db.ExecContext(ctx, `
			UPDATE apply_comments SET pending_freeze_github_comment_id = NULL
			WHERE apply_id = ? AND comment_state = ? AND pending_freeze_github_comment_id IS NOT NULL
		`, applyID, commentState)
		return err
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_comments c
		JOIN applies a ON a.id = c.apply_id
		SET c.pending_freeze_github_comment_id = NULL
		WHERE c.apply_id = ? AND c.comment_state = ? AND c.pending_freeze_github_comment_id IS NOT NULL AND a.lease_token = ?
	`, applyID, commentState, lease.Token)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read apply comment pending-freeze clear rows affected for apply %d state %s: %w", applyID, commentState, err)
	}
	if rows == 0 {
		// Zero rows can mean the row was missing, the marker was already clear,
		// or the lease was lost. The first two are benign; surface only a lost
		// lease.
		if err := ensureApplyLeaseStillOwned(ctx, s.db, lease); err != nil {
			return err
		}
	}
	return nil
}

// scanApplyComment scans a single apply comment row, returning nil if not found.
func scanApplyComment(row *sql.Row) (*storage.ApplyComment, error) {
	comment, err := scanApplyCommentInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return comment, err
}

// scanApplyComments scans multiple apply comment rows.
func scanApplyComments(rows *sql.Rows) ([]*storage.ApplyComment, error) {
	var comments []*storage.ApplyComment
	for rows.Next() {
		comment, err := scanApplyCommentInto(rows)
		if err != nil {
			return nil, err
		}
		comments = append(comments, comment)
	}
	return comments, rows.Err()
}

// scanApplyCommentInto scans apply comment data from any scanner (Row or Rows).
func scanApplyCommentInto(s scanner) (*storage.ApplyComment, error) {
	var comment storage.ApplyComment
	err := s.Scan(
		&comment.ID, &comment.ApplyID, &comment.CommentState,
		&comment.GitHubCommentID, &comment.PostedVolume, &comment.PendingFreezeCommentID, &comment.EditCount,
		&comment.LastEditedAt, &comment.SupersededAt, &comment.CreatedAt, &comment.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &comment, nil
}
