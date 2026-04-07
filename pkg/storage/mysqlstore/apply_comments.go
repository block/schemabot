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
const applyCommentColumns = `id, apply_id, comment_state, github_comment_id, created_at, updated_at`

// applyCommentStore implements storage.ApplyCommentStore using MySQL.
type applyCommentStore struct {
	db *sql.DB
}

// Upsert creates or updates a comment record.
// On conflict (same apply_id + comment_state), updates the github_comment_id.
func (s *applyCommentStore) Upsert(ctx context.Context, comment *storage.ApplyComment) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO apply_comments (apply_id, comment_state, github_comment_id)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE
			github_comment_id = VALUES(github_comment_id)
	`, comment.ApplyID, comment.CommentState, comment.GitHubCommentID)
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

// DeleteByApply removes all comment records for an apply.
func (s *applyCommentStore) DeleteByApply(ctx context.Context, applyID int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM apply_comments WHERE apply_id = ?`, applyID)
	return err
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
		&comment.GitHubCommentID, &comment.CreatedAt, &comment.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &comment, nil
}
