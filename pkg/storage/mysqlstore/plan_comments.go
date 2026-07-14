// plan_comments.go implements PlanCommentStore for tracking posted plan
// comments so a newer plan comment for the same database can minimize the
// ones it supersedes on GitHub.
package mysqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// planCommentColumns lists all columns for SELECT queries.
const planCommentColumns = `id, repository, pull_request, database_name, database_type, environment_scope, head_sha, github_comment_id, github_node_id, minimized_at, created_at, updated_at`

// planCommentStore implements storage.PlanCommentStore using MySQL.
type planCommentStore struct {
	db *sql.DB
}

// Insert stores a newly posted plan comment and sets comment.ID.
func (s *planCommentStore) Insert(ctx context.Context, comment *storage.PlanComment) error {
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO plan_comments (repository, pull_request, database_name, database_type, environment_scope, head_sha, github_comment_id, github_node_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, comment.Repository, comment.PullRequest, comment.DatabaseName, comment.DatabaseType,
		comment.EnvironmentScope, comment.HeadSHA, comment.GitHubCommentID, comment.GitHubNodeID)
	if err != nil {
		return fmt.Errorf("insert plan comment for %s#%d database %s: %w", comment.Repository, comment.PullRequest, comment.DatabaseName, err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("read plan comment insert id for %s#%d database %s: %w", comment.Repository, comment.PullRequest, comment.DatabaseName, err)
	}
	comment.ID = id
	return nil
}

// ListUnminimizedForSlot returns the not-yet-minimized comments for a
// (repository, pull_request, database) slot, ordered by id ascending.
func (s *planCommentStore) ListUnminimizedForSlot(ctx context.Context, repo string, pr int, database, databaseType string) ([]*storage.PlanComment, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+planCommentColumns+`
		FROM plan_comments
		WHERE repository = ? AND pull_request = ? AND database_name = ? AND database_type = ? AND minimized_at IS NULL
		ORDER BY id
	`, repo, pr, database, databaseType)
	if err != nil {
		return nil, fmt.Errorf("query unminimized plan comments for %s#%d database %s: %w", repo, pr, database, err)
	}
	defer utils.CloseAndLog(rows)

	var comments []*storage.PlanComment
	for rows.Next() {
		comment, err := scanPlanComment(rows)
		if err != nil {
			return nil, fmt.Errorf("scan plan comment for %s#%d database %s: %w", repo, pr, database, err)
		}
		comments = append(comments, comment)
	}
	return comments, rows.Err()
}

// MarkMinimized stamps minimized_at after the GitHub minimize call succeeded.
// An already-minimized row is not an error.
func (s *planCommentStore) MarkMinimized(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE plan_comments SET minimized_at = NOW()
		WHERE id = ? AND minimized_at IS NULL
	`, id)
	if err != nil {
		return fmt.Errorf("mark plan comment %d minimized: %w", id, err)
	}
	return nil
}

// scanPlanComment scans plan comment data from any scanner (Row or Rows).
func scanPlanComment(s scanner) (*storage.PlanComment, error) {
	var comment storage.PlanComment
	err := s.Scan(
		&comment.ID, &comment.Repository, &comment.PullRequest,
		&comment.DatabaseName, &comment.DatabaseType, &comment.EnvironmentScope,
		&comment.HeadSHA, &comment.GitHubCommentID, &comment.GitHubNodeID,
		&comment.MinimizedAt, &comment.CreatedAt, &comment.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &comment, nil
}
