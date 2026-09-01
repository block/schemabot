// plan_comments.go implements PlanCommentStore for tracking posted plan
// comments so a newer plan comment for the same database can minimize the
// ones it supersedes on GitHub.
package sqlstore

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
	db       *rebindDB
	identity identityInserter
	dialect  Dialect
}

// Insert stores a newly posted plan comment and sets comment.ID.
func (s *planCommentStore) Insert(ctx context.Context, comment *storage.PlanComment) error {
	canonicalizePlanCommentIdentity(comment)

	id, err := s.identity.InsertID(ctx, s.db, `
		INSERT INTO plan_comments (repository, pull_request, database_name, database_type, environment_scope, head_sha, github_comment_id, github_node_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, comment.Repository, comment.PullRequest, comment.DatabaseName, comment.DatabaseType,
		comment.EnvironmentScope, comment.HeadSHA, comment.GitHubCommentID, comment.GitHubNodeID)
	if err != nil {
		return fmt.Errorf("insert plan comment for %s#%d database %s: %w", comment.Repository, comment.PullRequest, comment.DatabaseName, err)
	}
	comment.ID = id
	return nil
}

// ListUnminimizedForSlot returns the not-yet-minimized comments for a
// (repository, pull_request, database) slot, ordered by id ascending.
func (s *planCommentStore) ListUnminimizedForSlot(ctx context.Context, repo string, pr int, database, databaseType string) ([]*storage.PlanComment, error) {
	repo = storage.CanonicalKey(repo)
	database = storage.CanonicalKey(database)
	databaseType = storage.CanonicalKey(databaseType)
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+planCommentColumns+`
		FROM plan_comments
		WHERE repository = ? AND pull_request = ? AND database_name = ? AND database_type = ? AND minimized_at IS NULL
		ORDER BY id
	`, repo, pr, database, databaseType)
	if err != nil {
		return nil, fmt.Errorf("query unminimized plan comments for %s#%d database %s: %w", repo, pr, database, err)
	}
	return collectPlanComments(rows, fmt.Sprintf("%s#%d database %s", repo, pr, database))
}

// ListUnminimizedForRepoPR returns the not-yet-minimized comments for a whole
// pull request, across every database, ordered by id ascending. The slot query
// above answers "what did this database's newest comment supersede"; this one
// answers "what is still expanded on this PR", which a caller that resolved no
// database has no other way to ask. The (repository, pull_request) prefix of
// the slot index serves it.
func (s *planCommentStore) ListUnminimizedForRepoPR(ctx context.Context, repo string, pr int) ([]*storage.PlanComment, error) {
	repo = storage.CanonicalKey(repo)
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+planCommentColumns+`
		FROM plan_comments
		WHERE repository = ? AND pull_request = ? AND minimized_at IS NULL
		ORDER BY id
	`, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("query unminimized plan comments for %s#%d: %w", repo, pr, err)
	}
	return collectPlanComments(rows, fmt.Sprintf("%s#%d", repo, pr))
}

// collectPlanComments drains a plan comment result set. scope names what was
// queried so a scan failure says which listing broke.
func collectPlanComments(rows *sql.Rows, scope string) ([]*storage.PlanComment, error) {
	defer utils.CloseAndLog(rows)

	var comments []*storage.PlanComment
	for rows.Next() {
		comment, err := scanPlanComment(rows)
		if err != nil {
			return nil, fmt.Errorf("scan plan comment for %s: %w", scope, err)
		}
		comments = append(comments, comment)
	}
	return comments, rows.Err()
}

// MarkMinimized stamps minimized_at after the GitHub minimize call succeeded.
// An already-minimized row is not an error.
func (s *planCommentStore) MarkMinimized(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE plan_comments
		SET minimized_at = `+s.dialect.CurrentTimestamp(TimestampPrecisionDefault)+`,
		    updated_at = `+s.dialect.CurrentTimestamp(TimestampPrecisionDefault)+`
		WHERE id = ? AND minimized_at IS NULL
	`, id)
	if err != nil {
		return fmt.Errorf("mark plan comment %d minimized: %w", id, err)
	}
	return nil
}

// canonicalizePlanCommentIdentity folds the identity keys that appear in this
// store's SQL predicates. EnvironmentScope is deliberately not folded: no
// query filters on it, and its consumers compare it in Go against values built
// fresh from configured environment names, so folding only the stored side
// would break those comparisons.
func canonicalizePlanCommentIdentity(comment *storage.PlanComment) {
	comment.Repository = storage.CanonicalKey(comment.Repository)
	comment.DatabaseName = storage.CanonicalKey(comment.DatabaseName)
	comment.DatabaseType = storage.CanonicalKey(comment.DatabaseType)
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
