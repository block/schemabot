// apply_comments.go implements ApplyCommentStore for tracking GitHub PR comment IDs.
// One comment per (apply_id, comment_state) combination.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// applyCommentColumns lists all columns for SELECT queries.
const applyCommentColumns = `id, apply_id, comment_state, github_comment_id, posted_phase, posted_cutover_operation_id, pending_freeze_github_comment_id, edit_count, last_edited_at, superseded_at, summarized_state, created_at, updated_at`

// applyCommentStore implements storage.ApplyCommentStore using MySQL.
type applyCommentStore struct {
	db      *rebindDB
	dialect Dialect
}

// applyCommentUpsertStatement renders Upsert's statement. Both renderings bind
// the seven inserted columns first; the leased rendering then binds the apply ID
// and the lease token.
//
// The unleased rendering is a plain VALUES upsert. The leased rendering selects
// its row from the leased applies row instead, so that row is the INSERT …
// SELECT's only source row and the token check gates the insert and the
// conflict update alike: a displaced driver's statement selects nothing and
// writes neither. The check goes through LeaseSourceFence so it serializes
// against a concurrent steal instead of passing against a token the
// statement's snapshot still holds, at whatever isolation level the storage
// session runs.
func applyCommentUpsertStatement(d Dialect, leased bool) string {
	const insert = "INSERT INTO apply_comments (apply_id, comment_state, github_comment_id, posted_phase, posted_cutover_operation_id, pending_freeze_github_comment_id, summarized_state) "
	upsert := d.UpsertClause(
		[]string{"apply_id", "comment_state"},
		[]UpsertAssignment{
			{Column: "github_comment_id"},
			{Column: "posted_phase"},
			{Column: "posted_cutover_operation_id"},
			{Column: "pending_freeze_github_comment_id"},
			{Column: "summarized_state"},
			{Column: "superseded_at", Expr: "NULL"},
			{Column: "updated_at", Expr: "NOW()"},
		},
	)
	if !leased {
		return insert + "VALUES (?, ?, ?, ?, ?, ?, ?) " + upsert
	}
	return insert + "SELECT ?, ?, ?, ?, ?, ?, ? FROM applies a WHERE a.id = ? AND " + d.LeaseSourceFence("applies", "a", "id", "lease_token") + " " + upsert
}

// Upsert creates or updates a comment record.
// On conflict (same apply_id + comment_state), updates the github_comment_id,
// posted_phase, posted_cutover_operation_id, pending_freeze_github_comment_id,
// and summarized_state so the row always describes the currently tracked
// comment and the freeze it may still owe. The conflict path also clears superseded_at (the row is live
// again) and stamps updated_at: that column is the summary-claim freshness
// signal read by ReclaimStaleSummaryClaim, not incidental audit metadata, so
// every mutation must renew it explicitly on both dialects.
func (s *applyCommentStore) Upsert(ctx context.Context, comment *storage.ApplyComment) error {
	lease, hasLease, err := applyLeaseFromContext(ctx, comment.ApplyID)
	if err != nil {
		return err
	}
	if !hasLease {
		_, err := s.db.ExecContext(ctx, applyCommentUpsertStatement(s.dialect, false),
			comment.ApplyID, comment.CommentState, comment.GitHubCommentID, comment.PostedPhase, comment.PostedCutoverOperationID, comment.PendingFreezeCommentID, comment.SummarizedState)
		return err
	}

	result, err := s.db.ExecContext(ctx, applyCommentUpsertStatement(s.dialect, true),
		comment.ApplyID, comment.CommentState, comment.GitHubCommentID, comment.PostedPhase, comment.PostedCutoverOperationID, comment.PendingFreezeCommentID, comment.SummarizedState, comment.ApplyID, lease.Token)
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
	if !hasLease {
		_, err := s.db.ExecContext(ctx, `
			UPDATE apply_comments
			SET edit_count = edit_count + 1, last_edited_at = NOW(), updated_at = NOW()
			WHERE apply_id = ? AND comment_state = ?
		`, applyID, commentState)
		if err != nil {
			return fmt.Errorf("increment edit count for apply %d state %s: %w", applyID, commentState, err)
		}
		return nil
	}
	query := s.dialect.JoinedUpdate(
		"apply_comments", "c", "applies", "a", "a.id = c.apply_id",
		[]JoinedUpdateAssignment{
			{Column: "edit_count", Expr: "c.edit_count + 1"},
			{Column: "last_edited_at", Expr: "NOW()"},
			{Column: "updated_at", Expr: "NOW()"},
		},
		"c.apply_id = ? AND c.comment_state = ? AND "+s.dialect.LeaseTokenFence("applies", "a", "id", "lease_token"),
	)
	result, err := s.db.ExecContext(ctx, query, applyID, commentState, lease.Token)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read apply comment edit rows affected for apply %d state %s: %w", applyID, commentState, err)
	}
	if rows == 0 {
		if err := ensureApplyLeaseStillOwned(ctx, s.db, lease); err != nil {
			return err
		}
	}
	return err
}

// DeleteByApply removes all comment records for an apply. It is deliberately
// lease-agnostic: it serves per-apply teardown where the apply row itself is
// being removed, so no live drive holds a lease that could fence it.
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
//
// Superseding the summary marker also re-arms terminal-summary publishing:
// ClaimSummaryComment treats a superseded posted marker as reclaimable, so any
// caller that supersedes a summary row is signing up the next terminal publish
// to post a fresh summary comment. Callers that merely observe a summary must
// not supersede it, or the apply gains a duplicate summary at its next
// terminal transition.
func (s *applyCommentStore) Supersede(ctx context.Context, applyID int64, commentState string) error {
	lease, hasLease, err := applyLeaseFromContext(ctx, applyID)
	if err != nil {
		return err
	}
	if !hasLease {
		_, err := s.db.ExecContext(ctx, `
			UPDATE apply_comments SET superseded_at = NOW(), updated_at = NOW()
			WHERE apply_id = ? AND comment_state = ? AND superseded_at IS NULL
		`, applyID, commentState)
		return err
	}
	query := s.dialect.JoinedUpdate(
		"apply_comments", "c", "applies", "a", "a.id = c.apply_id",
		[]JoinedUpdateAssignment{
			{Column: "superseded_at", Expr: "NOW()"},
			{Column: "updated_at", Expr: "NOW()"},
		},
		"c.apply_id = ? AND c.comment_state = ? AND c.superseded_at IS NULL AND "+s.dialect.LeaseTokenFence("applies", "a", "id", "lease_token"),
	)
	result, err := s.db.ExecContext(ctx, query, applyID, commentState, lease.Token)
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
			UPDATE apply_comments SET pending_freeze_github_comment_id = NULL, updated_at = NOW()
			WHERE apply_id = ? AND comment_state = ? AND pending_freeze_github_comment_id IS NOT NULL
		`, applyID, commentState)
		return err
	}
	query := s.dialect.JoinedUpdate(
		"apply_comments", "c", "applies", "a", "a.id = c.apply_id",
		[]JoinedUpdateAssignment{
			{Column: "pending_freeze_github_comment_id", Expr: "NULL"},
			{Column: "updated_at", Expr: "NOW()"},
		},
		"c.apply_id = ? AND c.comment_state = ? AND c.pending_freeze_github_comment_id IS NOT NULL AND "+s.dialect.LeaseTokenFence("applies", "a", "id", "lease_token"),
	)
	result, err := s.db.ExecContext(ctx, query, applyID, commentState, lease.Token)
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

// ClaimSummaryComment atomically claims the terminal summary publish for an
// apply in terminalState by inserting the summary marker as a claim sentinel
// (github_comment_id = 0). The unique key on (apply_id, comment_state) makes
// this a race-safe first-writer-wins: the insert that lands the row wins the
// claim.
//
// An existing row that records a posted summary for an earlier terminal state
// does not block the claim, since the current terminal state's summary is
// still owed. Two shapes qualify: a row a resume rotation superseded (a stop's
// summary consumed when the apply resumed), and an active row whose
// summarized_state names a different terminal state that the apply can leave
// (a stop's summary the apply reached its next terminal state before any
// rotation consumed, or one a rotation marked rotated past). A row recording a
// settled state always blocks: a settled outcome is final, so a claimant
// asking for any other state is acting on a stale read of the apply. The
// conditional update converts either back into a claim sentinel, and exactly
// one concurrent claimant sees the affected row. Only a row that records a
// posted comment is reclaimable this way — a claim sentinel belongs to a
// publish that is still in flight or crashed mid-publish, and stealing it here
// would hand two writers the same claim; abandoned sentinels are recovered by
// ReclaimStaleSummaryClaim instead. An active row with no summarized_state
// predates the state being recorded and blocks the claim as before. Losing
// both paths means another writer already claimed or posted the summary for
// the current terminal state. Deliberately lease-agnostic — the claim itself
// is the exactly-once authority, so a publisher whose apply lease was
// re-claimed can still win or lose cleanly.
func (s *applyCommentStore) ClaimSummaryComment(ctx context.Context, applyID int64, terminalState string) (bool, error) {
	insertIfAbsent := s.dialect.InsertIfAbsent([]string{"apply_id", "comment_state"})
	result, err := s.db.ExecContext(ctx, `
		INSERT`+insertIfAbsent.Modifier+` INTO apply_comments (apply_id, comment_state, github_comment_id)
		VALUES (?, ?, 0)
		`+insertIfAbsent.Suffix+`
	`, applyID, state.Comment.Summary)
	if err != nil {
		return false, fmt.Errorf("claim summary comment for apply %d (%s): %w", applyID, terminalState, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read summary claim rows affected for apply %d (%s): %w", applyID, terminalState, err)
	}
	if rows == 1 {
		return true, nil
	}

	// The converted sentinel starts a new claim: stamping updated_at restarts
	// its staleness window so ReclaimStaleSummaryClaim cannot immediately hand
	// the same claim to a second publisher.
	settled := settledApplyStates()
	args := []any{applyID, state.Comment.Summary, terminalState}
	args = append(args, stringArgs(settled)...)
	result, err = s.db.ExecContext(ctx, `
		UPDATE apply_comments
		SET github_comment_id = 0, superseded_at = NULL, summarized_state = NULL, updated_at = NOW()
		WHERE apply_id = ? AND comment_state = ? AND github_comment_id != 0
		  AND (superseded_at IS NOT NULL
		       OR (summarized_state IS NOT NULL AND summarized_state != ?
		           AND summarized_state NOT IN (`+placeholders(len(settled))+`)))
	`, args...)
	if err != nil {
		return false, fmt.Errorf("reclaim earlier summary comment for apply %d (%s): %w", applyID, terminalState, err)
	}
	rows, err = result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read earlier summary reclaim rows affected for apply %d (%s): %w", applyID, terminalState, err)
	}
	return rows == 1, nil
}

// SupersedeSummaryForResume consumes the summary marker a resume rotation
// rotated for: it supersedes the marker only while it still records
// githubCommentID and the apply is still active. Returns whether it consumed
// the marker. A marker that records another comment, or a claim sentinel,
// belongs to a terminal publish that came after the summary the rotation saw,
// and an apply that is terminal again owes its summary to the publish that
// terminalized it. Consuming the marker in either case would make that
// publish lose its claim, or leave the apply's next resume without the marker
// it rotates on, so both are left in place. With a lease in context the write
// requires it, and a lost lease surfaces as storage.ErrApplyLeaseLost.
func (s *applyCommentStore) SupersedeSummaryForResume(ctx context.Context, applyID int64, githubCommentID int64) (bool, error) {
	return s.updateResumeSummaryMarker(ctx, applyID, githubCommentID, "supersede", isActiveApplyState,
		"superseded_at = NOW()", nil)
}

// MarkSummaryRotatedPast records that a resume rotation rotated past the
// summary marker after the apply had already reached its next terminal state.
// The marker keeps its posted comment and stays active, so the next resume
// rotates on it; naming no terminal state, it no longer blocks the claim of
// the state the apply reached, and reconciliation finds that summary owed when
// its publish already lost the claim. With a lease in context the write
// requires it, and a lost lease surfaces as storage.ErrApplyLeaseLost.
func (s *applyCommentStore) MarkSummaryRotatedPast(ctx context.Context, applyID int64, githubCommentID int64) (bool, error) {
	return s.updateResumeSummaryMarker(ctx, applyID, githubCommentID, "mark rotated past", state.IsTerminalApplyState,
		"summarized_state = ?", []any{storage.SummaryRotatedPast})
}

// updateResumeSummaryMarker applies a resume rotation's write to the summary
// marker it rotated for: the active marker that still records githubCommentID,
// while the apply's state satisfies applyStateAllows. A claim sentinel is
// never the rotation's marker. assignment is the SET clause, its placeholders
// bound by assignmentArgs, and every write stamps updated_at. Returns whether
// the marker was updated.
//
// The apply row is share-locked before its state is checked and stays locked
// until the marker write commits, so a state transition cannot land between
// the check and the write: the transition waits for the commit, and its own
// summary claim then sees the marker this write left. Both dialects' locking
// reads return the latest committed row at READ COMMITTED.
func (s *applyCommentStore) updateResumeSummaryMarker(
	ctx context.Context, applyID, githubCommentID int64, operation string,
	applyStateAllows func(string) bool, assignment string, assignmentArgs []any,
) (bool, error) {
	if githubCommentID == 0 {
		return false, fmt.Errorf("%s summary of apply %d for resume: no posted summary comment given; a claim sentinel is never the rotation's marker", operation, applyID)
	}
	lease, hasLease, err := applyLeaseFromContext(ctx, applyID)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, fmt.Errorf("begin %s summary comment %d of apply %d for resume: %w", operation, githubCommentID, applyID, err)
	}
	defer rollbackTx(ctx, tx, operation+" summary for resume")

	var applyState, leaseToken string
	err = tx.QueryRowContext(ctx, `SELECT state, lease_token FROM applies WHERE id = ? FOR SHARE`, applyID).Scan(&applyState, &leaseToken)
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("%s summary comment %d for resume: apply %d does not exist: %w", operation, githubCommentID, applyID, storage.ErrApplyNotFound)
	}
	if err != nil {
		return false, fmt.Errorf("lock apply %d to %s summary comment %d for resume: %w", applyID, operation, githubCommentID, err)
	}
	if hasLease && leaseToken != lease.Token {
		return false, fmt.Errorf("apply lease for apply %d is no longer current: %w", applyID, storage.ErrApplyLeaseLost)
	}
	if !applyStateAllows(applyState) {
		return false, nil
	}

	args := append([]any{}, assignmentArgs...)
	args = append(args, applyID, state.Comment.Summary, githubCommentID)
	result, err := tx.ExecContext(ctx, `
		UPDATE apply_comments
		SET `+assignment+`, updated_at = NOW()
		WHERE apply_id = ? AND comment_state = ? AND github_comment_id = ? AND superseded_at IS NULL
	`, args...)
	if err != nil {
		return false, fmt.Errorf("%s summary comment %d of apply %d for resume: %w", operation, githubCommentID, applyID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read %s rows affected for summary comment %d of apply %d: %w", operation, githubCommentID, applyID, err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit %s summary comment %d of apply %d for resume: %w", operation, githubCommentID, applyID, err)
	}
	return rows == 1, nil
}

// ReclaimStaleSummaryClaim takes over a summary claim sentinel abandoned by a
// crashed publisher: still github_comment_id = 0 and not updated for at least
// storage.SummaryClaimStaleAfter. Bumping updated_at transfers ownership, and
// because concurrent reclaimers race on the same conditional update, exactly
// one sees an affected row and wins.
func (s *applyCommentStore) ReclaimStaleSummaryClaim(ctx context.Context, applyID int64) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_comments
		SET updated_at = NOW()
		WHERE apply_id = ? AND comment_state = ?
		  AND github_comment_id = 0
		  AND updated_at < `+s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, ParameterIntervalAmount(), IntervalSecond)+`
	`, applyID, state.Comment.Summary, int64(storage.SummaryClaimStaleAfter.Seconds()))
	if err != nil {
		return false, fmt.Errorf("reclaim stale summary claim for apply %d: %w", applyID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read summary claim reclaim rows affected for apply %d: %w", applyID, err)
	}
	return rows == 1, nil
}

// ClaimProgressCommentAuthority claims — or renews for its current holder —
// the durable authority to edit an apply's tracked progress comment while the
// parent apply lease is legitimately unheld. It is a single conditional update
// on the tracked progress comment row, so concurrent claimants race on the row
// lock and exactly one sees the affected row: the update lands when the row
// records no observer, when the caller already holds the authority (a
// renewal, which also refreshes the heartbeat), or when the recorded holder's
// heartbeat is older than storage.ProgressCommentAuthorityStaleAfter (a
// crashed holder being taken over). Zero affected rows means another observer
// holds a fresh authority or no tracked progress comment row exists — either
// way the caller must skip its GitHub side effect. Deliberately
// lease-agnostic, mirroring ClaimSummaryComment: the claim is the authority.
func (s *applyCommentStore) ClaimProgressCommentAuthority(ctx context.Context, applyID int64, owner string) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_comments
		SET observer_owner = ?, observer_heartbeat_at = NOW(), updated_at = NOW()
		WHERE apply_id = ? AND comment_state = ?
		  AND (observer_owner IS NULL
		       OR observer_owner = ?
		       OR observer_heartbeat_at < `+s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, ParameterIntervalAmount(), IntervalSecond)+`)
	`, owner, applyID, state.Comment.Progress, owner, int64(storage.ProgressCommentAuthorityStaleAfter.Seconds()))
	if err != nil {
		return false, fmt.Errorf("claim progress-comment authority for apply %d owner %s: %w", applyID, owner, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read progress-comment authority claim rows affected for apply %d owner %s: %w", applyID, owner, err)
	}
	if rows == 1 {
		return true, nil
	}
	// MySQL reports rows changed, not rows matched: a holder renewing inside
	// the heartbeat column's timestamp granularity writes identical values and
	// reports zero rows. Distinguish that held-by-the-caller no-op from a lost
	// or unclaimable authority by re-reading the recorded owner.
	var current sql.NullString
	err = s.db.QueryRowContext(ctx, `
		SELECT observer_owner FROM apply_comments WHERE apply_id = ? AND comment_state = ?
	`, applyID, state.Comment.Progress).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("verify progress-comment authority for apply %d owner %s: %w", applyID, owner, err)
	}
	return current.Valid && current.String == owner, nil
}

// ReleaseSummaryClaim deletes the summary claim sentinel for an apply so a
// later publisher can retry without waiting out the stale-claim window. Only
// the sentinel form (github_comment_id = 0) is deleted — a marker that already
// records a posted comment is never released. A missing sentinel is not an
// error: the claim may have been converted to a real comment or reclaimed by
// another writer in the meantime.
func (s *applyCommentStore) ReleaseSummaryClaim(ctx context.Context, applyID int64) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM apply_comments
		WHERE apply_id = ? AND comment_state = ? AND github_comment_id = 0
	`, applyID, state.Comment.Summary)
	if err != nil {
		return fmt.Errorf("release summary claim for apply %d: %w", applyID, err)
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
		&comment.GitHubCommentID, &comment.PostedPhase, &comment.PostedCutoverOperationID, &comment.PendingFreezeCommentID, &comment.EditCount,
		&comment.LastEditedAt, &comment.SupersededAt, &comment.SummarizedState, &comment.CreatedAt, &comment.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &comment, nil
}
