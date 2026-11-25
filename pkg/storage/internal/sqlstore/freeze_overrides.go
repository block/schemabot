// freeze_overrides.go implements FreezeOverrideStore, the record of requests
// to let one change through the freezes that hold it.
package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// freezeOverrideColumns lists all columns for SELECT queries.
const freezeOverrideColumns = `id, override_identifier, environment, action, target_id, database_name, repository,
	pull_request, pr_author, blockers, reason, requested_by, approved_by, approved_at, expires_at, created_at`

// freezeOverrideStore implements storage.FreezeOverrideStore. Expiry is
// compared against the database clock, the same clock the freezes use.
type freezeOverrideStore struct {
	db         *rebindDB
	dialect    Dialect
	identity   identityInserter
	classifier ErrorClassifier
}

// Create records a request and returns it as stored.
func (s *freezeOverrideStore) Create(ctx context.Context, request *storage.FreezeOverrideRequest) (*storage.FreezeOverrideRequest, error) {
	return s.create(ctx, request, false)
}

// CreateOnReason records a request approved by its requester at creation.
func (s *freezeOverrideStore) CreateOnReason(ctx context.Context, request *storage.FreezeOverrideRequest) (*storage.FreezeOverrideRequest, error) {
	return s.create(ctx, request, true)
}

// create records a request, approved by its requester when onReason is set.
func (s *freezeOverrideStore) create(ctx context.Context, request *storage.FreezeOverrideRequest, onReason bool) (*storage.FreezeOverrideRequest, error) {
	if err := request.Validate(); err != nil {
		return nil, fmt.Errorf("create freeze override request: %w", err)
	}
	now, err := readFreezeClock(ctx, s.db, s.dialect)
	if err != nil {
		return nil, fmt.Errorf("create freeze override request %s: %w", request.Identifier, err)
	}
	if latest := now.Add(storage.FreezeOverrideMaxLifetime); request.ExpiresAt.After(latest) {
		return nil, fmt.Errorf("create freeze override request %s: expiry %s is after %s: %w",
			request.Identifier, request.ExpiresAt.UTC().Format(time.RFC3339), latest.Format(time.RFC3339), storage.ErrFreezeOverrideLifetime)
	}
	blockers, err := json.Marshal(request.Blockers)
	if err != nil {
		return nil, fmt.Errorf("encode blockers of freeze override request %s: %w", request.Identifier, err)
	}
	// A request admitted on its reason is approved by its requester as it is
	// written, by the same clock Approve uses.
	approvedBy, approvedAt := "NULL", "NULL"
	if onReason {
		approvedBy, approvedAt = "?", s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)
	}
	args := []any{request.Identifier, storage.CanonicalKey(request.Environment), request.Action, request.TargetID,
		storage.CanonicalKey(request.Database), storage.CanonicalKey(request.Repository), request.PullRequest,
		nullString(strings.TrimSpace(request.PRAuthor)), string(blockers), strings.TrimSpace(request.Reason),
		strings.TrimSpace(request.RequestedBy), freezeTime(request.ExpiresAt)}
	if onReason {
		args = append(args, strings.TrimSpace(request.RequestedBy))
	}
	id, err := s.identity.InsertID(ctx, s.db, `
		INSERT INTO freeze_override_requests (
			override_identifier, environment, action, target_id, database_name, repository,
			pull_request, pr_author, blockers, reason, requested_by, expires_at, approved_by, approved_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, `+approvedBy+`, `+approvedAt+`)
	`, args...)
	if err != nil {
		if s.classifier.IsDuplicateKey(err) {
			return nil, fmt.Errorf("create freeze override request %s: %w", request.Identifier, storage.ErrFreezeOverrideExists)
		}
		return nil, fmt.Errorf("create freeze override request %s: %w", request.Identifier, err)
	}
	created, err := s.getByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("read freeze override request %s after insert: %w", request.Identifier, err)
	}
	return created, nil
}

// Get returns the request with this identifier.
func (s *freezeOverrideStore) Get(ctx context.Context, identifier string) (*storage.FreezeOverrideRequest, error) {
	request, err := scanFreezeOverride(s.db.QueryRowContext(ctx, `
		SELECT `+freezeOverrideColumns+`
		FROM freeze_override_requests
		WHERE override_identifier = ?
	`, identifier))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storage.ErrFreezeOverrideNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read freeze override request %s: %w", identifier, err)
	}
	return request, nil
}

// Approve records an approval. The requester and the pull request's author
// are refused before the update. The update only matches a request that is
// unapproved and unexpired by the database clock, so of two racing approvals
// exactly one lands; when it matches nothing, the row is read back to say why.
func (s *freezeOverrideStore) Approve(ctx context.Context, identifier, approver string) (*storage.FreezeOverrideRequest, error) {
	approver = strings.TrimSpace(approver)
	if approver == "" {
		return nil, fmt.Errorf("approve freeze override request %s: the approver is required", identifier)
	}
	request, err := s.Get(ctx, identifier)
	if err != nil {
		return nil, fmt.Errorf("approve freeze override request %s: %w", identifier, err)
	}
	if request.IsPersonOf(approver) {
		return nil, fmt.Errorf("approve freeze override request %s as %s: %w", identifier, approver, storage.ErrFreezeOverrideSelfApproval)
	}
	now := s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)
	result, err := s.db.ExecContext(ctx, `
		UPDATE freeze_override_requests
		SET approved_by = ?, approved_at = `+now+`
		WHERE override_identifier = ? AND approved_at IS NULL AND expires_at > `+now+`
	`, approver, identifier)
	if err != nil {
		return nil, fmt.Errorf("approve freeze override request %s: %w", identifier, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("approve freeze override request %s: read rows affected: %w", identifier, err)
	}
	request, err = s.Get(ctx, identifier)
	if err != nil {
		return nil, fmt.Errorf("read freeze override request %s after approving it: %w", identifier, err)
	}
	if affected > 0 {
		return request, nil
	}
	if request.ApprovedAt != nil {
		return nil, fmt.Errorf("approve freeze override request %s: approved by %s at %s: %w",
			identifier, request.ApprovedBy, request.ApprovedAt.Format(time.RFC3339), storage.ErrFreezeOverrideAlreadyApproved)
	}
	return nil, fmt.Errorf("approve freeze override request %s: expired at %s: %w",
		identifier, request.ExpiresAt.Format(time.RFC3339), storage.ErrFreezeOverrideExpired)
}

// ListApproved returns the approved, unexpired requests for one change, most
// recently approved first.
func (s *freezeOverrideStore) ListApproved(ctx context.Context, target storage.FreezeOverrideTarget) ([]*storage.FreezeOverrideRequest, error) {
	return s.listForTarget(ctx, target, "approved", "approved_at IS NOT NULL", "approved_at DESC, id DESC")
}

// ListPending returns the unapproved, unexpired requests for one change,
// newest first.
func (s *freezeOverrideStore) ListPending(ctx context.Context, target storage.FreezeOverrideTarget) ([]*storage.FreezeOverrideRequest, error) {
	return s.listForTarget(ctx, target, "pending", "approved_at IS NULL", "id DESC")
}

// listForTarget lists one change's unexpired requests in one approval state.
// state, where and order are constants, never caller input.
func (s *freezeOverrideStore) listForTarget(ctx context.Context, target storage.FreezeOverrideTarget, state, where, order string) ([]*storage.FreezeOverrideRequest, error) {
	now := s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+freezeOverrideColumns+`
		FROM freeze_override_requests
		WHERE environment = ? AND action = ? AND target_id = ?
			AND `+where+` AND expires_at > `+now+`
		ORDER BY `+order+`
	`, storage.CanonicalKey(target.Environment), target.Action, target.TargetID)
	if err != nil {
		return nil, fmt.Errorf("list %s freeze override requests for %s %s in %s: %w",
			state, target.Action, target.TargetID, target.Environment, err)
	}
	defer utils.CloseAndLog(rows)

	var requests []*storage.FreezeOverrideRequest
	for rows.Next() {
		request, err := scanFreezeOverride(rows)
		if err != nil {
			return nil, fmt.Errorf("list %s freeze override requests for %s %s in %s: %w",
				state, target.Action, target.TargetID, target.Environment, err)
		}
		requests = append(requests, request)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list %s freeze override requests for %s %s in %s: %w",
			state, target.Action, target.TargetID, target.Environment, err)
	}
	return requests, nil
}

func (s *freezeOverrideStore) getByID(ctx context.Context, id int64) (*storage.FreezeOverrideRequest, error) {
	return scanFreezeOverride(s.db.QueryRowContext(ctx, `
		SELECT `+freezeOverrideColumns+`
		FROM freeze_override_requests
		WHERE id = ?
	`, id))
}

func scanFreezeOverride(row interface{ Scan(dest ...any) error }) (*storage.FreezeOverrideRequest, error) {
	var (
		request    storage.FreezeOverrideRequest
		prAuthor   sql.NullString
		blockers   []byte
		approvedBy sql.NullString
		approvedAt sql.NullTime
	)
	if err := row.Scan(&request.ID, &request.Identifier, &request.Environment, &request.Action, &request.TargetID,
		&request.Database, &request.Repository, &request.PullRequest, &prAuthor, &blockers, &request.Reason,
		&request.RequestedBy, &approvedBy, &approvedAt, &request.ExpiresAt, &request.CreatedAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(blockers, &request.Blockers); err != nil {
		return nil, fmt.Errorf("decode blockers of freeze override request %s: %w", request.Identifier, err)
	}
	request.PRAuthor = prAuthor.String
	request.ApprovedBy = approvedBy.String
	if approvedAt.Valid {
		at := approvedAt.Time.UTC()
		request.ApprovedAt = &at
	}
	request.ExpiresAt = request.ExpiresAt.UTC()
	return &request, nil
}
