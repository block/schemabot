package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

type notificationStore struct {
	db      *rebindDB
	dialect Dialect
}

func (s *notificationStore) FindUnpublished(ctx context.Context, lookback time.Duration, limit int) ([]*storage.Apply, error) {
	if lookback <= 0 || limit <= 0 {
		return nil, fmt.Errorf("find unpublished applies: positive lookback and limit required")
	}
	cutoff := s.dialect.RelativeTime(TimestampPrecisionMicrosecond, BeforeCurrentTime, ParameterIntervalAmount(), IntervalMicrosecond)
	now := s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)
	rows, err := s.db.QueryContext(ctx, `SELECT `+applyColumnsForApplyAlias+`
 FROM applies a
 LEFT JOIN apply_notifications n ON n.apply_id = a.id
 WHERE a.completed_at >= `+cutoff+`
 AND a.state = ? AND n.published_at IS NULL
 AND (n.claimed_until IS NULL OR n.claimed_until <= `+now+`)
 ORDER BY a.completed_at, a.id LIMIT ?`, lookback.Microseconds(), state.Apply.Completed, limit)
	if err != nil {
		return nil, fmt.Errorf("find completed applies awaiting publication: %w", err)
	}
	defer utils.CloseAndLog(rows)
	var applies []*storage.Apply
	for rows.Next() {
		apply, err := scanApplyInto(rows)
		if err != nil {
			return nil, fmt.Errorf("scan unpublished apply: %w", err)
		}
		applies = append(applies, apply)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read unpublished applies: %w", err)
	}
	return applies, nil
}

func (s *notificationStore) Claim(ctx context.Context, applyID int64, token string, ttl time.Duration) (bool, error) {
	if applyID <= 0 || token == "" || len(token) > 64 || ttl <= 0 {
		return false, fmt.Errorf("claim apply notification: valid apply ID, token and positive TTL required")
	}
	insert := s.dialect.InsertIfAbsent([]string{"apply_id"})
	if _, err := s.db.ExecContext(ctx, `INSERT `+insert.Modifier+` INTO apply_notifications (apply_id) VALUES (?) `+insert.Suffix, applyID); err != nil {
		return false, fmt.Errorf("create notification tracking for apply %d: %w", applyID, err)
	}
	expiry := s.dialect.RelativeTime(TimestampPrecisionMicrosecond, AfterCurrentTime, ParameterIntervalAmount(), IntervalMicrosecond)
	now := s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)
	result, err := s.db.ExecContext(ctx, `UPDATE apply_notifications SET claim_token = ?, claimed_until = `+expiry+`
 WHERE apply_id = ? AND published_at IS NULL AND (claimed_until IS NULL OR claimed_until <= `+now+`)`, token, ttl.Microseconds(), applyID)
	return notificationWriteResult(result, err, applyID, "claim")
}

func (s *notificationStore) MarkPublished(ctx context.Context, applyID int64, token string) (bool, error) {
	now := s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)
	result, err := s.db.ExecContext(ctx, `UPDATE apply_notifications
 SET published_at = `+now+`, claim_token = NULL, claimed_until = NULL, last_error = NULL
 WHERE apply_id = ? AND claim_token = ? AND claimed_until > `+now+` AND published_at IS NULL`, applyID, token)
	return notificationWriteResult(result, err, applyID, "mark published")
}

func (s *notificationStore) Release(ctx context.Context, applyID int64, token, message string) (bool, error) {
	now := s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)
	result, err := s.db.ExecContext(ctx, `UPDATE apply_notifications
 SET claim_token = NULL, claimed_until = NULL, last_error = ?
 WHERE apply_id = ? AND claim_token = ? AND claimed_until > `+now+` AND published_at IS NULL`, message, applyID, token)
	return notificationWriteResult(result, err, applyID, "release")
}

func notificationWriteResult(result sql.Result, err error, applyID int64, operation string) (bool, error) {
	if err != nil {
		return false, fmt.Errorf("%s notification for apply %d: %w", operation, applyID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read %s notification result for apply %d: %w", operation, applyID, err)
	}
	return rows > 0, nil
}

func (s *notificationStore) ObservationCeiling(ctx context.Context) (int64, time.Time, error) {
	var id int64
	var now time.Time
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(id), 0), `+s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)+` FROM applies`).Scan(&id, &now)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("pin notification observation ceiling: %w", err)
	}
	return id, now, nil
}

func (s *notificationStore) ObservePage(ctx context.Context, afterID, throughID int64, limit int) ([]storage.NotificationObservation, error) {
	if afterID < 0 || throughID < afterID || limit <= 0 {
		return nil, fmt.Errorf("observe notification backlog: ordered nonnegative IDs and positive limit required")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT a.id, a.state, a.completed_at, n.published_at
 FROM (SELECT id, state, completed_at FROM applies WHERE id > ? AND id <= ? ORDER BY id LIMIT ?) a
 LEFT JOIN apply_notifications n ON n.apply_id = a.id ORDER BY a.id`, afterID, throughID, limit)
	if err != nil {
		return nil, fmt.Errorf("observe notification backlog after apply %d: %w", afterID, err)
	}
	defer utils.CloseAndLog(rows)
	var samples []storage.NotificationObservation
	for rows.Next() {
		var sample storage.NotificationObservation
		var applyState string
		var publishedAt sql.NullTime
		if err := rows.Scan(&sample.ID, &applyState, &sample.CompletedAt, &publishedAt); err != nil {
			return nil, fmt.Errorf("scan notification observation: %w", err)
		}
		sample.Completed = applyState == state.Apply.Completed
		sample.Published = publishedAt.Valid
		samples = append(samples, sample)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read notification observations: %w", err)
	}
	return samples, nil
}
