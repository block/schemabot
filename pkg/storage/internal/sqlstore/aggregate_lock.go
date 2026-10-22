package sqlstore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"
)

// WithAggregateCheckLock holds a session advisory lock across the full fold,
// including GitHub publication, so an older snapshot cannot publish last.
func (s *checkStore) WithAggregateCheckLock(ctx context.Context, repo string, pr int, fold func(context.Context) error) error {
	if s.locker == nil {
		return fmt.Errorf("aggregate publication for %s#%d requires a session advisory locker", repo, pr)
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open aggregate lock connection for %s#%d: %w", repo, pr, err)
	}
	sum := sha256.Sum256(fmt.Appendf(nil, "%s#%d", repo, pr))
	name := fmt.Sprintf("schemabot_check_%x", sum[:16])
	acquired, err := s.locker.Acquire(ctx, conn.lockerConn(), name, 5*time.Second)
	if err != nil {
		closeApplyTargetLockConn(ctx, conn, name, "acquire aggregate lock")
		return fmt.Errorf("lock aggregate %s#%d: %w", repo, pr, err)
	}
	if !acquired {
		closeApplyTargetLockConn(ctx, conn, name, "acquire aggregate lock")
		return fmt.Errorf("aggregate %s#%d is being published by another fold", repo, pr)
	}
	defer releaseApplyTargetLockConn(ctx, s.locker, conn, name, "publish aggregate")
	return fold(ctx)
}
