package sqlstore

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/stretchr/testify/require"
)

type aggregateTestLocker struct{ namedlock.Locker }

func (aggregateTestLocker) Acquire(context.Context, *sql.Conn, string, time.Duration) (bool, error) {
	return true, nil
}
func (aggregateTestLocker) Release(context.Context, *sql.Conn, string) (bool, error) {
	return true, nil
}

func TestAggregateLockReusesPinnedSession(t *testing.T) {
	binder := &countingBinder{}
	db, connector := newRecordingRebindDB(t, binder)
	db.pool.SetMaxOpenConns(1)
	s := NewWithDependencies(Dependencies{DB: db.pool, Binder: binder, Dialect: MySQLDialect{}, Locker: aggregateTestLocker{}})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, s.checks.WithAggregateCheckLock(ctx, "org/repo", 1, func(ctx context.Context) error {
		_, err := s.checks.GetByPR(ctx, "org/repo", 1)
		return err
	}))
	require.EqualValues(t, 0, db.pool.Stats().WaitCount)
	require.Equal(t, 0, db.pool.Stats().InUse)
	assertReboundExactlyOnce(t, binder, connector)
	// The scoped session does not leak into subsequent ordinary pool queries.
	_, err := s.checks.GetByPR(ctx, "org/repo", 1)
	require.NoError(t, err)
}
