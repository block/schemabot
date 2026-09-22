//go:build integration

package api

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/schema"
)

// queueAtTheDeadlineLocker grants the lock only once the convergence's budget
// has already been spent, which is what a queue long enough to consume the
// whole budget looks like from the front of it. It embeds the real Postgres
// locker so the release the caller defers runs the genuine statement.
type queueAtTheDeadlineLocker struct{ namedlock.Postgres }

func (queueAtTheDeadlineLocker) Acquire(ctx context.Context, _ *sql.Conn, _ string, _ time.Duration) (bool, error) {
	<-ctx.Done()
	return true, nil
}

// A convergence that waits out its whole budget in the advisory-lock queue and
// is then granted the lock converges nothing and says why. Proceeding would run
// storage DDL on a context that is already done — an engine started with no
// budget to finish in, and a failure that names a cancelled statement rather
// than the queue that caused it.
func TestEnsureSchemaPostgresRefusesALockGrantedWithNoBudgetLeft(t *testing.T) {
	dsn, _ := startPostgresStorage(t)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// A budget this short stands in for a whole one spent waiting: the
	// bootstrapper derives its context from the budget it is given, so the path
	// under test is the same one a real queue produces.
	const budget = 2 * time.Second
	o := newEnsureSchemaOptions(WithDialect(schema.DialectPostgres), WithConvergenceTimeout(budget))

	err := ensurePostgresSchema(t.Context(), dsn, logger, o, queueAtTheDeadlineLocker{})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Contains(t, err.Error(), "which the lock wait consumed")
	assert.Contains(t, err.Error(), budget.String())
	assert.Contains(t, logs.String(), "the wait consumed the whole budget")
}
