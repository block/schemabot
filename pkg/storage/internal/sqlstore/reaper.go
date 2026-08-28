package sqlstore

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/spirit/pkg/utils"
)

// strandedSweep identifies one of the reaper's sweeps to reapUnderElection: the
// advisory lock that elects it, the error to report when another instance holds
// that lock, and the phrase naming what it settles in errors and warnings.
type strandedSweep struct {
	lockName string
	busy     error
	subject  string
}

// reapUnderElection runs one sweep on a connection holding its advisory lock,
// so a single instance in the fleet pays for the scan per pass. The election is
// an efficiency gate, not a safety one — every reaper write is guarded and
// idempotent, so concurrent sweeps would be correct, just wasteful.
//
// Each sweep owns its own lock, so sweeps never serialize on each other.
func reapUnderElection[T any](
	ctx context.Context,
	db *rebindDB,
	locker namedlock.Locker,
	sweep strandedSweep,
	reap func(context.Context) ([]T, error),
) ([]T, error) {
	if locker == nil {
		return nil, fmt.Errorf("reap %s requires an advisory locker; reapers cannot elect a single instance without one", sweep.subject)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("get %s reaper connection: %w", sweep.subject, err)
	}
	defer utils.CloseAndLog(conn)

	// Do not wait for the lock: whoever holds it is doing this pass's work, and
	// this instance's next tick is soon enough.
	acquired, err := locker.Acquire(ctx, conn.lockerConn(), sweep.lockName, 0)
	if err != nil {
		return nil, fmt.Errorf("acquire %s reaper lock: %w", sweep.subject, err)
	}
	if !acquired {
		return nil, sweep.busy
	}
	defer func() {
		// A held lock parks every instance's reaper until this session is
		// retired, so the two ways it can survive the pass are reported apart:
		// the release errored, or it ran and reported the lock was not held.
		released, err := locker.Release(context.WithoutCancel(ctx), conn.lockerConn(), sweep.lockName)
		if err != nil {
			slog.WarnContext(ctx, "failed to release a reaper lock; reapers stay blocked until this session is retired",
				"lock", sweep.lockName, "sweep", sweep.subject, "error", err)
			return
		}
		if !released {
			slog.WarnContext(ctx, "a reaper lock was not held at release; another session may have taken it",
				"lock", sweep.lockName, "sweep", sweep.subject)
		}
	}()

	return reap(ctx)
}
