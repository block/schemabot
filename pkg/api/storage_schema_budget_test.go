package api

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// A caller that says nothing converges the way a pod does. The boot path is
// the one that cannot afford to wait — a pod converging is a pod not yet
// serving, and one holding the advisory lock is every other pod not serving
// either — so the short budget has to be what an unconsidered call gets.
func TestEnsureSchemaOptions_DefaultBudgetIsTheBootBudget(t *testing.T) {
	t.Parallel()

	assert.Equal(t, EnsureSchemaTimeout, newEnsureSchemaOptions().convergenceTimeout)
}

// The deliberate path defaults to the operator budget without each of its
// entry points having to ask, and a caller's own budget still wins over that
// default — which is what carries an operator's --timeout through.
func TestStorageApplyOptions_DefaultsToTheOperatorBudgetAndYieldsToACaller(t *testing.T) {
	t.Parallel()

	defaulted := newEnsureSchemaOptions(storageApplyOptions(nil)...)
	assert.Equal(t, apitypes.DefaultStorageApplyTimeout, defaulted.convergenceTimeout,
		"a convergence somebody asked for must not inherit the budget a boot needs")

	const chosen = 12 * time.Minute
	overridden := newEnsureSchemaOptions(
		storageApplyOptions([]EnsureSchemaOption{WithConvergenceTimeout(chosen)})...)
	assert.Equal(t, chosen, overridden.convergenceTimeout)
}

// A budget under the minimum is refused before any dialect is dispatched to,
// rather than read as "no limit" or rounded to one. An unbounded convergence
// holds the storage bootstrap advisory lock on a statement that will never
// finish, and every pod that boots behind it fails its own lock wait; a
// sub-second one is enforced at a coarser granularity than it was named at,
// and the statement budget derived from it could sit at or above the ceiling
// it is meant to undercut.
func TestEnsureSchema_RefusesABudgetUnderTheMinimum(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for name, budget := range map[string]time.Duration{
		"zero":                   0,
		"negative":               -time.Second,
		"a millisecond":          time.Millisecond,
		"just under the minimum": MinConvergenceTimeout - time.Nanosecond,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// A DSN that would fail on contact, to prove the refusal happens
			// before anything is dialed.
			err := EnsureSchema("root@tcp(127.0.0.1:1)/schemabot", logger,
				WithConvergenceTimeout(budget))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "convergence timeout must be at least 1s")
		})
	}
}

// The shortest budget the wire can name is admitted, so a caller who names it
// is refused by the database they could not reach, not by the budget check.
func TestEnsureSchema_AdmitsTheMinimumBudget(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := EnsureSchema("root@tcp(127.0.0.1:1)/schemabot", logger,
		WithConvergenceTimeout(MinConvergenceTimeout))
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "convergence timeout must be at least")
}

// A convergence that runs out of budget names the budget it actually had. An
// operator who gave the run an hour and is told it did not finish in five
// minutes goes looking for a timeout that never fired, on the one path where
// the elapsed time is the first thing they check.
func TestEnsureSchemaTimeoutError_NamesTheBudgetTheRunHad(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := ensureSchemaTimeoutError(ctx, apitypes.DefaultStorageApplyTimeout, 3, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), apitypes.DefaultStorageApplyTimeout.String())
	assert.NotContains(t, err.Error(), EnsureSchemaTimeout.String(),
		"a convergence that ran on the operator budget must not report the boot budget")
}

// The write deadline a convergence route lifts to follows the convergence's
// own budget. Pinned to any single value it would close the connection under
// every convergence that asked for longer — the DDL running on server-side
// while the operator sees a truncated response and cannot tell whether their
// storage was converged.
func TestStorageSchemaApplyWriteBudget_CoversTheConvergenceItBounds(t *testing.T) {
	t.Parallel()

	for _, convergence := range []time.Duration{
		EnsureSchemaTimeout,
		apitypes.DefaultStorageApplyTimeout,
	} {
		budget := storageSchemaApplyWriteBudget(convergence)
		assert.Greater(t, budget, convergence,
			"the response deadline must outlast the work it is waiting on")
		assert.GreaterOrEqual(t, budget, convergence+2*StorageSchemaPlanTimeout,
			"a convergence is bracketed by two diffs, and the deadline covers all three")
	}
}
