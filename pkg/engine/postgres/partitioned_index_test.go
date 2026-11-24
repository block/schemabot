package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/pg-sprite/pkg/dbconn"
	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/progress"

	"github.com/block/schemabot/pkg/engine"
)

const partitionedIndexDDL = "CREATE INDEX users_email_idx ON public.users (email)"

// partitionedIndexBuildCall records what the drive handed the partitioned
// index build.
type partitionedIndexBuildCall struct {
	sql         string
	budget      executor.ConcurrentBudget
	deadline    time.Time
	hasDeadline bool
}

// scriptPartitionedIndex swaps the partitioned index build seam for run,
// recording each call, and restores the executor when the test ends.
func scriptPartitionedIndex(t *testing.T, run func(ctx context.Context) error) *[]partitionedIndexBuildCall {
	t.Helper()
	var calls []partitionedIndexBuildCall
	previous := partitionedIndexBuild
	partitionedIndexBuild = func(ctx context.Context, _ dbconn.Config, _ *pgxpool.Pool, _ preflight.PreflightedTable, sql string, budget executor.ConcurrentBudget, _ *progress.Tracker) (executor.PartitionedIndexReport, error) {
		deadline, ok := ctx.Deadline()
		calls = append(calls, partitionedIndexBuildCall{sql: sql, budget: budget, deadline: deadline, hasDeadline: ok})
		return executor.PartitionedIndexReport{Index: "users_email_idx", Partitions: 2}, run(ctx)
	}
	t.Cleanup(func() { partitionedIndexBuild = previous })
	return &calls
}

func partitionedIndexChange(bound time.Duration) nativeApply {
	return nativeApply{namespace: "public", table: "users", sql: partitionedIndexDDL,
		partitionedIndex: true, concurrentIndexMaxDuration: bound}
}

// TestBuildPartitionedIndexBoundsEachPartitionAndTheWholeBuild proves the
// build runs the reviewed statement with the configured bound as every
// partition's budget, under a deadline of that same bound for the whole build,
// so one apply of a partitioned index is bounded like a concurrent build.
func TestBuildPartitionedIndexBoundsEachPartitionAndTheWholeBuild(t *testing.T) {
	const bound = 3 * time.Hour
	calls := scriptPartitionedIndex(t, func(context.Context) error { return nil })

	before := time.Now()
	err := buildPartitionedIndex(t.Context(), dbconn.Config{}, nil, preflight.PreflightedTable{},
		partitionedIndexChange(bound), newTestTracker(t), slog.New(slog.DiscardHandler))

	require.NoError(t, err)
	require.Len(t, *calls, 1)
	call := (*calls)[0]
	assert.Equal(t, partitionedIndexDDL, call.sql)
	assert.Equal(t, executor.ConcurrentBudget{Overall: bound}, call.budget)
	require.True(t, call.hasDeadline, "the whole build must run under a deadline")
	assert.WithinDuration(t, before.Add(bound), call.deadline, time.Minute)
}

// TestBuildPartitionedIndexNamesItsOwnDeadline proves a build its own deadline
// ended is reported as the bound, which the drive retries to resume, while a
// build the drive's context ended is reported as that cancellation.
func TestBuildPartitionedIndexNamesItsOwnDeadline(t *testing.T) {
	waitForContext := func(ctx context.Context) error {
		<-ctx.Done()
		return fmt.Errorf("build partition index: %w", executor.ErrCancelledByCaller)
	}
	scriptPartitionedIndex(t, waitForContext)

	err := buildPartitionedIndex(t.Context(), dbconn.Config{}, nil, preflight.PreflightedTable{},
		partitionedIndexChange(10*time.Millisecond), newTestTracker(t), slog.New(slog.DiscardHandler))
	var boundErr *partitionedIndexBoundError
	require.ErrorAs(t, err, &boundErr)
	assert.False(t, isCancellation(err), "a build that ran past its bound is not a cancellation")
	failure := classifyApplyFailure(err, "users")
	assert.True(t, failure.retryable)
	assert.Equal(t, "the partitioned index build ran past postgres.concurrent_index_max_duration (10ms) before every partition had its index; the retry resumes from the partitions already built", failure.detail)

	driveCtx, cancel := context.WithCancel(t.Context())
	cancel()
	err = buildPartitionedIndex(driveCtx, dbconn.Config{}, nil, preflight.PreflightedTable{},
		partitionedIndexChange(time.Hour), newTestTracker(t), slog.New(slog.DiscardHandler))
	assert.False(t, errors.As(err, &boundErr), "the drive's own cancellation is not the bound")
	assert.True(t, isCancellation(err))
}

// TestBuildPartitionedIndexRefusesAnUnsetBound proves a change built without
// the drive's bound never reaches the executor.
func TestBuildPartitionedIndexRefusesAnUnsetBound(t *testing.T) {
	calls := scriptPartitionedIndex(t, func(context.Context) error { return nil })

	err := buildPartitionedIndex(t.Context(), dbconn.Config{}, nil, preflight.PreflightedTable{},
		partitionedIndexChange(0), newTestTracker(t), slog.New(slog.DiscardHandler))

	require.ErrorContains(t, err, "build bound is unset")
	assert.Empty(t, *calls)
}

// An operator can cancel a partitioned index build like a concurrent one. The
// apply settles cancelled, and the outcome tells the operator that the
// partitions already built keep their index and that applying the plan again
// resumes the build, since nothing is dropped.
func TestCancelEndsAPartitionedIndexBuildAndKeepsItsProgress(t *testing.T) {
	scripted := newScriptedExecutor(func(*progress.Tracker) error {
		return errors.New("the executor must be ended by its context, not released")
	})
	eng := New()
	const key = "task-a"
	applyChange(t, eng, scripted, key, partitionedIndexDDL, slog.New(slog.NewTextHandler(io.Discard, nil)))
	env := scripted.envelope(t)
	assert.True(t, env.change.partitionedIndex)
	assert.Equal(t, DefaultConcurrentIndexMaxDuration, env.change.concurrentIndexMaxDuration)
	scripted.tracker(t)

	result, err := eng.Cancel(t.Context(), &engine.ControlRequest{ResumeState: &engine.ResumeState{MigrationContext: key}})
	require.NoError(t, err)
	assert.True(t, result.Accepted)

	terminal := pollProgress(t, eng, key)
	assert.Equal(t, engine.StateCancelled, terminal.State)
	assert.Equal(t, partitionedIndexCancelledDetail, terminal.ErrorMessage)
}

// A partitioned index build that runs past its bound keeps the partitions it
// finished, so the apply fails retryable and the retry resumes from them.
func TestPartitionedIndexBuildPastItsBoundIsRetried(t *testing.T) {
	scripted := newScriptedExecutor(func(*progress.Tracker) error {
		return fmt.Errorf("build PostgreSQL index on partitioned table %q: %w", "users", &partitionedIndexBoundError{bound: time.Hour})
	})
	eng := New()
	const key = "task-a"
	applyChange(t, eng, scripted, key, partitionedIndexDDL, slog.New(slog.NewTextHandler(io.Discard, nil)))
	scripted.tracker(t)
	scripted.release()

	require.Eventually(t, func() bool {
		return pollProgress(t, eng, key).State.IsTerminal()
	}, backgroundApplyDeadline, 10*time.Millisecond)
	terminal := pollProgress(t, eng, key)
	assert.Equal(t, engine.StateFailed, terminal.State)
	assert.True(t, terminal.Retryable)
	assert.Equal(t, "the partitioned index build ran past postgres.concurrent_index_max_duration (1h0m0s) before every partition had its index; the retry resumes from the partitions already built", terminal.ErrorMessage)
}
