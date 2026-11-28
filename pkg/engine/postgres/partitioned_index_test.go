package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
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
	return scriptPartitionedIndexReport(t, executor.PartitionedIndexReport{Index: "users_email_idx", Partitions: 2}, run)
}

// scriptPartitionedIndexReport is scriptPartitionedIndex with the report the
// build returns, so a test can say which partitions the run finished.
func scriptPartitionedIndexReport(t *testing.T, report executor.PartitionedIndexReport, run func(ctx context.Context) error) *[]partitionedIndexBuildCall {
	t.Helper()
	var calls []partitionedIndexBuildCall
	previous := partitionedIndexBuild
	partitionedIndexBuild = func(ctx context.Context, _ dbconn.Config, _ *pgxpool.Pool, _ preflight.PreflightedTable, sql string, budget executor.ConcurrentBudget, _ *progress.Tracker) (executor.PartitionedIndexReport, error) {
		deadline, ok := ctx.Deadline()
		calls = append(calls, partitionedIndexBuildCall{sql: sql, budget: budget, deadline: deadline, hasDeadline: ok})
		return report, run(ctx)
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
// ended after it finished a partition is reported as the bound, which the
// drive retries to resume, while a build the drive's context ended is
// reported as that cancellation.
func TestBuildPartitionedIndexNamesItsOwnDeadline(t *testing.T) {
	waitForContext := func(ctx context.Context) error {
		<-ctx.Done()
		return fmt.Errorf("build partition index: %w", executor.ErrCancelledByCaller)
	}
	scriptPartitionedIndexReport(t, executor.PartitionedIndexReport{Index: "users_email_idx", Partitions: 2,
		Worked: []executor.PartitionIndexResult{{Schema: "public", Table: "events_2025", Index: "events_2025_email_idx"}}}, waitForContext)

	err := buildPartitionedIndex(t.Context(), dbconn.Config{}, nil, preflight.PreflightedTable{},
		partitionedIndexChange(10*time.Millisecond), newTestTracker(t), slog.New(slog.DiscardHandler))
	var boundErr *partitionedIndexBoundError
	require.ErrorAs(t, err, &boundErr)
	assert.False(t, isCancellation(err), "a build that ran past its bound is not a cancellation")
	assert.Equal(t, 1, boundErr.partitionsBuilt)
	assert.Nil(t, classifyRefusal(err, "users"), "a run that finished a partition made progress a retry keeps")
	failure := classifyApplyFailure(err, "users", nil)
	assert.True(t, failure.retryable)
	assert.Equal(t, "the partitioned index build ran past postgres.concurrent_index_max_duration (10ms) before every partition had its index; the retry resumes from the partitions already built", failure.detail)

	driveCtx, cancel := context.WithCancel(t.Context())
	cancel()
	err = buildPartitionedIndex(driveCtx, dbconn.Config{}, nil, preflight.PreflightedTable{},
		partitionedIndexChange(time.Hour), newTestTracker(t), slog.New(slog.DiscardHandler))
	assert.False(t, errors.As(err, &boundErr), "the drive's own cancellation is not the bound")
	assert.True(t, isCancellation(err))
}

// A partition that needs longer than the bound is never finished, so a build
// that ran past its bound without finishing any partition is refused rather
// than retried: each retry would rebuild the same partition for the whole
// bound again. The refusal names the option that grants more time.
func TestBuildPartitionedIndexPastItsBoundWithNoPartitionFinishedIsRefused(t *testing.T) {
	scriptPartitionedIndex(t, func(ctx context.Context) error {
		<-ctx.Done()
		return fmt.Errorf("build partition index: %w", executor.ErrCancelledByCaller)
	})

	err := buildPartitionedIndex(t.Context(), dbconn.Config{}, nil, preflight.PreflightedTable{},
		partitionedIndexChange(10*time.Millisecond), newTestTracker(t), slog.New(slog.DiscardHandler))
	var boundErr *partitionedIndexBoundError
	require.ErrorAs(t, err, &boundErr)
	assert.Zero(t, boundErr.partitionsBuilt)
	r := classifyRefusal(err, "users")
	require.NotNil(t, r)
	assert.Equal(t, "partitioned-index-bound-exceeded", r.reason)
	assert.Equal(t, "the partitioned index build ran past postgres.concurrent_index_max_duration (10ms) before it finished any partition", r.cause)
	assert.Equal(t, "raise postgres.concurrent_index_max_duration and re-run", r.remedy)
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
		return fmt.Errorf("build PostgreSQL index on partitioned table %q: %w", "users", &partitionedIndexBoundError{bound: time.Hour, partitionsBuilt: 1})
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

// A partitioned index build that runs past its bound before it finishes any
// partition fails without a retry, and the detail tells the operator to raise
// the bound, since applying the same plan again would spend the same bound on
// the same partition.
func TestPartitionedIndexBuildPastItsBoundWithNoPartitionFinishedFails(t *testing.T) {
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
	assert.False(t, terminal.Retryable, "a retry would spend the same bound on the same partition")
	assert.Contains(t, terminal.ErrorMessage, "the partitioned index build ran past postgres.concurrent_index_max_duration (1h0m0s) before it finished any partition")
	assert.Contains(t, terminal.ErrorMessage, "raise postgres.concurrent_index_max_duration and re-run")
}

// A leaf partition's build that fails on its rows leaves an invalid index on
// that partition, which the engine drops through pg-sprite's proven drop,
// named by the leaf's own build statement. RC-4: only a data fault whose
// leftover pg-sprite proved the build's own, on a named partition, is
// dropped. A drop that fails keeps the data-fault refusal, names the
// partition and where the leftover still checks writes, and never claims it
// was removed.
func TestBuildPartitionedIndexDropsTheLeftoverAPartitionDataFaultLeft(t *testing.T) {
	const leafDDL = "CREATE INDEX CONCURRENTLY users_email_idx_p123 ON public.users_2026 USING btree (email)"
	duplicate := &pgconn.PgError{Code: "23505"}
	leaf := func(build error, cleanup error, table string) *executor.InvalidIndexError {
		return &executor.InvalidIndexError{Schema: "public", Index: "users_email_idx_p123", Table: table,
			Build: build, Cleanup: cleanup}
	}
	for name, tc := range map[string]struct {
		verdict *executor.InvalidIndexError
		drop    dropScript
		drops   int
		// detail is the data-fault refusal's detail; empty when the
		// failure is not refused as a data fault.
		detail string
	}{
		"dropped": {
			verdict: leaf(duplicate, executor.ErrBuildLeftInvalidIndex, "users_2026"), drop: droppedOneIndex, drops: 1,
			detail: `index "public"."users_email_idx_p123" ("users_2026") was not built: rows duplicate its key (23505), and its invalid leftover was dropped; correct the rows, then re-run`,
		},
		"drop failed after quarantining the leftover": {
			verdict: leaf(duplicate, executor.ErrBuildLeftInvalidIndex, "users_2026"), drops: 1,
			drop: dropFailed(&executor.InvalidIndexError{Schema: "public", Index: "pgsprite_abandoned_42", Table: "users_2026",
				Cleanup: executor.ErrInvalidIndexBuilderUnobservable}),
			detail: `index "public"."users_email_idx_p123" ("users_2026") was not built: rows duplicate its key (23505), and its invalid leftover still rejects writes that duplicate its key; ` +
				`an operator must drop it, named "public"."pgsprite_abandoned_42" (see the PostgreSQL invalid-index recovery guidance), then correct the rows and re-run`,
		},
		"drop failed without saying where the leftover is": {
			verdict: leaf(duplicate, executor.ErrBuildLeftInvalidIndex, "users_2026"), drops: 1,
			drop: dropFailed(errors.New("lock not granted")),
			detail: `index "public"."users_email_idx_p123" ("users_2026") was not built: rows duplicate its key (23505), and its invalid leftover still rejects writes that duplicate its key; ` +
				`an operator must drop it, under this name or a pgsprite_abandoned_<oid> name (see the PostgreSQL invalid-index recovery guidance), then correct the rows and re-run`,
		},
		"not a data fault": {
			verdict: leaf(&pgconn.PgError{Code: "40P01"}, executor.ErrBuildLeftInvalidIndex, "users_2026"),
		},
		"leftover not proven the build's own": {
			verdict: leaf(duplicate, executor.ErrAbandonmentUnproven, "users_2026"),
			detail: `index "public"."users_email_idx_p123" was not built: rows duplicate its key (23505), and an invalid index not proven its own holds the name; ` +
				`correct the rows, then re-run; the rerun removes the invalid index once it proves no build holds it`,
		},
		"no partition named": {
			verdict: leaf(duplicate, executor.ErrBuildLeftInvalidIndex, ""),
			detail: `index "public"."users_email_idx_p123" was not built: rows duplicate its key (23505), and its invalid leftover still rejects writes that duplicate its key; ` +
				`correct the rows, then re-run; the rerun removes the invalid index once it proves no build holds it`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			drop := tc.drop
			if drop == nil {
				drop = dropFailed(errors.New("no drop is expected"))
			}
			calls := scriptConcurrentIndex(t, func() error { return errors.New("no build is expected") },
				func() error { return errors.New("no rebuild is expected") }, drop)
			scriptPartitionedIndex(t, func(context.Context) error { return tc.verdict })

			err := buildPartitionedIndex(t.Context(), dbconn.Config{}, nil, preflight.PreflightedTable{},
				partitionedIndexChange(time.Hour), newTestTracker(t), slog.New(slog.DiscardHandler))

			require.Error(t, err)
			require.Len(t, calls.drop, tc.drops)
			if tc.drops > 0 {
				assert.Equal(t, leafDDL, calls.drop[0].sql)
				assert.Equal(t, executor.ConcurrentBudget{CallerOwned: true}, calls.drop[0].budget)
			}
			r := classifyRefusal(err, "users")
			if tc.detail == "" {
				assert.Nil(t, r, "a failure that is not a data fault is not refused as one")
				return
			}
			require.NotNil(t, r)
			assert.Equal(t, "data-fault", r.reason)
			assert.Equal(t, tc.detail, r.detail)
		})
	}
}
