package postgres

import (
	"log/slog"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/enginetest"
)

// engineWithSettledApply builds a PostgreSQL engine whose one tracked
// concurrent index build has already published the given terminal result.
func engineWithSettledApply(t *testing.T, state engine.State, phase, detail string) *Engine {
	t.Helper()
	eng := New()
	change := nativeApply{namespace: "public", table: "users", sql: concurrentIndexDDL, steps: 1, concurrentIndex: true}
	eng.claimProgress(cancelTestKey, progressResult(state, phase, time.Now(), change, detail), newTestTracker(t), slog.New(slog.DiscardHandler), true, nil)
	return eng
}

func postgresProgressRequest() *engine.ProgressRequest {
	return &engine.ProgressRequest{ResumeState: &engine.ResumeState{MigrationContext: cancelTestKey}}
}

// The PostgreSQL engine's contract-suite run. The engine tracks its applies in
// this process, so "nonexistent" means no tracked apply under the request's
// identity, and "already completed" means the tracked build published its
// completed result before the cancel arrived. Stop is declined for every
// PostgreSQL change — a concurrent index build has no resumable midpoint and
// plain DDL commits or fails on its own — so the stop cases have no
// completed-versus-nonexistent distinction to pin.
func TestEngineConformance(t *testing.T) {
	enginetest.Run(t, enginetest.Harness{
		CancelAlreadyCompleted: func(t *testing.T) enginetest.ControlFixture {
			return enginetest.ControlFixture{
				Engine: engineWithSettledApply(t, engine.StateCompleted, "completed", ""),
				Req:    cancelRequest(),
			}
		},
		CancelNonexistent: func(t *testing.T) enginetest.ControlFixture {
			return enginetest.ControlFixture{
				Engine: New(),
				Req:    cancelRequest(),
			}
		},
		TerminalProgress: func(t *testing.T) []enginetest.ProgressFixture {
			return []enginetest.ProgressFixture{
				{
					Name:   "completed",
					Engine: engineWithSettledApply(t, engine.StateCompleted, "completed", ""),
					Req:    postgresProgressRequest(),
					Want:   engine.StateCompleted,
				},
				{
					Name:   "failed",
					Engine: engineWithSettledApply(t, engine.StateFailed, "failed", "index build failed"),
					Req:    postgresProgressRequest(),
					Want:   engine.StateFailed,
				},
				{
					Name:   "cancelled",
					Engine: engineWithSettledApply(t, engine.StateCancelled, "cancelled", "Concurrent index build cancelled"),
					Req:    postgresProgressRequest(),
					Want:   engine.StateCancelled,
				},
			}
		},
		Skips: map[enginetest.Case]string{
			enginetest.CaseStopAlreadyCompleted:    "PostgreSQL declines stop for every schema change with a typed UnsupportedOperationError: a concurrent index build has no resumable midpoint and plain DDL commits or fails on its own, so a stop is never answered from the change's state.",
			enginetest.CaseStopNonexistent:         "PostgreSQL declines stop for every schema change with a typed UnsupportedOperationError before looking the change up, so a stop of a nonexistent change is not distinguishable from any other stop.",
			enginetest.CaseNotReadyDistinguishable: "PostgreSQL drives schema changes in-process against a direct connection to the target: there is no remote backend that could accept an operation later but not yet, so no operation is ever rejected as not-ready.",
		},
	})
}
