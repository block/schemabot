package spirit

import (
	"log/slog"
	"os"
	"testing"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/enginetest"
)

// newConformanceEngine builds a Spirit engine with no tracked schema change,
// the state a fresh process (or one whose change was consumed) is in.
func newConformanceEngine() *Engine {
	return New(Config{Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))})
}

// engineWithTrackedChange builds a Spirit engine whose in-process schema
// change has already reached the given state.
func engineWithTrackedChange(state engine.State, errorMessage string) *Engine {
	eng := newConformanceEngine()
	eng.runningSchemaChange = &runningSchemaChange{
		state:        state,
		errorMessage: errorMessage,
		database:     "testdb",
		tables:       []string{"users"},
	}
	return eng
}

func spiritControlRequest() *engine.ControlRequest {
	return &engine.ControlRequest{Database: "testdb"}
}

// Spirit's contract-suite run. Spirit tracks its schema change in-process, so
// "nonexistent" means no tracked change in this process, and "already
// completed" means the tracked change reached its completed state before the
// command arrived.
func TestEngineConformance(t *testing.T) {
	enginetest.Run(t, enginetest.Harness{
		CancelAlreadyCompleted: func(t *testing.T) enginetest.ControlFixture {
			return enginetest.ControlFixture{
				Engine: engineWithTrackedChange(engine.StateCompleted, ""),
				Req:    spiritControlRequest(),
			}
		},
		StopAlreadyCompleted: func(t *testing.T) enginetest.ControlFixture {
			return enginetest.ControlFixture{
				Engine: engineWithTrackedChange(engine.StateCompleted, ""),
				Req:    spiritControlRequest(),
			}
		},
		CancelNonexistent: func(t *testing.T) enginetest.ControlFixture {
			return enginetest.ControlFixture{
				Engine: newConformanceEngine(),
				Req:    spiritControlRequest(),
			}
		},
		StopNonexistent: func(t *testing.T) enginetest.ControlFixture {
			return enginetest.ControlFixture{
				Engine: newConformanceEngine(),
				Req:    spiritControlRequest(),
			}
		},
		TerminalProgress: func(t *testing.T) []enginetest.ProgressFixture {
			return []enginetest.ProgressFixture{
				{
					Name:   "completed",
					Engine: engineWithTrackedChange(engine.StateCompleted, ""),
					Req:    &engine.ProgressRequest{Database: "testdb"},
					Want:   engine.StateCompleted,
				},
				{
					Name:   "failed",
					Engine: engineWithTrackedChange(engine.StateFailed, "copy failed"),
					Req:    &engine.ProgressRequest{Database: "testdb"},
					Want:   engine.StateFailed,
				},
				{
					Name:   "cancelled",
					Engine: engineWithTrackedChange(engine.StateCancelled, ""),
					Req:    &engine.ProgressRequest{Database: "testdb"},
					Want:   engine.StateCancelled,
				},
				{
					Name:   "stopped",
					Engine: engineWithTrackedChange(engine.StateStopped, ""),
					Req:    &engine.ProgressRequest{Database: "testdb"},
					Want:   engine.StateStopped,
				},
			}
		},
		Skips: map[enginetest.Case]string{
			enginetest.CaseNotReadyDistinguishable: "Spirit drives schema changes in-process against a direct MySQL connection: there is no remote backend that could accept an operation later but not yet, so no operation is ever rejected as not-ready.",
		},
	})
}
