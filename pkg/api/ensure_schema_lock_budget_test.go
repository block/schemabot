package api

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
)

// Reaching the front of the advisory-lock queue is not the same as being able
// to use it. The wait and the work share one budget, so a convergence can be
// granted the lock with nothing left to converge under it; that case names
// itself in the log and refuses, rather than starting DDL on a dead context and
// reporting whatever the driver says about it. The budget that was spent and a
// stop that arrived are separate findings and never report as one.
func TestEnsureSchemaBudgetAfterLock(t *testing.T) {
	// Deliberately neither of the two defaults, so a refusal that named a
	// constant instead of the budget this convergence ran under would fail.
	const budget = 97 * time.Second

	tests := []struct {
		name        string
		ctx         func(t *testing.T) context.Context
		wantErr     error
		wantErrText string
		wantLog     string
	}{
		{
			name:    "budget remaining",
			ctx:     func(t *testing.T) context.Context { return t.Context() },
			wantErr: nil,
		},
		{
			name: "deadline consumed by the wait",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
				t.Cleanup(cancel)
				return ctx
			},
			wantErr:     context.DeadlineExceeded,
			wantErrText: "which the lock wait consumed",
			wantLog:     "the wait consumed the whole budget",
		},
		{
			name: "convergence stopped while waiting",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx
			},
			wantErr:     context.Canceled,
			wantErrText: "after the convergence was stopped",
			wantLog:     "after the convergence was stopped",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

			err := ensureSchemaBudgetAfterLock(tt.ctx(t), logger, schema.DialectPostgres, "schemabot", budget)

			if tt.wantErr == nil {
				require.NoError(t, err)
				assert.Empty(t, logs.String(), "a convergence with budget left says nothing")
				return
			}
			require.ErrorIs(t, err, tt.wantErr)
			assert.Contains(t, err.Error(), tt.wantErrText)
			assert.Contains(t, err.Error(), ensureSchemaLockName)
			assert.Contains(t, logs.String(), tt.wantLog)
			assert.Contains(t, logs.String(), "database=schemabot")

			// A spent budget is the one finding that has a duration to report;
			// a stop names none, because nothing timed out.
			if errors.Is(tt.wantErr, context.DeadlineExceeded) {
				assert.Contains(t, err.Error(), budget.String())
			} else {
				assert.NotContains(t, err.Error(), budget.String())
			}
		})
	}
}
