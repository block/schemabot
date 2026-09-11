package api

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
)

// Reaching the front of the advisory-lock queue is not the same as being able
// to use it. The wait and the work share one deadline, so a bootstrap can be
// granted the lock with nothing left to converge under it; that case names
// itself in the log and refuses, rather than starting DDL on a dead context and
// reporting whatever the driver says about it.
func TestEnsureSchemaBudgetAfterLock(t *testing.T) {
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
			wantLog:     "the wait consumed the whole bootstrap deadline",
		},
		{
			name: "bootstrap canceled while waiting",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx
			},
			wantErr:     context.Canceled,
			wantErrText: "after the bootstrap was canceled",
			wantLog:     "after the bootstrap was canceled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

			err := ensureSchemaBudgetAfterLock(tt.ctx(t), logger, schema.DialectPostgres, "schemabot")

			if tt.wantErr == nil {
				require.NoError(t, err)
				assert.Empty(t, logs.String(), "a bootstrap with budget left says nothing")
				return
			}
			require.ErrorIs(t, err, tt.wantErr)
			assert.Contains(t, err.Error(), tt.wantErrText)
			assert.Contains(t, err.Error(), ensureSchemaLockName)
			assert.Contains(t, logs.String(), tt.wantLog)
			assert.Contains(t, logs.String(), "database=schemabot")
		})
	}
}
