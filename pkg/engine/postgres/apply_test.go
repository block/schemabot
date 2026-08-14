package postgres

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// TestClassifyRefusal pins the boundary between permanent refusals (retrying
// cannot succeed until the plan, target, or provisioning changes) and
// operational failures (retry may succeed). Lock-budget exhaustion sits on
// the operational side: the statement is native-safe and only lost a bounded
// race with concurrent lock holders.
func TestClassifyRefusal(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantReason string
	}{
		{
			name:       "privilege error is a refusal with provisioning detail",
			err:        fmt.Errorf("check privileges: %w", &preflight.PrivilegeError{Grant: "GRANT ALTER ON users TO app"}),
			wantReason: "insufficient-privileges",
		},
		{
			name:       "statement budget exhaustion is a refusal",
			err:        fmt.Errorf("execute: %w", &executor.BudgetError{Cause: executor.CauseStatement, Budget: time.Second}),
			wantReason: "not-native-safe-budget-exceeded",
		},
		{
			name: "lock budget exhaustion is operational",
			err:  fmt.Errorf("execute: %w", &executor.BudgetError{Cause: executor.CauseLock, Budget: time.Second}),
		},
		{
			name:       "oversized table is a refusal",
			err:        fmt.Errorf("preflight: %w", &preflight.SizeError{TotalBytes: 2, LimitBytes: 1}),
			wantReason: "table-too-large",
		},
		{
			name:       "missing table is a refusal",
			err:        fmt.Errorf("preflight: %w", preflight.ErrTableNotFound),
			wantReason: "table-not-found",
		},
		{
			name:       "non-table relation is a refusal",
			err:        fmt.Errorf("preflight: %w", preflight.ErrNotTable),
			wantReason: "not-a-table",
		},
		{
			name: "untyped error is operational",
			err:  errors.New("dial tcp: connection refused"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := classifyRefusal(tt.err, "users")
			if tt.wantReason == "" {
				assert.Nil(t, r)
				return
			}
			require.NotNil(t, r)
			assert.Equal(t, tt.wantReason, r.reason)
			assert.NotEmpty(t, r.detail)
		})
	}
}

// TestValidateOptimisticApplyRefusesNonNativeShape proves acceptance-time
// validation refuses statement shapes the native-safe path cannot execute,
// without touching the target database.
func TestValidateOptimisticApplyRefusesNonNativeShape(t *testing.T) {
	req := &engine.ApplyRequest{
		Database: "app",
		Changes: []engine.SchemaChange{{
			Namespace: "public",
			TableChanges: []engine.TableChange{{
				Table: "users", DDL: "CREATE TABLE public.users (id bigint PRIMARY KEY)",
			}},
		}},
		Credentials: &engine.Credentials{DSN: "postgres://localhost/app"},
	}

	_, err := validateOptimisticApply(req)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a shape the native-safe path executes")
}
