//go:build integration

package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// TestEnginePlanCreateTable proves the adapter derives an executable CREATE
// TABLE plan from desired state when the live PostgreSQL table is absent.
func TestEnginePlanCreateTable(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "plan_test")
	req := &engine.PlanRequest{
		Database: "plan_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text NOT NULL)",
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)
	assert.False(t, result.NoChanges)
	require.Len(t, result.Changes, 1)
	require.Len(t, result.Changes[0].TableChanges, 1)
	change := result.Changes[0].TableChanges[0]
	assert.Equal(t, "public", result.Changes[0].Namespace)
	assert.Equal(t, "users", change.Table)
	assert.Contains(t, change.DDL, "CREATE TABLE public.users")
	assert.Empty(t, change.ExecutionMode)
}
