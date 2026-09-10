package tern

import (
	"context"
	"log/slog"
	"testing"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// exemptTablesEngine plans a clean target whose only finding is the set of
// live tables the verdict exempted. Only Plan is exercised.
type exemptTablesEngine struct {
	engine.Engine
	exempt []*engine.ExemptTables
}

func (e exemptTablesEngine) Name() string { return "postgres" }

func (e exemptTablesEngine) Plan(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
	return &engine.PlanResult{
		PlanID:       "plan-clean",
		NoChanges:    true,
		ExemptTables: e.exempt,
	}, nil
}

// TestPlanWithNoChangesDisclosesExemptTables verifies a plan that proposes no
// DDL still carries the exempt-table disclosure to the control plane. A clean
// plan skips storage, and that is exactly the plan where a reviewer needs to
// see that an undeclared table was exempted rather than overlooked.
func TestPlanWithNoChangesDisclosesExemptTables(t *testing.T) {
	store := &fakePlanStore{createErr: assert.AnError}
	client := &LocalClient{
		config:  LocalConfig{Database: "app", Type: storage.DatabaseTypePostgres},
		storage: &fakePlanStorage{plans: store},
		logger:  slog.Default(),
		postgresEngine: exemptTablesEngine{exempt: []*engine.ExemptTables{{
			Namespace: "app",
			Tables:    []string{"orders_archive_2024"},
			Reason:    "archive naming",
		}}},
	}

	resp, err := client.Plan(t.Context(), &ternv1.PlanRequest{
		SchemaFiles: map[string]*ternv1.SchemaFiles{"app": {Files: map[string]string{"orders.sql": "CREATE TABLE orders (id bigint)"}}},
	})
	require.NoError(t, err)

	assert.Nil(t, store.created, "a plan with no changes is not stored")
	assert.Equal(t, "plan-clean", resp.PlanId)
	require.Len(t, resp.ExemptTables, 1)
	assert.Equal(t, "app", resp.ExemptTables[0].Namespace)
	assert.Equal(t, []string{"orders_archive_2024"}, resp.ExemptTables[0].Tables)
	assert.Equal(t, "archive naming", resp.ExemptTables[0].Reason)
}
