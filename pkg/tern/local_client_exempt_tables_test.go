package tern

import (
	"context"
	"log/slog"
	"testing"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// exemptTablesEngine plans one namespace at a time, reporting the live tables
// the test recorded as exempt for the namespace it was asked about. A test that
// wants the plan to propose DDL sets changes; otherwise the target is clean.
// Only Plan is exercised.
type exemptTablesEngine struct {
	engine.Engine
	exempt  map[string][]*engine.ExemptTables
	changes []engine.SchemaChange
}

func (e exemptTablesEngine) Name() string { return "postgres" }

func (e exemptTablesEngine) Plan(_ context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
	return &engine.PlanResult{
		PlanID:       "plan-" + req.Database,
		NoChanges:    len(e.changes) == 0,
		Changes:      e.changes,
		ExemptTables: e.exempt[req.Database],
	}, nil
}

func archiveExempt(namespace string, tables ...string) *engine.ExemptTables {
	return &engine.ExemptTables{Namespace: namespace, Tables: tables, Reason: "archive naming"}
}

func exemptTablesClient(store *fakePlanStore, eng exemptTablesEngine) *LocalClient {
	return &LocalClient{
		config:         LocalConfig{Database: "app", Type: storage.DatabaseTypePostgres},
		storage:        &fakePlanStorage{plans: store},
		logger:         slog.Default(),
		postgresEngine: eng,
	}
}

func appSchemaFiles() map[string]*ternv1.SchemaFiles {
	return map[string]*ternv1.SchemaFiles{"app": {Files: map[string]string{"orders.sql": "CREATE TABLE orders (id bigint)"}}}
}

func assertExemptGroup(t *testing.T, got *ternv1.ExemptTables, namespace string, tables ...string) {
	t.Helper()
	assert.Equal(t, namespace, got.Namespace)
	assert.Equal(t, tables, got.Tables)
	assert.Equal(t, "archive naming", got.Reason)
}

// TestPlanWithNoChangesDisclosesExemptTables verifies a plan that proposes no
// DDL still carries the exempt-table disclosure to the control plane. A clean
// plan skips storage, and that is exactly the plan where a reviewer needs to
// see that an undeclared table was exempted rather than overlooked.
func TestPlanWithNoChangesDisclosesExemptTables(t *testing.T) {
	store := &fakePlanStore{createErr: assert.AnError}
	client := exemptTablesClient(store, exemptTablesEngine{
		exempt: map[string][]*engine.ExemptTables{"app": {archiveExempt("app", "orders_archive_2024")}},
	})

	resp, err := client.Plan(t.Context(), &ternv1.PlanRequest{SchemaFiles: appSchemaFiles()})
	require.NoError(t, err)

	assert.Nil(t, store.created, "a plan with no changes is not stored")
	assert.Equal(t, "plan-app", resp.PlanId)
	require.Len(t, resp.ExemptTables, 1)
	assertExemptGroup(t, resp.ExemptTables[0], "app", "orders_archive_2024")
}

// TestPlanWithChangesDisclosesExemptTables verifies the disclosure also rides
// the stored-plan response, next to the DDL it qualifies: a plan that both
// proposes a change and exempted a live table tells the reviewer about both.
func TestPlanWithChangesDisclosesExemptTables(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 7}
	client := exemptTablesClient(store, exemptTablesEngine{
		exempt:  map[string][]*engine.ExemptTables{"app": {archiveExempt("app", "orders_archive_2024", "events_archive_2025_01")}},
		changes: []engine.SchemaChange{{Namespace: "app", TableChanges: alterUsersEmail()}},
	})

	resp, err := client.Plan(t.Context(), &ternv1.PlanRequest{SchemaFiles: appSchemaFiles()})
	require.NoError(t, err)

	require.NotNil(t, store.created, "a plan with changes is stored")
	require.Len(t, resp.Changes, 1)
	require.Len(t, resp.ExemptTables, 1)
	assertExemptGroup(t, resp.ExemptTables[0], "app", "orders_archive_2024", "events_archive_2025_01")
}

// TestPlanMySQLNamespacesKeepsEveryNamespacesExemptTables verifies a
// multi-namespace plan carries each namespace's exempt tables, in namespace
// order, rather than only the first namespace's. Each namespace is planned on
// its own, so a group dropped here is a live table the reviewer is never told
// the verdict skipped.
func TestPlanMySQLNamespacesKeepsEveryNamespacesExemptTables(t *testing.T) {
	client := &LocalClient{config: LocalConfig{
		Database:  "commerce",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: "root:pass@tcp(127.0.0.1:3306)/commerce",
	}}
	eng := exemptTablesEngine{exempt: map[string][]*engine.ExemptTables{
		"orders":   {archiveExempt("orders", "line_items_archive_2024")},
		"payments": {archiveExempt("payments", "charges_archive_2023", "charges_archive_2024")},
	}}

	result, err := client.planMySQLNamespacesWithEngine(t.Context(), eng, &ternv1.PlanRequest{}, schema.SchemaFiles{
		"payments": {Files: map[string]string{"charges.sql": "CREATE TABLE `charges` (`id` int)"}},
		"orders":   {Files: map[string]string{"line_items.sql": "CREATE TABLE `line_items` (`id` int)"}},
	})
	require.NoError(t, err)

	require.Len(t, result.ExemptTables, 2, "every namespace that exempted a table is disclosed")
	assert.Equal(t, archiveExempt("orders", "line_items_archive_2024"), result.ExemptTables[0])
	assert.Equal(t, archiveExempt("payments", "charges_archive_2023", "charges_archive_2024"), result.ExemptTables[1])
}
