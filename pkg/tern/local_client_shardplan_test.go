package tern

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/statement"
)

// namedPlanEngine is a fake engine that implements Plan and Name, so the full
// LocalClient.Plan path (which renders the engine name into the response) can
// run without a registered built-in engine. Other methods are inherited from
// the embedded nil interface and must not be called.
type namedPlanEngine struct {
	engine.Engine
	name   string
	planFn func(context.Context, *engine.PlanRequest) (*engine.PlanResult, error)
}

func (e namedPlanEngine) Name() string { return e.name }

func (e namedPlanEngine) Plan(ctx context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
	return e.planFn(ctx, req)
}

func shardPlanTestClient(t *testing.T, store *fakePlanStore, result *engine.PlanResult) *LocalClient {
	t.Helper()
	return &LocalClient{
		config:            LocalConfig{Database: "commerce", Type: storage.DatabaseTypeVitess},
		storage:           &fakePlanStorage{plans: store},
		planetscaleEngine: namedPlanEngine{name: "planetscale", planFn: func(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) { return result, nil }},
		logger:            slog.Default(),
	}
}

func alterUsersEmail() []engine.TableChange {
	return []engine.TableChange{{
		Table:     "users",
		Operation: statement.StatementAlterTable,
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}}
}

// A sharded engine emits one SchemaChange per (namespace, shard). LocalClient.Plan
// records each shard's membership in NamespacePlanData.Shards (so apply-create can
// rebuild per-shard operation groups) while deduping the repeated table into the
// namespace-level stored tables.
func TestPlanRecordsPerShardSchemaChanges(t *testing.T) {
	store := &fakePlanStore{
		getFn:    func(string) (*storage.Plan, error) { return nil, nil },
		createID: 7,
	}
	result := &engine.PlanResult{
		PlanID: "plan_sharded",
		Changes: []engine.SchemaChange{
			{Namespace: "resolute", Shard: engine.Shard{Name: "-80"}, TableChanges: alterUsersEmail()},
			{Namespace: "resolute", Shard: engine.Shard{Name: "80-"}, TableChanges: alterUsersEmail()},
		},
	}
	c := shardPlanTestClient(t, store, result)

	_, err := c.Plan(t.Context(), &ternv1.PlanRequest{Database: "commerce"})
	require.NoError(t, err)

	require.NotNil(t, store.created, "a plan with changes must be persisted")
	ns := store.created.Namespaces["resolute"]
	require.NotNil(t, ns, "namespace plan data must exist for the changed keyspace")
	require.Len(t, ns.Shards, 2)
	assert.Equal(t, storage.ShardPlan{Shard: "-80", Namespace: "resolute", NeedsChange: true}, ns.Shards[0])
	assert.Equal(t, storage.ShardPlan{Shard: "80-", Namespace: "resolute", NeedsChange: true}, ns.Shards[1])
	require.Len(t, ns.Tables, 1, "the table repeated across shards is deduped at the namespace level")
	assert.Equal(t, "users", ns.Tables[0].Table)
}

// A non-sharded engine emits a SchemaChange with a zero Shard targeting the whole
// namespace; it contributes no shard rows.
func TestPlanNonShardedChangeHasNoShardRows(t *testing.T) {
	store := &fakePlanStore{
		getFn:    func(string) (*storage.Plan, error) { return nil, nil },
		createID: 8,
	}
	result := &engine.PlanResult{
		PlanID:  "plan_unsharded",
		Changes: []engine.SchemaChange{{Namespace: "resolute", TableChanges: alterUsersEmail()}},
	}
	c := shardPlanTestClient(t, store, result)

	_, err := c.Plan(t.Context(), &ternv1.PlanRequest{Database: "commerce"})
	require.NoError(t, err)

	require.NotNil(t, store.created)
	ns := store.created.Namespaces["resolute"]
	require.NotNil(t, ns)
	assert.Empty(t, ns.Shards)
	require.Len(t, ns.Tables, 1)
}

// Plan surfaces per-shard membership on the response (not just in stored plan
// data) so callers can display per-shard drift.
func TestPlanSurfacesShardPlanOnResponse(t *testing.T) {
	store := &fakePlanStore{
		getFn:    func(string) (*storage.Plan, error) { return nil, nil },
		createID: 7,
	}
	result := &engine.PlanResult{
		PlanID: "plan_sharded",
		Changes: []engine.SchemaChange{
			{Namespace: "resolute", Shard: engine.Shard{Name: "-80"}, TableChanges: alterUsersEmail()},
			{Namespace: "resolute", Shard: engine.Shard{Name: "80-"}, TableChanges: alterUsersEmail()},
		},
	}
	c := shardPlanTestClient(t, store, result)

	resp, err := c.Plan(t.Context(), &ternv1.PlanRequest{Database: "commerce"})
	require.NoError(t, err)
	require.Len(t, resp.Shards, 2)
	assert.Equal(t, "-80", resp.Shards[0].Shard)
	assert.Equal(t, "resolute", resp.Shards[0].Namespace)
	assert.True(t, resp.Shards[0].NeedsChange)
	assert.Equal(t, "80-", resp.Shards[1].Shard)
	// The repeated table collapses to a single namespace-level proto change.
	require.Len(t, resp.Changes, 1)
	require.Len(t, resp.Changes[0].TableChanges, 1)
}
