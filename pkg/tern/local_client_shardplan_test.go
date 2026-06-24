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

// An instance-local sharded engine emits per-shard fanout metadata in
// engine.SchemaChange.Shards. LocalClient.Plan must carry that into the stored
// plan's NamespacePlanData.Shards, mirroring the gRPC plan path's
// protoShardPlansToStorage, so apply-create can rebuild per-shard operation
// groups after the plan request has returned.
func TestPlanCarriesEngineShardPlanIntoStorage(t *testing.T) {
	store := &fakePlanStore{
		getFn:    func(string) (*storage.Plan, error) { return nil, nil },
		createID: 7,
	}
	result := &engine.PlanResult{
		PlanID: "plan_sharded",
		Changes: []engine.SchemaChange{{
			Namespace: "resolute",
			TableChanges: []engine.TableChange{{
				Table:     "users",
				Operation: statement.StatementAlterTable,
				DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			}},
			Shards: []engine.ShardPlan{
				{Shard: "-80", Namespace: "resolute", NeedsChange: true},
				{Shard: "80-", Namespace: "resolute", NeedsChange: false},
			},
		}},
	}
	c := &LocalClient{
		config:            LocalConfig{Database: "commerce", Type: storage.DatabaseTypeVitess},
		storage:           &fakePlanStorage{plans: store},
		planetscaleEngine: namedPlanEngine{name: "planetscale", planFn: func(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) { return result, nil }},
		logger:            slog.Default(),
	}

	_, err := c.Plan(t.Context(), &ternv1.PlanRequest{Database: "commerce"})
	require.NoError(t, err)

	require.NotNil(t, store.created, "a plan with changes must be persisted")
	ns := store.created.Namespaces["resolute"]
	require.NotNil(t, ns, "namespace plan data must exist for the changed keyspace")
	require.Len(t, ns.Shards, 2)
	assert.Equal(t, storage.ShardPlan{Shard: "-80", Namespace: "resolute", NeedsChange: true}, ns.Shards[0])
	assert.Equal(t, storage.ShardPlan{Shard: "80-", Namespace: "resolute", NeedsChange: false}, ns.Shards[1])
}

// A change with an empty shard namespace inherits the resolved plan namespace so
// apply-create groups the shard under the same namespace as its table changes.
// (The gRPC path defaults an empty namespace to "default"; the in-process path
// uses the plan namespace because that is what the fanout matches against.)
func TestPlanShardPlanDefaultsNamespaceToPlanNamespace(t *testing.T) {
	store := &fakePlanStore{
		getFn:    func(string) (*storage.Plan, error) { return nil, nil },
		createID: 8,
	}
	result := &engine.PlanResult{
		PlanID: "plan_default_ns",
		Changes: []engine.SchemaChange{{
			Namespace: "resolute",
			TableChanges: []engine.TableChange{{
				Table:     "users",
				Operation: statement.StatementAlterTable,
				DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			}},
			Shards: []engine.ShardPlan{{Shard: "-80", NeedsChange: true}},
		}},
	}
	c := &LocalClient{
		config:            LocalConfig{Database: "commerce", Type: storage.DatabaseTypeVitess},
		storage:           &fakePlanStorage{plans: store},
		planetscaleEngine: namedPlanEngine{name: "planetscale", planFn: func(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) { return result, nil }},
		logger:            slog.Default(),
	}

	_, err := c.Plan(t.Context(), &ternv1.PlanRequest{Database: "commerce"})
	require.NoError(t, err)

	require.NotNil(t, store.created)
	ns := store.created.Namespaces["resolute"]
	require.NotNil(t, ns)
	require.Len(t, ns.Shards, 1)
	assert.Equal(t, "resolute", ns.Shards[0].Namespace)
}

// An empty (or whitespace) shard name would produce an invalid sharded operation
// key, so Plan fails closed rather than persisting it — matching the gRPC path's
// protoShardPlansToStorage validation.
func TestPlanRejectsEmptyShardName(t *testing.T) {
	store := &fakePlanStore{
		getFn:    func(string) (*storage.Plan, error) { return nil, nil },
		createID: 9,
	}
	result := &engine.PlanResult{
		PlanID: "plan_bad_shard",
		Changes: []engine.SchemaChange{{
			Namespace: "resolute",
			TableChanges: []engine.TableChange{{
				Table:     "users",
				Operation: statement.StatementAlterTable,
				DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			}},
			Shards: []engine.ShardPlan{{Shard: "  ", Namespace: "resolute", NeedsChange: true}},
		}},
	}
	c := &LocalClient{
		config:            LocalConfig{Database: "commerce", Type: storage.DatabaseTypeVitess},
		storage:           &fakePlanStorage{plans: store},
		planetscaleEngine: namedPlanEngine{name: "planetscale", planFn: func(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) { return result, nil }},
		logger:            slog.Default(),
	}

	_, err := c.Plan(t.Context(), &ternv1.PlanRequest{Database: "commerce"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty shard")
	assert.Nil(t, store.created, "must not persist a plan with an invalid shard")
}

// Plan surfaces per-shard membership on the response (not just in stored plan
// data) so callers can display per-shard drift, matching the gRPC path.
func TestPlanSurfacesShardPlanOnResponse(t *testing.T) {
	store := &fakePlanStore{
		getFn:    func(string) (*storage.Plan, error) { return nil, nil },
		createID: 7,
	}
	result := &engine.PlanResult{
		PlanID: "plan_sharded",
		Changes: []engine.SchemaChange{{
			Namespace: "resolute",
			TableChanges: []engine.TableChange{{
				Table:     "users",
				Operation: statement.StatementAlterTable,
				DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			}},
			Shards: []engine.ShardPlan{
				{Shard: "-80", Namespace: "resolute", NeedsChange: true},
				{Shard: "80-", Namespace: "resolute", NeedsChange: false},
			},
		}},
	}
	c := &LocalClient{
		config:            LocalConfig{Database: "commerce", Type: storage.DatabaseTypeVitess},
		storage:           &fakePlanStorage{plans: store},
		planetscaleEngine: namedPlanEngine{name: "planetscale", planFn: func(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) { return result, nil }},
		logger:            slog.Default(),
	}

	resp, err := c.Plan(t.Context(), &ternv1.PlanRequest{Database: "commerce"})
	require.NoError(t, err)
	require.Len(t, resp.Shards, 2)
	assert.Equal(t, "-80", resp.Shards[0].Shard)
	assert.Equal(t, "resolute", resp.Shards[0].Namespace)
	assert.True(t, resp.Shards[0].NeedsChange)
	assert.Equal(t, "80-", resp.Shards[1].Shard)
	assert.False(t, resp.Shards[1].NeedsChange)
}
