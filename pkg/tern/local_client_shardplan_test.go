package tern

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
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
		Operation: ddl.StatementAlterTable,
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
	// Each shard records its own changes (here identical) so a later divergence is
	// representable; the namespace-level Tables stay deduped.
	wantChange := storage.TableChange{
		Namespace: "resolute",
		Table:     "users",
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		Operation: "alter",
	}
	assert.Equal(t, storage.ShardPlan{Shard: "-80", Namespace: "resolute", Changes: []storage.TableChange{wantChange}}, ns.Shards[0])
	assert.Equal(t, storage.ShardPlan{Shard: "80-", Namespace: "resolute", Changes: []storage.TableChange{wantChange}}, ns.Shards[1])
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
	assert.Equal(t, "80-", resp.Shards[1].Shard)
	// Each shard surfaces its own changes so the control plane can rebuild the
	// fan-out (and present per-shard divergence) from the response.
	require.Len(t, resp.Shards[0].Changes, 1)
	assert.Equal(t, "users", resp.Shards[0].Changes[0].TableName)
	require.Len(t, resp.Shards[1].Changes, 1)
	assert.Equal(t, "users", resp.Shards[1].Changes[0].TableName)
	// The repeated table collapses to a single namespace-level proto change.
	require.Len(t, resp.Changes, 1)
	require.Len(t, resp.Changes[0].TableChanges, 1)
}

func TestPlanPreservesUnsafeMetadataInStoredPlanAndResponse(t *testing.T) {
	store := &fakePlanStore{
		getFn:    func(string) (*storage.Plan, error) { return nil, nil },
		createID: 9,
	}
	unsafeDrop := []engine.TableChange{{
		Table:        "users",
		Operation:    ddl.StatementDropTable,
		DDL:          "DROP TABLE `users`",
		IsUnsafe:     true,
		UnsafeReason: "DROP TABLE removes all data",
	}}
	result := &engine.PlanResult{
		PlanID: "plan_unsafe",
		Changes: []engine.SchemaChange{
			{Namespace: "resolute", Shard: engine.Shard{Name: "-80"}, TableChanges: unsafeDrop},
		},
	}
	c := shardPlanTestClient(t, store, result)

	resp, err := c.Plan(t.Context(), &ternv1.PlanRequest{Database: "commerce"})
	require.NoError(t, err)

	require.NotNil(t, store.created)
	require.Len(t, store.created.Namespaces["resolute"].Tables, 1)
	stored := store.created.Namespaces["resolute"].Tables[0]
	assert.True(t, stored.IsUnsafe)
	assert.Equal(t, "DROP TABLE removes all data", stored.UnsafeReason)
	require.Len(t, store.created.Shards, 1)
	require.Len(t, store.created.Shards[0].Changes, 1)
	assert.True(t, store.created.Shards[0].Changes[0].IsUnsafe)
	assert.Equal(t, "DROP TABLE removes all data", store.created.Shards[0].Changes[0].UnsafeReason)

	require.Len(t, resp.Changes, 1)
	require.Len(t, resp.Changes[0].TableChanges, 1)
	assert.True(t, resp.Changes[0].TableChanges[0].IsUnsafe)
	assert.Equal(t, "DROP TABLE removes all data", resp.Changes[0].TableChanges[0].UnsafeReason)
	require.Len(t, resp.Shards, 1)
	require.Len(t, resp.Shards[0].Changes, 1)
	assert.True(t, resp.Shards[0].Changes[0].IsUnsafe)
	assert.Equal(t, "DROP TABLE removes all data", resp.Shards[0].Changes[0].UnsafeReason)
}

func alterUsersEmailWithRows(rows int64) []engine.TableChange {
	tc := alterUsersEmail()
	tc[0].EstimatedRows = &rows
	bytes := rows * 100
	tc[0].EstimatedBytes = &bytes
	return tc
}

// A sharded plan's namespace view reports the whole table's size: per-shard
// row estimates are summed, the largest single shard is kept (the biggest
// chunk a shard-at-a-time apply works through at once), and the shard span
// is counted. The per-shard rows keep each shard's own estimate.
func TestPlanAggregatesShardTableSizes(t *testing.T) {
	store := &fakePlanStore{
		getFn:    func(string) (*storage.Plan, error) { return nil, nil },
		createID: 10,
	}
	result := &engine.PlanResult{
		PlanID: "plan_sized",
		Changes: []engine.SchemaChange{
			{Namespace: "resolute", Shard: engine.Shard{Name: "-80"}, TableChanges: alterUsersEmailWithRows(100)},
			{Namespace: "resolute", Shard: engine.Shard{Name: "80-"}, TableChanges: alterUsersEmailWithRows(250)},
		},
	}
	c := shardPlanTestClient(t, store, result)

	resp, err := c.Plan(t.Context(), &ternv1.PlanRequest{Database: "commerce"})
	require.NoError(t, err)

	require.NotNil(t, store.created)
	require.Len(t, store.created.Namespaces["resolute"].Tables, 1)
	stored := store.created.Namespaces["resolute"].Tables[0]
	require.NotNil(t, stored.EstimatedRows)
	assert.Equal(t, int64(350), *stored.EstimatedRows)
	assert.Equal(t, 2, stored.ShardCount)
	require.NotNil(t, stored.LargestShardRows)
	assert.Equal(t, int64(250), *stored.LargestShardRows)
	require.NotNil(t, stored.EstimatedBytes)
	assert.Equal(t, int64(35_000), *stored.EstimatedBytes)

	// Per-shard rows keep each shard's own estimate, unaggregated.
	shards := store.created.Namespaces["resolute"].Shards
	require.Len(t, shards, 2)
	require.NotNil(t, shards[0].Changes[0].EstimatedRows)
	assert.Equal(t, int64(100), *shards[0].Changes[0].EstimatedRows)
	assert.Zero(t, shards[0].Changes[0].ShardCount)

	// The proto response carries the same namespace-level aggregates.
	require.Len(t, resp.Changes, 1)
	require.Len(t, resp.Changes[0].TableChanges, 1)
	protoChange := resp.Changes[0].TableChanges[0]
	require.NotNil(t, protoChange.EstimatedRows)
	assert.Equal(t, int64(350), *protoChange.EstimatedRows)
	assert.Equal(t, int32(2), protoChange.ShardCount)
	require.NotNil(t, protoChange.LargestShardRows)
	assert.Equal(t, int64(250), *protoChange.LargestShardRows)
	require.NotNil(t, protoChange.EstimatedBytes)
	assert.Equal(t, int64(35_000), *protoChange.EstimatedBytes)
}

// When any shard lacks an estimate, the namespace row totals are omitted
// rather than understating the table's size — the shard count still reports
// the span.
func TestPlanShardTableSizesOmittedWhenAnyShardMissing(t *testing.T) {
	store := &fakePlanStore{
		getFn:    func(string) (*storage.Plan, error) { return nil, nil },
		createID: 11,
	}
	result := &engine.PlanResult{
		PlanID: "plan_partial_sizes",
		Changes: []engine.SchemaChange{
			{Namespace: "resolute", Shard: engine.Shard{Name: "-80"}, TableChanges: alterUsersEmailWithRows(100)},
			{Namespace: "resolute", Shard: engine.Shard{Name: "80-"}, TableChanges: alterUsersEmail()},
		},
	}
	c := shardPlanTestClient(t, store, result)

	resp, err := c.Plan(t.Context(), &ternv1.PlanRequest{Database: "commerce"})
	require.NoError(t, err)

	require.NotNil(t, store.created)
	stored := store.created.Namespaces["resolute"].Tables[0]
	assert.Nil(t, stored.EstimatedRows)
	assert.Nil(t, stored.LargestShardRows)
	assert.Equal(t, 2, stored.ShardCount)

	protoChange := resp.Changes[0].TableChanges[0]
	assert.Nil(t, protoChange.EstimatedRows)
	assert.Nil(t, protoChange.LargestShardRows)
	assert.Equal(t, int32(2), protoChange.ShardCount)
}

// An engine that aggregates a sharded target itself emits unsharded changes;
// its own size values pass through the namespace view untouched.
func TestPlanUnshardedTableSizesPassThrough(t *testing.T) {
	store := &fakePlanStore{
		getFn:    func(string) (*storage.Plan, error) { return nil, nil },
		createID: 12,
	}
	rows, largest := int64(48_200_000), int64(24_600_000)
	tc := alterUsersEmail()
	tc[0].EstimatedRows = &rows
	tc[0].ShardCount = 2
	tc[0].LargestShardRows = &largest
	result := &engine.PlanResult{
		PlanID:  "plan_self_aggregated",
		Changes: []engine.SchemaChange{{Namespace: "resolute", TableChanges: tc}},
	}
	c := shardPlanTestClient(t, store, result)

	resp, err := c.Plan(t.Context(), &ternv1.PlanRequest{Database: "commerce"})
	require.NoError(t, err)

	require.NotNil(t, store.created)
	stored := store.created.Namespaces["resolute"].Tables[0]
	require.NotNil(t, stored.EstimatedRows)
	assert.Equal(t, rows, *stored.EstimatedRows)
	assert.Equal(t, 2, stored.ShardCount)
	require.NotNil(t, stored.LargestShardRows)
	assert.Equal(t, largest, *stored.LargestShardRows)

	protoChange := resp.Changes[0].TableChanges[0]
	require.NotNil(t, protoChange.EstimatedRows)
	assert.Equal(t, rows, *protoChange.EstimatedRows)
	assert.Equal(t, int32(2), protoChange.ShardCount)
}
