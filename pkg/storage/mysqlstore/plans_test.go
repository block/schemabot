//go:build integration

package mysqlstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

func TestPlanStore_RoundTripsShardPlans(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	planID, err := store.Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: "plan_shards",
		Database:       "commerce",
		DatabaseType:   storage.DatabaseTypeVitess,
		Deployment:     "primary",
		Target:         "commerce-target",
		Repository:     "org/repo",
		PullRequest:    123,
		SchemaPath:     "schema/commerce",
		Environment:    "staging",
		SchemaFiles: schema.SchemaFiles{
			"commerce": {Files: map[string]string{"users.sql": "CREATE TABLE `users` (`id` bigint unsigned NOT NULL)"}},
		},
		Namespaces: map[string]*storage.NamespacePlanData{
			"commerce": {
				Tables: []storage.TableChange{{
					Namespace: "commerce",
					Table:     "users",
					DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
					Operation: "alter",
				}},
			},
		},
		Shards: []storage.ShardPlan{
			{Namespace: "commerce", Shard: "80-", NeedsChange: false},
			{Namespace: "commerce", Shard: "-80", NeedsChange: true},
		},
		CreatedAt: time.Now(),
	})
	require.NoError(t, err)

	got, err := store.Plans().GetByID(ctx, planID)
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, []storage.ShardPlan{
		{Namespace: "commerce", Shard: "-80", NeedsChange: true},
		{Namespace: "commerce", Shard: "80-", NeedsChange: false},
	}, got.Shards)
	require.Contains(t, got.Namespaces, "commerce")
	assert.Equal(t, got.Shards, got.Namespaces["commerce"].Shards)
	assert.Equal(t, []storage.TableChange{{
		Namespace: "commerce",
		Table:     "users",
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		Operation: "alter",
	}}, got.Namespaces["commerce"].Tables)
}

func TestPlanStore_LoadsPlansWithoutShardPlans(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	_, err := store.Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: "plan_no_shards",
		Database:       "commerce",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "primary",
		Target:         "commerce-target",
		Repository:     "org/repo",
		PullRequest:    123,
		SchemaPath:     "schema/commerce",
		Environment:    "staging",
		Namespaces: map[string]*storage.NamespacePlanData{
			"commerce": {
				Tables: []storage.TableChange{{Namespace: "commerce", Table: "users", Operation: "alter"}},
			},
		},
		CreatedAt: time.Now(),
	})
	require.NoError(t, err)

	got, err := store.Plans().Get(ctx, "plan_no_shards")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Empty(t, got.Shards)
	require.Contains(t, got.Namespaces, "commerce")
	assert.Empty(t, got.Namespaces["commerce"].Shards)
}

func TestPlanStore_RoundTripsNilPlanDataAsNull(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	_, err := store.Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: "plan_nil_data",
		Database:       "commerce",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "primary",
		Target:         "commerce-target",
		Repository:     "org/repo",
		PullRequest:    123,
		SchemaPath:     "schema/commerce",
		Environment:    "staging",
		CreatedAt:      time.Now(),
	})
	require.NoError(t, err)

	var planData string
	err = testDB.QueryRowContext(ctx, "SELECT plan_data FROM plans WHERE plan_identifier = ?", "plan_nil_data").Scan(&planData)
	require.NoError(t, err)
	assert.Equal(t, "null", planData)
}
