//go:build integration

package sqlstore

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
	store := NewMySQL(testDB)

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
					Namespace:    "commerce",
					Table:        "users",
					DDL:          "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
					Operation:    "alter",
					IsUnsafe:     true,
					UnsafeReason: "DROP COLUMN removes data",
				}},
			},
		},
		Shards: []storage.ShardPlan{
			{Namespace: "commerce", Shard: "80-"},
			// -80 carries its own per-shard DDL; the round-trip must preserve it.
			{Namespace: "commerce", Shard: "-80", Changes: []storage.TableChange{{
				Namespace:    "commerce",
				Table:        "users",
				DDL:          "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
				Operation:    "alter",
				IsUnsafe:     true,
				UnsafeReason: "DROP COLUMN removes data",
			}}},
		},
		CreatedAt: time.Now(),
	})
	require.NoError(t, err)

	got, err := store.Plans().GetByID(ctx, planID)
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, []storage.ShardPlan{
		{Namespace: "commerce", Shard: "-80", Changes: []storage.TableChange{{
			Namespace:    "commerce",
			Table:        "users",
			DDL:          "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			Operation:    "alter",
			IsUnsafe:     true,
			UnsafeReason: "DROP COLUMN removes data",
		}}},
		{Namespace: "commerce", Shard: "80-"},
	}, got.Shards, "per-shard Changes must round-trip through plan_data JSON")
	require.Contains(t, got.Namespaces, "commerce")
	assert.Equal(t, got.Shards, got.Namespaces["commerce"].Shards)
	assert.Equal(t, []storage.TableChange{{
		Namespace:    "commerce",
		Table:        "users",
		DDL:          "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		Operation:    "alter",
		IsUnsafe:     true,
		UnsafeReason: "DROP COLUMN removes data",
	}}, got.Namespaces["commerce"].Tables)
}

func TestPlanStore_LoadsPlansWithoutShardPlans(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

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
	store := NewMySQL(testDB)

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

// TestPlanStore_ListFiltersAndOrdersNewestFirst seeds plans across databases,
// environments, and sources (PR-generated and ad-hoc) and verifies that List
// applies each filter independently and returns plans newest first, with a
// deterministic id tiebreak when two plans share a created_at second.
func TestPlanStore_ListFiltersAndOrdersNewestFirst(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	base := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	create := func(identifier, database, environment, repo string, pr int, createdAt time.Time) {
		t.Helper()
		_, err := store.Plans().Create(ctx, &storage.Plan{
			PlanIdentifier: identifier,
			Database:       database,
			DatabaseType:   storage.DatabaseTypeMySQL,
			Environment:    environment,
			Repository:     repo,
			PullRequest:    pr,
			SchemaFiles: schema.SchemaFiles{
				database: {Files: map[string]string{"users.sql": "CREATE TABLE `users` (`id` bigint unsigned NOT NULL)"}},
			},
			CreatedAt: createdAt,
		})
		require.NoError(t, err)
	}

	create("plan_orders_pr_old", "orders", "staging", "org/repo", 7, base)
	create("plan_orders_adhoc", "orders", "production", "", 0, base.Add(time.Second))
	create("plan_billing_pr", "billing", "staging", "org/repo", 7, base.Add(2*time.Second))
	create("plan_orders_same_second", "orders", "staging", "", 0, base.Add(2*time.Second))

	identifiers := func(plans []*storage.Plan) []string {
		out := make([]string, 0, len(plans))
		for _, plan := range plans {
			out = append(out, plan.PlanIdentifier)
		}
		return out
	}

	all, err := store.Plans().List(ctx, storage.ListPlansOptions{Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, []string{"plan_orders_same_second", "plan_billing_pr", "plan_orders_adhoc", "plan_orders_pr_old"}, identifiers(all),
		"same-second plans must order by id descending")
	for _, plan := range all {
		assert.Nil(t, plan.SchemaFiles, "List must not hydrate SchemaFiles for %s", plan.PlanIdentifier)
	}

	byDatabase, err := store.Plans().List(ctx, storage.ListPlansOptions{Database: "orders", Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, []string{"plan_orders_same_second", "plan_orders_adhoc", "plan_orders_pr_old"}, identifiers(byDatabase))

	byEnvironment, err := store.Plans().List(ctx, storage.ListPlansOptions{Environment: "staging", Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, []string{"plan_orders_same_second", "plan_billing_pr", "plan_orders_pr_old"}, identifiers(byEnvironment))

	byPR, err := store.Plans().List(ctx, storage.ListPlansOptions{Repository: "org/repo", PullRequest: 7, Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, []string{"plan_billing_pr", "plan_orders_pr_old"}, identifiers(byPR))

	since, err := store.Plans().List(ctx, storage.ListPlansOptions{Since: base.Add(2 * time.Second), Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, []string{"plan_orders_same_second", "plan_billing_pr"}, identifiers(since))

	limited, err := store.Plans().List(ctx, storage.ListPlansOptions{Limit: 2})
	require.NoError(t, err)
	assert.Equal(t, []string{"plan_orders_same_second", "plan_billing_pr"}, identifiers(limited))
}

// TestPlanStore_ListRejectsNonPositiveLimit verifies that an unbounded listing
// is refused rather than silently returning nothing or everything.
func TestPlanStore_ListRejectsNonPositiveLimit(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	_, err := store.Plans().List(ctx, storage.ListPlansOptions{})
	require.ErrorContains(t, err, "limit must be positive")
}

// TestPlanStore_ListRejectsPullRequestWithoutRepository verifies that a PR
// filter without a repository is refused rather than silently matching plans
// with the same PR number across every repository.
func TestPlanStore_ListRejectsPullRequestWithoutRepository(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	_, err := store.Plans().List(ctx, storage.ListPlansOptions{PullRequest: 42, Limit: 10})
	require.ErrorContains(t, err, "repository filter is required")
}
