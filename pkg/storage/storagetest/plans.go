package storagetest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

// TestPlans runs the behavioral parity suite for storage.PlanStore.
//
// GetByLock is not exercised: it is unimplemented and returns
// storage.ErrNotImplemented. When an implementation lands, it joins this
// family.
func TestPlans(t *testing.T, h Harness) {
	t.Run("CanonicalizesIdentityKeys", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		plan := &storage.Plan{
			PlanIdentifier: "plan_mixed_case",
			Database:       "OrdersDB",
			DatabaseType:   "MySQL",
			Repository:     "MixedCase/Sample-Repo",
			PullRequest:    42,
			Environment:    "Staging",
			CreatedAt:      time.Now().UTC().Truncate(time.Second),
		}
		_, err := store.Plans().Create(ctx, plan)
		require.NoError(t, err)

		assert.Equal(t, "ordersdb", plan.Database)
		assert.Equal(t, "mysql", plan.DatabaseType)
		assert.Equal(t, "mixedcase/sample-repo", plan.Repository)
		assert.Equal(t, "staging", plan.Environment)

		byPR, err := store.Plans().GetByPR(ctx, "MIXEDCASE/SAMPLE-REPO", 42)
		require.NoError(t, err)
		require.Len(t, byPR, 1)
		assert.Equal(t, "plan_mixed_case", byPR[0].PlanIdentifier)

		listed, err := store.Plans().List(ctx, storage.ListPlansOptions{
			Database: "ORDERSDB", Environment: "STAGING",
			Repository: "MIXEDCASE/SAMPLE-REPO", PullRequest: 42, Limit: 10,
		})
		require.NoError(t, err)
		require.Len(t, listed, 1)
		assert.Equal(t, "plan_mixed_case", listed[0].PlanIdentifier)

		require.NoError(t, store.Plans().DeleteByPR(ctx, "MIXEDCASE/SAMPLE-REPO", 42))
		deleted, err := store.Plans().Get(ctx, "plan_mixed_case")
		require.NoError(t, err)
		assert.Nil(t, deleted)
	})

	t.Run("Create_And_Get", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// Missing plans are nil, not errors, for both lookup keys.
		got, err := store.Plans().Get(ctx, "plan_missing")
		require.NoError(t, err)
		require.Nil(t, got)
		got, err = store.Plans().GetByID(ctx, 99999)
		require.NoError(t, err)
		require.Nil(t, got)

		// Backdated so the round-trip tolerance below cannot be satisfied by
		// a store that discards the caller's time and stamps its own clock.
		created := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
		planID, err := store.Plans().Create(ctx, &storage.Plan{
			PlanIdentifier: "plan_round_trip",
			Database:       "commerce",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Deployment:     "primary",
			Target:         "commerce-target",
			Repository:     "org/repo",
			PullRequest:    123,
			SchemaPath:     "schema/commerce",
			Environment:    "staging",
			HeadSHA:        "sha_head",
			SchemaFiles: schema.SchemaFiles{
				"commerce": {Files: map[string]string{"users.sql": "CREATE TABLE `users` (`id` bigint unsigned NOT NULL)"}},
			},
			CreatedAt: created,
		})
		require.NoError(t, err)
		require.Positive(t, planID)

		assertStored := func(got *storage.Plan) {
			t.Helper()
			require.NotNil(t, got)
			assert.Equal(t, planID, got.ID)
			assert.Equal(t, "plan_round_trip", got.PlanIdentifier)
			assert.Equal(t, "commerce", got.Database)
			assert.Equal(t, storage.DatabaseTypeMySQL, got.DatabaseType)
			assert.Equal(t, "primary", got.Deployment)
			assert.Equal(t, "commerce-target", got.Target)
			assert.Equal(t, "org/repo", got.Repository)
			assert.Equal(t, 123, got.PullRequest)
			assert.Equal(t, "schema/commerce", got.SchemaPath)
			assert.Equal(t, "staging", got.Environment)
			assert.Equal(t, "sha_head", got.HeadSHA)
			require.Contains(t, got.SchemaFiles, "commerce")
			assert.Equal(t, "CREATE TABLE `users` (`id` bigint unsigned NOT NULL)",
				got.SchemaFiles["commerce"].Files["users.sql"])
			assert.WithinDuration(t, created, got.CreatedAt, time.Second,
				"the caller-provided creation time must round-trip, not be replaced by a store-side clock")
		}

		byIdentifier, err := store.Plans().Get(ctx, "plan_round_trip")
		require.NoError(t, err)
		assertStored(byIdentifier)

		byID, err := store.Plans().GetByID(ctx, planID)
		require.NoError(t, err)
		assertStored(byID)
	})

	t.Run("Create_DuplicateIdentifier", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		plan := storage.Plan{
			PlanIdentifier: "plan_dup",
			Database:       "commerce",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Repository:     "org/repo",
			PullRequest:    123,
			Environment:    "staging",
			CreatedAt:      time.Now().UTC().Truncate(time.Second),
		}
		_, err := store.Plans().Create(ctx, &plan)
		require.NoError(t, err)

		second := plan
		_, err = store.Plans().Create(ctx, &second)
		require.ErrorIs(t, err, storage.ErrPlanIDExists)
	})

	t.Run("RoundTripsShardPlans", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		change := storage.TableChange{
			Namespace:    "commerce",
			Table:        "users",
			DDL:          "ALTER TABLE `users` DROP COLUMN `email`",
			Operation:    "alter",
			IsUnsafe:     true,
			UnsafeReason: "DROP COLUMN removes data",
		}
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
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce": {Tables: []storage.TableChange{change}},
			},
			Shards: []storage.ShardPlan{
				{Namespace: "commerce", Shard: "80-"},
				// -80 carries its own per-shard DDL; the round-trip must preserve it.
				{Namespace: "commerce", Shard: "-80", Changes: []storage.TableChange{change}},
			},
			CreatedAt: time.Now().UTC().Truncate(time.Second),
		})
		require.NoError(t, err)

		got, err := store.Plans().GetByID(ctx, planID)
		require.NoError(t, err)
		require.NotNil(t, got)

		assert.Equal(t, []storage.ShardPlan{
			{Namespace: "commerce", Shard: "-80", Changes: []storage.TableChange{change}},
			{Namespace: "commerce", Shard: "80-"},
		}, got.Shards, "per-shard Changes must round-trip through plan data")
		require.Contains(t, got.Namespaces, "commerce")
		assert.Equal(t, got.Shards, got.Namespaces["commerce"].Shards)
		assert.Equal(t, []storage.TableChange{change}, got.Namespaces["commerce"].Tables)
	})

	t.Run("LoadsPlansWithoutShardPlans", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// A plan with namespace data but no shard plans reloads with the
		// shard lists absent rather than fabricated.
		_, err := store.Plans().Create(ctx, &storage.Plan{
			PlanIdentifier: "plan_no_shards",
			Database:       "commerce",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Repository:     "org/repo",
			PullRequest:    123,
			Environment:    "staging",
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce": {
					Tables: []storage.TableChange{{Namespace: "commerce", Table: "users", Operation: "alter"}},
				},
			},
			CreatedAt: time.Now().UTC().Truncate(time.Second),
		})
		require.NoError(t, err)

		got, err := store.Plans().Get(ctx, "plan_no_shards")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Nil(t, got.Shards)
		require.Contains(t, got.Namespaces, "commerce")
		assert.Nil(t, got.Namespaces["commerce"].Shards)
	})

	t.Run("RoundTripsVSchemaMetadata", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// A namespace's gate-facing VSchema change-metadata must survive the
		// round-trip alongside its artifact: the apply-time unsafe gate reads
		// the recorded deletions and mutations from the reloaded plan, so a
		// stored VSchema removal keeps requiring opt-in instead of failing
		// closed as an ambiguous record.
		metadata := map[string]string{
			storage.PlanMetadataVSchemaChanged:   "true",
			storage.PlanMetadataVSchemaDeletions: `[{"kind":"vindex","name":"email_idx","reason":"removing vindex email_idx changes query routing"}]`,
			storage.PlanMetadataVSchemaMutations: `[{"kind":"vindex_type","name":"user_idx","reason":"changing vindex user_idx type re-computes keyspace ids"}]`,
		}
		_, err := store.Plans().Create(ctx, &storage.Plan{
			PlanIdentifier: "plan_vschema_meta",
			Database:       "commerce",
			DatabaseType:   storage.DatabaseTypeVitess,
			Repository:     "org/repo",
			PullRequest:    123,
			Environment:    "staging",
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce": {
					Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`},
					Metadata:  metadata,
				},
			},
			CreatedAt: time.Now().UTC().Truncate(time.Second),
		})
		require.NoError(t, err)

		got, err := store.Plans().Get(ctx, "plan_vschema_meta")
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Contains(t, got.Namespaces, "commerce")
		assert.Equal(t, metadata, got.Namespaces["commerce"].Metadata)

		changes := got.UnsafeVSchemaChanges()
		require.Len(t, changes, 2)
		assert.Equal(t, "removing vindex email_idx changes query routing", changes[0].Reason)
		assert.Equal(t, "changing vindex user_idx type re-computes keyspace ids", changes[1].Reason)
	})

	t.Run("EmptyPlanDataRoundTripsAsAbsent", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// A plan without namespace data or shard plans reloads with both
		// absent, not as empty containers that later consumers could misread
		// as a recorded no-change verdict.
		_, err := store.Plans().Create(ctx, &storage.Plan{
			PlanIdentifier: "plan_no_data",
			Database:       "commerce",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Repository:     "org/repo",
			PullRequest:    123,
			Environment:    "staging",
			CreatedAt:      time.Now().UTC().Truncate(time.Second),
		})
		require.NoError(t, err)

		got, err := store.Plans().Get(ctx, "plan_no_data")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Nil(t, got.Namespaces)
		assert.Nil(t, got.Shards)
	})

	t.Run("GetByPR_NewestFirstWithIDTiebreak", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		base := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
		createPlanAt(t, store, "plan_pr_old", "orders", "org/repo", 7, base)
		createPlanAt(t, store, "plan_pr_tie_a", "orders", "org/repo", 7, base.Add(time.Second))
		createPlanAt(t, store, "plan_pr_tie_b", "orders", "org/repo", 7, base.Add(time.Second))
		createPlanAt(t, store, "plan_other_pr", "orders", "org/repo", 8, base.Add(2*time.Second))
		createPlanAt(t, store, "plan_other_repo", "orders", "org/other", 7, base.Add(2*time.Second))

		plans, err := store.Plans().GetByPR(ctx, "org/repo", 7)
		require.NoError(t, err)
		assert.Equal(t, []string{"plan_pr_tie_b", "plan_pr_tie_a", "plan_pr_old"}, planIdentifiers(plans),
			"same-second plans must order by id descending")

		empty, err := store.Plans().GetByPR(ctx, "org/repo", 999)
		require.NoError(t, err)
		assert.Empty(t, empty)
	})

	t.Run("List_FiltersAndOrdersNewestFirst", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// Plans across databases, environments, and sources (PR-generated and
		// ad-hoc): List must apply each filter independently and return plans
		// newest first with a deterministic id tiebreak on created_at ties.
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
		// Near-miss plans for the PR filter: the same PR number in another
		// repository, and another PR in the same repository. Only the
		// unfiltered listing may include them.
		create("plan_other_repo_same_pr", "billing", "production", "org/other", 7, base)
		create("plan_same_repo_other_pr", "billing", "production", "org/repo", 8, base)

		all, err := store.Plans().List(ctx, storage.ListPlansOptions{Limit: 10})
		require.NoError(t, err)
		assert.Equal(t, []string{"plan_orders_same_second", "plan_billing_pr", "plan_orders_adhoc", "plan_same_repo_other_pr", "plan_other_repo_same_pr", "plan_orders_pr_old"}, planIdentifiers(all),
			"same-second plans must order by id descending")
		for _, plan := range all {
			assert.Nil(t, plan.SchemaFiles, "List must not hydrate SchemaFiles for %s", plan.PlanIdentifier)
		}

		byDatabase, err := store.Plans().List(ctx, storage.ListPlansOptions{Database: "orders", Limit: 10})
		require.NoError(t, err)
		assert.Equal(t, []string{"plan_orders_same_second", "plan_orders_adhoc", "plan_orders_pr_old"}, planIdentifiers(byDatabase))

		byEnvironment, err := store.Plans().List(ctx, storage.ListPlansOptions{Environment: "staging", Limit: 10})
		require.NoError(t, err)
		assert.Equal(t, []string{"plan_orders_same_second", "plan_billing_pr", "plan_orders_pr_old"}, planIdentifiers(byEnvironment))

		byPR, err := store.Plans().List(ctx, storage.ListPlansOptions{Repository: "org/repo", PullRequest: 7, Limit: 10})
		require.NoError(t, err)
		assert.Equal(t, []string{"plan_billing_pr", "plan_orders_pr_old"}, planIdentifiers(byPR))

		since, err := store.Plans().List(ctx, storage.ListPlansOptions{Since: base.Add(2 * time.Second), Limit: 10})
		require.NoError(t, err)
		assert.Equal(t, []string{"plan_orders_same_second", "plan_billing_pr"}, planIdentifiers(since))

		limited, err := store.Plans().List(ctx, storage.ListPlansOptions{Limit: 2})
		require.NoError(t, err)
		assert.Equal(t, []string{"plan_orders_same_second", "plan_billing_pr"}, planIdentifiers(limited))

		unmatched, err := store.Plans().List(ctx, storage.ListPlansOptions{Database: "payments", Limit: 10})
		require.NoError(t, err)
		assert.Empty(t, unmatched, "a filter matching nothing lists as empty, not an error")
	})

	t.Run("List_RejectsNonPositiveLimit", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// An unbounded listing is refused rather than silently returning
		// nothing or everything.
		_, err := store.Plans().List(ctx, storage.ListPlansOptions{})
		require.ErrorContains(t, err, "limit must be positive")
	})

	t.Run("List_RejectsPullRequestWithoutRepository", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// A PR filter without a repository is refused rather than silently
		// matching plans with the same PR number across every repository.
		_, err := store.Plans().List(ctx, storage.ListPlansOptions{PullRequest: 42, Limit: 10})
		require.ErrorContains(t, err, "repository filter is required")
	})

	t.Run("Delete", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		planID := createPlanAt(t, store, "plan_delete", "orders", "org/repo", 7,
			time.Now().UTC().Truncate(time.Second))

		require.NoError(t, store.Plans().Delete(ctx, planID))

		got, err := store.Plans().Get(ctx, "plan_delete")
		require.NoError(t, err)
		assert.Nil(t, got)

		require.ErrorIs(t, store.Plans().Delete(ctx, planID), storage.ErrPlanNotFound)
	})

	t.Run("DeleteByPR", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		base := time.Now().UTC().Truncate(time.Second)
		createPlanAt(t, store, "plan_pr42_a", "orders", "org/repo", 42, base)
		createPlanAt(t, store, "plan_pr42_b", "billing", "org/repo", 42, base)
		createPlanAt(t, store, "plan_pr7", "orders", "org/repo", 7, base)
		createPlanAt(t, store, "plan_other_repo_pr42", "orders", "org/other", 42, base)

		require.NoError(t, store.Plans().DeleteByPR(ctx, "org/repo", 42))

		// Only the named repository's PR is deleted; the same PR number in
		// another repository and other PRs in the same repository survive.
		remaining, err := store.Plans().List(ctx, storage.ListPlansOptions{Limit: 10})
		require.NoError(t, err)
		assert.Equal(t, []string{"plan_other_repo_pr42", "plan_pr7"}, planIdentifiers(remaining))

		// Deleting a PR with no plans is a no-op, not an error.
		require.NoError(t, store.Plans().DeleteByPR(ctx, "org/repo", 999))
	})

	t.Run("Create_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Plans().Create(t.Context(), &storage.Plan{
			PlanIdentifier: "plan_err",
			Database:       "orders",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Environment:    "staging",
			CreatedAt:      time.Now().UTC().Truncate(time.Second),
		})
		require.Error(t, err)
		require.NotErrorIs(t, err, storage.ErrPlanIDExists,
			"an unreachable database must surface as a driver error, not a duplicate-identifier verdict")
	})

	t.Run("Get_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Plans().Get(t.Context(), "plan_err")
		require.Error(t, err)
	})

	t.Run("GetByID_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Plans().GetByID(t.Context(), 1)
		require.Error(t, err)
	})

	t.Run("GetByPR_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Plans().GetByPR(t.Context(), "org/repo", 1)
		require.Error(t, err)
	})

	t.Run("List_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Plans().List(t.Context(), storage.ListPlansOptions{Limit: 10})
		require.Error(t, err)
	})

	t.Run("Delete_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		err := store.Plans().Delete(t.Context(), 1)
		require.Error(t, err)
		require.NotErrorIs(t, err, storage.ErrPlanNotFound,
			"an unreachable database must surface as a driver error, not a missing-plan verdict")
	})

	t.Run("DeleteByPR_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.Plans().DeleteByPR(t.Context(), "org/repo", 1))
	})
}

// createPlanAt stores a minimal plan with an explicit creation time and
// returns its ID. Times are whole-second so ordering assertions hold on every
// dialect's timestamp precision.
func createPlanAt(t *testing.T, store storage.Storage, identifier, database, repo string, pr int, createdAt time.Time) int64 {
	t.Helper()
	id, err := store.Plans().Create(t.Context(), &storage.Plan{
		PlanIdentifier: identifier,
		Database:       database,
		DatabaseType:   storage.DatabaseTypeMySQL,
		Repository:     repo,
		PullRequest:    pr,
		Environment:    "staging",
		CreatedAt:      createdAt,
	})
	require.NoError(t, err)
	return id
}

// planIdentifiers projects plans onto their external identifiers for
// order-sensitive assertions.
func planIdentifiers(plans []*storage.Plan) []string {
	out := make([]string, 0, len(plans))
	for _, plan := range plans {
		out = append(out, plan.PlanIdentifier)
	}
	return out
}
