package tern

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/statement"
)

// alterUsersEmailShardPlan is the recomputed plan for a sharded engine: the same
// reviewed ALTER on a single named shard, used by the shard-scoped drift tests.
func alterUsersEmailShardPlan(shard string) *engine.PlanResult {
	return &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			Namespace: "testapp",
			Shard:     engine.Shard{Name: shard},
			TableChanges: []engine.TableChange{{
				Table:     "users",
				Operation: statement.StatementAlterTable,
				DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			}},
		}},
	}
}

// A non-primary deployment whose recomputed local plan exactly matches the
// reviewed DDL materializes the plan: there is no drift to block.
func TestDriftGuard_MatchMaterializes(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 5}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

	got, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_ok",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(5), got.ID)
}

// Whitespace and quoting differences between the recomputed DDL and the reviewed
// DDL are normalized away by canonicalization, so they are not drift.
func TestDriftGuard_CanonicalizationTolerant(t *testing.T) {
	recomputed := &engine.PlanResult{Changes: []engine.SchemaChange{{
		Namespace: "testapp",
		TableChanges: []engine.TableChange{{
			Table:     "users",
			Operation: statement.StatementAlterTable,
			DDL:       "ALTER TABLE users ADD COLUMN email varchar(255)",
		}},
	}}}
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 6}
	c := newPlanMaterializeClientWithPlan(store, recomputed)

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_canon",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.NoError(t, err)
}

// A reviewed change this deployment would not plan (local schema already has the
// column) fails closed rather than replaying unreviewed DDL.
func TestDriftGuard_MissingReviewedChangeFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, &engine.PlanResult{}) // recomputes no changes

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_drift",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "drifted")
	assert.Contains(t, err.Error(), "re-plan against the current live schema", "the error must tell a blocked operator how to recover")
	assert.Nil(t, store.created, "must not materialize a drifted plan")
}

// A change this deployment would plan that was never reviewed (local schema is
// behind the desired files in a way the primary did not see) fails closed.
func TestDriftGuard_UnexpectedLocalChangeFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_extra",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "orders", Ddl: "CREATE TABLE `orders` (`id` bigint)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_CREATE, Namespace: "testapp"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "drifted")
}

// Different DDL for the same table/operation is drift even though the
// namespace/table/action triple matches.
func TestDriftGuard_DifferentDDLSameTableFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_diff_ddl",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `phone` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "drifted")
}

// A shard-scoped apply is drift-checked against this deployment's re-plan
// restricted to the dispatch's shard. When the reviewed DDL matches that shard's
// recomputed change, the plan materializes.
func TestDriftGuard_ShardScopedMatchMaterializes(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 8}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailShardPlan("-80"))

	got, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId:       "plan_shard_ok",
		TargetShards: []string{"-80"},
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(8), got.ID)
}

// A shard-scoped apply whose reviewed DDL targets one shard but whose live
// re-plan only needs the change on a different shard fails closed: the targeted
// shard has drifted from the reviewed plan.
func TestDriftGuard_ShardScopedWrongShardFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailShardPlan("80-"))

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId:       "plan_shard_drift",
		TargetShards: []string{"-80"},
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "drifted")
	assert.Nil(t, store.created, "must not materialize a drifted shard plan")
}

// More than one target shard is a malformed dispatch (the per-shard fan-out
// emits one shard per operation), so the guard fails closed.
func TestDriftGuard_MultipleTargetShardsFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailShardPlan("-80"))

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId:       "plan_shard_multi",
		TargetShards: []string{"-80", "80-"},
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "shard")
}

// A vschema change the reviewed plan carries but this deployment would not plan
// is drift, even when the table DDL matches exactly.
func TestDriftGuard_VSchemaParityFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	// Recomputed plan has the table change but no vschema change.
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_vschema",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
			{TableName: "VSchema: testapp", ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA, Namespace: "testapp"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "vschema")
}

// A matching vschema change on both sides is not drift.
func TestDriftGuard_VSchemaParityMatches(t *testing.T) {
	recomputed := &engine.PlanResult{Changes: []engine.SchemaChange{{
		Namespace: "testapp",
		Metadata:  map[string]string{"vschema_changed": "true"},
	}}}
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 9}
	c := newPlanMaterializeClientWithPlan(store, recomputed)

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_vschema_ok",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "VSchema: testapp", ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA, Namespace: "testapp"},
		},
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"testapp": {Files: map[string]string{vSchemaArtifactName: `{"sharded":true}`}},
		},
	})

	require.NoError(t, err)
}

// An engine failure during recompute surfaces as an error: the guard never
// fails open when it cannot recompute.
func TestDriftGuard_RecomputeErrorFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClient(store)
	c.config.TargetDSN = "user:pass@tcp(127.0.0.1:3306)/testapp"
	c.spiritEngine = fakePlanEngine{
		planFn: func(ctx context.Context, _ *engine.PlanRequest) (*engine.PlanResult, error) {
			return nil, errors.New("engine boom")
		},
	}

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_engine_err",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "recompute local plan")
}

// canonicalDDLForDrift must fail closed on DDL it cannot parse: ddl.Canonicalize
// returns its input unchanged on a parse failure, so without this guard an
// unparseable statement would silently compare by raw text and could mask drift.
func TestCanonicalDDLForDrift_FailsClosed(t *testing.T) {
	t.Run("unparseable DDL is rejected", func(t *testing.T) {
		_, err := canonicalDDLForDrift("this is not valid sql")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unparseable DDL")
	})

	t.Run("empty DDL is rejected", func(t *testing.T) {
		_, err := canonicalDDLForDrift("   ")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty DDL")
	})

	t.Run("multi-statement DDL is rejected", func(t *testing.T) {
		// ddl.Canonicalize only canonicalizes the first statement, so a
		// multi-statement payload would silently drop the trailing statements
		// and mask drift on them. It must fail closed instead.
		_, err := canonicalDDLForDrift("ALTER TABLE `users` ADD COLUMN `email` varchar(255); ALTER TABLE `users` ADD COLUMN `phone` varchar(255)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exactly one DDL statement")
	})

	t.Run("parseable DDL is canonicalized", func(t *testing.T) {
		// Whitespace and unquoted identifiers normalize to the same canonical form
		// regardless of incidental formatting, so equivalent DDL compares equal.
		spaced, err := canonicalDDLForDrift("ALTER TABLE   users   ADD COLUMN email varchar(255)")
		require.NoError(t, err)
		quoted, err := canonicalDDLForDrift("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		require.NoError(t, err)
		assert.Equal(t, quoted, spaced)
		assert.NotEmpty(t, spaced)
	})
}
