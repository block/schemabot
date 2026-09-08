package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/webhook/templates"
)

func TestPlannedDestructiveTables_CollectsFromBothViews(t *testing.T) {
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "keyspace",
			TableChanges: []*apitypes.TableChangeResponse{
				{TableName: "users", ChangeType: "alter"},
				{TableName: "orders", ChangeType: "drop"},
			},
		}},
		Shards: []*apitypes.ShardPlanResponse{{
			Namespace: "keyspace",
			Shard:     "-80",
			Changes: []*apitypes.TableChangeResponse{
				{TableName: "audit_log", ChangeType: "drop"},
				{TableName: "orders", ChangeType: "drop"},
			},
		}},
	}

	assert.Equal(t, []string{"audit_log", "orders"}, plannedDestructiveTables(planResp))
}

// An ALTER that destroys something a table keeps — a dropped column, a dropped
// index — comes out of the same window as a dropped table and is attributed the
// same way. The set matches what --allow-unsafe already gates.
func TestPlannedDestructiveTables_CollectsUnsafeAlters(t *testing.T) {
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "keyspace",
			TableChanges: []*apitypes.TableChangeResponse{
				{TableName: "orders", ChangeType: "alter", IsUnsafe: true, UnsafeReason: "DROP COLUMN discards the column's data"},
				{TableName: "customers", ChangeType: "alter", IsUnsafe: true, UnsafeReason: "DROP INDEX is not guarded"},
				{TableName: "users", ChangeType: "alter"},
			},
		}},
	}

	assert.Equal(t, []string{"customers", "orders"}, plannedDestructiveTables(planResp),
		"an additive alter stays out; a destructive one is attributed by its table")
}

// The unsafe gate reads only the namespace-level changes: a destructive change
// visible only on an individual shard is outside it, so applying never
// solicits --allow-unsafe consent for that table.
func TestUnsafeGateTables_OmitsShardOnlyDestruction(t *testing.T) {
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "keyspace",
			TableChanges: []*apitypes.TableChangeResponse{
				{TableName: "orders", ChangeType: "drop"},
				{TableName: "users", ChangeType: "alter"},
			},
		}},
		Shards: []*apitypes.ShardPlanResponse{{
			Namespace: "keyspace",
			Shard:     "-80",
			Changes: []*apitypes.TableChangeResponse{
				{TableName: "audit_log", ChangeType: "drop"},
				{TableName: "orders", ChangeType: "drop"},
			},
		}},
	}

	gated := unsafeGateTables(planResp)

	assert.Contains(t, gated, "orders", "a namespace-level drop passes through the opt-in gate")
	assert.NotContains(t, gated, "audit_log", "a shard-only drop never solicits consent")
	assert.NotContains(t, gated, "users", "an additive alter is not gated at all")
}

func TestPlannedDestructiveTables_NoDestructiveChanges(t *testing.T) {
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace:    "keyspace",
			TableChanges: []*apitypes.TableChangeResponse{{TableName: "users", ChangeType: "create"}},
		}},
	}

	assert.Empty(t, plannedDestructiveTables(planResp))
	assert.Empty(t, plannedDestructiveTables(nil))
}

func TestRenderPlanComment_AttributedChangeNamesOwnerAndStillOffersApply(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"DROP TABLE `reconcile_state`"},
		}},
		AttributedChanges: []templates.AttributedChangeData{{
			Table:       "reconcile_state",
			Repository:  "block/schemabot",
			PullRequest: 42,
		}},
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "🛑 **Check before applying**: 1 destructive change SchemaBot cannot attribute to this PR")
	assert.Contains(t, rendered, "[block/schemabot#42](https://github.com/block/schemabot/pull/42)")
	// Reconciling the live database to the declared schema stays the operator's
	// call, so the attribution informs the decision without removing it.
	assert.Contains(t, rendered, "▶️ **To apply**")
	assert.Contains(t, rendered, "schemabot apply -e staging")
}

func TestRenderPlanComment_UnresolvedDropOwnershipReadsAsUnresolved(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"DROP TABLE `reconcile_state`"},
		}},
		AttributedChanges: []templates.AttributedChangeData{{Table: "reconcile_state", Unresolved: true}},
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "ownership could not be established; see server logs")
	assert.Contains(t, rendered, "▶️ **To apply**")
}

// The attribution disclosure coaches re-planning, which is only actionable
// before the apply starts: the plan comment carries it for review, and the
// locked apply comment omits it — an apply already in flight has no re-plan
// move left, and the --allow-unsafe opt-in already solicited consent for the
// destruction.
func TestRenderPlanComment_LockedApplyCommentOmitsAttributedChanges(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"ALTER TABLE `drinks` DROP COLUMN `test`"},
		}},
		AttributedChanges: []templates.AttributedChangeData{{
			Table:       "drinks",
			Repository:  "block/schemabot",
			PullRequest: 42,
		}},
		IsLocked:     true,
		LockOwner:    "block/schemabot#7",
		LockAcquired: "2026-08-22 00:13:52 UTC",
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "🔒 **Lock acquired by**")
	assert.NotContains(t, rendered, "Check before applying")
	assert.NotContains(t, rendered, "block/schemabot#42")

	data.IsLocked = false
	data.LockOwner = ""
	data.LockAcquired = ""
	planRendered := templates.RenderPlanComment(data)

	assert.Contains(t, planRendered, "🛑 **Check before applying**")
	assert.Contains(t, planRendered, "[block/schemabot#42](https://github.com/block/schemabot/pull/42)")
}

// A destructive change confined to individual shards never passes through the
// --allow-unsafe opt-in gate, so consent for it was never solicited: the
// locked auto-apply comment keeps the disclosure — it is the operator's only
// notice of that destruction.
func TestRenderPlanComment_LockedApplyCommentKeepsUngatedAttributedChange(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"ALTER TABLE `drinks` DROP COLUMN `test`"},
		}},
		AttributedChanges: []templates.AttributedChangeData{{
			Table:             "drinks",
			Repository:        "block/schemabot",
			PullRequest:       42,
			OutsideUnsafeGate: true,
		}},
		IsLocked:     true,
		LockOwner:    "block/schemabot#7",
		LockAcquired: "2026-08-22 00:13:52 UTC",
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "🔒 **Lock acquired by**")
	assert.Contains(t, rendered, "🛑 **Check before applying**")
	assert.Contains(t, rendered, "[block/schemabot#42](https://github.com/block/schemabot/pull/42)")
}

// A locked comment downgraded to manual confirmation pauses for apply-confirm,
// so the operator still holds the decision the attribution informs: the
// disclosure renders alongside the confirmation it coaches.
func TestRenderPlanComment_ManualConfirmationKeepsAttributedChanges(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"ALTER TABLE `drinks` DROP COLUMN `test`"},
		}},
		AttributedChanges: []templates.AttributedChangeData{{
			Table:       "drinks",
			Repository:  "block/schemabot",
			PullRequest: 42,
		}},
		IsLocked:                   true,
		LockOwner:                  "block/schemabot#7",
		LockAcquired:               "2026-08-22 00:13:52 UTC",
		AutoConfirmDowngradeReason: "Could not verify plan — confirm manually",
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "⚠️ **Automatic apply paused**")
	assert.Contains(t, rendered, "🛑 **Check before applying**")
	assert.Contains(t, rendered, "[block/schemabot#42](https://github.com/block/schemabot/pull/42)")
	assert.Contains(t, rendered, "schemabot apply-confirm -e staging")
}

// The unsafe-blocked comment is where an operator is told how to override the
// block, so it carries the attribution alongside the override it coaches.
func TestRenderUnsafeChangesBlocked_CarriesAttributedChange(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"DROP TABLE `reconcile_state`"},
		}},
		UnsafeChanges: []templates.UnsafeChangeData{{
			Table:  "reconcile_state",
			Reason: "DROP TABLE removes all data",
		}},
		AttributedChanges: []templates.AttributedChangeData{{
			Table:       "reconcile_state",
			Repository:  "block/schemabot",
			PullRequest: 42,
		}},
	}

	rendered := templates.RenderUnsafeChangesBlocked(data)

	assert.Contains(t, rendered, "🛑 **Check before applying**")
	assert.Contains(t, rendered, "[block/schemabot#42](https://github.com/block/schemabot/pull/42)")
	assert.Contains(t, rendered, "--allow-unsafe")
}

func TestRenderMultiEnvPlanComment_AttributedChangeAnnotatesItsOwnEnvironmentOnly(t *testing.T) {
	staging := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"DROP TABLE `reconcile_state`"},
		}},
		AttributedChanges: []templates.AttributedChangeData{{
			Table:       "reconcile_state",
			Repository:  "block/schemabot",
			PullRequest: 42,
		}},
	}
	production := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "production",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
	}

	rendered := templates.RenderMultiEnvPlanComment(templates.MultiEnvPlanCommentData{
		Database:     "testdb",
		DatabaseType: "mysql",
		IsMySQL:      true,
		Environments: []string{"staging", "production"},
		Plans: map[string]*templates.PlanCommentData{
			"staging":    &staging,
			"production": &production,
		},
		Errors: map[string]string{},
	})

	assert.Contains(t, rendered, "🛑 **Check before applying**")
	// The sequence walks the environments in order and is offered in full: the
	// attribution belongs to staging's drop, not to production's unrelated add.
	assert.Contains(t, rendered, "▶️ **To apply**")
	assert.Contains(t, rendered, "schemabot apply -e staging")
}
