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

// Every attributed table is one the --allow-unsafe gate solicits consent for,
// a drop confined to one divergent shard included, so no attributed
// destruction reaches an automatic apply unconsented. The gate also covers a
// created table's lint error, which destroys nothing and is not attributed.
func TestUnsafeGateCoversEveryAttributedTable(t *testing.T) {
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "keyspace",
			TableChanges: []*apitypes.TableChangeResponse{
				{TableName: "orders", ChangeType: "drop"},
				{TableName: "users", ChangeType: "alter"},
				{TableName: "stations", ChangeType: "create", IsUnsafe: true, UnsafeReason: "column `opened_at` is TIMESTAMP, which overflows in 2038"},
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

	gated := make([]string, 0)
	for _, unsafe := range planResp.UnsafeChanges() {
		gated = append(gated, unsafe.Table)
	}

	assert.Subset(t, gated, plannedDestructiveTables(planResp), "every attributed table is gated")
	assert.ElementsMatch(t, []string{"audit_log", "orders", "stations"}, gated,
		"a shard-only drop solicits consent, an additive alter is not gated at all")
	assert.Equal(t, []string{"audit_log", "orders"}, plannedDestructiveTables(planResp),
		"the created table's lint error is gated but not attributed")
}

// A table the plan creates is not on the target yet, so an unsafe finding on
// it, such as a lint error on one of its columns, destroys nothing and is not
// attributed, even when another open pull request last changed a table by
// that name. An unsafe ALTER beside it still is.
func TestPlannedDestructiveTables_LeavesOutCreatedTables(t *testing.T) {
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "keyspace",
			TableChanges: []*apitypes.TableChangeResponse{
				{TableName: "stations", ChangeType: "create", IsUnsafe: true, UnsafeReason: "column `opened_at` is TIMESTAMP, which overflows in 2038"},
				{TableName: "docks", ChangeType: "alter", IsUnsafe: true, UnsafeReason: "DROP COLUMN discards the column's data"},
			},
		}},
		Shards: []*apitypes.ShardPlanResponse{{
			Namespace: "keyspace",
			Shard:     "-80",
			Changes: []*apitypes.TableChangeResponse{
				{TableName: "bikes", ChangeType: "create", IsUnsafe: true, UnsafeReason: "column `serviced_at` is TIMESTAMP, which overflows in 2038"},
			},
		}},
	}

	assert.Equal(t, []string{"docks"}, plannedDestructiveTables(planResp))
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

	assert.Contains(t, rendered, "⚠️ **Check before applying**: 1 destructive change SchemaBot cannot attribute to this PR")
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

	assert.Contains(t, planRendered, "⚠️ **Check before applying**")
	assert.Contains(t, planRendered, "[block/schemabot#42](https://github.com/block/schemabot/pull/42)")
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
		IsLocked:                  true,
		LockOwner:                 "block/schemabot#7",
		LockAcquired:              "2026-08-22 00:13:52 UTC",
		PendingManualConfirmation: true,
		PausedApplyCause: &templates.PausedApplyCauseData{
			Heading: "The plan this apply would be checked against could not be read",
			Remedy:  "Nothing has run. Review the statements above, then confirm to apply them.",
		},
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "⚠️ **The plan this apply would be checked against could not be read**")
	assert.Contains(t, rendered, "**Confirmation required** — review the plan above, then confirm manually:")
	assert.Contains(t, rendered, "⚠️ **Check before applying**")
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

	assert.Contains(t, rendered, "⚠️ **Check before applying**")
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

	assert.Contains(t, rendered, "⚠️ **Check before applying**")
	// The sequence walks the environments in order and is offered in full: the
	// attribution belongs to staging's drop, not to production's unrelated add.
	assert.Contains(t, rendered, "▶️ **To apply**")
	assert.Contains(t, rendered, "schemabot apply -e staging")
}

// A target that runs its own plan can drop something on a table the primary
// plan leaves alone, so the ownership lookup covers the tables of every
// unsafe change the rendered target plans carry beyond the primary plan's,
// each once, and leaves out VSchema changes, which are no table's, and
// changes that create their table, which destroy nothing. A rollout
// the comment does not render target plans for adds nothing.
func TestTargetPlanDestructiveTables(t *testing.T) {
	drift := &templates.DeploymentDriftData{
		Computed: true, Clean: true, Independent: true,
		Deployments: []templates.DeploymentDriftEntry{{Deployment: "primary", Target: "eu", Primary: true}, {Deployment: "primary", Target: "us"}, {Deployment: "primary", Target: "ap"}},
		Plans: []templates.DeploymentPlanGroup{
			{Members: []string{"primary/eu"}, Primary: true},
			{Members: []string{"primary/us", "primary/ap"}, Changes: []templates.KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `users` DROP COLUMN `legacy`"}}},
				UnsafeChanges: []templates.UnsafeChangeData{
					{Table: "users", Reason: "DROP COLUMN removes data", ChangeType: "alter"},
					{Table: "users", Reason: "DROP INDEX removes an index", ChangeType: "alter", Targets: []string{"primary/ap"}, TotalTargets: 2},
					{Table: "reconcile_state", Reason: "DROP TABLE removes all data", ChangeType: "drop", Targets: []string{"primary/us"}, TotalTargets: 2},
					{Table: "testapp/vschema.json", Reason: "removes vindex", ChangeType: apitypes.VSchemaChangeType},
					{Table: "stations", Reason: "column `opened_at` is TIMESTAMP, which overflows in 2038", ChangeType: "create"},
				}},
		},
	}
	assert.Equal(t, []string{"reconcile_state", "users"}, targetPlanDestructiveTables(drift))

	drift.Clean = false
	assert.Empty(t, targetPlanDestructiveTables(drift), "a blocked rollup renders no target plans")
	assert.Empty(t, targetPlanDestructiveTables(nil))
}
