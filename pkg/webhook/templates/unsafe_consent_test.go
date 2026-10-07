package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/glyph"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func unsafeConsentPlan(env string) *PlanCommentData {
	return &PlanCommentData{
		Database: "payments", Environment: env, IsMySQL: true, DatabaseType: "mysql",
		Changes: []KeyspaceChangeData{{
			Keyspace: "payments",
			Statements: []string{
				"ALTER TABLE `transfer_events` DROP COLUMN `legacy_ref`",
				"DROP TABLE `refund_backfill`",
			},
		}},
		HasUnsafeChanges: true,
		UnsafeChanges: []UnsafeChangeData{
			{Table: "transfer_events", Reason: "DROP COLUMN discards the column's data"},
			{Table: "refund_backfill", Reason: "DROP TABLE removes all data"},
		},
	}
}

// fencedCommands returns the contents of every fenced block in a comment, which
// is what a reader copies and pastes.
func fencedCommands(t *testing.T, comment string) []string {
	t.Helper()
	parts := strings.Split(comment, "```")
	require.Equal(t, 1, len(parts)%2, "comment has an unbalanced fence")
	var blocks []string
	for i := 1; i < len(parts); i += 2 {
		blocks = append(blocks, strings.TrimSpace(parts[i]))
	}
	return blocks
}

// A plan with unsafe changes states, in the sentence leading into the apply
// command, that the apply needs --allow-unsafe and which tables it confirms, so
// the reader meets the requirement before copying the command rather than in a
// rejection comment. The flag stays out of the pasteable command: consenting to
// destroy data takes typing it.
func TestRenderPlanComment_UnsafeConsentLeadsIntoPlainCommand(t *testing.T) {
	out := RenderPlanComment(*unsafeConsentPlan("staging"))

	assert.Contains(t, fencedCommands(t, out), "schemabot apply -e staging")
	for _, block := range fencedCommands(t, out) {
		assert.NotContains(t, block, "--allow-unsafe", "the pasteable command never carries the consent flag")
	}
	assert.Contains(t, out, "▶️ **To apply**, add `--allow-unsafe` to confirm 2 unsafe changes (`transfer_events`, `refund_backfill`):\n```\nschemabot apply -e staging\n```")
	assert.NotContains(t, out, glyph.Attention+" This plan", "the footer adds no second warning glyph")
}

// The consent counts each finding but names each table once, in plan order.
func TestRenderPlanComment_UnsafeConsentNamesEachTableOnce(t *testing.T) {
	data := unsafeConsentPlan("staging")
	data.UnsafeChanges = []UnsafeChangeData{
		{Table: "transfer_events", Reason: "DROP COLUMN discards the column's data"},
		{Table: "refunds", Reason: "DROP COLUMN discards the column's data"},
		{Table: "transfer_events", Reason: "DROP INDEX removes the index"},
		{Table: "ledger", Reason: "DROP TABLE removes all data"},
	}
	out := RenderPlanComment(*data)
	assert.Contains(t, out, "to confirm 4 unsafe changes (`transfer_events`, `refunds`, `ledger`):")

	data.UnsafeChanges = data.UnsafeChanges[:1]
	out = RenderPlanComment(*data)
	assert.Contains(t, out, "to confirm 1 unsafe change (`transfer_events`):")
}

// A sharded change that only some shards carry is named with its shards, and a
// VSchema change by its namespace, matching the labels in the unsafe findings.
func TestRenderPlanComment_UnsafeConsentNamesShardsAndVSchema(t *testing.T) {
	data := unsafeConsentPlan("staging")
	data.UnsafeChanges = []UnsafeChangeData{
		{Table: "mutes", Reason: "DROP COLUMN removes data and is irreversible", Shards: []string{"40-80"}, TotalShards: 4},
		{Table: "commerce_sharded/vschema.json", VSchemaNamespace: "commerce_sharded", Reason: "lookup vindex `customers_email_lookup` is removed"},
		{Table: "commerce_sharded/vschema.json", VSchemaNamespace: "commerce_sharded", Reason: "table `customers` no longer uses vindex `customers_email_lookup`"},
	}
	out := RenderPlanComment(*data)
	assert.Contains(t, out, "to confirm 3 unsafe changes (`mutes` on shard `40-80`, `commerce_sharded` VSchema):")
}

// When the plan undoes a change another open pull request applied, the
// attribution rides on that table's unsafe finding, so each change is
// explained once and the reader meets it where they review the change. The
// apply instruction stays one short clause and prescribes nothing about the
// other pull request, which may land or may have been abandoned.
func TestRenderPlanComment_AttributionRidesOnTheUnsafeFinding(t *testing.T) {
	data := unsafeConsentPlan("staging")
	data.Repository = "acme/payments"
	data.AttributedChanges = []AttributedChangeData{{Table: "transfer_events", Repository: "acme/payments", PullRequest: 4790}}
	out := RenderPlanComment(*data)

	assert.Contains(t, out, "1. `transfer_events`: DROP COLUMN discards the column's data (changed by open PR [#4790](https://github.com/acme/payments/pull/4790))\n2. `refund_backfill`: DROP TABLE removes all data\n")
	assert.Contains(t, out, "a change another PR applied before merging shows up here as one to undo")
	assert.NotContains(t, out, "Check before applying", "the attribution is not repeated in a section of its own")
	_, footer, found := strings.Cut(out, "▶️ **To apply**")
	require.True(t, found, "the comment offers an apply")
	assert.Equal(t, ", add `--allow-unsafe` to confirm 2 unsafe changes (`transfer_events`, `refund_backfill`):\n```\nschemabot apply -e staging\n```\n", footer)
}

// An owner in another repository is named with its repository, and a table
// whose ownership could not be established says so on its finding.
func TestRenderPlanComment_AttributionNotesForOtherRepoAndUnknownOwner(t *testing.T) {
	data := unsafeConsentPlan("staging")
	data.Repository = "acme/payments"
	data.AttributedChanges = []AttributedChangeData{
		{Table: "transfer_events", Repository: "acme/ledger", PullRequest: 12},
		{Table: "refund_backfill", Unresolved: true},
	}
	out := RenderPlanComment(*data)
	assert.Contains(t, out, "1. `transfer_events`: DROP COLUMN discards the column's data (changed by open PR [acme/ledger#12](https://github.com/acme/ledger/pull/12))\n")
	assert.Contains(t, out, "2. `refund_backfill`: DROP TABLE removes all data (ownership could not be established)\n")
	assert.NotContains(t, out, "Check before applying")
}

// An attributed table the unsafe warning does not list keeps the attribution
// section, so folding never drops a disclosure.
func TestRenderPlanComment_AttributionKeepsItsSectionWhenNotAnUnsafeFinding(t *testing.T) {
	data := unsafeConsentPlan("staging")
	data.AttributedChanges = []AttributedChangeData{{Table: "ledger_holds", Repository: "acme/payments", PullRequest: 4790}}
	out := RenderPlanComment(*data)
	assert.Contains(t, out, "**Check before applying**: 1 destructive change SchemaBot cannot attribute to this PR")
	assert.NotContains(t, out, "(changed by open PR")
}

// A paused comment's apply-confirm carries --allow-unsafe forward, so the
// comment lists the unsafe changes it consents to: the plan can have changed
// since the operator first opted in, and confirming must not run an unsafe
// change no comment showed. An attribution rides on its finding there as on
// the plan comment. The comment of an apply already running lists none, as it
// reached the apply only under the opt-in.
func TestRenderPlanComment_PausedCommentListsTheUnsafeChangesItConsentsTo(t *testing.T) {
	data := unsafeConsentPlan("staging")
	data.AttributedChanges = []AttributedChangeData{{Table: "transfer_events", Repository: "acme/payments", PullRequest: 4790}}
	data.IsLocked = true
	data.AllowUnsafe = true
	data.PendingManualConfirmation = true
	out := RenderPlanComment(*data)
	assert.Contains(t, out, "⚠️ **Issues**: 2 unsafe changes detected\n1. `transfer_events`: DROP COLUMN discards the column's data (changed by open PR [acme/payments#4790](https://github.com/acme/payments/pull/4790))\n2. `refund_backfill`: DROP TABLE removes all data\n")
	assert.NotContains(t, out, "Check before applying", "the attribution folds into its finding")
	assert.Contains(t, fencedCommands(t, out), "schemabot apply-confirm -e staging --allow-unsafe")

	data.PendingManualConfirmation = false
	out = RenderPlanComment(*data)
	assert.NotContains(t, out, "unsafe change")
	assert.NotContains(t, out, "transfer_events`: DROP COLUMN")
}

// A plan without unsafe changes keeps the plain instruction.
func TestRenderPlanComment_NoUnsafeConsentWithoutUnsafeChanges(t *testing.T) {
	data := unsafeConsentPlan("staging")
	data.HasUnsafeChanges = false
	data.UnsafeChanges = nil
	out := RenderPlanComment(*data)
	assert.NotContains(t, out, "--allow-unsafe")
	assert.Contains(t, out, "▶️ **To apply**, comment:\n```\nschemabot apply -e staging\n```")
}

// The locked apply comment already carries the operator's consent in its
// apply-confirm command, so it gets no consent instruction.
func TestRenderPlanComment_NoUnsafeConsentOnLockedApply(t *testing.T) {
	data := unsafeConsentPlan("staging")
	data.IsLocked = true
	data.AllowUnsafe = true
	data.PendingManualConfirmation = true
	out := RenderPlanComment(*data)
	assert.NotContains(t, out, "to confirm 2 unsafe changes")
	assert.Contains(t, fencedCommands(t, out), "schemabot apply-confirm -e staging --allow-unsafe")
}

// On a multi-environment plan, each environment's apply instruction carries
// the consent only when that environment's own plan has unsafe changes.
func TestRenderMultiEnvPlanComment_UnsafeConsentPerEnvironment(t *testing.T) {
	render := func(staging, production *PlanCommentData) string {
		return RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
			Database: "payments", IsMySQL: true, DatabaseType: "mysql",
			Environments: []string{"staging", "production"},
			Plans:        map[string]*PlanCommentData{"staging": staging, "production": production},
		})
	}
	clean := unsafeConsentPlan("staging")
	clean.HasUnsafeChanges = false
	clean.UnsafeChanges = nil

	out := render(clean, unsafeConsentPlan("production"))
	assert.Contains(t, out, "▶️ **To apply** these changes, start with the first environment:\n```\nschemabot apply -e staging\n```")
	assert.Contains(t, out, "After verifying staging, apply to production. Add `--allow-unsafe` to confirm 2 unsafe changes (`transfer_events`, `refund_backfill`):\n```\nschemabot apply -e production\n```")
	for _, block := range fencedCommands(t, out) {
		assert.NotContains(t, block, "--allow-unsafe")
	}

	out = render(unsafeConsentPlan("staging"), unsafeConsentPlan("production"))
	assert.Contains(t, out, "▶️ **To apply** these changes, start with the first environment. Add `--allow-unsafe` to confirm 2 unsafe changes (`transfer_events`, `refund_backfill`):\n```\nschemabot apply -e staging\n```")

	out = RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "payments", IsMySQL: true, DatabaseType: "mysql",
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{"staging": unsafeConsentPlan("staging"), "production": nil},
	})
	assert.Contains(t, out, "▶️ **To apply** these changes, add `--allow-unsafe` to confirm 2 unsafe changes (`transfer_events`, `refund_backfill`):\n```\nschemabot apply -e staging\n```")
}

func refusedChangePlan(env string) *PlanCommentData {
	data := unsafeConsentPlan(env)
	data.BlockedChanges = []BlockedChangeData{{Table: "orders", Reason: "statement for table \"orders\" must be rewritten into a form the engine can execute natively, then re-planned"}}
	return data
}

// A plan that carries a change the engine refuses fails its apply whatever the
// flags, so the footer offers no apply and no --allow-unsafe: it says why and
// offers the re-plan that follows fixing the change.
func TestRenderPlanComment_RefusedChangeOffersReplanNotApply(t *testing.T) {
	data := refusedChangePlan("staging")
	data.ScopedDatabase = "payments"
	out := RenderPlanComment(*data)

	_, footer, found := strings.Cut(out, "\n---\n")
	require.True(t, found, "the comment has a footer")
	assert.Equal(t, "\nThe engine refuses a change in this plan (see **Cannot apply** above), so its apply fails whatever its flags. After fixing it, re-plan:\n```\nschemabot plan -e staging -d payments\n```\n", footer)
	assert.NotContains(t, out, "--allow-unsafe")
	assert.NotContains(t, out, "▶️ **To apply**")
	for _, block := range fencedCommands(t, out) {
		assert.NotContains(t, block, "schemabot apply")
	}
}

// Across environments, one whose plan carries a refused change is offered a
// re-plan in place of its apply, an earlier clean one keeps its apply, and a
// later one waits on it.
func TestRenderMultiEnvPlanComment_RefusedChangeOffersReplanNotApply(t *testing.T) {
	render := func(staging, production *PlanCommentData) string {
		return RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
			Database: "payments", IsMySQL: true, DatabaseType: "mysql",
			Environments: []string{"staging", "production"},
			Plans:        map[string]*PlanCommentData{"staging": staging, "production": production},
		})
	}

	out := render(unsafeConsentPlan("staging"), refusedChangePlan("production"))
	assert.Contains(t, out, "▶️ **To apply** these changes, add `--allow-unsafe` to confirm 2 unsafe changes (`transfer_events`, `refund_backfill`):\n```\nschemabot apply -e staging\n```")
	assert.Contains(t, out, "The engine refuses a change in the **production** plan (see **Cannot apply** above), so its apply fails whatever its flags. After fixing it, re-plan:\n```\nschemabot plan -e production\n```")
	assert.NotContains(t, fencedCommands(t, out), "schemabot apply -e production")

	out = render(refusedChangePlan("staging"), unsafeConsentPlan("production"))
	assert.Contains(t, out, "The engine refuses a change in the **staging** plan (see **Cannot apply** above), so its apply fails whatever its flags. After fixing it, re-plan:\n```\nschemabot plan -e staging\n```")
	assert.Contains(t, out, glyph.Attention+" **Production** applies only after staging, and staging's plan carries a change the engine refuses (see above).")
	assert.NotContains(t, out, "▶️ **To apply**")
	assert.NotContains(t, out, "No changes to apply.")
	for _, block := range fencedCommands(t, out) {
		assert.NotContains(t, block, "schemabot apply")
	}
}

// The consent names at most a handful of tables and counts the rest, so a plan
// that drops many tables keeps its instruction to one readable line. The
// finding count still covers every change.
func TestRenderPlanComment_UnsafeConsentBoundsTheTableList(t *testing.T) {
	data := unsafeConsentPlan("staging")
	data.UnsafeChanges = nil
	for _, table := range []string{"t1", "t2", "t3", "t4", "t5", "t6", "t7"} {
		data.UnsafeChanges = append(data.UnsafeChanges, UnsafeChangeData{Table: table, Reason: "DROP TABLE removes all data"})
	}
	out := RenderPlanComment(*data)
	assert.Contains(t, out, "▶️ **To apply**, add `--allow-unsafe` to confirm 7 unsafe changes (`t1`, `t2`, `t3`, `t4`, `t5` and 2 more):\n```")

	data.UnsafeChanges = data.UnsafeChanges[:5]
	out = RenderPlanComment(*data)
	assert.Contains(t, out, "to confirm 5 unsafe changes (`t1`, `t2`, `t3`, `t4`, `t5`):")
}

// When the rollup is blocked or could not be computed, the comment has no plan
// per target to count from, so the consent counts the primary target's unsafe
// changes and says the flag covers any the other targets carry. The apply plans
// the rollout again, so the instruction stays: a rollup clean by then runs.
func TestRenderPlanComment_UnsafeConsentScopedToPrimaryWithoutTargetPlans(t *testing.T) {
	for name, drift := range map[string]*DeploymentDriftData{
		"blocked":      {Computed: true, Clean: false, Deployments: previewRolloutMembers()},
		"not computed": {Computed: false},
	} {
		t.Run(name, func(t *testing.T) {
			data := unsafeConsentPlan("staging")
			data.DeploymentDrift = drift
			out := RenderPlanComment(*data)
			assert.Contains(t, out, "▶️ **To apply**, add `--allow-unsafe` to confirm 2 unsafe changes on the primary target (`transfer_events`, `refund_backfill`) and any on the other targets:\n```\nschemabot apply -e staging\n```")
		})
	}

	data := unsafeConsentPlan("staging")
	data.DeploymentDrift = &DeploymentDriftData{Computed: true, Clean: true, Deployments: previewRolloutMembers()}
	out := RenderPlanComment(*data)
	assert.Contains(t, out, "to confirm 2 unsafe changes (`transfer_events`, `refund_backfill`):", "a clean mirrored rollup runs the primary plan everywhere, so its count covers every target")
}

// --allow-unsafe consents to every target's disclosed unsafe changes, so the
// instruction counts and names another target's changes too, and asks for the
// flag even when the reviewed plan carries none of its own.
func TestRenderPlanComment_UnsafeConsentCoversOtherTargets(t *testing.T) {
	data := convergedPrimaryPlanData()
	data.DeploymentDrift.Plans[1].UnsafeChanges = []UnsafeChangeData{
		{Table: "users", Reason: "has_timestamp: column created_at uses TIMESTAMP", ChangeType: "alter"},
		{Table: "legacy", Reason: "DROP TABLE removes all data", ChangeType: "drop", Targets: []string{"primary/testapp_3"}, TotalTargets: 2},
	}
	out := RenderPlanComment(data)
	_, footer, found := strings.Cut(out, "▶️ **To apply**")
	require.True(t, found, "the comment offers an apply")
	assert.Equal(t, ", add `--allow-unsafe` to confirm 2 unsafe changes (`users` on targets `primary/testapp_2`, `primary/testapp_3`; `legacy` on target `primary/testapp_3`):\n```\nschemabot apply -e production\n```\n", footer)
}
