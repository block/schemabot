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
	assert.Contains(t, out, "▶️ **To apply** all schema changes from this PR, comment the command below with `--allow-unsafe` added to confirm the 2 unsafe changes on `transfer_events` and `refund_backfill`:\n```\nschemabot apply -e staging\n```")
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
	assert.Contains(t, out, "to confirm the 4 unsafe changes on `transfer_events`, `refunds`, and `ledger`:")

	data.UnsafeChanges = data.UnsafeChanges[:1]
	out = RenderPlanComment(*data)
	assert.Contains(t, out, "to confirm the unsafe change on `transfer_events`:")
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
	assert.Contains(t, out, "to confirm the 3 unsafe changes on `mutes` (shard `40-80`) and the `commerce_sharded` VSchema:")
}

// When the changes being removed were applied by another open pull request,
// the instruction leads with re-planning, since that is the expected path, and
// offers consent only as the exception. It names that pull request itself, so
// the reader acts from the footer without scrolling back to the attribution.
func TestRenderPlanComment_UnsafeConsentLeadsWithReplanForAttributedChanges(t *testing.T) {
	data := unsafeConsentPlan("staging")
	data.AttributedChanges = []AttributedChangeData{{Table: "transfer_events", Repository: "acme/payments", PullRequest: 4790}}
	out := RenderPlanComment(*data)

	assert.Contains(t, out, "▶️ **To apply** all schema changes from this PR, first merge [acme/payments#4790](https://github.com/acme/payments/pull/4790) or bring this PR up to date with it, then re-plan. To apply as planned anyway, comment the command below with `--allow-unsafe` added:\n```\nschemabot apply -e staging\n```")
	for _, block := range fencedCommands(t, out) {
		assert.NotContains(t, block, "--allow-unsafe")
	}
}

// A plan without unsafe changes keeps the plain instruction.
func TestRenderPlanComment_NoUnsafeConsentWithoutUnsafeChanges(t *testing.T) {
	data := unsafeConsentPlan("staging")
	data.HasUnsafeChanges = false
	data.UnsafeChanges = nil
	out := RenderPlanComment(*data)
	assert.NotContains(t, out, "--allow-unsafe")
	assert.Contains(t, out, "▶️ **To apply** all schema changes from this PR, comment:\n```\nschemabot apply -e staging\n```")
}

// The locked apply comment already carries the operator's consent in its
// apply-confirm command, so it gets no consent instruction.
func TestRenderPlanComment_NoUnsafeConsentOnLockedApply(t *testing.T) {
	data := unsafeConsentPlan("staging")
	data.IsLocked = true
	data.AllowUnsafe = true
	data.PendingManualConfirmation = true
	out := RenderPlanComment(*data)
	assert.NotContains(t, out, "to confirm the 2 unsafe changes")
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
	assert.Contains(t, out, "After verifying staging, apply to production. Comment the command below with `--allow-unsafe` added to confirm the 2 unsafe changes on `transfer_events` and `refund_backfill`:\n```\nschemabot apply -e production\n```")
	for _, block := range fencedCommands(t, out) {
		assert.NotContains(t, block, "--allow-unsafe")
	}

	out = render(unsafeConsentPlan("staging"), unsafeConsentPlan("production"))
	assert.Contains(t, out, "▶️ **To apply** these changes, start with the first environment. Comment the command below with `--allow-unsafe` added to confirm the 2 unsafe changes on `transfer_events` and `refund_backfill`:\n```\nschemabot apply -e staging\n```")

	out = RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "payments", IsMySQL: true, DatabaseType: "mysql",
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{"staging": unsafeConsentPlan("staging"), "production": nil},
	})
	assert.Contains(t, out, "▶️ **To apply** these changes, comment the command below with `--allow-unsafe` added to confirm the 2 unsafe changes on `transfer_events` and `refund_backfill`:\n```\nschemabot apply -e staging\n```")
}

// The instruction names every owning pull request once, and tags a table
// whose ownership could not be established in the consent list, so it stays
// accurate for several owners and for unknown ones without asking the reader
// to investigate.
func TestRenderPlanComment_UnsafeConsentAccurateForUnknownAndMultipleOwners(t *testing.T) {
	data := unsafeConsentPlan("staging")
	data.AttributedChanges = []AttributedChangeData{
		{Table: "transfer_events", Repository: "acme/payments", PullRequest: 4790},
		{Table: "refund_backfill", Repository: "acme/payments", PullRequest: 4791},
		{Table: "refund_events", Repository: "acme/payments", PullRequest: 4790},
	}
	out := RenderPlanComment(*data)
	assert.Contains(t, out, "first merge [acme/payments#4790](https://github.com/acme/payments/pull/4790) and [acme/payments#4791](https://github.com/acme/payments/pull/4791) or bring this PR up to date with them, then re-plan.")

	data.UnsafeChanges = append(data.UnsafeChanges,
		UnsafeChangeData{Table: "ledger", Reason: "DROP COLUMN discards the column's data"},
		UnsafeChangeData{Table: "mutes", Reason: "DROP COLUMN removes data and is irreversible", Shards: []string{"40-80"}, TotalShards: 4},
	)
	data.AttributedChanges = []AttributedChangeData{{Table: "ledger", Unresolved: true}, {Table: "mutes", Unresolved: true}}
	out = RenderPlanComment(*data)
	assert.Contains(t, out, "▶️ **To apply** all schema changes from this PR, comment the command below with `--allow-unsafe` added to confirm the 4 unsafe changes on `transfer_events`, `refund_backfill`, `ledger` (not traced to any PR), and `mutes` (shard `40-80`, not traced to any PR):")

	data.AttributedChanges = []AttributedChangeData{
		{Table: "transfer_events", Repository: "acme/payments", PullRequest: 4790},
		{Table: "ledger", Unresolved: true},
	}
	out = RenderPlanComment(*data)
	assert.Contains(t, out, "first merge [acme/payments#4790](https://github.com/acme/payments/pull/4790) or bring this PR up to date with it, then re-plan. To apply as planned anyway, comment the command below with `--allow-unsafe` added to confirm the 4 unsafe changes on `transfer_events`, `refund_backfill`, `ledger` (not traced to any PR), and `mutes` (shard `40-80`):")
	_, footer, found := strings.Cut(out, "▶️ **To apply**")
	require.True(t, found, "the comment offers an apply")
	assert.NotContains(t, footer, "Check before applying", "the footer stands on its own")
	assert.NotContains(t, footer, "check who", "the footer asks for no investigation SchemaBot could not do")
}

// A plan that also carries a change the engine refuses fails its apply
// whatever the flags, so the instruction does not offer --allow-unsafe.
func TestRenderPlanComment_NoUnsafeConsentWhenEngineBlocksAChange(t *testing.T) {
	data := unsafeConsentPlan("staging")
	data.BlockedChanges = []BlockedChangeData{{Table: "orders", Reason: "statement for table \"orders\" must be rewritten into a form the engine can execute natively, then re-planned"}}
	out := RenderPlanComment(*data)
	assert.NotContains(t, out, "--allow-unsafe")
	assert.Contains(t, out, "▶️ **To apply** all schema changes from this PR, comment:\n```\nschemabot apply -e staging\n```")
}
