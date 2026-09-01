package templates

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Lint warnings belong on the plan comment, where the operator reviews them
// before applying. On the locked apply comment they are noise — the operator
// already saw them at plan time — so the apply comment omits them.
func TestRenderPlanComment_LintShownOnPlanNotOnApply(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		LintViolations: []LintViolationData{
			{Message: "Column added without DEFAULT value", Table: "users", LinterName: "no_default"},
		},
	}

	plan := RenderPlanComment(data)
	assert.Contains(t, plan, "💡 **Lint Warnings**:", "the plan comment surfaces lint for review")
	assert.Contains(t, plan, "Column added without DEFAULT value")

	data.IsLocked = true
	apply := RenderPlanComment(data)
	assert.NotContains(t, apply, "Lint Warnings", "the locked apply comment omits lint as noise")
	assert.NotContains(t, apply, "Column added without DEFAULT value")
}

// Unsafe-change warnings belong on the plan comment, where the operator reviews
// them before applying. Unsafe changes only reach an apply after the operator
// acknowledged them with --allow-unsafe (apply-confirm re-checks and blocks
// otherwise), so the locked apply comment omits the warning block as noise.
func TestRenderPlanComment_UnsafeShownOnPlanNotOnApply(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` DROP COLUMN `email`"},
		}},
		HasUnsafeChanges: true,
		AllowUnsafe:      true,
		UnsafeChanges: []UnsafeChangeData{
			{Table: "users", Reason: "DROP COLUMN is destructive"},
		},
	}

	plan := RenderPlanComment(data)
	assert.Contains(t, plan, "**Issues**: 1 unsafe change detected", "the plan comment surfaces unsafe changes for review")
	assert.Contains(t, plan, "DROP COLUMN is destructive")

	data.IsLocked = true
	apply := RenderPlanComment(data)
	assert.NotContains(t, apply, "**Issues**: 1 unsafe change detected", "the locked apply comment omits the unsafe warning as noise")
	assert.NotContains(t, apply, "DROP COLUMN is destructive")
	assert.NotContains(t, apply, "Destructive drop guidance", "the drop guidance rides inside the unsafe block and is omitted with it")
	assert.Contains(t, apply, "DROP COLUMN `email`", "the DDL itself stays visible on the apply comment")
}

// A sharded plan whose shards diverge renders "what applies where": one DDL
// block per distinct change set, each labelled with the shards it applies to.
func TestRenderPlanComment_ShardedDivergent(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "cdb_resolute", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{{
			Keyspace:   "cdb_resolute_sharded",
			Statements: []string{"ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"},
			Shards: []KeyspaceShardChange{
				{Shard: "-40", Statements: []string{"ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"}},
				{Shard: "80-", Statements: []string{"ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"}},
				{Shard: "40-80", Statements: []string{"ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`), ADD COLUMN `reason` varchar(255)"}},
			},
		}},
	})

	assert.Contains(t, out, "Shards diverge — what applies where:")
	assert.Contains(t, out, "**shards `-40`, `80-`**", "shards sharing a change are grouped")
	assert.Contains(t, out, "**shard `40-80`**", "the drifted shard is its own group")
	assert.Contains(t, out, "ADD COLUMN `reason`", "the divergent statement is shown")
	assert.Equal(t, 2, strings.Count(out, "```sql"), "one DDL block per group")
}

// A partially-applied keyspace — some shards already have the change, the rest
// don't — is divergent: the satisfied shards render as an "already applied"
// group alongside the changing shards' DDL, instead of being hidden (which
// would mislead the operator into reading it as a clean uniform apply).
func TestRenderPlanComment_ShardedPartiallyApplied(t *testing.T) {
	stmt := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	out := RenderPlanComment(PlanCommentData{
		Database: "cdb_resolute", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{{
			Keyspace: "cdb_resolute_sharded",
			Shards: []KeyspaceShardChange{
				{Shard: "-40", Satisfied: true}, // already has the index
				{Shard: "40-80", Statements: []string{stmt}},
				{Shard: "80-c0", Statements: []string{stmt}},
				{Shard: "c0-", Statements: []string{stmt}},
			},
		}},
	})

	assert.Contains(t, out, "Shards diverge — what applies where:", "a partially-applied keyspace is divergent")
	assert.Contains(t, out, "Already applied — no change.", "satisfied shards are surfaced, not hidden")
	assert.Contains(t, out, "**shard `-40`**", "the satisfied shard is named")
	assert.Contains(t, out, "**shards `40-80`, `80-c0`, `c0-`**", "the changing shards share one group")
	assert.Equal(t, 1, strings.Count(out, "```sql"), "the satisfied group shows no empty code block")
}

// A uniform sharded plan (every shard the same change) shows the DDL once with
// no divergence header — but still names which shards are affected.
func TestRenderPlanComment_ShardedUniform(t *testing.T) {
	stmt := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	out := RenderPlanComment(PlanCommentData{
		Database: "cdb_resolute", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{{
			Keyspace:   "cdb_resolute_sharded",
			Statements: []string{stmt},
			Shards: []KeyspaceShardChange{
				{Shard: "-40", Statements: []string{stmt}},
				{Shard: "80-", Statements: []string{stmt}},
			},
		}},
	})

	assert.NotContains(t, out, "diverge", "a uniform plan is not grouped")
	assert.Contains(t, out, "**shards `-40`, `80-`**", "a uniform plan still names the affected shards")
	assert.Equal(t, 1, strings.Count(out, "```sql"), "the shared DDL is shown once")
}

// A sharded plan whose DDL lives only per-shard (no collapsed namespace-level
// Statements) must not short-circuit to "no changes" — the count incorporates
// the per-shard statements.
func TestRenderPlanComment_ShardedOnlyPerShardDDLNotMiscounted(t *testing.T) {
	stmt := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	out := RenderPlanComment(PlanCommentData{
		Database: "cdb_resolute", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{{
			Keyspace: "cdb_resolute_sharded",
			// No namespace-level Statements — only per-shard.
			Shards: []KeyspaceShardChange{
				{Shard: "-40", Statements: []string{stmt}},
				{Shard: "80-", Statements: []string{stmt}},
			},
		}},
	})

	assert.NotContains(t, out, "No schema changes", "per-shard-only DDL is still counted as a change")
	assert.Contains(t, out, "```sql", "the per-shard DDL is rendered")
}

// A plan whose DDL is only per-shard still reports its raw statement count in
// the summary when the same plan also carries a vschema update — the vschema
// clause must not hide DDL the plan will run.
func TestRenderPlanComment_PerShardDDLCountedAlongsideVSchema(t *testing.T) {
	stmt := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	out := RenderPlanComment(PlanCommentData{
		Database: "orders", Environment: "staging", DatabaseType: "vitess",
		Changes: []KeyspaceChangeData{{
			Keyspace: "orders_sharded",
			// No namespace-level Statements — only per-shard.
			Shards: []KeyspaceShardChange{
				{Shard: "-80", Statements: []string{stmt}},
				{Shard: "80-", Statements: []string{stmt}},
			},
			VSchemaChanged: true,
			VSchemaDiff:    `{"tables": {"mutes": {}}}`,
		}},
	})

	assert.Contains(t, out, "📋 **Plan**: 1 DDL statement, **1** vschema update")
}

// A uniform plan across a wide keyspace leads with how much of the keyspace
// the change covers instead of walling the comment with every range: the
// heading reads "all N shards" and the names stay reachable behind a
// collapsed block.
func TestRenderPlanComment_ShardedUniformManyShardsCollapse(t *testing.T) {
	stmt := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	shards := make([]KeyspaceShardChange, 0, 32)
	for i := range 32 {
		shards = append(shards, KeyspaceShardChange{Shard: fmt.Sprintf("s%02d", i), Statements: []string{stmt}})
	}
	out := RenderPlanComment(PlanCommentData{
		Database: "cdb_resolute", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{{
			Keyspace:   "cdb_resolute_sharded",
			Statements: []string{stmt},
			Shards:     shards,
		}},
	})

	assert.Contains(t, out, "<summary><b>all 32 shards</b></summary>", "a uniform wide keyspace leads with its coverage on one collapsed line")
	assert.NotContains(t, out, "**shards `", "the heading does not enumerate ranges inline")
	assert.Contains(t, out, "`s00`, `s01`", "the collapsed block lists the shard names")
	assert.Contains(t, out, "`s31`", "the collapsed block lists every shard")
	assert.Equal(t, 1, strings.Count(out, "```sql"), "the shared DDL is shown once")
}

// A divergent plan whose larger group is still a subset of the keyspace names
// its coverage as a fraction ("N of M shards"), so the operator can tell at a
// glance how much of the keyspace each change set touches.
func TestRenderPlanComment_ShardedDivergentWideGroupShowsFraction(t *testing.T) {
	idx := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	drift := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`), ADD COLUMN `reason` varchar(255)"
	shards := make([]KeyspaceShardChange, 0, 16)
	for i := range 15 {
		shards = append(shards, KeyspaceShardChange{Shard: fmt.Sprintf("s%02d", i), Statements: []string{idx}})
	}
	shards = append(shards, KeyspaceShardChange{Shard: "s15", Statements: []string{drift}})
	out := RenderPlanComment(PlanCommentData{
		Database: "cdb_resolute", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{{
			Keyspace:   "cdb_resolute_sharded",
			Statements: []string{idx},
			Shards:     shards,
		}},
	})

	assert.Contains(t, out, "Shards diverge — what applies where:")
	assert.Contains(t, out, "<summary><b>15 of 16 shards</b></summary>", "a wide subset names its coverage as a fraction on one collapsed line")
	assert.Contains(t, out, "**shard `s15`**", "a small group still names its shards inline")
	assert.Contains(t, out, "ADD COLUMN `reason`", "the divergent statement is shown")
}

// An unsafe change spanning a wide set of shards states its keyspace coverage
// on the finding line — the line the reviewer consents against must never
// leave a subset reading like whole-keyspace coverage — while the full names
// stay reachable in the DDL section's collapsed shard groups.
func TestRenderPlanComment_UnsafeChangeWideShardsStatesCoverage(t *testing.T) {
	stmt := "ALTER TABLE `mutes` ADD INDEX a, DROP COLUMN `x`"
	names := make([]string, 0, 12)
	shards := make([]KeyspaceShardChange, 0, 12)
	for i := range 12 {
		name := fmt.Sprintf("s%02d", i)
		names = append(names, name)
		shards = append(shards, KeyspaceShardChange{Shard: name, Statements: []string{stmt}})
	}
	cases := []struct {
		name        string
		totalShards int
		want        string
	}{
		{name: "a subset states its fraction of the keyspace", totalShards: 32, want: "`mutes` (12 of 32 shards)"},
		{name: "whole-keyspace coverage says all", totalShards: 12, want: "`mutes` (all 12 shards)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := RenderPlanComment(PlanCommentData{
				Database: "cdb_resolute", Environment: "staging", DatabaseType: "strata",
				HasUnsafeChanges: true,
				UnsafeChanges:    []UnsafeChangeData{{Table: "mutes", Reason: "DROP COLUMN removes data", Shards: names, TotalShards: tc.totalShards}},
				Changes: []KeyspaceChangeData{{
					Keyspace: "cdb_resolute_sharded",
					Shards:   shards,
				}},
			})

			assert.Contains(t, out, tc.want, "the finding line states coverage against the keyspace")
			assert.NotContains(t, out, "(shards `s00`", "the finding line does not enumerate ranges")
		})
	}
}

// The gate-line shard suffix reads inline names when few, and keyspace
// coverage when wide; with no known total it falls back to a bare count
// rather than overstating coverage.
func TestPlanShardList(t *testing.T) {
	wide := make([]string, 12)
	for i := range wide {
		wide[i] = fmt.Sprintf("s%02d", i)
	}
	cases := []struct {
		name        string
		shards      []string
		totalShards int
		want        string
	}{
		{name: "one shard reads inline", shards: []string{"-40"}, totalShards: 4, want: "shard `-40`"},
		{name: "few shards read inline", shards: []string{"-40", "40-80"}, totalShards: 4, want: "shards `-40`, `40-80`"},
		{name: "a wide subset states its fraction", shards: wide, totalShards: 32, want: "12 of 32 shards"},
		{name: "a wide whole keyspace says all", shards: wide, totalShards: 12, want: "all 12 shards"},
		{name: "an unknown total falls back to a bare count", shards: wide, totalShards: 0, want: "12 shards"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, planShardList(tc.shards, tc.totalShards))
		})
	}
}

// The inline/collapsed pivot sits exactly at shardNamesInlineLimit: a group
// at the limit still names every shard inline, one past it leads with its
// coverage on a collapsed line.
func TestWriteShardGroupHeading_InlineLimitBoundary(t *testing.T) {
	shardNames := func(n int) []string {
		names := make([]string, n)
		for i := range names {
			names[i] = fmt.Sprintf("s%02d", i)
		}
		return names
	}

	t.Run("at the limit the shards read inline", func(t *testing.T) {
		var sb strings.Builder
		writeShardGroupHeading(&sb, shardNames(shardNamesInlineLimit), shardNamesInlineLimit)
		assert.Contains(t, sb.String(), "**shards `s00`", "the heading names shards inline")
		assert.Contains(t, sb.String(), fmt.Sprintf("`s%02d`**", shardNamesInlineLimit-1), "every shard is named")
		assert.NotContains(t, sb.String(), "<details>", "no collapsed block at the limit")
	})

	t.Run("past the limit the heading collapses to coverage", func(t *testing.T) {
		var sb strings.Builder
		writeShardGroupHeading(&sb, shardNames(shardNamesInlineLimit+1), shardNamesInlineLimit+1)
		assert.Contains(t, sb.String(), fmt.Sprintf("<summary><b>all %d shards</b></summary>", shardNamesInlineLimit+1), "the heading leads with coverage")
		assert.Contains(t, sb.String(), "`s00`, `s01`", "the collapsed block lists the names")
	})
}

// An unsafe change confined to one shard is flagged with that shard in the
// unsafe-changes warning.
func TestRenderPlanComment_UnsafeShardChangeShowsShard(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "cdb_resolute", Environment: "staging", DatabaseType: "strata",
		HasUnsafeChanges: true,
		UnsafeChanges:    []UnsafeChangeData{{Table: "mutes", Reason: "DROP COLUMN removes data", Shards: []string{"40-80"}}},
		Changes: []KeyspaceChangeData{{
			Keyspace: "cdb_resolute_sharded",
			Shards: []KeyspaceShardChange{
				// One combined ALTER per table per shard; the drifted shard's single
				// statement also drops a column (multiple statements for one table are
				// not supported — they are combined into one ALTER upstream).
				{Shard: "-40", Statements: []string{"ALTER TABLE `mutes` ADD INDEX a"}},
				{Shard: "40-80", Statements: []string{"ALTER TABLE `mutes` ADD INDEX a, DROP COLUMN `x`"}},
			},
		}},
	})

	assert.Contains(t, out, "**Issues**: 1 unsafe change detected")
	assert.Contains(t, out, "`mutes` (shard `40-80`)", "the unsafe change names the shard it applies to")
	assert.Contains(t, out, "DROP COLUMN `x`", "the drop is shown in that shard's combined ALTER")
}

// Plan errors can carry raw engine text with internal endpoints and newlines.
// Each rendered error bullet must redact endpoints and stay on one Markdown
// line so an error cannot escape its list item.
func TestRenderPlanComment_ErrorsSanitized(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		Errors: []string{"dial tcp db-primary.internal:3306: refused\nsecond line"},
	})

	assert.NotContains(t, out, "db-primary.internal", "internal endpoints are redacted")
	assert.Contains(t, out, "- dial tcp [endpoint redacted]: refused second line\n",
		"the error bullet stays on one line")
}

// An error entry that sanitizes to nothing renders no bullet, and a plan whose
// errors all sanitize to nothing renders no Errors section at all.
func TestRenderPlanComment_EmptyErrorsRenderNothing(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		Errors: []string{" \n\t ", "real failure"},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "- real failure\n")
	assert.NotContains(t, out, "- \n", "a whitespace-only error renders no bullet")

	data.Errors = []string{" \n\t "}
	out = RenderPlanComment(data)
	assert.NotContains(t, out, "**Errors**:", "an all-empty errors list renders no section")
}

// HTML in an error is escaped after sanitization, so a message quoting markup
// (e.g. a parse error echoing a <details> tag) renders as text instead of
// folding the rest of the comment into an unlabeled collapse.
func TestRenderPlanComment_ErrorsEscapeHTML(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		Errors: []string{"read <nil> & retry"},
	})

	assert.Contains(t, out, "- read &lt;nil&gt; &amp; retry\n",
		"HTML metacharacters are escaped after sanitization")
	assert.NotContains(t, out, "<nil>")
}
