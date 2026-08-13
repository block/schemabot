package templates

import (
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
	assert.Contains(t, plan, "**Issues**: **1** unsafe change detected", "the plan comment surfaces unsafe changes for review")
	assert.Contains(t, plan, "DROP COLUMN is destructive")

	data.IsLocked = true
	apply := RenderPlanComment(data)
	assert.NotContains(t, apply, "**Issues**: **1** unsafe change detected", "the locked apply comment omits the unsafe warning as noise")
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

	assert.Contains(t, out, "**Issues**: **1** unsafe change detected")
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
