package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A statement the direct execution policy routes to native MySQL DDL is
// disclosed in its own ⚙️ section, naming the table and the planner's reason
// (which carries the row estimate), with a fixed footer on the semantics the
// operator consents to. The section also renders on the locked apply comment —
// confirming against that comment is the consent.
func TestRenderPlanComment_DirectShownOnPlanAndApply(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)"},
		}},
		DirectChanges: []DirectChangeData{
			{Table: "users", Reason: "dropping primary key is not supported; runs as native MySQL DDL on a table with ~1,240 rows"},
		},
	}

	plan := RenderPlanComment(data)
	assert.Contains(t, plan, "⚙️ **Direct execution**: 1 change will run as native MySQL DDL")
	assert.Contains(t, plan, "`users`: dropping primary key is not supported; runs as native MySQL DDL on a table with ~1,240 rows")
	assert.Contains(t, plan, "the change is **not revertible**")
	assert.Contains(t, plan, "`--defer-cutover` does not apply")

	data.IsLocked = true
	apply := RenderPlanComment(data)
	assert.Contains(t, apply, "⚙️ **Direct execution**", "the locked apply comment keeps the direct disclosure")
	assert.Contains(t, apply, "Confirming the apply consents to this.")
}

func TestRenderPlanComment_DirectEscapesReasonMarkdown(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `user_events_v2` DROP PRIMARY KEY"},
		}},
		DirectChanges: []DirectChangeData{
			{Table: "user_events_v2", Reason: "table user_events_v2 | column `event_id` runs directly"},
		},
	})

	assert.Contains(t, out, "table user\\_events\\_v2 \\| column \\`event\\_id\\` runs directly")
}

func TestRenderPlanComment_DirectSanitizesReason(t *testing.T) {
	reason := "refused by db-primary.internal:3306\n\n## Injected heading\n- fake item"
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace: "testapp", Statements: []string{"ALTER TABLE `users` DROP PRIMARY KEY"},
		}},
		DirectChanges: []DirectChangeData{{Table: "users", Reason: reason}},
	})

	assert.NotContains(t, out, "\n## Injected heading")
	assert.NotContains(t, out, "db-primary.internal:3306")
	assert.Contains(t, out, "- `users`: refused by \\[endpoint redacted\\] ## Injected heading - fake item\n")
}

// A direct change confined to specific shards names them, matching the
// blocked section's shard scoping.
func TestRenderPlanComment_DirectNamesShards(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp_sharded",
			Statements: []string{"ALTER TABLE `users` DROP PRIMARY KEY"},
		}},
		DirectChanges: []DirectChangeData{
			{Table: "users", Reason: "dropping primary key is not supported; runs as native MySQL DDL on a table with ~40 rows", Shards: []string{"-40", "40-80"}},
		},
	})

	assert.Contains(t, out, "`users` (shards `-40`, `40-80`): dropping primary key is not supported")
}

// The direct-execution consent copy is keyed by database type: MySQL-family
// databases (including Strata, whose shards are MySQL) disclose MySQL
// semantics, and a database type without registered copy gets the
// conservative engine-neutral disclosure rather than inheriting MySQL's.
func TestDirectConsentCopy_KeyedByDatabaseType(t *testing.T) {
	mysqlHeader, mysqlFooter := directConsentCopy("mysql", true)
	assert.Equal(t, "native MySQL DDL", mysqlHeader)
	assert.Contains(t, mysqlFooter, "writes to each table are blocked while its statement runs")

	strataHeader, strataFooter := directConsentCopy("strata", false)
	assert.Equal(t, mysqlHeader, strataHeader, "Strata shards run the same native MySQL DDL")
	assert.Equal(t, mysqlFooter, strataFooter)

	otherHeader, otherFooter := directConsentCopy("postgres", false)
	assert.Equal(t, "native DDL", otherHeader)
	assert.Contains(t, otherFooter, "each table is unavailable while its statement runs")
	assert.Contains(t, otherFooter, "**not revertible**")
	assert.Contains(t, otherFooter, "Confirming the apply consents to this.")
}

// A multi-environment plan renders each environment's own direct section,
// since the direct execution policy is configured per environment.
func TestRenderMultiEnvPlanComment_DirectPerEnvironment(t *testing.T) {
	stagingPlan := &PlanCommentData{
		Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)"},
		}},
		DirectChanges: []DirectChangeData{
			{Table: "users", Reason: "dropping primary key is not supported; runs as native MySQL DDL on a table with ~40 rows"},
		},
	}
	productionPlan := &PlanCommentData{
		Environment: "production", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` DROP PRIMARY KEY"},
		}},
		BlockedChanges: []BlockedChangeData{
			{Table: "users", Reason: "dropping primary key is not supported"},
		},
	}

	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", DatabaseType: "mysql", IsMySQL: true,
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{"staging": stagingPlan, "production": productionPlan},
	})

	assert.Contains(t, out, "⚙️ **Direct execution**", "staging's section discloses its direct route")
	assert.Contains(t, out, "⛔ **Cannot apply**", "production's section discloses its block")
}

// An apply on a plan containing engine-blocked statements is rejected with a
// comment that shows the plan, names each blocked table and reason, and gives
// no retry instructions — no flag lets a refused statement through.
func TestRenderBlockedChangesApplyRejected(t *testing.T) {
	out := RenderBlockedChangesApplyRejected(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` DROP PRIMARY KEY"},
		}},
		BlockedChanges: []BlockedChangeData{
			{Table: "users", Reason: "dropping primary key is not supported; direct execution is enabled but the table has ~2,400,000 rows, above the configured limit of 1,000,000"},
		},
	})

	assert.Contains(t, out, "**⛔ Apply rejected**: 1 planned change the engine refuses to execute")
	assert.Contains(t, out, "`users`: dropping primary key is not supported")
	assert.Contains(t, out, "above the configured limit of 1,000,000")
	assert.Contains(t, out, "Fix what each reason names")
	assert.NotContains(t, out, "--allow-unsafe", "a guaranteed failure must not coach an unsafe override")
	assert.NotContains(t, out, "retry", "no retry of this command can succeed")
}

// An engine refusal reason is untrusted error text: endpoints are redacted,
// the reason stays on one line, and Markdown constructs cannot alter rendering.
func TestRenderBlockedChangesApplyRejectedSanitizesReason(t *testing.T) {
	out := RenderBlockedChangesApplyRejected(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` DROP PRIMARY KEY"},
		}},
		BlockedChanges: []BlockedChangeData{{
			Table: "users", Reason: "refused by db-primary.internal:3306\n*event_id* | <details>",
		}},
	})

	assert.NotContains(t, out, "db-primary.internal", "internal endpoints are redacted")
	assert.Contains(t, out, "`users`: refused by \\[endpoint redacted\\] \\*event\\_id\\* \\| \\<details>\n",
		"the reason stays on one line with Markdown escaped")
}
