package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/engine"
	"github.com/stretchr/testify/assert"
)

// A statement the direct execution policy routes to native MySQL DDL is
// disclosed in its own ⚙️ section, naming the table and its measured size,
// with a footer on what running it does to the table. The policy approves the
// change, so the section asks for no confirmation, and it also renders on the
// locked apply comment of the apply that runs it.
func TestRenderPlanComment_DirectShownOnPlanAndApply(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)"},
		}},
		DirectChanges: []DirectChangeData{
			{Table: "users", Reason: "the table has ~1,240 rows"},
		},
	}

	plan := RenderPlanComment(data)
	assert.Contains(t, plan, "⚙️ **Direct execution**: 1 change will run as native MySQL DDL, not through Spirit")
	assert.Contains(t, plan, "`users`: the table has ~1,240 rows")
	assert.Contains(t, plan, "Transactions blocking a table's metadata lock are killed so its statement can take the lock, and writes to each table are blocked until its statement finishes.\n")
	assert.NotContains(t, plan, "Confirming the apply", "the policy approves a direct change, so the disclosure asks for no confirmation")
	assert.NotContains(t, plan, "revertible", "a MySQL direct change is undone like any other MySQL change, so no revert warning is shown")
	assert.NotContains(t, plan, "--defer-cutover", "a plan with no --defer-cutover apply behind it does not mention the flag")

	data.IsLocked = true
	apply := RenderPlanComment(data)
	assert.Contains(t, apply, "⚙️ **Direct execution**", "the locked apply comment keeps the direct disclosure")
	assert.Contains(t, apply, "Transactions blocking a table's metadata lock are killed so its statement can take the lock, and writes to each table are blocked until its statement finishes.\n")
	assert.NotContains(t, apply, "--defer-cutover")
}

// An apply that passed --defer-cutover defers the cutover of the plan's
// engine-driven changes only, so the direct disclosure on its locked comment
// says the flag leaves the direct statements alone.
func TestRenderPlanComment_DirectNotesDeferCutoverOnlyWhenPassed(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true, IsLocked: true, DeferCutover: true,
		Changes: []KeyspaceChangeData{{
			Keyspace: "testapp",
			Statements: []string{
				"ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)",
				"ALTER TABLE `orders` ADD COLUMN `notes` text",
			},
		}},
		DirectChanges: []DirectChangeData{
			{Table: "users", Reason: "the table has ~1,240 rows"},
		},
	}

	apply := RenderPlanComment(data)
	assert.Contains(t, apply, "Transactions blocking a table's metadata lock are killed so its statement can take the lock, and writes to each table are blocked until its statement finishes. `--defer-cutover` does not apply to these direct statements: they have no cutover to defer.\n")
}

// A paused --defer-cutover apply whose every change runs as direct execution
// suggests an apply-confirm without the flag, since apply-confirm rejects it on
// such a plan. The disclosure still notes the flag leaves the direct statements
// alone, and a mixed plan keeps the flag in its suggested command.
func TestRenderPlanComment_PausedAllDirectConfirmOmitsDeferCutover(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true, IsLocked: true,
		PendingManualConfirmation: true, DeferCutover: true, AllChangesDirect: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)"},
		}},
		DirectChanges: []DirectChangeData{
			{Table: "users", Reason: "the table has ~1,240 rows"},
		},
	}

	paused := RenderPlanComment(data)
	assert.Contains(t, paused, "```\nschemabot apply-confirm -e staging\n```")
	assert.Contains(t, paused, "`--defer-cutover` does not apply to these direct statements: they have no cutover to defer.")

	data.AllChangesDirect = false
	assert.Contains(t, RenderPlanComment(data), "```\nschemabot apply-confirm -e staging --defer-cutover\n```",
		"a plan with engine-driven changes keeps the flag the operator passed")
}

// apply-confirm reads its own flags, so a paused comment notes that
// --defer-cutover leaves the direct statements alone even when the paused
// apply did not pass the flag: the operator can still add it when confirming.
func TestRenderPlanComment_PausedDirectNotesDeferCutover(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true, IsLocked: true, PendingManualConfirmation: true,
		Changes: []KeyspaceChangeData{{
			Keyspace: "testapp",
			Statements: []string{
				"ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)",
				"ALTER TABLE `orders` ADD COLUMN `notes` text",
			},
		}},
		DirectChanges: []DirectChangeData{
			{Table: "users", Reason: "the table has ~1,240 rows"},
		},
	}

	paused := RenderPlanComment(data)
	assert.Contains(t, paused, "`--defer-cutover` does not apply to these direct statements: they have no cutover to defer.")
	assert.Contains(t, paused, "```\nschemabot apply-confirm -e staging\n```", "the suggested command adds no flag the operator did not pass")
}

func TestPreviewCommentApplyRolloutDeferred(t *testing.T) {
	out := PreviewCommentApplyRolloutDeferred()
	assert.Contains(t, out, "### Target `eu`")
	assert.Contains(t, out, "### Target `us`")
	assert.Contains(t, out, "DROP PRIMARY KEY")
	assert.Contains(t, out, "ADD INDEX `idx_user_id`")
	assert.Contains(t, out, "`--defer-cutover` does not apply to these direct statements")
	assert.Contains(t, out, "schemabot apply-confirm -e production --allow-unsafe --defer-cutover\n")
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
			{Table: "users", Reason: "the table has ~40 rows", Shards: []string{"-40", "40-80"}},
		},
	})

	assert.Contains(t, out, "`users` (shards `-40`, `40-80`): the table has ~40 rows")
}

// The direct-execution disclosure copy is keyed by database type: MySQL-family
// databases (including Strata, whose shards are MySQL) disclose MySQL
// semantics, and a database type without registered copy gets the
// conservative engine-neutral disclosure rather than inheriting MySQL's.
func TestDirectDisclosureCopy_KeyedByDatabaseType(t *testing.T) {
	mysqlHeader, mysqlConsequence := directDisclosureCopy("mysql", true)
	assert.Equal(t, "native MySQL DDL, not through Spirit", mysqlHeader)
	assert.Equal(t, "Transactions blocking a table's metadata lock are killed so its statement can take the lock, and writes to each table are blocked until its statement finishes.", mysqlConsequence,
		"MySQL direct statements kill the transactions blocking them, so the disclosure says so")

	strataHeader, strataConsequence := directDisclosureCopy("strata", false)
	assert.Equal(t, mysqlHeader, strataHeader, "Strata shards run the same native MySQL DDL")
	assert.Equal(t, mysqlConsequence, strataConsequence)

	otherHeader, otherConsequence := directDisclosureCopy("postgres", false)
	assert.Equal(t, "native DDL", otherHeader)
	assert.Contains(t, otherConsequence, "Each table is unavailable until its statement finishes")
	assert.Contains(t, otherConsequence, "**not revertible**")
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
			{Table: "users", Reason: "the table has ~40 rows"},
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

func TestRenderBlockedChangesApplyRejectedListsIndependentCauses(t *testing.T) {
	reason := engine.JoinBlockedCauses([]string{
		"planner requires a table rewrite; choose a supported statement",
		"table exceeds the native-safe size ceiling; use an online path",
	})
	out := RenderBlockedChangesApplyRejected(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		BlockedChanges: []BlockedChangeData{{Table: "users", Reason: reason}},
	})

	assert.Contains(t, out, "- `users`: planner requires a table rewrite; choose a supported statement\n  - table exceeds the native-safe size ceiling; use an online path\n")
}

// Every cause is composed from quoted identifiers, so each nested cause is
// Markdown-escaped exactly as the first one is.
func TestRenderBlockedChangesApplyRejectedEscapesEveryCause(t *testing.T) {
	out := RenderBlockedChangesApplyRejected(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		BlockedChanges: []BlockedChangeData{{Table: "users", Reason: engine.JoinBlockedCauses([]string{
			"planner requires a table rewrite; choose a supported statement",
			"table `users` exceeds the *native-safe* size ceiling; use an online path",
		})}},
	})

	assert.Contains(t, out, "- `users`: planner requires a table rewrite; choose a supported statement\n")
	assert.Contains(t, out, "  - table \\`users\\` exceeds the \\*native-safe\\* size ceiling; use an online path\n")
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

// When every target's own plan renders, a direct change is disclosed under the
// targets that run it, naming them when only some of the group does. The
// primary plan's own direct changes move under its group rather than repeat
// plan-wide, and stay plan-wide when its group carries none, so the comment
// never leaves a direct statement undisclosed.
func TestRenderPlanComment_DirectDisclosedPerTargetGroup(t *testing.T) {
	const alter = "ALTER TABLE `users` ADD COLUMN `nickname` varchar(64)"
	direct := DirectChangeData{Table: "users", Reason: "the table has ~1,240 rows"}
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes:       []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{alter}}},
		DirectChanges: []DirectChangeData{direct},
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: true, Independent: true,
			Deployments: []DeploymentDriftEntry{{Deployment: "payments-001"}, {Deployment: "payments-002"}, {Deployment: "payments-003"}},
			Plans: []DeploymentPlanGroup{{
				Members: []string{"payments-001", "payments-002", "payments-003"},
				Primary: true,
				Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{alter}}},
				DirectChanges: []DirectChangeData{{
					Table: "users", Reason: "the table has ~1,240 rows",
					Targets: []string{"payments-001", "payments-003"}, TotalTargets: 3,
				}},
			}},
		},
	}

	out := RenderPlanComment(data)
	assert.Equal(t, 1, strings.Count(out, "**Direct execution**"), "the primary plan's direct change is disclosed once, under its group")
	assert.Contains(t, out, "- `users` on targets `payments-001`, `payments-003`: the table has ~1,240 rows\n",
		"only the targets that run it directly are named")

	data.DeploymentDrift.Plans[0].DirectChanges = nil
	fallback := RenderPlanComment(data)
	assert.Equal(t, 1, strings.Count(fallback, "**Direct execution**"), "a group that carries none leaves the plan-wide disclosure in place")
	assert.Contains(t, fallback, "- `users`: the table has ~1,240 rows\n")
}
