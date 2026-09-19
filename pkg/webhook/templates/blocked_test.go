package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/engine"
	"github.com/stretchr/testify/assert"
)

func TestWriteEngineReasonItem(t *testing.T) {
	single := "planner refused the statement; rewrite the column type"
	var sb strings.Builder
	writeEngineReasonItem(&sb, "`users`", single)
	assert.Equal(t, "- `users`: "+single+"\n", sb.String())

	multiple := engine.JoinBlockedCauses([]string{single, "table exceeds the native-safe size ceiling; use an online path"})
	sb.Reset()
	writeEngineReasonItem(&sb, "`users`", multiple)
	assert.Equal(t, "- `users`: "+single+"\n  - table exceeds the native-safe size ceiling; use an online path\n", sb.String())

	// A reason made only of characters the sanitizer strips is empty for
	// rendering purposes: the table line stands alone rather than ending in a
	// colon with nothing after it.
	sb.Reset()
	writeEngineReasonItem(&sb, "`users`", "\u200b\u202e")
	assert.Equal(t, "- `users`\n", sb.String())

	// The same applies per cause: a stripped-to-empty second cause is dropped
	// rather than rendered as an empty nested bullet.
	sb.Reset()
	writeEngineReasonItem(&sb, "`users`", engine.JoinBlockedCauses([]string{single, "\u200b"}))
	assert.Equal(t, "- `users`: "+single+"\n", sb.String())
}

// A statement the engine refuses is disclosed in its own ⛔ section, naming the
// table and the engine's reason verbatim, separate from unsafe warnings. Unlike
// unsafe changes it cannot be acknowledged away, so the section also renders on
// the locked apply comment — the operator must see the guaranteed failure
// before confirming.
func TestRenderPlanComment_BlockedShownOnPlanAndApply(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)"},
		}},
		BlockedChanges: []BlockedChangeData{
			{Table: "users", Reason: "dropping primary key is not supported"},
		},
	}

	plan := RenderPlanComment(data)
	assert.Contains(t, plan, "⛔ **Cannot apply**: 1 change the engine refuses to execute")
	assert.Contains(t, plan, "`users`: dropping primary key is not supported")
	assert.Contains(t, plan, "An apply will fail on these statements.")

	data.IsLocked = true
	apply := RenderPlanComment(data)
	assert.Contains(t, apply, "⛔ **Cannot apply**", "the locked apply comment keeps the blocked disclosure")
	assert.Contains(t, apply, "dropping primary key is not supported")
}

func TestRenderPlanComment_BlockedEscapesReasonMarkdown(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `user_events_v2` DROP PRIMARY KEY"},
		}},
		BlockedChanges: []BlockedChangeData{
			{Table: "user_events_v2", Reason: "table user_events_v2 | column *event_id* is unsupported"},
		},
	})

	assert.Contains(t, out, "table user\\_events\\_v2 \\| column \\*event\\_id\\* is unsupported")
}

func TestRenderPlanComment_BlockedSanitizesReason(t *testing.T) {
	reason := "refused by db-primary.internal:3306\n\n## Injected heading\n- fake item"
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace: "testapp", Statements: []string{"ALTER TABLE `users` DROP PRIMARY KEY"},
		}},
		BlockedChanges: []BlockedChangeData{{Table: "users", Reason: reason}},
	})

	assert.NotContains(t, out, "\n## Injected heading")
	assert.NotContains(t, out, "db-primary.internal:3306")
	assert.Contains(t, out, "- `users`: refused by \\[endpoint redacted\\] ## Injected heading - fake item\n")
}

// A plan adding a foreign key — the most common statement the engine refuses —
// discloses the refusal with the engine's foreign-key reason, alongside any
// other blocked changes in the same section.
func TestRenderPlanComment_BlockedForeignKey(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace: "testapp",
			Statements: []string{
				"ALTER TABLE `orders` ADD CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
			},
		}},
		BlockedChanges: []BlockedChangeData{
			{Table: "orders", Reason: "adding foreign key constraints is not supported"},
		},
	})

	assert.Contains(t, out, "⛔ **Cannot apply**: 1 change the engine refuses to execute")
	assert.Contains(t, out, "`orders`: adding foreign key constraints is not supported")
	assert.Contains(t, out, "An apply will fail on these statements.")
}

// A blocked change confined to specific shards names them, matching the unsafe
// section's shard scoping.
func TestRenderPlanComment_BlockedNamesShards(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp_sharded",
			Statements: []string{"ALTER TABLE `users` DROP PRIMARY KEY"},
		}},
		BlockedChanges: []BlockedChangeData{
			{Table: "users", Reason: "dropping primary key is not supported", Shards: []string{"-40", "40-80"}},
		},
	})

	assert.Contains(t, out, "`users` (shards `-40`, `40-80`): dropping primary key is not supported")
}

// A multi-environment plan renders each environment's own blocked section, so
// an environment whose plan carries a refused statement discloses it in place.
func TestRenderMultiEnvPlanComment_BlockedPerEnvironment(t *testing.T) {
	stagingPlan := &PlanCommentData{
		Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
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

	assert.Contains(t, out, "⛔ **Cannot apply**")
	assert.Contains(t, out, "dropping primary key is not supported")
}
