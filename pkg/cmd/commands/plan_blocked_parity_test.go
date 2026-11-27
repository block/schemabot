package commands

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/presentation"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

const eventsTooLarge = "the table is 1.4 GiB with its indexes, over the 1.0 GiB limit SchemaBot sets for a native-safe apply, not a PostgreSQL limit; raise postgres.native_safe_table_size_limit_bytes to change a table this size, then plan again"

// eventsNotNullPlan makes events.kind NOT NULL on a table past the size limit:
// four statements, each refused for the same reason.
func eventsNotNullPlan() *apitypes.PlanResponse {
	var steps []*apitypes.TableChangeResponse
	for _, ddl := range []string{
		`ALTER TABLE "public"."events" ADD CONSTRAINT "events_kind_not_null_check" CHECK ("kind" IS NOT NULL) NOT VALID;`,
		`ALTER TABLE "public"."events" VALIDATE CONSTRAINT "events_kind_not_null_check";`,
		`ALTER TABLE "public"."events" ALTER COLUMN "kind" SET NOT NULL;`,
		`ALTER TABLE "public"."events" DROP CONSTRAINT "events_kind_not_null_check";`,
	} {
		steps = append(steps, &apitypes.TableChangeResponse{
			TableName: "events", DDL: ddl, ChangeType: "alter",
			ExecutionMode: engine.ExecutionModeBlocked, ModeReason: eventsTooLarge,
		})
	}
	plan := planWithTablesAndEngine("postgres", steps...)
	plan.DatabaseType = "postgres"
	return plan
}

// A plan the engine refuses reads the same on both surfaces: the CLI plan and
// the PR plan comment name the same table, the same count of refused
// statements, the same reason, and the same next step. The CLI plan closes on
// that verdict, after its summary line, so a refused plan never ends reading
// as one that applies.
func TestBlockedPlanReadsTheSameInTheCLIAndThePRComment(t *testing.T) {
	plan := eventsNotNullPlan()

	cli := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	comment := webhooktemplates.RenderPlanComment(webhooktemplates.PlanCommentData{
		Database: "testapp", Environment: "staging", DatabaseType: "postgres",
		Changes:        []webhooktemplates.KeyspaceChangeData{{Keyspace: "public", Statements: []string{plan.Changes[0].TableChanges[0].DDL}}},
		BlockedChanges: webhooktemplates.BlockedChangesData(plan),
	})

	heading := "Cannot apply: 4 changes the engine refuses to execute"
	assert.Contains(t, cli, "⛔ "+heading+"\n  - events (4 steps): "+eventsTooLarge+"\n", cli)
	assert.Contains(t, strings.ReplaceAll(comment, "**", ""), "⛔ "+heading+"\n", comment)
	assert.Contains(t, comment, "`events` (4 steps)", comment)
	assert.Contains(t, comment, "raise postgres.native_safe_table_size_limit_bytes", comment)
	assert.Contains(t, cli, presentation.BlockedChangesRemedy, cli)
	assert.Contains(t, comment, presentation.BlockedChangesRemedy, comment)

	summaryAt := strings.Index(cli, "📋 Plan: 1 table to alter")
	verdictAt := strings.Index(cli, "⛔ "+heading)
	require.NotEqual(t, -1, summaryAt, cli)
	assert.Greater(t, verdictAt, summaryAt, "the verdict follows the summary:\n%s", cli)
	assert.Equal(t, presentation.BlockedChangesRemedy, lastLine(cli), "the plan ends on what to do next:\n%s", cli)
}

// apply's preview of a refused plan ends on the same verdict, so the refusal
// that follows it repeats what the preview already showed.
func TestApplyPreviewOfABlockedPlanEndsOnTheVerdict(t *testing.T) {
	cli := stripAnsi(captureStdout(func() { writePlanBody(eventsNotNullPlan(), true) }))
	assert.Contains(t, cli, "⛔ Cannot apply: 4 changes the engine refuses to execute\n  - events (4 steps): ", cli)
	assert.Equal(t, presentation.BlockedChangesRemedy, lastLine(cli), cli)
}

// A change refused on one shard of a sharded plan names that shard on both
// surfaces.
func TestBlockedChangeOnOneShardNamesTheShardOnBothSurfaces(t *testing.T) {
	refused := &apitypes.TableChangeResponse{
		TableName: "orders", DDL: `ALTER TABLE "public"."orders" ADD COLUMN "note" text;`, ChangeType: "alter",
		ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "SchemaBot lacks a grant it needs",
	}
	runs := &apitypes.TableChangeResponse{TableName: "orders", DDL: refused.DDL, ChangeType: "alter"}
	plan := planWithTablesAndEngine("postgres", runs)
	plan.Shards = []*apitypes.ShardPlanResponse{
		{Namespace: "public", Shard: "-80", Changes: []*apitypes.TableChangeResponse{runs}},
		{Namespace: "public", Shard: "80-", Changes: []*apitypes.TableChangeResponse{refused}},
	}

	cli := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	comment := webhooktemplates.RenderPlanComment(webhooktemplates.PlanCommentData{
		Database: "testapp", Environment: "staging", DatabaseType: "postgres",
		Changes:        []webhooktemplates.KeyspaceChangeData{{Keyspace: "public", Statements: []string{refused.DDL}}},
		BlockedChanges: webhooktemplates.BlockedChangesData(plan),
	})

	assert.Contains(t, cli, "⛔ Cannot apply: 1 change the engine refuses to execute\n  - orders (shard 80-): SchemaBot lacks a grant it needs\n", cli)
	assert.Contains(t, comment, "`orders` (shard `80-`)", comment)
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	return lines[len(lines)-1]
}
