package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
)

func alterUsersIn(namespace, column string) *apitypes.TableChangeResponse {
	return &apitypes.TableChangeResponse{
		TableName:  "users",
		Namespace:  namespace,
		DDL:        "ALTER TABLE users ADD COLUMN " + column + " INT",
		ChangeType: "ALTER",
	}
}

// The same table name in two namespaces is two tables to alter: the summary
// keys a table by its namespace, not by its bare name.
func TestWritePlanBody_CountsATableOncePerNamespace(t *testing.T) {
	plan := &apitypes.PlanResponse{
		Database: "app",
		Changes: []*apitypes.SchemaChangeResponse{
			{Namespace: "app_a", TableChanges: []*apitypes.TableChangeResponse{alterUsersIn("app_a", "a")}},
			{Namespace: "app_b", TableChanges: []*apitypes.TableChangeResponse{alterUsersIn("app_b", "b")}},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Contains(t, out, "📋 Plan: 2 tables to alter", "%s", out)
}

// A sharded plan is summarized from what its shards change, the set the DDL
// block shows and the PR plan comment counts (UX-6): a shard that creates a
// table its sibling alters is a table to create and a table to alter, not the
// one entry the namespace-level changes collapse it to.
func TestWritePlanBody_CountsWhatADivergentShardAdds(t *testing.T) {
	createUsers := &apitypes.TableChangeResponse{TableName: "users", DDL: "CREATE TABLE users (id BIGINT)", ChangeType: "CREATE"}
	alterUsers := alterUsersIn("", "email")
	plan := &apitypes.PlanResponse{
		Database: "commerce",
		Engine:   "planetscale",
		Changes: []*apitypes.SchemaChangeResponse{
			{Namespace: "commerce", TableChanges: []*apitypes.TableChangeResponse{createUsers}},
		},
		Shards: []*apitypes.ShardPlanResponse{
			{Namespace: "commerce", Shard: "-80", Changes: []*apitypes.TableChangeResponse{createUsers}},
			{Namespace: "commerce", Shard: "80-", Changes: []*apitypes.TableChangeResponse{alterUsers}},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Contains(t, out, "📋 Plan: 1 table to create, 1 table to alter", "%s", out)
	assert.Contains(t, out, "ADD COLUMN email", "the divergent shard's statement is shown, so it is counted")
}

// A plan whose only work is a finalize the engine asked for is not a clean
// plan: the CLI names the keyspace, says the finalize runs after its DDL, and
// counts it in the summary instead of reporting no schema changes.
func TestWritePlanBody_FinalizeOnlyPlanIsNotClean(t *testing.T) {
	plan := &apitypes.PlanResponse{
		Database: "commerce",
		Engine:   "strata",
		Changes: []*apitypes.SchemaChangeResponse{
			{Namespace: "payments", Metadata: map[string]string{apitypes.NeedsFinalizerMetadataKey: "true"}},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.NotContains(t, out, "No schema changes detected", "%s", out)
	assert.Contains(t, out, "payments", "%s", out)
	assert.Contains(t, out, "~ Finalized by the engine once every shard's DDL has landed", "%s", out)
	assert.Contains(t, out, "1 keyspace to finalize", "%s", out)
}

// A Strata keyspace changes its VSchema and adds a table, and its engine also
// asks to finalize it. The CLI hides the VSchema section for this engine, so it
// shows the finalize line instead, and the keyspace is not rendered as its
// header and DDL alone. It is counted once, as a VSchema change.
func TestWritePlanBody_FinalizeLineShowsForAVSchemaChangingKeyspace(t *testing.T) {
	plan := &apitypes.PlanResponse{
		Database: "commerce",
		Engine:   "strata",
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace:    "payments",
			TableChanges: []*apitypes.TableChangeResponse{{TableName: "refunds", ChangeType: "create", DDL: "CREATE TABLE `refunds` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"}},
			Metadata: map[string]string{
				apitypes.VSchemaChangedMetadataKey: "true",
				apitypes.NeedsFinalizerMetadataKey: "true",
			},
		}},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Contains(t, out, "~ Finalized by the engine once every shard's DDL has landed", "%s", out)
	assert.Contains(t, out, "1 VSchema change", "%s", out)
	assert.NotContains(t, out, "to finalize", "%s", out)
}
