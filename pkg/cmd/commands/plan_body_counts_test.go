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
