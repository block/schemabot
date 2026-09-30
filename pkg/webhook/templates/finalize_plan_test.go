package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A keyspace's engine asks to finalize it after its DDL, with no VSchema
// document in the plan. The plan comment counts the finalize as a change, so
// a plan made only of it never reads as "No schema changes", and names the
// keyspace with a line saying when the finalize runs.
func TestRenderPlanComment_FinalizeOnlyKeyspaceIsAChange(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "payments", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{
			{Keyspace: "payments", Finalize: true},
			{Keyspace: "ledger"},
		},
	})

	assert.NotContains(t, out, "No schema changes")
	assert.Contains(t, out, "📋 **Plan**: **1** keyspace to finalize")
	assert.Contains(t, out, keyspaceFinalizeNote)
	assert.NotContains(t, out, "`ledger`", "a keyspace with nothing to do is not listed")
}

// A keyspace that adds a table and is finalized afterward shows only its DDL:
// the finalize is part of that work, so it gets no line or count of its own.
// A keyspace whose only work is a finalize still gets both.
func TestRenderPlanComment_FinalizeBesideDDLShowsOnlyTheDDL(t *testing.T) {
	stmt := "CREATE TABLE `refunds` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
	out := RenderPlanComment(PlanCommentData{
		Database: "payments", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{
			{Keyspace: "payments", Statements: []string{stmt}, Finalize: true},
			{Keyspace: "ledger", Statements: []string{stmt}},
		},
	})

	assert.Contains(t, out, "📋 **Plan**: **2** tables to create\n")
	assert.NotContains(t, out, keyspaceFinalizeNote)

	mixed := RenderPlanComment(PlanCommentData{
		Database: "payments", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{
			{Keyspace: "payments", Statements: []string{stmt}, Finalize: true},
			{Keyspace: "ledger", Finalize: true},
		},
	})

	assert.Contains(t, mixed, "📋 **Plan**: **1** table to create, **1** keyspace to finalize\n")
	assert.Equal(t, 1, strings.Count(mixed, keyspaceFinalizeNote), "only the keyspace whose only work is a finalize carries the line")
	assert.Less(t, strings.Index(mixed, "`ledger`"), strings.Index(mixed, keyspaceFinalizeNote))
}

// A keyspace whose VSchema changes is shown as a VSchema update even when its
// engine also asks to finalize it: the VSchema section already says the
// finalizer runs, so the finalize is not counted or noted twice.
func TestRenderPlanComment_VSchemaChangeSubsumesFinalize(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "payments", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{{
			Keyspace: "payments", VSchemaChanged: true, VSchemaDiff: `{"tables": {"refunds": {}}}`, Finalize: true,
		}},
	})

	assert.Contains(t, out, "📋 **Plan**: **1** vschema update\n")
	assert.NotContains(t, out, keyspaceFinalizeNote)
}

// The aggregate check's Change column reads the same count the comment does.
func TestSummarizeChanges_CountsFinalizes(t *testing.T) {
	stmt := "CREATE TABLE `refunds` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
	assert.Equal(t, "1 keyspace to finalize", SummarizeChanges(PlanCommentData{
		DatabaseType: "strata",
		Changes:      []KeyspaceChangeData{{Keyspace: "payments", Finalize: true}},
	}))
	assert.Equal(t, "1 create · 1 keyspace to finalize", SummarizeChanges(PlanCommentData{
		DatabaseType: "strata",
		Changes: []KeyspaceChangeData{
			{Keyspace: "payments", Statements: []string{stmt}, Finalize: true},
			{Keyspace: "ledger", Finalize: true},
		},
	}))
}

// A sharded keyspace carries its DDL per shard, so a finalize beside that DDL
// gets no line of its own. A sharded keyspace whose every shard already
// matches has no DDL to show, so its finalize keeps its line and count and the
// keyspace is never an empty header.
func TestRenderPlanComment_ShardedKeyspaceFinalize(t *testing.T) {
	stmt := "CREATE TABLE `refunds` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
	ledger := KeyspaceChangeData{Keyspace: "ledger", Statements: []string{stmt}}

	withDDL := RenderPlanComment(PlanCommentData{
		Database: "payments", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{{Keyspace: "payments", Finalize: true, Shards: []KeyspaceShardChange{
			{Shard: "-80", Statements: []string{stmt}},
			{Shard: "80-", Statements: []string{stmt}},
		}}, ledger},
	})
	assert.NotContains(t, withDDL, keyspaceFinalizeNote)
	assert.Contains(t, withDDL, "📋 **Plan**: **2** tables to create\n")

	satisfied := RenderPlanComment(PlanCommentData{
		Database: "payments", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{{Keyspace: "payments", Finalize: true, Shards: []KeyspaceShardChange{
			{Shard: "-80", Satisfied: true},
			{Shard: "80-", Satisfied: true},
		}}, ledger},
	})
	assert.Contains(t, satisfied, "#### Keyspace: `payments`\n"+keyspaceFinalizeNote)
	assert.Contains(t, satisfied, "📋 **Plan**: **1** table to create, **1** keyspace to finalize\n")
}
