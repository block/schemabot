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

// A keyspace that adds a table and is finalized afterward shows its DDL and the
// finalize line, and the summary counts both.
func TestRenderPlanComment_FinalizeAlongsideDDL(t *testing.T) {
	stmt := "CREATE TABLE `refunds` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
	out := RenderPlanComment(PlanCommentData{
		Database: "payments", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{
			{Keyspace: "payments", Statements: []string{stmt}, Finalize: true},
			{Keyspace: "ledger", Statements: []string{stmt}},
		},
	})

	assert.Contains(t, out, "📋 **Plan**: **2** tables to create, **1** keyspace to finalize")
	assert.Equal(t, 1, strings.Count(out, keyspaceFinalizeNote), "only the finalized keyspace carries the line")
	assert.Less(t, strings.Index(out, "`payments`"), strings.Index(out, keyspaceFinalizeNote))
	assert.Less(t, strings.Index(out, keyspaceFinalizeNote), strings.Index(out, "`ledger`"))
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
	assert.Equal(t, "1 create · 2 keyspaces to finalize", SummarizeChanges(PlanCommentData{
		DatabaseType: "strata",
		Changes: []KeyspaceChangeData{
			{Keyspace: "payments", Statements: []string{stmt}, Finalize: true},
			{Keyspace: "ledger", Finalize: true},
		},
	}))
}
