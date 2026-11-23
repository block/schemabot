package templates

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/presentation"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

// The CLI and the PR plan comment say the same sentence about a drop_tables
// entry that names no table, with the same glyph (UX-5). Only the quoting
// differs: the comment sets the key and the entry in code spans, the terminal
// quotes the entry.
func TestUnmatchedDropTablesSurfacesSaySameSentence(t *testing.T) {
	entries := []string{"_events_olds", "_Events_Old"}

	cli := captureStdout(t, func() { WriteUnmatchedDropTables(entries) })
	comment := webhooktemplates.RenderPlanComment(webhooktemplates.PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true, DatabaseType: "mysql",
		UnmatchedDropTables: entries,
	})

	for _, entry := range entries {
		sentence := "drop_tables entry " + entry + " names no table on the target, so this plan drops nothing for it. Fix the name, or remove the entry if the table is already gone."
		assert.Contains(t, strings.ReplaceAll(cli, `"`, ""), "⚠️  "+sentence+"\n")
		assert.Contains(t, strings.ReplaceAll(comment, "`", ""), "⚠️ "+sentence+"\n")
	}
}

// A list past the limit names the same first entries and counts the rest in
// the same sentence on both surfaces.
func TestManyUnmatchedDropTablesSurfacesCountTheSameRest(t *testing.T) {
	var entries []string
	for i := range presentation.UnmatchedDropTablesListLimit + 3 {
		entries = append(entries, fmt.Sprintf("_events_old_%02d", i))
	}

	cli := captureStdout(t, func() { WriteUnmatchedDropTables(entries) })
	comment := webhooktemplates.RenderPlanComment(webhooktemplates.PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true, DatabaseType: "mysql",
		UnmatchedDropTables: entries,
	})

	more := "3 more drop_tables entries name no table on the target, so this plan drops nothing for them. Fix the names, or remove the entries whose tables are already gone."
	assert.Contains(t, cli, "⚠️  "+more+"\n")
	assert.Contains(t, strings.ReplaceAll(comment, "`", ""), "⚠️ "+more+"\n")
	last := entries[presentation.UnmatchedDropTablesListLimit-1]
	assert.Contains(t, cli, last)
	assert.Contains(t, comment, last)
	assert.NotContains(t, cli, entries[presentation.UnmatchedDropTablesListLimit])
	assert.NotContains(t, comment, entries[presentation.UnmatchedDropTablesListLimit])
}

// Nothing is written when every entry matched.
func TestWriteUnmatchedDropTablesWritesNothingWhenEveryEntryMatched(t *testing.T) {
	assert.Empty(t, captureStdout(t, func() { WriteUnmatchedDropTables(nil) }))
}
