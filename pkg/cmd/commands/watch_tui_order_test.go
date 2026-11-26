package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
)

// The watch view lists a failed table first, ahead of the stopped, running,
// queued, and finished ones, and tables in the same state keep the order they
// arrived in, as the terminal and the PR comment list them (UX-5).
func TestSortTablesByProgressLeadsWithAFailedTable(t *testing.T) {
	tables := []templates.TableProgress{
		{TableName: "payments", Status: state.Task.Pending},
		{TableName: "refunds", Status: state.Task.Pending},
		{TableName: "users", Status: state.Task.Completed},
		{TableName: "sessions", Status: state.Task.Running},
		{TableName: "invoices", Status: state.Task.Stopped},
		{TableName: "orders", Status: state.Task.Failed},
	}
	sortTablesByProgress(tables)

	var got []string
	for _, table := range tables {
		got = append(got, table.TableName)
	}
	assert.Equal(t, []string{"orders", "invoices", "sessions", "payments", "refunds", "users"}, got)
}
