package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func TestFormatProgressComment(t *testing.T) {
	apply := &storage.Apply{
		ApplyIdentifier: "apply-abc123",
		Database:        "testdb",
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Running,
	}

	tasks := []*storage.Task{
		{
			TableName:       "users",
			DDL:             "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			State:           state.Task.Running,
			RowsCopied:      45000,
			RowsTotal:       100000,
			ProgressPercent: 45,
			ETASeconds:      120,
		},
		{
			TableName: "orders",
			DDL:       "ALTER TABLE orders ADD INDEX idx_status (status)",
			State:     state.Task.Pending,
		},
	}

	body := formatProgressComment(apply, tasks)

	// Template renders rich progress bars instead of a table
	assert.Contains(t, body, "testdb")
	assert.Contains(t, body, "staging")
	assert.Contains(t, body, "`users`")
	assert.Contains(t, body, "`orders`")
	assert.Contains(t, body, "45%")
	assert.Contains(t, body, "In Progress")
	// Should NOT contain the old table format
	assert.NotContains(t, body, "| Table |")
	assert.NotContains(t, body, "|-------|")
}

func TestFormatProgressComment_NoTasks(t *testing.T) {
	apply := &storage.Apply{
		Database:    "testdb",
		Environment: "staging",
		Engine:      "spirit",
		State:       state.Apply.Pending,
	}

	body := formatProgressComment(apply, nil)

	assert.Contains(t, body, "testdb")
	assert.Contains(t, body, "Starting")
}

func TestFormatProgressComment_WithError(t *testing.T) {
	apply := &storage.Apply{
		Database:     "testdb",
		Environment:  "staging",
		Engine:       "spirit",
		State:        state.Apply.Failed,
		ErrorMessage: "connection refused",
	}

	body := formatProgressComment(apply, nil)

	assert.Contains(t, body, "connection refused")
	assert.Contains(t, body, "Failed")
}

func TestCommentObserverShouldDeferCutoverUsesPersistedApplyOption(t *testing.T) {
	apply := &storage.Apply{
		Options: storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
	}
	observer := &CommentObserver{}

	assert.True(t, observer.shouldDeferCutover(apply))
}

func TestFormatCutoverComment(t *testing.T) {
	apply := &storage.Apply{
		ApplyIdentifier: "apply-abc123",
		Database:        "testdb",
		Environment:     "production",
		State:           state.Apply.WaitingForCutover,
	}

	tasks := []*storage.Task{
		{TableName: "users", State: state.Task.WaitingForCutover, ReadyToComplete: true},
		{TableName: "orders", State: state.Task.WaitingForCutover, ReadyToComplete: true},
	}

	body := formatCutoverComment(apply, tasks)

	assert.Contains(t, body, "Cutover")
	assert.Contains(t, body, "testdb")
	assert.Contains(t, body, "`users`")
	assert.Contains(t, body, "`orders`")
}

func TestFormatSummaryComment(t *testing.T) {
	apply := &storage.Apply{
		Database:    "testdb",
		Environment: "staging",
		State:       state.Apply.Completed,
	}

	tasks := []*storage.Task{
		{TableName: "users", State: state.Task.Completed},
		{TableName: "orders", State: state.Task.Completed},
	}

	body := formatSummaryComment(apply, tasks)

	assert.Contains(t, body, "Schema Change Applied")
	assert.Contains(t, body, "`users`")
	assert.Contains(t, body, "`orders`")
	assert.Contains(t, body, "applied successfully")
}

func TestFormatSummaryComment_Failed(t *testing.T) {
	apply := &storage.Apply{
		Database:     "testdb",
		Environment:  "staging",
		State:        state.Apply.Failed,
		ErrorMessage: "schema change failed: duplicate column",
	}

	tasks := []*storage.Task{
		{TableName: "users", State: state.Task.Failed},
	}

	body := formatSummaryComment(apply, tasks)

	assert.Contains(t, body, "duplicate column")
	assert.Contains(t, body, "Failed")
	assert.Contains(t, body, "To retry")
}

func TestBuildApplyCommentData(t *testing.T) {
	apply := &storage.Apply{
		ApplyIdentifier: "apply-abc123",
		Database:        "testdb",
		Environment:     "staging",
		State:           state.Apply.Running,
		Engine:          "spirit",
	}

	tasks := []*storage.Task{
		{
			TableName:       "users",
			Namespace:       "myns",
			DDL:             "ALTER TABLE users ADD COLUMN x INT",
			State:           state.Task.Running,
			RowsCopied:      500,
			RowsTotal:       1000,
			ProgressPercent: 50,
			ETASeconds:      60,
			IsInstant:       false,
		},
		{
			TableName: "orders",
			DDL:       "CREATE TABLE orders (...)",
			State:     state.Task.Pending,
			IsInstant: true,
		},
	}

	data := buildApplyCommentData(apply, tasks)

	assert.Equal(t, "apply-abc123", data.ApplyID)
	assert.Equal(t, "testdb", data.Database)
	assert.Equal(t, "staging", data.Environment)
	assert.Equal(t, state.Apply.Running, data.State)
	assert.Len(t, data.Tables, 2)
	assert.Equal(t, "myns", data.Tables[0].Namespace)
	assert.Equal(t, int64(500), data.Tables[0].RowsCopied)
	assert.Equal(t, int64(60), data.Tables[0].ETASeconds)
	assert.True(t, data.Tables[1].IsInstant)
}

func TestBuildApplyCommentData_DefaultNamespace(t *testing.T) {
	apply := &storage.Apply{
		Database: "testdb",
		State:    state.Apply.Running,
	}
	tasks := []*storage.Task{
		{TableName: "users", State: state.Task.Running},
	}

	data := buildApplyCommentData(apply, tasks)

	assert.Equal(t, "testdb", data.Tables[0].Namespace, "empty namespace should default to database name")
}
