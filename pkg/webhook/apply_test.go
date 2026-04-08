package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func TestAggregatePercent(t *testing.T) {
	tests := []struct {
		name   string
		tasks  []*storage.Task
		expect int
	}{
		{
			name:   "empty tasks",
			tasks:  nil,
			expect: 0,
		},
		{
			name: "single task",
			tasks: []*storage.Task{
				{ProgressPercent: 50},
			},
			expect: 50,
		},
		{
			name: "multiple tasks average",
			tasks: []*storage.Task{
				{ProgressPercent: 100},
				{ProgressPercent: 50},
				{ProgressPercent: 0},
			},
			expect: 50,
		},
		{
			name: "all complete",
			tasks: []*storage.Task{
				{ProgressPercent: 100},
				{ProgressPercent: 100},
			},
			expect: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, aggregatePercent(tt.tasks))
		})
	}
}

func TestFormatProgressComment(t *testing.T) {
	apply := &storage.Apply{
		Database:    "testdb",
		Environment: "staging",
		Engine:      "spirit",
		State:       state.Apply.Running,
	}

	tasks := []*storage.Task{
		{
			TableName:       "users",
			DDL:             "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			State:           state.Task.Running,
			ProgressPercent: 45,
			ETASeconds:      120,
		},
		{
			TableName: "orders",
			DDL:       "ALTER TABLE orders ADD INDEX idx_status (status)",
			State:     state.Task.Pending,
			IsInstant: true,
		},
	}

	body := formatProgressComment(apply, tasks)

	assert.Contains(t, body, "testdb")
	assert.Contains(t, body, "staging")
	assert.Contains(t, body, "spirit")
	assert.Contains(t, body, "`running`")
	assert.Contains(t, body, "`users`")
	assert.Contains(t, body, "`orders`")
	assert.Contains(t, body, "45%")
	assert.Contains(t, body, "instant")
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
	assert.Contains(t, body, "`pending`")
	assert.NotContains(t, body, "Table")
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
}

func TestFormatCutoverComment(t *testing.T) {
	apply := &storage.Apply{
		Database:    "testdb",
		Environment: "production",
		State:       state.Apply.WaitingForCutover,
	}

	tasks := []*storage.Task{
		{TableName: "users", ReadyToComplete: true},
		{TableName: "orders", ReadyToComplete: true},
		{TableName: "items", ReadyToComplete: false},
	}

	body := formatCutoverComment(apply, tasks)

	assert.Contains(t, body, "Cutover Ready")
	assert.Contains(t, body, "testdb")
	assert.Contains(t, body, "2/3 table(s) ready for cutover")
	assert.Contains(t, body, "schemabot cutover -e production")
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

	assert.Contains(t, body, "Schema Change Complete")
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

func TestTruncateDDL(t *testing.T) {
	tests := []struct {
		name   string
		ddl    string
		maxLen int
		expect string
	}{
		{
			name:   "short DDL",
			ddl:    "ALTER TABLE users ADD COLUMN x INT",
			maxLen: 60,
			expect: "ALTER TABLE users ADD COLUMN x INT",
		},
		{
			name:   "long DDL truncated",
			ddl:    "ALTER TABLE users ADD COLUMN very_long_column_name VARCHAR(255) NOT NULL DEFAULT ''",
			maxLen: 40,
			expect: "ALTER TABLE users ADD COLUMN very_lon...",
		},
		{
			name:   "multiline DDL takes first line",
			ddl:    "ALTER TABLE users\nADD COLUMN x INT",
			maxLen: 60,
			expect: "ALTER TABLE users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, truncateDDL(tt.ddl, tt.maxLen))
		})
	}
}

func TestFormatETA(t *testing.T) {
	tests := []struct {
		seconds int
		expect  string
	}{
		{30, "30s"},
		{90, "1m"},
		{3700, "1h 1m"},
	}

	for _, tt := range tests {
		t.Run(tt.expect, func(t *testing.T) {
			assert.Equal(t, tt.expect, formatETA(tt.seconds))
		})
	}
}

func TestFormatTaskProgress(t *testing.T) {
	tests := []struct {
		name   string
		task   *storage.Task
		expect string
	}{
		{
			name:   "instant",
			task:   &storage.Task{IsInstant: true},
			expect: "instant",
		},
		{
			name:   "completed",
			task:   &storage.Task{State: state.Task.Completed},
			expect: "done",
		},
		{
			name:   "with percent and ETA",
			task:   &storage.Task{ProgressPercent: 42, ETASeconds: 300},
			expect: "42% (ETA 5m)",
		},
		{
			name:   "with percent no ETA",
			task:   &storage.Task{ProgressPercent: 10},
			expect: "10%",
		},
		{
			name:   "no progress",
			task:   &storage.Task{},
			expect: "-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, formatTaskProgress(tt.task))
		})
	}
}

func TestStateEmoji(t *testing.T) {
	// Just verify each state returns a non-empty string
	states := []string{
		state.Apply.Completed,
		state.Apply.Failed,
		state.Apply.Stopped,
		state.Apply.Reverted,
		state.Apply.Running,
		state.Apply.Pending,
	}

	for _, s := range states {
		assert.NotEmpty(t, stateEmoji(s), "stateEmoji(%q) should not be empty", s)
	}
}
