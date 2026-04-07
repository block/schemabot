package commands

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
)

// captureOutput captures stdout during fn execution.
func captureOutput(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer utils.CloseAndLog(r)
	defer func() { os.Stdout = old }()

	os.Stdout = w
	fn()
	utils.CloseAndLog(w)

	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	require.NoError(t, err)
	return buf.String()
}

func TestLogfmtNeedsQuoting(t *testing.T) {
	assert.True(t, logfmtNeedsQuoting(""), "empty string needs quoting")
	assert.True(t, logfmtNeedsQuoting("hello world"), "spaces need quoting")
	assert.True(t, logfmtNeedsQuoting("key=val"), "equals needs quoting")
	assert.True(t, logfmtNeedsQuoting(`say "hi"`), "quotes need quoting")
	assert.True(t, logfmtNeedsQuoting("has\\backslash"), "backslash needs quoting")
	assert.True(t, logfmtNeedsQuoting("line\nbreak"), "newline needs quoting")
	assert.False(t, logfmtNeedsQuoting("simple"), "simple string doesn't need quoting")
	assert.False(t, logfmtNeedsQuoting("42%"), "percentage doesn't need quoting")
}

func TestLogfmtEscape(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"no escaping needed", "simple", "simple"},
		{"backslash", `a\b`, `a\\b`},
		{"quote", `say "hi"`, `say \"hi\"`},
		{"newline", "line\nbreak", `line\nbreak`},
		{"carriage return", "line\rbreak", `line\rbreak`},
		{"tab", "col\tcol", `col\tcol`},
		{"mixed", "err: \"bad\"\ndetail\\end", `err: \"bad\"\ndetail\\end`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(logfmtEscape(nil, tt.in))
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLogEmitter_Emit(t *testing.T) {
	t.Run("with apply ID", func(t *testing.T) {
		e := &logEmitter{applyID: "apply-abc123"}
		output := captureOutput(t, func() {
			e.emit("msg", "Test message", "key", "value")
		})

		assert.Contains(t, output, "apply_id=apply-abc123")
		assert.Contains(t, output, "msg=")
		assert.Contains(t, output, "key=value")
	})

	t.Run("without apply ID", func(t *testing.T) {
		e := &logEmitter{}
		output := captureOutput(t, func() {
			e.emit("msg", "Test message")
		})

		assert.NotContains(t, output, "apply_id=")
		assert.Contains(t, output, "msg=")
	})

	t.Run("values with spaces are quoted", func(t *testing.T) {
		e := &logEmitter{}
		output := captureOutput(t, func() {
			e.emit("msg", "Table started", "ddl", "ALTER TABLE `users` ADD COLUMN name VARCHAR(255)")
		})

		assert.Contains(t, output, `ddl="ALTER TABLE`)
	})

	t.Run("timestamp prefix", func(t *testing.T) {
		e := &logEmitter{}
		output := captureOutput(t, func() {
			e.emit("msg", "test")
		})

		// Should start with an RFC3339 timestamp
		parts := strings.SplitN(output, " ", 2)
		require.Len(t, parts, 2)
		_, err := time.Parse(time.RFC3339, parts[0])
		assert.NoError(t, err, "first token should be RFC3339 timestamp, got: %s", parts[0])
	})
}

func TestLogEmitter_EmitTableStateChange(t *testing.T) {
	tests := []struct {
		name       string
		status     string
		pct        int32
		wantMsg    string
		wantFields []string
	}{
		{
			name:       "completed",
			status:     state.Apply.Completed,
			wantMsg:    "Table complete",
			wantFields: []string{"table=users", "task_id=task-abc", "duration="},
		},
		{
			name:       "failed",
			status:     state.Apply.Failed,
			wantMsg:    "Table failed",
			wantFields: []string{"table=users", "task_id=task-abc", "duration="},
		},
		{
			name:       "waiting for cutover",
			status:     state.Apply.WaitingForCutover,
			wantMsg:    "Waiting for cutover",
			wantFields: []string{"table=users", "task_id=task-abc"},
		},
		{
			name:       "cutting over",
			status:     state.Apply.CuttingOver,
			wantMsg:    "Cutting over",
			wantFields: []string{"table=users", "task_id=task-abc"},
		},
		{
			name:       "stopped with progress",
			status:     state.Apply.Stopped,
			pct:        45,
			wantMsg:    "Table stopped",
			wantFields: []string{"table=users", "task_id=task-abc", "progress=45%"},
		},
		{
			name:       "stopped without progress",
			status:     state.Apply.Stopped,
			pct:        0,
			wantMsg:    "Table stopped",
			wantFields: []string{"table=users", "task_id=task-abc"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &logEmitter{applyID: "apply-test"}
			ts := &tableLogState{startedAt: time.Now().Add(-10 * time.Second), taskID: "task-abc"}
			tbl := &apitypes.TableProgressResponse{
				TableName:       "users",
				PercentComplete: tt.pct,
			}

			output := captureOutput(t, func() {
				e.emitTableStateChange(tbl, tt.status, ts)
			})

			assert.Contains(t, output, fmt.Sprintf(`msg="%s"`, tt.wantMsg))
			for _, field := range tt.wantFields {
				assert.Contains(t, output, field)
			}
		})
	}
}

func TestLogEmitter_EmitProgressHeartbeat(t *testing.T) {
	t.Run("with ETA from progress detail", func(t *testing.T) {
		e := &logEmitter{applyID: "apply-test"}
		ts := &tableLogState{taskID: "task-orders-1"}
		tbl := &apitypes.TableProgressResponse{
			TableName:       "orders",
			PercentComplete: 45,
			RowsCopied:      99450,
			RowsTotal:       221000,
			ProgressDetail:  "99450/221000 45.00% copyRows ETA 5m 30s",
		}

		output := captureOutput(t, func() {
			e.emitProgressHeartbeat(tbl, ts)
		})

		assert.Contains(t, output, `msg="Copying rows"`)
		assert.Contains(t, output, "table=orders")
		assert.Contains(t, output, "task_id=task-orders-1")
		assert.Contains(t, output, "progress=45%")
		assert.Contains(t, output, "rows=99,450/221,000")
		assert.Contains(t, output, "eta=")
	})

	t.Run("with ETA from ETASeconds", func(t *testing.T) {
		e := &logEmitter{}
		ts := &tableLogState{}
		tbl := &apitypes.TableProgressResponse{
			TableName:       "products",
			TaskID:          "task-products-1",
			PercentComplete: 20,
			RowsCopied:      10000,
			RowsTotal:       50000,
			ETASeconds:      120,
		}

		output := captureOutput(t, func() {
			e.emitProgressHeartbeat(tbl, ts)
		})

		assert.Contains(t, output, "table=products")
		assert.Contains(t, output, "task_id=task-products-1")
		assert.Contains(t, output, "progress=20%")
		assert.Contains(t, output, "rows=10,000/50,000")
		assert.Contains(t, output, "eta=")
	})

	t.Run("clamps percent to 100", func(t *testing.T) {
		e := &logEmitter{}
		ts := &tableLogState{}
		tbl := &apitypes.TableProgressResponse{
			TableName:       "orders",
			PercentComplete: 105,
			RowsCopied:      230000,
			RowsTotal:       221000,
		}

		output := captureOutput(t, func() {
			e.emitProgressHeartbeat(tbl, ts)
		})

		assert.Contains(t, output, "progress=100%")
	})

	t.Run("no task_id when absent", func(t *testing.T) {
		e := &logEmitter{}
		ts := &tableLogState{}
		tbl := &apitypes.TableProgressResponse{
			TableName:       "config",
			PercentComplete: 50,
			RowsCopied:      500,
			RowsTotal:       1000,
		}

		output := captureOutput(t, func() {
			e.emitProgressHeartbeat(tbl, ts)
		})

		assert.NotContains(t, output, "task_id=")
	})
}

func TestLogEmitter_EmitApplySummary(t *testing.T) {
	t.Run("all succeeded", func(t *testing.T) {
		e := &logEmitter{applyID: "apply-test"}
		tableStates := map[string]*tableLogState{
			"users":    {status: state.Apply.Completed},
			"orders":   {status: state.Apply.Completed},
			"products": {status: state.Apply.Completed},
		}

		output := captureOutput(t, func() {
			e.emitApplySummary("completed", tableStates, time.Now().Add(-2*time.Minute), "")
		})

		assert.Contains(t, output, `msg="Apply completed"`)
		assert.Contains(t, output, "succeeded=3")
		assert.Contains(t, output, "failed=0")
		assert.NotContains(t, output, "stopped=")
		assert.NotContains(t, output, "error=")
	})

	t.Run("mixed results", func(t *testing.T) {
		e := &logEmitter{applyID: "apply-test"}
		tableStates := map[string]*tableLogState{
			"users":  {status: state.Apply.Completed},
			"orders": {status: state.Apply.Failed},
		}

		output := captureOutput(t, func() {
			e.emitApplySummary("failed", tableStates, time.Now().Add(-30*time.Second), "schema change failed: syntax error")
		})

		assert.Contains(t, output, `msg="Apply failed"`)
		assert.Contains(t, output, "succeeded=1")
		assert.Contains(t, output, "failed=1")
		assert.Contains(t, output, `error="schema change failed: syntax error"`)
	})

	t.Run("with stopped tables", func(t *testing.T) {
		e := &logEmitter{}
		tableStates := map[string]*tableLogState{
			"users":    {status: state.Apply.Completed},
			"orders":   {status: state.Apply.Stopped},
			"products": {status: state.Apply.Stopped},
		}

		output := captureOutput(t, func() {
			e.emitApplySummary("stopped", tableStates, time.Now(), "")
		})

		assert.Contains(t, output, `msg="Apply stopped"`)
		assert.Contains(t, output, "succeeded=1")
		assert.Contains(t, output, "stopped=2")
	})
}

func TestIsActiveStatus(t *testing.T) {
	assert.False(t, isActiveStatus(state.Apply.Completed))
	assert.False(t, isActiveStatus(state.Apply.Failed))
	assert.False(t, isActiveStatus(state.Apply.Stopped))
	assert.True(t, isActiveStatus(state.Apply.Running))
	assert.True(t, isActiveStatus(state.Apply.Pending))
	assert.True(t, isActiveStatus(state.Apply.WaitingForCutover))
	assert.True(t, isActiveStatus(state.Apply.CuttingOver))
}

func TestTableKVs(t *testing.T) {
	t.Run("includes task_id from tableLogState", func(t *testing.T) {
		ts := &tableLogState{taskID: "task-123"}
		tbl := &apitypes.TableProgressResponse{TableName: "users"}
		kvs := tableKVs("Test", tbl, ts)
		assert.Equal(t, []string{"msg", "Test", "table", "users", "task_id", "task-123"}, kvs)
	})

	t.Run("falls back to TaskID from response", func(t *testing.T) {
		ts := &tableLogState{}
		tbl := &apitypes.TableProgressResponse{TableName: "users", TaskID: "task-456"}
		kvs := tableKVs("Test", tbl, ts)
		assert.Equal(t, []string{"msg", "Test", "table", "users", "task_id", "task-456"}, kvs)
	})

	t.Run("no task_id when absent everywhere", func(t *testing.T) {
		ts := &tableLogState{}
		tbl := &apitypes.TableProgressResponse{TableName: "users"}
		kvs := tableKVs("Test", tbl, ts)
		assert.Equal(t, []string{"msg", "Test", "table", "users"}, kvs)
	})
}
