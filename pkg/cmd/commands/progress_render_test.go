package commands

import (
	"fmt"
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProgressRenderingUsesTargetDialect verifies watched applies preserve the
// target database's identifier quoting in operator-facing DDL.
func TestProgressRenderingUsesTargetDialect(t *testing.T) {
	const rawDDL = "ALTER TABLE sessions ADD COLUMN email text, ADD COLUMN age int"
	tests := []struct {
		name         string
		databaseType string
		dialect      schema.Dialect
		expectedDDL  string
		quoted       bool
	}{
		{
			name:         "Postgres identifiers remain unquoted",
			databaseType: "postgres",
			dialect:      schema.DialectPostgres,
			expectedDDL:  "ALTER TABLE sessions\n    ADD COLUMN email text,\n    ADD COLUMN age int;",
		},
		{
			name:         "MySQL identifiers use backticks",
			databaseType: "mysql",
			dialect:      schema.DialectMySQL,
			expectedDDL:  "ALTER TABLE `sessions`\n    ADD COLUMN `email` text,\n    ADD COLUMN `age` int;",
			quoted:       true,
		},
		{
			name:         "missing database type preserves SQL",
			databaseType: "",
			dialect:      schema.Dialect(""),
			expectedDDL:  rawDDL + ";",
		},
		{
			name:         "unrecognized database type preserves SQL",
			databaseType: "spanner",
			dialect:      schema.Dialect("spanner"),
			expectedDDL:  rawDDL + ";",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &apitypes.ProgressResponse{
				DatabaseType: tt.databaseType,
				Tables: []*apitypes.TableProgressResponse{{
					TableName:  "sessions",
					ChangeType: "alter",
					Status:     "queued",
					DDL:        rawDDL,
				}},
			}

			data := templates.ParseProgressResponse(resp)
			require.Len(t, data.Tables, 1)
			assert.Equal(t, tt.dialect, data.Tables[0].Dialect)
			if tt.expectedDDL != "" {
				assert.Equal(t, tt.expectedDDL, ddl.FormatDDLForDialect(data.Tables[0].Dialect, rawDDL))
			}

			out := templates.FormatTableProgress(data.Tables[0])
			if tt.quoted {
				assert.Contains(t, out, "`sessions`")
				assert.Contains(t, out, "`email`")
			} else {
				assert.Contains(t, out, "sessions")
				assert.Contains(t, out, "email")
				assert.NotContains(t, out, "`")
			}
		})
	}
}

// TestProgressRenderingNormalizesRawEngineStatuses pins the operator-facing
// rendering that `status`, `progress`, and the watch TUI produce for the raw
// engine/storage statuses an API response carries. All three commands feed the
// response through templates.ParseProgressResponse and the shared table
// formatter, so this guards the normalization boundary at the command layer:
// raw statuses must render as named phases, not leak through as raw strings.
func TestProgressRenderingNormalizesRawEngineStatuses(t *testing.T) {
	resp := &apitypes.ProgressResponse{
		State: "running",
		Tables: []*apitypes.TableProgressResponse{
			{TableName: "done_tbl", Status: "complete", RowsCopied: 200, RowsTotal: 200},
			{TableName: "verify_tbl", Status: "checksum"},
			{TableName: "cutover_tbl", Status: "ready_to_complete", RowsCopied: 100, RowsTotal: 100},
			{TableName: "queued_tbl", Status: "queued"},
		},
	}

	data := templates.ParseProgressResponse(resp)

	var b strings.Builder
	for _, tbl := range data.Tables {
		b.WriteString(templates.FormatTableProgress(tbl))
	}
	out := b.String()

	assert.Contains(t, out, "✓ Complete")
	assert.Contains(t, out, "Checksumming to verify data")
	assert.Contains(t, out, "Waiting for cutover")
	assert.Contains(t, out, "⏳ Queued")

	// The raw statuses themselves must not leak into the rendering.
	assert.NotContains(t, out, "Status: complete")
	assert.NotContains(t, out, "ready_to_complete")
	assert.NotContains(t, out, "Status: queued")
}

// A view explains every paced table while offering the shared reference once,
// including namespaced and multi-deployment output in both status and watch.
func TestProgressThrottleReferenceOncePerView(t *testing.T) {
	for _, namespace := range []string{"", "shop"} {
		for _, multi := range []bool{false, true} {
			name := fmt.Sprintf("namespace=%s/multi=%t", namespace, multi)
			t.Run(name, func(t *testing.T) {
				tables := make([]templates.TableProgress, 8)
				for i := range tables {
					tables[i] = templates.TableProgress{TableName: fmt.Sprintf("orders_%d", i), Namespace: namespace, Deployment: "primary", Status: state.Task.Running, Throttled: true, ThrottleReason: "commit-latency 120ms >= 100ms", RowsTotal: 100, RowsCopied: 50, PercentComplete: 50}
				}
				var operations []templates.ProgressOperation
				if multi {
					tables[len(tables)-1].Deployment = "secondary"
					operations = []templates.ProgressOperation{{Deployment: "primary", State: state.ApplyOperation.Running}, {Deployment: "secondary", State: state.ApplyOperation.Running}}
				}
				data := templates.ProgressData{State: state.Apply.Running, Tables: tables, Operations: operations}
				model := WatchModel{initialized: true, state: state.Apply.Running, tables: tables, operations: operations}
				outputs := []string{captureOutput(t, func() { templates.WriteProgress(data) }), model.View()}
				for _, output := range outputs {
					assert.Equal(t, 1, strings.Count(output, ui.ThrottleDocURL), output)
					assert.Equal(t, 8, strings.Count(output, "Throttled: commit-latency"), output)
				}
			})
		}
	}
}
