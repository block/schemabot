package commands

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProgressRenderingUsesTargetDialect(t *testing.T) {
	resp := &apitypes.ProgressResponse{
		DatabaseType: "postgres",
		Tables: []*apitypes.TableProgressResponse{
			{
				TableName:  "sessions",
				ChangeType: "create",
				Status:     "queued",
				DDL:        "CREATE TABLE sessions (id uuid PRIMARY KEY, payload jsonb)",
			},
		},
	}

	data := templates.ParseProgressResponse(resp)
	require.Len(t, data.Tables, 1)
	assert.Equal(t, schema.DialectPostgres, data.Tables[0].Dialect)

	out := templates.FormatTableProgress(data.Tables[0])
	assert.Contains(t, out, "id uuid")
	assert.Contains(t, out, "payload jsonb")
	assert.NotContains(t, out, "`")
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
