package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/internal/templates"
)

// TestPreviewPlanDisclosuresSelectIndividually verifies each plan-comment
// disclosure preview is selectable on its own, not only inside the "all"
// listings, so an operator checking a rendering can ask for just that one.
func TestPreviewPlanDisclosuresSelectIndividually(t *testing.T) {
	tests := []struct {
		previewType templates.PreviewType
		want        string
	}{
		{templates.PreviewCommentPlanIgnoredNamespaces, "excluded from this plan by `ignore_namespaces`"},
		{templates.PreviewCommentPlanExemptTables, "Ignored tables in namespace"},
		{templates.PreviewCommentPlanUnmanagedSchema, "- `services/inventory/schema` declares database `inventory`"},
		// The reason is engine prose, so it is escaped like any other. An
		// underscore inside a word cannot start emphasis, so it is left as written.
		{templates.PreviewCommentPlanIgnoreTables, "(ignore_tables): `flyway_schema_history`"},
	}
	for _, tt := range tests {
		t.Run(string(tt.previewType), func(t *testing.T) {
			var runErr error
			out := captureStdout(func() {
				cmd := &PreviewCmd{Type: string(tt.previewType)}
				runErr = cmd.Run(&Globals{})
			})
			require.NoError(t, runErr)
			assert.Contains(t, out, tt.want)
		})
	}
}
