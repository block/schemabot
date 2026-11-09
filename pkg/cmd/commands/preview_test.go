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
		// The reason is engine prose, so it is escaped like any other: the
		// underscore renders as itself and cannot start emphasis.
		{templates.PreviewCommentPlanIgnoreTables, `(ignore\_tables): ` + "`flyway_schema_history`"},
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

// A live TUI preview replays a scenario that never ends, so pressing Esc is
// how the preview finishes. That is a success for the preview, not an operator
// leaving an apply behind, so it exits 0; any other watch result is kept.
func TestPreviewTUI_LeavingTheLivePreviewSucceeds(t *testing.T) {
	assert.NoError(t, previewWatchExit(detachedFromWatch()))
	assert.NoError(t, previewWatchExit(nil))
	assert.Equal(t, 1, ExitCodeFor(previewWatchExit(ErrSilent)), "a watch that could not follow the scenario still fails")
}
