package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/internal/templates"
)

// TestPreviewPlanDisclosuresSelectIndividually verifies the two plan-comment
// disclosure previews are selectable on their own, not only inside the "all"
// listings, so an operator checking a rendering can ask for just that one.
func TestPreviewPlanDisclosuresSelectIndividually(t *testing.T) {
	tests := []struct {
		previewType templates.PreviewType
		want        string
	}{
		{templates.PreviewCommentPlanIgnoredNamespaces, "excluded from this plan by `ignore_namespaces`"},
		{templates.PreviewCommentPlanExemptTables, "exempt from the undeclared-table verdict"},
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
