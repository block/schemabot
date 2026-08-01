package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The mooted-cancel note must state plainly that the cancel did not take
// effect and the change is live, naming the requester when known, so the
// operator who issued the cancel is not left believing it worked.
func TestRenderMootedCancelNote(t *testing.T) {
	note := RenderMootedCancelNote("armand")
	assert.Contains(t, note, "**Cancel did not take effect**")
	assert.Contains(t, note, "completed on the engine before the cancel requested by @armand could act")
	assert.Contains(t, note, "The change is live on the target.")

	anonymous := RenderMootedCancelNote("")
	assert.Contains(t, anonymous, "**Cancel did not take effect**")
	assert.NotContains(t, anonymous, "@")
	assert.Contains(t, anonymous, "before the cancel could act")
}

func TestPreviewCommentSummaryMootedCancel(t *testing.T) {
	result := PreviewCommentSummaryMootedCancel()

	assert.Contains(t, result, "✅ Schema Change Applied")
	assert.Contains(t, result, "**Cancel did not take effect**")
	assert.Contains(t, result, "@jackjackbits")
}
