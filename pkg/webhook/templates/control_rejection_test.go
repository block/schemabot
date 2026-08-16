package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderControlRejections(t *testing.T) {
	t.Run("renders nothing when no command was rejected", func(t *testing.T) {
		assert.Empty(t, RenderControlRejections(nil))
	})

	t.Run("names every rejected command and its reason", func(t *testing.T) {
		out := RenderControlRejections([]ControlRejectionData{
			{Operation: "cutover", Message: "deploy request is not ready to cut over", RequestedBy: "octocat"},
			{Operation: "volume", Message: "no active schema change"},
		})

		assert.Contains(t, out, "[!WARNING]")
		assert.Contains(t, out, "`cutover` was accepted but did not take effect (requested by `octocat`): deploy request is not ready to cut over")
		assert.Contains(t, out, "`volume` was accepted but did not take effect: no active schema change")
		assert.NotContains(t, out, "`volume` was accepted but did not take effect (requested by")
	})

	t.Run("an engine error cannot leak endpoints or escape the quote block", func(t *testing.T) {
		out := RenderControlRejections([]ControlRejectionData{{
			Operation: "stop",
			Message:   "dial tcp 10.1.2.3:3306: connection refused\n## not a heading",
		}})

		assert.NotContains(t, out, "10.1.2.3")
		assert.Contains(t, out, "[endpoint redacted]")
		for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
			assert.True(t, strings.HasPrefix(line, ">"), "every line stays inside the blockquote: %q", line)
		}
	})
}
