package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The checklist is what turns a ladder of gates into one reading. When
// everything behind the blocking gate is ready it says so in a line, because
// "your retry will get through" is the whole answer; when something is not, it
// names each one, so the operator fixes them together.
func TestAppendPreflightChecklist(t *testing.T) {
	t.Run("no rows leaves the comment untouched", func(t *testing.T) {
		body := "## Review Required\n\nApprove first.\n"
		assert.Equal(t, body, AppendPreflightChecklist(body, nil))
	})

	t.Run("all remaining gates ready collapses to one line", func(t *testing.T) {
		rendered := AppendPreflightChecklist("## Review Required\n", []PreflightRow{
			{Gate: "PR checks", Status: PreflightReady},
			{Gate: "Database lock", Status: PreflightReady},
		})
		assert.Contains(t, rendered, "Remaining before this apply can run")
		assert.Contains(t, rendered, "Nothing else blocks it.")
		assert.NotContains(t, rendered, "| Gate | Status |",
			"a table of nothing-to-do is noise next to the rejection it follows")
	})

	t.Run("uncleared gates are named one per row", func(t *testing.T) {
		rendered := AppendPreflightChecklist("## Review Required\n", []PreflightRow{
			{Gate: "PR checks", Status: PreflightBlocked, Detail: "2 checks not passing"},
			{Gate: "Prior environments", Status: PreflightReady},
			{Gate: "Database lock", Status: PreflightUnknown, Detail: "Lock state could not be read"},
		})
		assert.Contains(t, rendered, "| PR checks | ⚠️ 2 checks not passing |")
		assert.Contains(t, rendered, "| Prior environments | ✅ Ready |")
		assert.Contains(t, rendered, "| Database lock | ℹ️ Lock state could not be read |")
	})

	t.Run("the support offer stays last", func(t *testing.T) {
		body := offerSupportChannel("## Review Required\n\nApprove first.\n")
		require.Contains(t, body, supportChannelOfferMarker)

		rendered := AppendPreflightChecklist(body, []PreflightRow{
			{Gate: "PR checks", Status: PreflightBlocked, Detail: "1 check not passing"},
		})
		assert.Less(t, strings.Index(rendered, "Remaining before this apply can run"),
			strings.Index(rendered, supportChannelOfferMarker))
		assert.Equal(t, 1, strings.Count(rendered, supportChannelOfferMarker))
	})
}
