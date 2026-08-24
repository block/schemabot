package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ui"
)

// enableHyperlinks turns on OSC 8 emission for one test; TestMain pins it off
// everywhere else.
func enableHyperlinks(t *testing.T) {
	t.Helper()
	ui.Hyperlinks = true
	t.Cleanup(func() { ui.Hyperlinks = false })
}

// The status list's SOURCE column shows where an apply came from: the full
// clickable PR URL for webhook-driven applies, and the short caller for
// CLI-driven ones so their origin still reads at a glance.
func TestApplySource(t *testing.T) {
	assert.Equal(t, "https://github.com/acme/shop/pull/412", applySource("github:octocat@acme/shop#412"))
	assert.Equal(t, "https://github.com/acme/shop/pull/412", applySource("acme/shop#412"))
	assert.Equal(t, "cli:jdoe", applySource("cli:jdoe@macbook.local"))
	assert.Equal(t, "jdoe@example.com", applySource("jdoe@example.com@somehost"))
	assert.Equal(t, "", applySource(""))
}

// The detail box separates who drove an apply from where it came from: a
// short Caller row plus a clickable Source row when the apply has PR
// provenance, and a single raw Caller row otherwise.
func TestCallerAndSourceBoxRows(t *testing.T) {
	t.Run("webhook caller splits into short caller and clickable source", func(t *testing.T) {
		rows := callerAndSourceBoxRows("github:octocat@acme/shop#412", "")
		assert.Equal(t, []BoxRow{
			{"Caller", "github:octocat"},
			{"Source", "https://github.com/acme/shop/pull/412"},
		}, rows)
	})

	t.Run("server-provided PR URL wins over the caller-derived one", func(t *testing.T) {
		rows := callerAndSourceBoxRows("github:octocat@acme/shop#412", "https://github.com/acme/shop/pull/999")
		assert.Equal(t, []BoxRow{
			{"Caller", "github:octocat"},
			{"Source", "https://github.com/acme/shop/pull/999"},
		}, rows)
	})

	t.Run("CLI caller stays one raw row keeping its host", func(t *testing.T) {
		rows := callerAndSourceBoxRows("cli:jdoe@macbook.local", "")
		assert.Equal(t, []BoxRow{{"Caller", "cli:jdoe@macbook.local"}}, rows)
	})

	t.Run("CLI caller keeps its host even alongside a server-provided source", func(t *testing.T) {
		rows := callerAndSourceBoxRows("cli:jdoe@macbook.local", "https://github.com/acme/shop/pull/412")
		assert.Equal(t, []BoxRow{
			{"Caller", "cli:jdoe@macbook.local"},
			{"Source", "https://github.com/acme/shop/pull/412"},
		}, rows)
	})

	t.Run("server-substituted bare location renders only the source row", func(t *testing.T) {
		rows := callerAndSourceBoxRows("acme/shop#412", "")
		assert.Equal(t, []BoxRow{{"Source", "https://github.com/acme/shop/pull/412"}}, rows)
	})

	t.Run("empty caller with a server URL still shows the source", func(t *testing.T) {
		rows := callerAndSourceBoxRows("", "https://github.com/acme/shop/pull/412")
		assert.Equal(t, []BoxRow{{"Source", "https://github.com/acme/shop/pull/412"}}, rows)
	})

	t.Run("no caller and no URL renders nothing", func(t *testing.T) {
		assert.Empty(t, callerAndSourceBoxRows("", ""))
	})
}

// On an interactive terminal the PR provenance renders as the short
// "owner/repo#pr" hyperlinked to the PR, so wide URLs stop dominating the
// table while staying clickable. Everywhere else (pipes, logs) the full URL
// renders instead — the tests above pin that path.
func TestApplySourceHyperlinked(t *testing.T) {
	enableHyperlinks(t)

	t.Run("list surfaces link the short PR name", func(t *testing.T) {
		assert.Equal(t,
			"\x1b]8;;https://github.com/acme/shop/pull/412\x1b\\acme/shop#412\x1b]8;;\x1b\\",
			applySource("github:octocat@acme/shop#412"))
	})

	t.Run("CLI callers are unaffected", func(t *testing.T) {
		assert.Equal(t, "cli:jdoe", applySource("cli:jdoe@macbook.local"))
	})

	t.Run("detail box links the short PR name", func(t *testing.T) {
		rows := callerAndSourceBoxRows("github:octocat@acme/shop#412", "")
		assert.Equal(t, []BoxRow{
			{"Caller", "github:octocat"},
			{"Source", "\x1b]8;;https://github.com/acme/shop/pull/412\x1b\\acme/shop#412\x1b]8;;\x1b\\"},
		}, rows)
	})

	t.Run("a server URL naming a different PR is its own display text", func(t *testing.T) {
		rows := callerAndSourceBoxRows("github:octocat@acme/shop#412", "https://github.com/acme/shop/pull/999")
		require.Len(t, rows, 2)
		assert.Equal(t,
			BoxRow{"Source", "\x1b]8;;https://github.com/acme/shop/pull/999\x1b\\https://github.com/acme/shop/pull/999\x1b]8;;\x1b\\"},
			rows[1])
	})
}

// A hyperlinked value carries zero-width escape bytes; the box must size and
// pad by visible width so its borders stay aligned.
func TestWriteBoxAlignsHyperlinkedValues(t *testing.T) {
	enableHyperlinks(t)
	output := captureStdout(t, func() {
		WriteBox([]BoxRow{
			{"Database", "orders-db"},
			{"Source", ui.Link("acme/shop#412", "https://github.com/acme/shop/pull/412")},
		}, "", nil)
	})

	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")
	require.Len(t, lines, 4)
	for _, line := range lines[1:] {
		assert.Equal(t, ui.VisibleWidth(lines[0]), ui.VisibleWidth(line), "line %q misaligned", line)
	}
	assert.Contains(t, output, "acme/shop#412")
}
