package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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

	t.Run("empty caller with a server URL still shows the source", func(t *testing.T) {
		rows := callerAndSourceBoxRows("", "https://github.com/acme/shop/pull/412")
		assert.Equal(t, []BoxRow{{"Source", "https://github.com/acme/shop/pull/412"}}, rows)
	})

	t.Run("no caller and no URL renders nothing", func(t *testing.T) {
		assert.Empty(t, callerAndSourceBoxRows("", ""))
	})
}
