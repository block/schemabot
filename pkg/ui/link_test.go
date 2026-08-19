package ui

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// setHyperlinks forces hyperlink emission on or off for the duration of a
// test, restoring the ambient detection afterwards.
func setHyperlinks(t *testing.T, enabled bool) {
	t.Helper()
	restore := Hyperlinks
	Hyperlinks = enabled
	t.Cleanup(func() { Hyperlinks = restore })
}

func TestLink(t *testing.T) {
	t.Run("interactive terminal wraps the text in an OSC 8 hyperlink", func(t *testing.T) {
		setHyperlinks(t, true)
		assert.Equal(t,
			"\x1b]8;;https://github.com/acme/shop/pull/412\x1b\\acme/shop#412\x1b]8;;\x1b\\",
			Link("acme/shop#412", "https://github.com/acme/shop/pull/412"))
	})

	t.Run("non-terminal output falls back to the bare URL", func(t *testing.T) {
		setHyperlinks(t, false)
		assert.Equal(t,
			"https://github.com/acme/shop/pull/412",
			Link("acme/shop#412", "https://github.com/acme/shop/pull/412"))
	})

	t.Run("a control byte in the URL falls back to plain output", func(t *testing.T) {
		setHyperlinks(t, true)
		assert.Equal(t,
			"https://example.com/\x1b]8;;evil",
			Link("acme/shop#412", "https://example.com/\x1b]8;;evil"))
		assert.Equal(t,
			"https://example.com/a\nb",
			Link("acme/shop#412", "https://example.com/a\nb"))
	})

	t.Run("a non-ASCII URL falls back to plain output", func(t *testing.T) {
		setHyperlinks(t, true)
		assert.Equal(t,
			"https://example.com/ünïcode",
			Link("acme/shop#412", "https://example.com/ünïcode"))
	})

	t.Run("a control rune in the text falls back to plain output", func(t *testing.T) {
		setHyperlinks(t, true)
		assert.Equal(t,
			"https://example.com/pr/1",
			Link("acme\x07shop#1", "https://example.com/pr/1"))
	})

	t.Run("printable Unicode text stays linked", func(t *testing.T) {
		setHyperlinks(t, true)
		assert.Equal(t,
			"\x1b]8;;https://example.com/pr/1\x1b\\dépôt#1\x1b]8;;\x1b\\",
			Link("dépôt#1", "https://example.com/pr/1"))
	})
}

// FORCE_HYPERLINK is the cross-ecosystem override convention: "0" or empty
// forces links off (terminals that silently drop the escape), any other
// value forces them on (environments that render links without a TTY).
func TestStdoutSupportsHyperlinksForceOverride(t *testing.T) {
	t.Run("zero and empty force hyperlinks off", func(t *testing.T) {
		t.Setenv("FORCE_HYPERLINK", "0")
		assert.False(t, stdoutSupportsHyperlinks())
		t.Setenv("FORCE_HYPERLINK", "")
		assert.False(t, stdoutSupportsHyperlinks())
	})

	t.Run("any other value forces hyperlinks on", func(t *testing.T) {
		t.Setenv("FORCE_HYPERLINK", "1")
		assert.True(t, stdoutSupportsHyperlinks())
	})

	t.Run("multiplexers disable hyperlinks unless forced", func(t *testing.T) {
		t.Setenv("TMUX", "/tmp/tmux-501/default,1234,0")
		assert.False(t, stdoutSupportsHyperlinks())
		t.Setenv("FORCE_HYPERLINK", "1")
		assert.True(t, stdoutSupportsHyperlinks())
	})
}

func TestVisibleWidth(t *testing.T) {
	t.Run("plain text counts its runes", func(t *testing.T) {
		assert.Equal(t, 5, VisibleWidth("hello"))
	})

	t.Run("a hyperlink counts only its display text", func(t *testing.T) {
		setHyperlinks(t, true)
		assert.Equal(t, len("acme/shop#412"),
			VisibleWidth(Link("acme/shop#412", "https://github.com/acme/shop/pull/412")))
	})

	t.Run("BEL-terminated OSC sequences are also zero width", func(t *testing.T) {
		assert.Equal(t, 4, VisibleWidth("\x1b]8;;https://example.com\x07text\x1b]8;;\x07"))
	})

	t.Run("SGR color codes are zero width", func(t *testing.T) {
		assert.Equal(t, 7, VisibleWidth("\x1b[32mRunning\x1b[0m"))
	})
}
