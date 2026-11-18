package ui

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setHyperlinks forces hyperlink emission on or off for the duration of a
// test, restoring the ambient detection afterwards.
func setHyperlinks(t *testing.T, enabled bool) {
	t.Helper()
	restore := Hyperlinks
	Hyperlinks = enabled
	t.Cleanup(func() { Hyperlinks = restore })
}

// clearEnv unsets key for the duration of a test, restoring any ambient
// value afterwards. t.Setenv alone cannot express "unset", and detection
// distinguishes an unset variable from an empty one.
func clearEnv(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	require.NoError(t, os.Unsetenv(key))
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

	t.Run("a control byte in the URL falls back to plain output with the control bytes stripped", func(t *testing.T) {
		setHyperlinks(t, true)
		assert.Equal(t,
			"https://example.com/]8;;evil",
			Link("acme/shop#412", "https://example.com/\x1b]8;;evil"))
		assert.Equal(t,
			"https://example.com/ab",
			Link("acme/shop#412", "https://example.com/a\nb"))
	})

	t.Run("a C1 or bidirectional control in the URL is stripped from the plain output", func(t *testing.T) {
		setHyperlinks(t, true)
		assert.Equal(t,
			"https://example.com/2Jevil",
			Link("acme/shop#412", "https://example.com/\u009b2Jevil"))
		assert.Equal(t,
			"https://example.com/live",
			Link("acme/shop#412", "https://example.com/\u202elive"))
	})

	t.Run("a C1 control in the text falls back to plain output", func(t *testing.T) {
		setHyperlinks(t, true)
		assert.Equal(t,
			"https://example.com/pr/1",
			Link("acme\u009b2Jshop#1", "https://example.com/pr/1"))
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
		clearEnv(t, "FORCE_HYPERLINK")
		t.Setenv("TMUX", "/tmp/tmux-501/default,1234,0")
		assert.False(t, stdoutSupportsHyperlinks())
		t.Setenv("FORCE_HYPERLINK", "1")
		assert.True(t, stdoutSupportsHyperlinks())
	})

	t.Run("a multiplexer TERM disables hyperlinks even without TMUX set", func(t *testing.T) {
		clearEnv(t, "FORCE_HYPERLINK")
		clearEnv(t, "TMUX")
		t.Setenv("TERM", "tmux-256color")
		assert.False(t, stdoutSupportsHyperlinks())
		t.Setenv("TERM", "screen-256color")
		assert.False(t, stdoutSupportsHyperlinks())
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

	t.Run("a default-emoji-presentation symbol occupies two cells", func(t *testing.T) {
		assert.Equal(t, 2, VisibleWidth("⛔"))
	})

	t.Run("an emoji variation sequence occupies two cells", func(t *testing.T) {
		assert.Equal(t, 2, VisibleWidth("⚠️"))
	})

	t.Run("a text-presentation symbol occupies one cell", func(t *testing.T) {
		assert.Equal(t, 1, VisibleWidth("⚠"))
	})

	t.Run("wide runes count double inside surrounding text", func(t *testing.T) {
		assert.Equal(t, 12, VisibleWidth("1 alter · ⛔"))
		assert.Equal(t, 22, VisibleWidth("1 create, 2 alter · ⚠️"))
	})

	t.Run("a variation selector without a narrow base adds nothing", func(t *testing.T) {
		assert.Equal(t, 2, VisibleWidth("⛔️"))
		assert.Equal(t, 0, VisibleWidth("️"))
	})
}

// Text a server sends, such as a failed apply's error, is printed straight to
// the operator's terminal. Escape sequences in it would act on the terminal,
// so every control rune is removed except the newlines and tabs that lay out a
// multi-line message.
func TestTerminalSafe(t *testing.T) {
	cases := map[string]struct {
		in, want string
	}{
		"plain text is unchanged":                                         {"duplicate key name 'idx_orders_source'", "duplicate key name 'idx_orders_source'"},
		"newlines and tabs are kept":                                      {"deploy request failed\n  [INVALID_VSCHEMA]\tusers", "deploy request failed\n  [INVALID_VSCHEMA]\tusers"},
		"a screen clear is removed":                                       {"lock wait timeout\x1b[2J\x1b[H", "lock wait timeout[2J[H"},
		"a window title is removed":                                       {"\x1b]0;owned\x07failed", "]0;ownedfailed"},
		"a carriage return is removed, so the line cannot be overwritten": {"failed\rcompleted", "failedcompleted"},
		"a C1 control sequence introducer is removed":                     {"failed\u009b2J", "failed2J"},
		"printable Unicode is kept":                                       {"table café · ⚠️", "table café · ⚠️"},
		"a right-to-left override is removed, so the text reads as sent":  {"apply \u202edeliaf\u202c completed", "apply deliaf completed"},
		"bidirectional isolates and marks are removed":                    {"a\u2066b\u2069c\u200ed\u200fe\u061cf", "abcdef"},
		"the joiner inside an emoji sequence is kept":                     {"👩\u200d💻 on call", "👩\u200d💻 on call"},
		"a byte that is not valid UTF-8 becomes the replacement rune":     {"failed\x9b2J\xff", "failed\ufffd2J\ufffd"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, TerminalSafe(tc.in))
		})
	}
}

// A value printed on one line, such as a lock owner, loses its line breaks
// too: with them it could add a line that reads as the CLI's own.
func TestTerminalSafeLine(t *testing.T) {
	cases := map[string]struct {
		in, want string
	}{
		"plain text is unchanged":             {"cli:jdoe@macbook.local", "cli:jdoe@macbook.local"},
		"a newline is removed":                {"jdoe\n     PR:    acme/shop#1", "jdoe     PR:    acme/shop#1"},
		"a tab is removed":                    {"jdoe\tadmin", "jdoeadmin"},
		"an escape sequence is removed":       {"jdoe\x1b]0;owned\x07", "jdoe]0;owned"},
		"a right-to-left override is removed": {"jdoe\u202enimda", "jdoenimda"},
		"printable Unicode is kept":           {"josé · 👩\u200d💻", "josé · 👩\u200d💻"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, TerminalSafeLine(tc.in))
		})
	}
}

func TestTerminalSafeInline(t *testing.T) {
	cases := map[string]struct {
		in, want string
	}{
		"plain text is unchanged":                      {"storage unavailable", "storage unavailable"},
		"a line break becomes a space":                 {"bad gateway\nretry the unlock", "bad gateway retry the unlock"},
		"a line break and its indent become a space":   {"bad gateway\r\n\t  retry the unlock", "bad gateway retry the unlock"},
		"a line separator becomes a space":             {"bad gateway\u2028retry", "bad gateway retry"},
		"white space at either end is removed":         {"\n bad gateway \n", "bad gateway"},
		"an escape sequence is removed":                {"bad gateway\x1b[2J", "bad gateway[2J"},
		"a control rune inside a word adds no space":   {"bad gate\x1bway", "bad gateway"},
		"a right-to-left override is removed":          {"bad\u202e gateway", "bad gateway"},
		"printable Unicode is kept":                    {"josé · 👩\u200d💻", "josé · 👩\u200d💻"},
		"text of only control runes and space is \"\"": {"\x1b\n\t ", ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, TerminalSafeInline(tc.in))
		})
	}
}

func TestPadVisible(t *testing.T) {
	t.Run("pads plain text to the requested cell count", func(t *testing.T) {
		assert.Equal(t, "ab   ", PadVisible("ab", 5))
	})

	t.Run("pads by cells, not bytes, so emoji columns line up", func(t *testing.T) {
		// "1 alter · ⚠️" is 14 bytes but 12 cells, so byte-based padding
		// would stop two cells short of the column.
		padded := PadVisible("1 alter · ⚠️", 14)
		assert.Equal(t, 14, VisibleWidth(padded))
		assert.Equal(t, "1 alter · ⚠️  ", padded)
	})

	t.Run("pads by cells, not bytes, for hyperlinked values", func(t *testing.T) {
		setHyperlinks(t, true)
		padded := PadVisible(Link("acme/shop#412", "https://github.com/acme/shop/pull/412"), 15)
		assert.Equal(t, 15, VisibleWidth(padded))
	})

	t.Run("returns a value already at or past the width unchanged", func(t *testing.T) {
		assert.Equal(t, "abcde", PadVisible("abcde", 5))
		assert.Equal(t, "abcdefg", PadVisible("abcdefg", 5))
	})
}
