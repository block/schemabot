package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeCommentError(t *testing.T) {
	t.Run("plain single-line error is unchanged", func(t *testing.T) {
		msg := "preflight enumReorder check failed: retained value 'NO_VERSION' moved from position 7 to 11"
		assert.Equal(t, msg, sanitizeCommentError(msg))
	})

	t.Run("surrounding whitespace is trimmed", func(t *testing.T) {
		assert.Equal(t, "boom", sanitizeCommentError("  boom \n"))
	})

	t.Run("whitespace-only becomes empty", func(t *testing.T) {
		assert.Empty(t, sanitizeCommentError("   \n\t  "))
	})

	t.Run("CRLF and bare CR normalize to LF", func(t *testing.T) {
		assert.Equal(t, "line one\nline two\nline three",
			sanitizeCommentError("line one\r\nline two\rline three"))
	})

	t.Run("control characters are stripped", func(t *testing.T) {
		assert.Equal(t, "redalert",
			sanitizeCommentError("red\x1b[31malert\x00\x07"))
	})

	t.Run("interior newlines and tabs survive", func(t *testing.T) {
		assert.Equal(t, "a\n\tb", sanitizeCommentError("a\n\tb"))
	})

	t.Run("Unicode format characters are stripped", func(t *testing.T) {
		assert.Equal(t, "renamed.txt",
			sanitizeCommentError("re\u202enamed.txt\u202c\u200b\u2066\u2069"))
	})

	t.Run("DSN fragment is redacted", func(t *testing.T) {
		got := sanitizeCommentError("dial failed: user:secret@tcp(db-primary.internal:3306)/orders?tls=true refused")
		assert.Equal(t, "dial failed: [endpoint redacted] refused", got)
		assert.NotContains(t, got, "secret")
		assert.NotContains(t, got, "db-primary")
	})

	t.Run("hostname with port is redacted", func(t *testing.T) {
		got := sanitizeCommentError("connect to db.internal.example.com:3306 timed out")
		assert.Equal(t, "connect to [endpoint redacted] timed out", got)
	})

	t.Run("single-label service endpoint is redacted", func(t *testing.T) {
		got := sanitizeCommentError("dial tcp mysql-primary:3306: connect: connection refused")
		assert.Equal(t, "dial tcp [endpoint redacted]: connect: connection refused", got)
		assert.NotContains(t, got, "mysql-primary")
	})

	t.Run("line:column references are not redacted", func(t *testing.T) {
		msg := "parse error at line 3:14 near 'ENUM'"
		assert.Equal(t, msg, sanitizeCommentError(msg))
	})

	t.Run("IP endpoints are redacted", func(t *testing.T) {
		got := sanitizeCommentError("dial tcp 10.1.2.3:3306: connect: connection refused from 192.168.0.1")
		assert.NotContains(t, got, "10.1.2.3")
		assert.NotContains(t, got, "192.168.0.1")
		assert.Contains(t, got, "[endpoint redacted]")
	})

	t.Run("schema-qualified identifiers are not redacted", func(t *testing.T) {
		msg := "table orders.line_items has no primary key"
		assert.Equal(t, msg, sanitizeCommentError(msg))
	})

	t.Run("long errors clamp by rune with truncation marker", func(t *testing.T) {
		got := sanitizeCommentError(strings.Repeat("é", maxCommentErrorLen+100))
		runes := []rune(got)
		assert.Len(t, runes, maxCommentErrorLen)
		assert.Equal(t, '…', runes[len(runes)-1])
	})

	t.Run("errors at the limit are not clamped", func(t *testing.T) {
		msg := strings.Repeat("x", maxCommentErrorLen)
		assert.Equal(t, msg, sanitizeCommentError(msg))
	})
}

func TestWriteErrorBlock(t *testing.T) {
	t.Run("multi-line error stays inside the blockquote", func(t *testing.T) {
		var sb strings.Builder
		writeErrorBlock(&sb, "first line\nsecond line")
		assert.Equal(t, "\n> ⚠️ **Error:** first line\n> second line\n", sb.String())
	})

	t.Run("whitespace-only error writes nothing", func(t *testing.T) {
		var sb strings.Builder
		writeErrorBlock(&sb, "  \n ")
		assert.Empty(t, sb.String())
	})
}

func TestWriteTableErrorLine(t *testing.T) {
	t.Run("whitespace-only error writes nothing", func(t *testing.T) {
		var sb strings.Builder
		writeTableErrorLine(&sb, " \n\t")
		assert.Empty(t, sb.String())
	})

	t.Run("endpoint detail is redacted in the rendered line", func(t *testing.T) {
		var sb strings.Builder
		writeTableErrorLine(&sb, "dial tcp 10.0.0.5:3306: i/o timeout")
		assert.Contains(t, sb.String(), "[endpoint redacted]")
		assert.NotContains(t, sb.String(), "10.0.0.5")
	})
}

func TestTaskErrorAddsDetail(t *testing.T) {
	t.Run("identical after sanitization is suppressed", func(t *testing.T) {
		assert.False(t, taskErrorAddsDetail("copy failed\r\n", "copy failed"))
	})

	t.Run("errors differing only in redacted endpoints are suppressed", func(t *testing.T) {
		assert.False(t, taskErrorAddsDetail(
			"dial tcp 10.0.0.5:3306: i/o timeout",
			"dial tcp 10.0.0.9:3306: i/o timeout"))
	})

	t.Run("distinct errors still add detail", func(t *testing.T) {
		assert.True(t, taskErrorAddsDetail("checksum mismatch on chunk 42", "copy failed"))
	})

	t.Run("whitespace-only task error adds nothing", func(t *testing.T) {
		assert.False(t, taskErrorAddsDetail("  \n", "copy failed"))
	})
}
