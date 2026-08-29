package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/glyph"
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

	redactionCases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "TLS CA bundle path",
			in:   "failed to connect: cannot load TLS root certificate /etc/schemabot/ca/bundle.pem: permission denied",
			want: "failed to connect: cannot load TLS root certificate [endpoint redacted]: permission denied",
		},
		{
			name: "port-less private hostname",
			in:   "connection to db-primary.foo.internal failed: timeout",
			want: "connection to [endpoint redacted] failed: timeout",
		},
		{
			name: "port-less managed database hostname",
			in:   "connection to xyz.us-east-1.rds.amazonaws.com failed: timeout",
			want: "connection to [endpoint redacted] failed: timeout",
		},
		{
			name: "contextual port-less hostname",
			in:   "connect to db-primary.private.example.com failed: timeout",
			want: "connect to [endpoint redacted] failed: timeout",
		},
		{
			name: "DNS resolver error",
			in:   "dial tcp: lookup db.internal on 10.0.0.2:53: no such host",
			want: "dial tcp: lookup [endpoint redacted] on [endpoint redacted]: no such host",
		},
		{
			name: "libpq authentication failure",
			in:   "failed to connect to user=app_writer database=orders host=db.internal: password authentication failed",
			want: "failed to connect to user=[endpoint redacted] database=[endpoint redacted] host=[endpoint redacted]: password authentication failed",
		},
		{
			name: "libpq dbname fragment",
			in:   "connect dbname=customer_data user=reporter sslmode=verify-full failed",
			want: "connect dbname=[endpoint redacted] user=[endpoint redacted] sslmode=verify-full failed",
		},
		{
			name: "pgconn SQLSTATE error",
			in:   `failed to connect: FATAL: no pg_hba.conf entry for host "10.20.30.40", user "app_writer", database "orders", no encryption (SQLSTATE 28000)`,
			want: `failed to connect: FATAL: no pg_hba.conf entry for host "[endpoint redacted]", user "[endpoint redacted]", database "[endpoint redacted]", no encryption (SQLSTATE 28000)`,
		},
		{
			name: "pq SQLSTATE error",
			in:   `pq: password authentication failed for user "app_writer" (SQLSTATE 28P01)`,
			want: `pq: password authentication failed for user "[endpoint redacted]" (SQLSTATE 28P01)`,
		},
		{
			name: "normal English and Go names",
			in:   "context.DeadlineExceeded while pkg/webhook.foo handled pkg/webhook/inbox.go",
			want: "context.DeadlineExceeded while pkg/webhook.foo handled pkg/webhook/inbox.go",
		},
	}
	for _, tc := range redactionCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitizeCommentError(tc.in))
		})
	}

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

func TestSanitizeInlineError(t *testing.T) {
	t.Run("newlines collapse so the error cannot escape its line", func(t *testing.T) {
		assert.Equal(t, "line one line two", SanitizeInlineError("line one\nline two"))
	})

	t.Run("endpoints are redacted", func(t *testing.T) {
		got := SanitizeInlineError("dial tcp db-primary.internal:3306: connection refused")
		assert.NotContains(t, got, "db-primary.internal")
		assert.Contains(t, got, "[endpoint redacted]")
	})

	t.Run("whitespace-only input is empty", func(t *testing.T) {
		assert.Empty(t, SanitizeInlineError(" \n\t "))
	})
}

func TestSanitizeCellError(t *testing.T) {
	t.Run("cell separators are neutralized", func(t *testing.T) {
		assert.Equal(t, "left / right", sanitizeCellError("left | right"))
	})

	t.Run("newlines collapse so the error cannot break the table row", func(t *testing.T) {
		assert.Equal(t, "a b", sanitizeCellError("a\nb"))
	})

	t.Run("long messages are clamped to the column width", func(t *testing.T) {
		got := sanitizeCellError(strings.Repeat("x", maxCellErrorLen+100))
		assert.Len(t, []rune(got), maxCellErrorLen)
		assert.True(t, strings.HasSuffix(got, "…"), "clamped message ends with a truncation marker")
	})
}

func TestWriteErrorBlock(t *testing.T) {
	t.Run("multi-line error stays inside the blockquote", func(t *testing.T) {
		var sb strings.Builder
		writeErrorBlock(&sb, glyph.Failed, "first line\nsecond line")
		assert.Equal(t, "\n> ❌ **Error:** first line\n> second line\n", sb.String())
	})

	t.Run("whitespace-only error writes nothing", func(t *testing.T) {
		var sb strings.Builder
		writeErrorBlock(&sb, glyph.Failed, "  \n ")
		assert.Empty(t, sb.String())
	})

	t.Run("HTML markup is escaped so it renders as text", func(t *testing.T) {
		var sb strings.Builder
		writeErrorBlock(&sb, glyph.Failed, "unexpected <img src=x> in output")
		assert.Contains(t, sb.String(), "&lt;img src=x&gt;")
		assert.NotContains(t, sb.String(), "<img")
	})
}

func TestWriteTableErrorLine(t *testing.T) {
	t.Run("whitespace-only error writes nothing", func(t *testing.T) {
		var sb strings.Builder
		writeTableErrorLine(&sb, glyph.Failed, " \n\t")
		assert.Empty(t, sb.String())
	})

	t.Run("endpoint detail is redacted in the rendered line", func(t *testing.T) {
		var sb strings.Builder
		writeTableErrorLine(&sb, glyph.Failed, "dial tcp 10.0.0.5:3306: i/o timeout")
		assert.Contains(t, sb.String(), "[endpoint redacted]")
		assert.NotContains(t, sb.String(), "10.0.0.5")
	})

	t.Run("HTML markup is escaped so it renders as text", func(t *testing.T) {
		var sb strings.Builder
		writeTableErrorLine(&sb, glyph.Failed, "expected <nil> but got <error>")
		assert.Contains(t, sb.String(), "&lt;nil&gt;")
		assert.NotContains(t, sb.String(), "<nil>")
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

func TestWriteVSchemaDiffFence(t *testing.T) {
	t.Run("small diff renders intact without a truncation marker", func(t *testing.T) {
		var sb strings.Builder
		writeVSchemaDiffFence(&sb, "--- current\n+++ new\n+ vindex hash", maxCommentVSchemaDiffLen)
		assert.Equal(t, "```diff\n--- current\n+++ new\n+ vindex hash\n```\n\n", sb.String())
	})

	t.Run("oversized diff is clamped with a visible marker", func(t *testing.T) {
		var sb strings.Builder
		writeVSchemaDiffFence(&sb, strings.Repeat("+", maxCommentVSchemaDiffLen+100), maxCommentVSchemaDiffLen)
		out := sb.String()
		assert.Less(t, len(out), maxCommentVSchemaDiffLen+200, "rendered section stays near the diff budget")
		assert.Contains(t, out, "Diff truncated to fit GitHub's comment size limit")
		assert.Contains(t, out, "```diff\n", "the fence still opens")
		assert.Contains(t, out, "\n```\n", "the fence still closes after the clamped diff")
	})

	t.Run("clamp never splits a UTF-8 rune", func(t *testing.T) {
		var sb strings.Builder
		writeVSchemaDiffFence(&sb, strings.Repeat("é", maxCommentVSchemaDiffLen), maxCommentVSchemaDiffLen)
		assert.True(t, strings.HasSuffix(strings.SplitAfter(sb.String(), "\n```")[0], "é\n```"),
			"the clamped diff ends on a whole rune before the closing fence")
	})
}

// The diff budget is shared by every VSchema entry in one comment, so a
// multi-keyspace apply with several oversized diffs still renders a section
// bounded near the per-comment budget — the shape that would otherwise push
// the whole body past GitHub's cap and freeze the status surface.
func TestVSchemaDiffBudget_SharedAcrossKeyspaces(t *testing.T) {
	assert.Equal(t, maxCommentVSchemaDiffLen, vschemaDiffBudget(0))
	assert.Equal(t, maxCommentVSchemaDiffLen, vschemaDiffBudget(1))
	assert.Equal(t, maxCommentVSchemaDiffLen/4, vschemaDiffBudget(4))

	changes := make([]apitypes.VSchemaChange, 0, 4)
	for _, ns := range []string{"ks_a", "ks_b", "ks_c", "ks_d"} {
		changes = append(changes, apitypes.VSchemaChange{
			Namespace: ns,
			Status:    "applied",
			Diff:      strings.Repeat("+", maxCommentVSchemaDiffLen),
		})
	}
	var sb strings.Builder
	writeVSchemaStatus(&sb, changes)
	assert.Less(t, sb.Len(), maxCommentVSchemaDiffLen+4*200,
		"four oversized keyspace diffs render one budget's worth of diff, not four")
	assert.Equal(t, 4, strings.Count(sb.String(), "Diff truncated to fit GitHub's comment size limit"))
}
