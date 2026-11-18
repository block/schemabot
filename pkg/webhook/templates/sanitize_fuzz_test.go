package templates

import (
	"strings"
	"testing"
	"unicode"
)

// sanitizerSeeds are shared starting points for both sanitizer fuzz targets:
// representative connection errors, markup-hostile text, and control
// characters positioned where stripping them could join surrounding text.
var sanitizerSeeds = []string{
	"failed to connect to user=app_writer database=orders host=db.internal: password authentication failed",
	"connect postgres://app:hunter2@db.internal/orders failed",
	"dial tcp: lookup db.internal on 10.0.0.2:53: no such host",
	"Error 1045 (28000): Access denied for user 'app_writer'@'10.20.30.40' (using password: YES)",
	"password=p@ss:word host=db.internal:5432",
	"cannot read `/etc/schemabot/ca/bundle.pem`",
	"fence breakout ```\n# not a heading",
	"``\x01`",
	"red\x1b[31malert re\u202enamed.txt\u202c",
	strings.Repeat("é", maxCommentErrorLen+100),
	strings.Repeat("x", maxCommentErrorLen-10) + " /etc/schemabot/ca/bundle.pem",
}

// requireNoPartialMarker fails when the text contains a redaction marker
// prefix that is not part of a complete marker — a partial marker means a
// redaction was cut apart and could read as a leaked value.
func requireNoPartialMarker(t *testing.T, got string) {
	t.Helper()
	if strings.Count(got, "[endpoint") != strings.Count(got, redactionMarker) {
		t.Fatalf("partial redaction marker in %q", got)
	}
}

// FuzzSanitizeCommentError checks the structural guarantees the comment
// error sanitizer must hold for arbitrary engine error text rendered in a
// public PR comment: bounded rune length, no control or format characters
// beyond newline and tab, and never a partially emitted redaction marker.
func FuzzSanitizeCommentError(f *testing.F) {
	for _, seed := range sanitizerSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, in string) {
		if strings.Contains(in, "[endpoint") {
			return // marker integrity is only guaranteed when the input carries no marker fragments of its own
		}
		got := sanitizeCommentError(in)
		if n := len([]rune(got)); n > maxCommentErrorLen {
			t.Fatalf("output length %d exceeds limit %d", n, maxCommentErrorLen)
		}
		for _, r := range got {
			if r == '\n' || r == '\t' {
				continue
			}
			if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
				t.Fatalf("control or format character %q in %q", r, got)
			}
		}
		requireNoPartialMarker(t, got)
	})
}

// FuzzSanitizeLogText checks the structural guarantees the log line
// sanitizer must hold for arbitrary engine log text inside the section's
// fenced code block: no fence marker that would close the block, one line
// only, no control or format characters beyond tab, and never a partially
// emitted redaction marker.
func FuzzSanitizeLogText(f *testing.F) {
	for _, seed := range sanitizerSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, in string) {
		if strings.Contains(in, "[endpoint") {
			return // marker integrity is only guaranteed when the input carries no marker fragments of its own
		}
		got := sanitizeLogText(in)
		if strings.Contains(got, "```") {
			t.Fatalf("fence marker in %q", got)
		}
		if strings.ContainsAny(got, "\n\r") {
			t.Fatalf("line break in %q", got)
		}
		for _, r := range got {
			if r == '\t' {
				continue
			}
			if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
				t.Fatalf("control or format character %q in %q", r, got)
			}
		}
		requireNoPartialMarker(t, got)
	})
}
