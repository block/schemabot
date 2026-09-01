package example

import "fmt"

// The fixtures are exercised by the analyzer, not by callers.
var _ = []any{bad, badEscape, badBare, badRaw, badInfo, goodConcat, goodStateGlyphs}

// Bad: literal severity glyph inline.
func bad() string {
	return "⛔ Apply blocked: 3 unsafe change(s) detected" // want `severity glyph ⛔ in string literal — use glyph.Refused from pkg/glyph`
}

// Bad: escape-spelled glyph — the analyzer sees the decoded value.
func badEscape(table string) string {
	return fmt.Sprintf("**`%s`**: \u274c Failed", table) // want `severity glyph ❌ in string literal — use glyph.Failed from pkg/glyph`
}

// Bad: base codepoint without the variation selector.
func badBare() string {
	return "⚠ unsafe change awaiting consent" // want `severity glyph ⚠ in string literal — use glyph.Attention from pkg/glyph`
}

// Bad: raw string literal.
func badRaw() string {
	return `🚨 destructive consent in effect` // want `severity glyph 🚨 in string literal — use glyph.Escalation from pkg/glyph`
}

// Bad: info glyph with variation selector.
func badInfo() string {
	return "ℹ️ nothing to do" // want `severity glyph ℹ in string literal — use glyph.Info from pkg/glyph`
}

// Good: the vocabulary constant (glyph.Refused at real call sites)
// concatenated into the message; the string parts carry no glyphs.
func goodConcat(refused string) string {
	return refused + " Apply blocked: 3 unsafe change(s) detected"
}

// Good: apply-state glyphs are a separate vocabulary and are not flagged.
func goodStateGlyphs() []string {
	return []string{
		"🚫 Cancelled",
		"⏹️  Stopped",
		"⏸️ Defer Cutover",
		"⏳ Revert window open",
		"↩️ Reverted",
		"✅ Completed",
	}
}
