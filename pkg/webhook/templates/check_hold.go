package templates

import (
	"fmt"
	"strings"
)

// CheckHoldData describes an apply that is about to change the live schema of
// a database this PR also targets, so the PR's SchemaBot check is held until
// the apply finishes.
type CheckHoldData struct {
	// ApplyIdentifier is the user-facing identifier of the apply causing the
	// hold.
	ApplyIdentifier string
	// RequestedBy is the apply's caller. Caller-influenced text — the
	// renderer neutralizes markdown-sensitive characters.
	RequestedBy string
	Database    string
	Environment string
}

// RenderCheckHold renders the PR comment posted when an apply on the same
// database moves this PR's check to action required before it starts. It
// explains why the check flipped without any activity on the PR itself, and
// what happens when the apply finishes.
func RenderCheckHold(data CheckHoldData) string {
	var sb strings.Builder
	sb.WriteString("## ⏸️ Schema Check On Hold")
	sb.WriteString(environmentTitleSuffix(data.Environment))
	sb.WriteString("\n\n")
	fmt.Fprintf(&sb, "Apply `%s`", sanitizeInlineCode(data.ApplyIdentifier))
	if data.RequestedBy != "" {
		fmt.Fprintf(&sb, " (requested by `%s`)", sanitizeInlineCode(data.RequestedBy))
	}
	fmt.Fprintf(&sb, " has started changing the live schema of `%s` in `%s` — the same database this PR's schema check was evaluated against.\n\n",
		sanitizeInlineCode(data.Database), sanitizeInlineCode(data.Environment))
	sb.WriteString("This PR's check verdict was computed against the schema that apply is replacing, so the check has been moved to **action required** to keep a merge from landing on a stale verdict while the apply runs.\n\n")
	sb.WriteString("**What happens next**\n\n")
	sb.WriteString("- No action is needed right now; the hold clears automatically.\n")
	sb.WriteString("- When the apply finishes, SchemaBot re-plans this PR against the resulting schema and refreshes this check.\n")
	sb.WriteString("- If the refreshed plan still matches the live schema, the check returns to passing on its own; if it shows new differences, review the refreshed plan before applying or merging.\n")
	return sb.String()
}

// sanitizeInlineCode neutralizes text rendered inside single-backtick inline
// code: backticks would terminate the span and newlines would break the
// surrounding markdown, so both are replaced with spaces and the result is
// trimmed.
func sanitizeInlineCode(s string) string {
	s = strings.ReplaceAll(s, "`", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.TrimSpace(s)
}
