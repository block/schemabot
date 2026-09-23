package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/ui"
)

// OversizedCommentData carries what the notice that replaces an oversized
// comment can still say about it.
type OversizedCommentData struct {
	// Title is the replaced comment's heading line, when it had one, so the
	// notice still says which comment it stands in for.
	Title string
	// RenderedBytes is the size of the body GitHub would have rejected.
	RenderedBytes int
}

// FitGitHubComment returns body when GitHub will accept it once the poster's
// footer is appended, and otherwise the notice that takes its place, reporting
// which. Everything rendered upstream is budgeted to fit, so the notice is the
// last line of defence: a comment whose fixed text alone — table headings,
// fences, truncation markers — exceeds the cap cannot be cut any further by
// the DDL budget. Posting the notice keeps the PR's status surface visible
// where a body GitHub rejects leaves it silent.
func FitGitHubComment(body string) (string, bool) {
	if len(body) <= commentBodyLimit {
		return body, false
	}
	return RenderOversizedComment(OversizedCommentData{
		Title:         headingLine(body),
		RenderedBytes: len(body),
	}), true
}

// headingLine returns body's first line when it is a markdown heading, and
// otherwise nothing. A heading is text SchemaBot wrote and is safe to repeat;
// any other first line could be the start of a construct the rest of the body
// was needed to close.
func headingLine(body string) string {
	first, _, _ := strings.Cut(body, "\n")
	if strings.HasPrefix(first, "#") {
		return first
	}
	return ""
}

// RenderOversizedComment renders the notice posted in place of a comment
// larger than GitHub accepts.
func RenderOversizedComment(data OversizedCommentData) string {
	var sb strings.Builder
	if data.Title != "" {
		sb.WriteString(data.Title + "\n\n")
	}
	fmt.Fprintf(&sb, "%s **This comment was too large to post.** SchemaBot rendered it at %s bytes and GitHub accepts at most %s in one comment, so this notice took its place. The schema change itself is unaffected; only this view of it is missing.\n\n",
		glyph.Attention, ui.FormatNumber(int64(data.RenderedBytes)), ui.FormatNumber(int64(GitHubIssueCommentMaxChars)))
	sb.WriteString("**What to do next:** the desired schema is in this PR's schema files, and an apply's progress is available from the CLI with `schemabot status`. The server logs record the rendering with this PR's identifiers.\n")
	writeSupportChannelOffer(&sb)
	return sb.String()
}
