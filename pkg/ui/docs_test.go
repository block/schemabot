package ui

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every documentation link rendered on a user-facing surface must resolve: the
// page has to exist in the docs tree, and an anchored link has to name a
// heading that is still there. A link that rots sends a reader who is already
// stuck to a 404, which is worse than the bare requirement it decorates.
func TestDocLinksResolve(t *testing.T) {
	require.NotEmpty(t, DocLinks, "DocLinks must list every rendered documentation link")

	docsDir := filepath.Join("..", "..", "docs")
	for _, link := range DocLinks {
		t.Run(link, func(t *testing.T) {
			rest, ok := strings.CutPrefix(link, DocsBaseURL)
			require.True(t, ok, "link must be built from DocsBaseURL")

			page, anchor, _ := strings.Cut(rest, "#")
			body, err := os.ReadFile(filepath.Join(docsDir, page))
			require.NoError(t, err, "docs page %q does not exist", page)

			if anchor == "" {
				return
			}
			assert.Contains(t, headingAnchors(string(body)), anchor,
				"docs page %q has no heading anchored at %q", page, anchor)
		})
	}
}

var (
	headingPattern    = regexp.MustCompile(`(?m)^#{1,6} +(.+?) *$`)
	nonAnchorRune     = regexp.MustCompile("[^a-z0-9 -]")
	markdownFormatter = regexp.MustCompile("[`*_]")
)

// headingAnchors returns the GitHub-generated anchor for every heading in a
// markdown document: lowercased, formatting and punctuation dropped, spaces
// turned into hyphens.
func headingAnchors(doc string) []string {
	var anchors []string
	for _, match := range headingPattern.FindAllStringSubmatch(doc, -1) {
		text := markdownFormatter.ReplaceAllString(match[1], "")
		text = nonAnchorRune.ReplaceAllString(strings.ToLower(text), "")
		anchors = append(anchors, strings.ReplaceAll(text, " ", "-"))
	}
	return anchors
}
