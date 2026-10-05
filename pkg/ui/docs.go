package ui

import "strings"

// DocsBaseURL is the prefix SchemaBot builds every documentation link it
// renders from. It is deliberately the project's canonical public home rather
// than a value derived from the configured GitHub host: that host serves
// users' schema repositories, which do not carry this project's docs, so a
// host-derived link would always be broken.
const DocsBaseURL = "https://github.com/block/schemabot/blob/main/docs/"

// Documentation links rendered on user- and operator-facing surfaces. Each
// names the one page that answers the question its surface raises, so a reader
// who has only the rendered comment in front of them still has somewhere to go
// (UX-4). Every link here is checked against the docs tree, so a page that
// moves or a heading that is reworded fails a test rather than shipping a
// comment that sends a first-time user to a 404.
const (
	// SchemaConfigDocURL documents the repository-side schemabot.yaml: which
	// fields it takes, where in the repository it belongs, and the schema
	// directory that sits beside it.
	SchemaConfigDocURL = DocsBaseURL + "github-app-setup.md#6-add-schemabotyaml-config-to-your-repository"

	// ThrottleDocURL explains each throttle signal and how to remediate it.
	// Rendered next to a throttle tip so an operator can jump from the
	// one-line tip to the full prose.
	ThrottleDocURL = DocsBaseURL + "throttle.md"
)

// DocLinks is every documentation link rendered on a user-facing surface. The
// link-validity test walks this set, so a new link is covered by adding it
// here rather than by remembering to extend a test.
var DocLinks = []string{
	SchemaConfigDocURL,
	ThrottleDocURL,
}

// DocRef is the short form a documentation link is displayed as on a terminal:
// the page's path inside the repository, such as "docs/throttle.md", the way a
// PR is displayed as "owner/repo#pr". It identifies the page on its own when a
// terminal drops the hyperlink escape, and stays short enough to sit on one
// line beside a label. A section anchor stays on the link target but is left
// off the display, since the page is what the reader recognizes. A URL outside
// the docs tree has no shorter form and is returned whole.
func DocRef(url string) string {
	path, ok := strings.CutPrefix(url, DocsBaseURL)
	if !ok {
		return url
	}
	if page, _, hasAnchor := strings.Cut(path, "#"); hasAnchor {
		path = page
	}
	return "docs/" + path
}
