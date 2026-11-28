package ui

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
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

// A documentation link written as a literal anywhere but docs.go is one
// TestDocLinksResolve never sees, so a reworded heading would break it with no
// failing test. Every link has to be built from DocsBaseURL here and listed in
// DocLinks instead.
func TestDocLinksAreOnlyBuiltInDocsGo(t *testing.T) {
	const docsPathMarker = "blob/main/docs/"
	root := filepath.Join("..", "..")
	registry := filepath.Join("pkg", "ui", "docs.go")
	fileSet := token.NewFileSet()
	var scanned int
	var strays []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "bin" || entry.Name() == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return fmt.Errorf("relative path of %s: %w", path, relErr)
		}
		file, parseErr := parser.ParseFile(fileSet, path, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			return fmt.Errorf("parse %s: %w", path, parseErr)
		}
		scanned++
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING || !strings.Contains(literal.Value, docsPathMarker) {
				return true
			}
			if rel != registry {
				strays = append(strays, fmt.Sprintf("%s:%d", rel, fileSet.Position(literal.Pos()).Line))
			}
			return true
		})
		return nil
	})
	require.NoError(t, err)
	require.NotZero(t, scanned, "no Go files found; the scan root is wrong")
	assert.Empty(t, strays,
		"documentation links must be built from DocsBaseURL in %s and listed in DocLinks", registry)
}

var (
	headingPattern    = regexp.MustCompile(`(?m)^#{1,6} +(.+?) *$`)
	nonAnchorRune     = regexp.MustCompile("[^a-z0-9 _-]")
	markdownFormatter = regexp.MustCompile("[`*]")
)

// headingAnchors returns the GitHub-generated anchor for every heading in a
// markdown document: lowercased, code and emphasis markers and punctuation
// dropped, spaces turned into hyphens. Underscores survive, as they do on
// GitHub, so a heading naming a snake_case identifier keeps its anchor.
func headingAnchors(doc string) []string {
	var anchors []string
	for _, match := range headingPattern.FindAllStringSubmatch(doc, -1) {
		text := markdownFormatter.ReplaceAllString(match[1], "")
		text = nonAnchorRune.ReplaceAllString(strings.ToLower(text), "")
		anchors = append(anchors, strings.ReplaceAll(text, " ", "-"))
	}
	return anchors
}

func TestDocRef(t *testing.T) {
	tests := map[string]struct {
		url  string
		want string
	}{
		"docs page":       {MySQLThrottleDocURL, "docs/mysql.md"},
		"anchor dropped":  {DocsBaseURL + "schema-config.md#layout", "docs/schema-config.md"},
		"outside docs":    {"https://example.com/guide", "https://example.com/guide"},
		"no shorter form": {"https://github.com/block/schemabot", "https://github.com/block/schemabot"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, DocRef(tc.url))
		})
	}
}
