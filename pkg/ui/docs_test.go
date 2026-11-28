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
	"slices"
	"strconv"
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

// Every documentation link constant in docs.go has to be listed in DocLinks,
// or TestDocLinksResolve never checks it.
func TestEveryDocURLConstantIsListedInDocLinks(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "docs.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err)

	declared := map[string]bool{}
	listed := map[string]bool{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for index, name := range value.Names {
				switch {
				case gen.Tok == token.CONST && name.Name != "DocsBaseURL":
					declared[name.Name] = true
				case gen.Tok == token.VAR && name.Name == "DocLinks":
					composite, ok := value.Values[index].(*ast.CompositeLit)
					require.True(t, ok, "DocLinks must be a slice literal")
					for _, element := range composite.Elts {
						ident, ok := element.(*ast.Ident)
						require.True(t, ok, "DocLinks must list the constants by name")
						listed[ident.Name] = true
					}
				}
			}
		}
	}
	require.NotEmpty(t, declared)
	for name := range declared {
		assert.True(t, listed[name], "%s is declared in docs.go but missing from DocLinks, so TestDocLinksResolve never checks it", name)
	}
}

// docsLinkMarkers are fragments of a link into this repository's tree. A
// shorter fragment than the docs path catches a link to another ref or a tree
// view, and the scan joins concatenated literals before matching, so a link
// split across a "+" is caught too.
var docsLinkMarkers = []string{"schemabot/blob/", "schemabot/tree/"}

// A documentation link written as a literal anywhere but the DocsBaseURL
// declaration is one TestDocLinksResolve never sees, so a reworded heading
// would break it with no failing test. Every link has to be built from
// DocsBaseURL in docs.go and listed in DocLinks instead.
func TestDocLinksAreOnlyBuiltInDocsGo(t *testing.T) {
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
			if spec, ok := node.(*ast.ValueSpec); ok && rel == registry && len(spec.Names) == 1 && spec.Names[0].Name == "DocsBaseURL" {
				return false
			}
			expr, ok := node.(ast.Expr)
			if !ok {
				return true
			}
			text, ok := stringConstant(expr)
			if !ok {
				return true
			}
			if slices.ContainsFunc(docsLinkMarkers, func(marker string) bool { return strings.Contains(text, marker) }) {
				strays = append(strays, fmt.Sprintf("%s:%d", rel, fileSet.Position(expr.Pos()).Line))
			}
			// A whole literal or concatenation was read, so its parts are not
			// read again.
			return false
		})
		return nil
	})
	require.NoError(t, err)
	require.NotZero(t, scanned, "no Go files found; the scan root is wrong")
	assert.Empty(t, strays,
		"documentation links must be built from DocsBaseURL in %s and listed in DocLinks", registry)
}

// stringConstant returns the text of a string literal, or of a "+" chain of
// string literals joined, and whether expr is one.
func stringConstant(expr ast.Expr) (string, bool) {
	switch node := expr.(type) {
	case *ast.BasicLit:
		if node.Kind != token.STRING {
			return "", false
		}
		text, err := strconv.Unquote(node.Value)
		return text, err == nil
	case *ast.ParenExpr:
		return stringConstant(node.X)
	case *ast.BinaryExpr:
		if node.Op != token.ADD {
			return "", false
		}
		left, ok := stringConstant(node.X)
		if !ok {
			return "", false
		}
		right, ok := stringConstant(node.Y)
		return left + right, ok
	}
	return "", false
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
