package github

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// authoredError matches the interface the API package defines for errors
// written for the PR author. The test matches it structurally, as the
// production types do.
type authoredError interface {
	error
	AuthoredText() (text string, quotesRepository bool)
}

// A marked error shows its own words however many callers wrapped it, and the
// words are exactly what Error reports, because none of these types carries a
// cause.
func TestAuthoredErrorsReturnTheirWordsThroughWraps(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "database named but absent from a scoped search",
			err:  &DatabaseNotFoundError{DatabaseName: "orders", SearchedDirs: []string{"schema/a", "schema/b"}, SearchScoped: true},
			want: "database 'orders' not found in its configured schema directories: schema/a, schema/b",
		},
		{
			name: "database the repository is not allowed to change",
			err:  &DatabaseNotFoundError{DatabaseName: "orders", SearchScoped: true},
			want: "database 'orders' accepts no schema changes from this repository; the server configures no schema directories for it here",
		},
		{
			name: "database absent from every discovered config",
			err:  &DatabaseNotFoundError{DatabaseName: "orders", AvailableDatabases: []string{"users", "billing"}},
			want: "database 'orders' not found. Available databases: users, billing",
		},
		{
			name: "schema root with no schema files",
			err:  &noSchemaFilesError{message: `no schema files found under schema/orders for environment "staging"`},
			want: `no schema files found under schema/orders for environment "staging"`,
		},
		{
			name: "database name matching two configs",
			err:  &ambiguousDatabaseConfigError{Database: "orders", Paths: []string{"a/orders", "b/orders"}},
			want: "ambiguous: database 'orders' matches multiple configs at: a/orders, b/orders",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.err.Error())

			wrapped := fmt.Errorf("create schema request: %w", fmt.Errorf("outer: %w", tt.err))
			authored, ok := errors.AsType[authoredError](wrapped)
			require.True(t, ok, "the marker must be found through wraps")
			text, quotesRepository := authored.AuthoredText()
			assert.Equal(t, tt.want, text)
			assert.False(t, quotesRepository, "SchemaBot wrote these words; no library composed them from repository input")
		})
	}
}

// The errors that carry a cause stay unmarked, so a renderer shows its fixed
// line for them instead of the GitHub API error they embed.
func TestErrorsCarryingACauseAreNotMarked(t *testing.T) {
	cause := errors.New("GET https://api.github.example/repos/o/r/contents/schemabot.yaml: 502 Bad Gateway")
	for name, err := range map[string]error{
		"config read":        &ConfigReadError{Path: "schema/schemabot.yaml", Err: cause},
		"github unavailable": &githubUnavailableError{err: cause},
		"graphql status":     &graphQLStatusError{status: "502 Bad Gateway", code: 502},
		"quota reserved":     &QuotaReservedError{Operation: "fetch_file_content", Remaining: 10, Reserve: 100},
		"unmatchable rule":   &UnmatchableCodeownersRuleError{Path: "schema/.schema"},
	} {
		t.Run(name, func(t *testing.T) {
			_, ok := errors.AsType[authoredError](fmt.Errorf("wrapped: %w", err))
			assert.False(t, ok)
		})
	}
}

// unmarkedErrorTypes lists every error type in this package that is not
// written for the PR author, with the reason. A new type with an Error method
// must either implement AuthoredText or be added here with a reason.
var unmarkedErrorTypes = map[string]string{
	"ConfigReadError":                "Error embeds the GitHub API error that stopped the read, and its own words, a bare \"fetch config at <path>\", say nothing without it",
	"githubUnavailableError":         "Error is the GitHub API failure itself: a network error, a timeout, or a 5xx response",
	"graphQLStatusError":             "Error is the status line of a GitHub endpoint answer, an internal detail",
	"QuotaReservedError":             "Error names the internal request budget, its reserve, and the reset time, which an author cannot act on",
	"UnmatchableCodeownersRuleError": "the review gate replaces it with its own sentence before anything is shown, and Unwrap holds the recovered panic",
}

// Every type with an Error method either has AuthoredText or is listed in
// unmarkedErrorTypes with a reason, and the list holds no stale entries.
func TestEveryErrorTypeChoosesWhetherItIsAuthored(t *testing.T) {
	errorTypes, authoredTypes := errorAndAuthoredTypes(t, ".")

	for name, reason := range unmarkedErrorTypes {
		assert.NotEmpty(t, strings.TrimSpace(reason), "unmarkedErrorTypes[%q] needs a reason", name)
		assert.Contains(t, errorTypes, name, "unmarkedErrorTypes[%q] names a type with no Error method; remove the stale entry", name)
		assert.NotContains(t, authoredTypes, name, "unmarkedErrorTypes[%q] has AuthoredText; remove it from the list", name)
	}
	for name := range errorTypes {
		_, unmarked := unmarkedErrorTypes[name]
		assert.True(t, authoredTypes[name] || unmarked,
			"%s has an Error method: give it AuthoredText, or add it to unmarkedErrorTypes with a reason", name)
	}
	for name := range authoredTypes {
		assert.Contains(t, errorTypes, name, "%s has AuthoredText but no Error method", name)
	}
}

// errorAndAuthoredTypes parses the non-test Go files of dir and returns the
// names of the types with an Error() string method and of the types with an
// AuthoredText() (string, bool) method.
func errorAndAuthoredTypes(t *testing.T, dir string) (errorTypes, authoredTypes map[string]bool) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	fset := token.NewFileSet()
	errorTypes = map[string]bool{}
	authoredTypes = map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}
			receiver := receiverTypeName(fn.Recv.List[0].Type)
			switch {
			case fn.Name.Name == "Error" && resultTypes(fn) == "string":
				errorTypes[receiver] = true
			case fn.Name.Name == "AuthoredText" && resultTypes(fn) == "string,bool":
				authoredTypes[receiver] = true
			}
		}
	}
	return errorTypes, authoredTypes
}

func receiverTypeName(expr ast.Expr) string {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return ""
}

// resultTypes joins the names of a method's result types, or returns "" when
// the method takes parameters.
func resultTypes(fn *ast.FuncDecl) string {
	if fn.Type.Params.NumFields() != 0 || fn.Type.Results == nil {
		return ""
	}
	var names []string
	for _, field := range fn.Type.Results.List {
		ident, ok := field.Type.(*ast.Ident)
		if !ok {
			return ""
		}
		count := max(len(field.Names), 1)
		for range count {
			names = append(names, ident.Name)
		}
	}
	return strings.Join(names, ",")
}
