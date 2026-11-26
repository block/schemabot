package webhook

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

// A marked error shows its own words however many callers wrapped it. These
// types carry no cause, so the words are exactly what Error reports.
func TestAuthoredErrorsReturnTheirWordsThroughWraps(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "environment the database does not have",
			err:  &environmentNotConfiguredError{Database: "orders", Environment: "staging"},
			want: `database "orders" environment "staging" is not configured on this server`,
		},
		{
			name: "schema config outside the allowed directories",
			err:  &schemaConfigOutsideAllowedDirsError{Database: "orders", DatabaseType: "mysql", SchemaPath: "schema/orders"},
			want: `schema config for database "orders" at "schema/orders" is outside server allowed_dirs`,
		},
		{
			name: "two schema configs for databases this deployment has not registered",
			err: &databaseNotRegisteredError{Configs: []unregisteredSchemaConfig{
				{Database: "orders", DatabaseType: "mysql", SchemaPath: "schema/orders"},
				{Database: "users", DatabaseType: "mysql", SchemaPath: "schema/users"},
			}},
			want: `schema config for database "orders" at "schema/orders", database "users" at "schema/users": database not registered on this deployment, and schema directory under no expected participant's paths`,
		},
		{
			name: "plan refused because the pull request is closed",
			err:  &closedPRRefreshError{repo: "octocat/hello-world", pr: 7},
			want: "PR #7 in octocat/hello-world is closed; SchemaBot refreshes check state only for open PRs",
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

// An error that embeds the cause it was built from stays unmarked, so a
// renderer shows its fixed line instead of the cause.
func TestSharedSchemaPeerUnreadableErrorIsNotMarked(t *testing.T) {
	err := &sharedSchemaPeerUnreadableError{
		Peer: "users", Database: "orders", Environment: "staging",
		Cause: errors.New("GET https://api.github.example/repos/o/r/contents/schemabot.yaml: 502 Bad Gateway"),
	}
	_, ok := errors.AsType[authoredError](fmt.Errorf("wrapped: %w", err))
	assert.False(t, ok)
}

// unmarkedErrorTypes lists every error type in this package that is not
// written for the PR author, with the reason. A new type with an Error method
// must either implement AuthoredText or be added here with a reason.
var unmarkedErrorTypes = map[string]string{
	"sharedSchemaPeerUnreadableError": "Error embeds the cause of the failed read, and its own words, \"read the schema files of <peer> ... to compare their ignore_tables\", state no failure without it",
	"rollbackConfirmRejectionError":   "a transparent wrapper that renders the wrapped error verbatim, so the wrapped error is the one to mark",
	"unlockRejectionError":            "a transparent wrapper that renders the wrapped error verbatim, so the wrapped error is the one to mark",
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
