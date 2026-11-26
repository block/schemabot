package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Checks that must hold on every engine iterate Engines, so an engine missing
// from it would skip them silently. Every Engine* string constant declared in
// this package must be listed.
func TestEnginesListsEveryEngineConstant(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "types.go", nil, 0)
	require.NoError(t, err)

	var declared []string
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for i, name := range value.Names {
				if !strings.HasPrefix(name.Name, "Engine") || i >= len(value.Values) {
					continue
				}
				lit, ok := value.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				engine, err := strconv.Unquote(lit.Value)
				require.NoError(t, err)
				declared = append(declared, engine)
			}
		}
	}
	require.NotEmpty(t, declared, "found no Engine* constants in types.go")
	assert.ElementsMatch(t, declared, Engines)
}
