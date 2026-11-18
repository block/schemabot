package templates

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every progress preview with SQL declares its dialect so generated examples
// exercise the real formatter instead of the unknown-dialect display fallback.
func TestProgressPreviewDialects(t *testing.T) {
	paths, err := filepath.Glob("preview_*.go")
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			positions := token.NewFileSet()
			file, err := parser.ParseFile(positions, path, nil, 0)
			require.NoError(t, err)
			check := func(literal *ast.CompositeLit) {
				var sql, dialect ast.Expr
				for _, element := range literal.Elts {
					field, ok := element.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					key, ok := field.Key.(*ast.Ident)
					if !ok {
						continue
					}
					switch key.Name {
					case "DDL":
						sql = field.Value
					case "Dialect":
						dialect = field.Value
					}
				}
				if sql == nil {
					return
				}
				if value, ok := sql.(*ast.BasicLit); ok && value.Value == `""` {
					return
				}
				assert.NotNil(t, dialect, "%s: SQL preview must declare its dialect", positions.Position(literal.Pos()))
				if value, ok := dialect.(*ast.BasicLit); ok {
					assert.NotEqual(t, `""`, value.Value, "%s: dialect must not be empty", positions.Position(literal.Pos()))
				}
			}
			ast.Inspect(file, func(node ast.Node) bool {
				literal, ok := node.(*ast.CompositeLit)
				if !ok {
					return true
				}
				if name, ok := literal.Type.(*ast.Ident); ok && name.Name == "TableProgress" {
					check(literal)
				}
				if array, ok := literal.Type.(*ast.ArrayType); ok {
					if name, ok := array.Elt.(*ast.Ident); ok && name.Name == "TableProgress" {
						for _, element := range literal.Elts {
							if child, ok := element.(*ast.CompositeLit); ok {
								check(child)
							}
						}
					}
				}
				return true
			})
		})
	}
}
