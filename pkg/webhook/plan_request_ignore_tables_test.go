package webhook

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every plan request the server builds for a repository carries that
// repository's ignore_tables. The planner resolves an entry against the live
// schema, so a request that omits the field is a request to plan the withheld
// table as a drop. The re-plan an apply runs is the sharp case: the refusal
// that catches such a drop derives what it checks from the same field, so an
// omission there disarms the guard that would otherwise report it.
//
// The builders are found by parsing the package rather than listed here, so a
// fifth one added later is held to the same rule.
func TestPlanRequestBuildersCarryIgnoreTables(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)

	fset := token.NewFileSet()
	builders := 0
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, parseErr, path)

		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			selector, ok := lit.Type.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "PlanRequest" {
				return true
			}
			builders++
			assert.True(t, litSetsField(lit, "IgnoreTables"),
				"%s builds a PlanRequest without IgnoreTables; the plan would drop every table the repository withholds",
				fset.Position(lit.Pos()))
			return true
		})
	}
	require.NotZero(t, builders, "found no PlanRequest builders to check")
}

// litSetsField reports whether a composite literal assigns the named field.
func litSetsField(lit *ast.CompositeLit, field string) bool {
	for _, element := range lit.Elts {
		kv, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if key, ok := kv.Key.(*ast.Ident); ok && key.Name == field {
			return true
		}
	}
	return false
}
