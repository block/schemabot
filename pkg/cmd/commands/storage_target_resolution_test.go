package commands

import (
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

// Resolving the storage target is a read of secret references, so the commands
// do it in exactly one place and pass the answer around (see
// storageSchemaTargetFlags.target). A second call site would be invisible in
// behavior — both resolutions usually agree — and would only show itself on the
// run where the value changed in between, which is the run that converges a
// database its own preview never looked at.
//
// Each resolution is also a call to someone else's secret store, recorded in
// its audit trail as one more read of SchemaBot's storage credential.
func TestStorageTargetIsResolvedInOnePlace(t *testing.T) {
	const (
		resolver = "resolveStorageTarget"
		memo     = "target"
	)

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	callers := map[string][]string{}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		require.NoError(t, err, "parse %s", name)

		var enclosing string
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.FuncDecl:
				enclosing = n.Name.Name
			case *ast.CallExpr:
				if ident, ok := n.Fun.(*ast.Ident); ok && ident.Name == resolver {
					callers[enclosing] = append(callers[enclosing], name)
				}
			}
			return true
		})
	}

	assert.Equal(t, map[string][]string{memo: {"storage_schema.go"}}, callers,
		"the storage target is resolved by %s alone; a caller that resolves its own would read the storage credential a second time and could get a different database than the one already previewed", memo)
}
