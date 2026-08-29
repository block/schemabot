package metrics

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKnownRecoveredPanicOperationsMatchCallSites pins the operation registry
// to its call sites in both directions. RecordRecoveredPanic silently relabels
// an unrecognised operation as "unknown" (the right default for cardinality),
// which means a call site whose operation is missing from
// knownRecoveredPanicOperations mislabels its metric with no compile or test
// failure — and a registry entry with no remaining call site is a dead label.
// Operations must be string literals so the registry stays auditable from this
// test alone.
func TestKnownRecoveredPanicOperationsMatchCallSites(t *testing.T) {
	callSites := map[string][]string{}
	fileSet := token.NewFileSet()
	root := filepath.Join("..", "..")
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "bin") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(fileSet, path, nil, 0)
		if parseErr != nil {
			return fmt.Errorf("parse %s: %w", path, parseErr)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "RecordRecoveredPanic" || len(call.Args) != 2 {
				return true
			}
			where := fmt.Sprintf("%s:%d", path, fileSet.Position(call.Pos()).Line)
			literal, ok := call.Args[1].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				assert.Fail(t, "non-literal recovered-panic operation",
					"%s: the operation passed to RecordRecoveredPanic must be a string literal so this test can audit the registry", where)
				return true
			}
			operation, unquoteErr := strconv.Unquote(literal.Value)
			require.NoError(t, unquoteErr)
			callSites[operation] = append(callSites[operation], where)
			return true
		})
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, callSites, "no RecordRecoveredPanic call sites found; the scan root or call matching is broken")

	for operation, sites := range callSites {
		assert.True(t, knownRecoveredPanicOperations[operation],
			"operation %q used at %s is missing from knownRecoveredPanicOperations, so its metric would be recorded as %q",
			operation, strings.Join(sites, ", "), "unknown")
	}
	for operation := range knownRecoveredPanicOperations {
		assert.NotEmpty(t, callSites[operation],
			"knownRecoveredPanicOperations contains %q but no call site records it", operation)
	}
}
