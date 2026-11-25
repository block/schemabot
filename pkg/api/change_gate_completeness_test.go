package api

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// changeGateSinks are the functions that admit new work: creating an apply,
// recording a start request, and recording a cutover request.
var changeGateSinks = []string{"enqueueApply", "persistStartRequestForOperator", cutoverRequestSink}

// changeGateChecks are the functions that run the change gates.
var changeGateChecks = []string{"checkChangeGates", "checkChangeGatesForApply"}

// cutoverRequestSink names any function that builds a cutover control request.
const cutoverRequestSink = "<cutover request literal>"

// Every way into a sink must pass through a function that runs the change
// gates. A new caller of a sink that skips checkChangeGates fails this test,
// so a freeze cannot be bypassed by a path added later.
//
// The call graph is built by name from this package's source. Function values
// and method calls count as calls, so the graph over-approximates; a path it
// finds that does not exist still has to be gated or explained here.
//
// A function counts as gated when it calls a gate anywhere, so this test
// cannot tell whether the gate runs before the write on every branch. The
// behavioral tests do: each admitting branch has one that a gate moved after
// its write fails (TestChangeGateRefusesApplyDuringFreeze,
// TestChangeGateHoldsStart, TestChangeGateHoldsStartOfAStoppedApply,
// TestChangeGateHoldsDeferredCutover).
func TestChangeGatesGuardEveryWayToAdmitWork(t *testing.T) {
	decls := parseAPIFuncDecls(t)

	callers := map[string][]string{}
	gated := map[string]bool{}
	for name, decl := range decls {
		for _, ref := range referencedNames(decl.Body) {
			if slices.Contains(changeGateChecks, ref) {
				gated[name] = true
			}
			if _, ok := decls[ref]; ok && ref != name {
				callers[ref] = append(callers[ref], name)
			}
		}
		if buildsCutoverRequest(decl.Body) {
			callers[cutoverRequestSink] = append(callers[cutoverRequestSink], name)
		}
	}

	for _, sink := range changeGateSinks {
		if sink != cutoverRequestSink {
			require.Contains(t, decls, sink, "sink %s no longer exists; update this test", sink)
		}
		require.NotEmpty(t, callers[sink], "nothing reaches %s; update this test", sink)
		for _, path := range ungatedPaths(sink, callers, gated) {
			t.Errorf("%s admits work without running checkChangeGates", strings.Join(path, " -> "))
		}
	}
}

// ungatedPaths walks up from sink and returns every caller chain that reaches
// a function with no callers without passing a gated function.
func ungatedPaths(sink string, callers map[string][]string, gated map[string]bool) [][]string {
	var paths [][]string
	var walk func(name string, path []string)
	walk = func(name string, path []string) {
		if gated[name] || slices.Contains(path, name) {
			return
		}
		path = append([]string{name}, path...)
		up := callers[name]
		if len(up) == 0 && name != sink {
			paths = append(paths, path)
			return
		}
		for _, caller := range up {
			walk(caller, path)
		}
	}
	walk(sink, nil)
	return paths
}

// parseAPIFuncDecls parses this package's non-test source and returns its
// functions and methods by name.
func parseAPIFuncDecls(t *testing.T) map[string]*ast.FuncDecl {
	t.Helper()
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	fset := token.NewFileSet()
	decls := map[string]*ast.FuncDecl{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		for _, d := range file.Decls {
			if fn, ok := d.(*ast.FuncDecl); ok && fn.Body != nil {
				// Methods on different types may share a name; merging them
				// over-approximates, which is the safe direction.
				if existing, dup := decls[fn.Name.Name]; dup {
					existing.Body.List = append(existing.Body.List, fn.Body.List...)
					continue
				}
				decls[fn.Name.Name] = fn
			}
		}
	}
	return decls
}

// referencedNames lists every identifier body names, including the method
// half of a selector.
func referencedNames(body *ast.BlockStmt) []string {
	var names []string
	ast.Inspect(body, func(n ast.Node) bool {
		if ident, ok := n.(*ast.Ident); ok {
			names = append(names, ident.Name)
		}
		return true
	})
	return names
}

// buildsCutoverRequest reports whether body builds an ApplyControlRequest for
// a cutover.
func buildsCutoverRequest(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := lit.Type.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "ApplyControlRequest" {
			return true
		}
		for _, elt := range lit.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Operation" {
				if value, ok := kv.Value.(*ast.SelectorExpr); ok && value.Sel.Name == "ControlOperationCutover" {
					found = true
				}
			}
		}
		return true
	})
	return found
}
