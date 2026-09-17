package metrics

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKnownDirectWriteAuthOperationsCoverNamedCallSites pins the operation
// registry to the authorization gates that name their operation outright.
// RecordDirectWriteAuthorization relabels an unrecognised operation as
// "unknown" — the right default for cardinality — so a gate whose operation is
// missing from knownDirectWriteAuthOperations records every one of its
// decisions under that label, with no compile or test failure to say so. A
// whole route's authorization decisions then become invisible to the dashboard
// that watches for scoped operators attempting what they were not granted.
//
// Only the forward direction is checked. The control routes pass their
// operation down through a shared helper as a parameter, so a registry entry
// with no literal call site is expected rather than dead, and asserting the
// reverse would fail on every one of them.
func TestKnownDirectWriteAuthOperationsCoverNamedCallSites(t *testing.T) {
	// The gates that take the operation as their third argument, after the
	// response writer and the request.
	const operationArgument = 2
	gates := map[string]bool{
		"authorizeDirectWrite":              true,
		"authorizeDirectAdminWrite":         true,
		"authorizeDirectDatabaseWrite":      true,
		"authorizeDirectWriteForStoredPlan": true,
		"finishDirectWriteDecision":         true,
		"authorizeStorageSchemaOperation":   true,
	}

	constants := map[string]string{}
	callSites := map[string][]string{}
	fileSet := token.NewFileSet()
	root := filepath.Join("..", "..")

	// Every constant first, then every call site. Collecting and scanning in
	// one walk resolves only the constants declared in already-walked files, so
	// a gate named through a constant from a later-sorting file reads as
	// "named by a parameter" and is skipped — which is a missing registry entry
	// going undetected, the one thing this test exists to catch.
	parsed := map[string]*ast.File{}
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
		parsed[path] = file
		collectStringConstants(file, filepath.Dir(path), constants)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, parsed, "no non-test Go files found; the scan root is wrong")

	for _, path := range slices.Sorted(maps.Keys(parsed)) {
		file := parsed[path]
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !gates[selector.Sel.Name] || len(call.Args) <= operationArgument {
				return true
			}
			operation, named := stringArgument(call.Args[operationArgument], filepath.Dir(path), constants)
			if !named {
				// A gate handed a parameter or a field takes its operation from
				// whoever called it, which this scan cannot follow. Those call
				// sites are audited where the name is written down instead.
				return true
			}
			callSites[operation] = append(callSites[operation], fmt.Sprintf("%s:%d", path, fileSet.Position(call.Pos()).Line))
			return true
		})
	}
	require.NotEmpty(t, callSites, "no named direct-write authorization call sites found; the scan root or gate matching is broken")

	// Every gate this test claims to watch has to exist. A gate named wrong
	// matches nothing, the scan stays green on the call sites it does find, and
	// the operations behind the misspelled gate are never checked at all.
	declared := map[string]bool{}
	for _, file := range parsed {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				declared[fn.Name.Name] = true
			}
		}
	}
	for gate := range gates {
		assert.True(t, declared[gate], "gate %q is not a function in this repository, so the call sites it is meant to cover are never scanned", gate)
	}

	for operation, sites := range callSites {
		assert.True(t, knownDirectWriteAuthOperations[operation],
			"operation %q gated at %s is missing from knownDirectWriteAuthOperations, so its authorization decisions would all record as %q",
			operation, strings.Join(sites, ", "), "unknown")
	}
}

// collectStringConstants records every package-level string constant by
// directory-qualified name, so an operation named through a constant resolves
// to the same value the compiler gives it.
func collectStringConstants(file *ast.File, dir string, into map[string]string) {
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.CONST {
			continue
		}
		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok || len(valueSpec.Names) != len(valueSpec.Values) {
				continue
			}
			for i, name := range valueSpec.Names {
				literal, ok := valueSpec.Values[i].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				value, err := strconv.Unquote(literal.Value)
				if err != nil {
					continue
				}
				into[dir+"."+name.Name] = value
			}
		}
	}
}

// stringArgument resolves an argument that names its value outright: a string
// literal, or an identifier bound to a string constant in the same package.
func stringArgument(arg ast.Expr, dir string, constants map[string]string) (string, bool) {
	switch expr := arg.(type) {
	case *ast.BasicLit:
		if expr.Kind != token.STRING {
			return "", false
		}
		value, err := strconv.Unquote(expr.Value)
		if err != nil {
			return "", false
		}
		return value, true
	case *ast.Ident:
		value, ok := constants[dir+"."+expr.Name]
		return value, ok
	default:
		return "", false
	}
}
