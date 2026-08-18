package storagetest

import (
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/packages"
)

// TestParityFamiliesAvoidExactTimeEquality prevents timestamp assertions that
// vary by storage dialect. Plain MySQL datetime columns round fractional
// seconds, while microsecond MySQL and PostgreSQL columns truncate them, so a
// written time.Time need not exactly equal its stored round-trip.
//
// WithinDuration and ordering assertions are allowed. Truncating a value to
// whole seconds before storing it is also safe; this lint deliberately checks
// assertion forms rather than tracing how compared values were constructed.
func TestParityFamiliesAvoidExactTimeEquality(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir: ".",
	}
	pkgs, err := packages.Load(cfg, ".")
	require.NoError(t, err)
	require.Len(t, pkgs, 1)
	require.Empty(t, pkgs[0].Errors, "load storagetest package")

	pkg := pkgs[0]
	for i, file := range pkg.Syntax {
		filename := pkg.CompiledGoFiles[i]
		if strings.HasSuffix(filename, "_test.go") {
			continue
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if isExactTimeAssertion(pkg.TypesInfo, call) {
				pos := pkg.Fset.Position(call.Pos())
				assert.Fail(t, "exact time equality assertion",
					"%s:%d: parity families must compare stored times with WithinDuration or ordering, not exact equality",
					filepath.Base(pos.Filename), pos.Line)
			}
			return true
		})
	}
}

func isExactTimeAssertion(info *types.Info, call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !isTestifyCall(info, selector) {
		return false
	}
	if isExactEqualityName(selector.Sel.Name) {
		// Only the expected/actual pair matters; a time.Time in the trailing
		// msgAndArgs diagnostics does not make the comparison time-sensitive.
		for _, arg := range valueArgs(info, selector, call.Args, 2) {
			if containsTimeType(info.TypeOf(arg)) {
				return true
			}
		}
		return false
	}
	if selector.Sel.Name != "True" && selector.Sel.Name != "False" {
		return false
	}

	condition := valueArgs(info, selector, call.Args, 1)
	if len(condition) != 1 {
		return false
	}
	arg := condition[0]
	for {
		unary, ok := arg.(*ast.UnaryExpr)
		if !ok || unary.Op != token.NOT {
			break
		}
		arg = unary.X
	}
	equalCall, ok := arg.(*ast.CallExpr)
	if !ok {
		return false
	}
	equalSelector, ok := equalCall.Fun.(*ast.SelectorExpr)
	return ok && equalSelector.Sel.Name == "Equal" && isTimeType(info.TypeOf(equalSelector.X))
}

// valueArgs returns up to count compared-value arguments of a testify call,
// skipping the leading *testing.T of the package-function form (the
// assert.New(t) method form binds it at construction) and the trailing
// msgAndArgs diagnostics.
func valueArgs(info *types.Info, selector *ast.SelectorExpr, args []ast.Expr, count int) []ast.Expr {
	start := 1
	if sig, ok := info.ObjectOf(selector.Sel).Type().(*types.Signature); ok && sig.Recv() != nil {
		start = 0
	}
	if start >= len(args) {
		return nil
	}
	return args[start:min(start+count, len(args))]
}

func isExactEqualityName(name string) bool {
	return name == "Equal" || name == "EqualValues" || name == "Exactly"
}

func isTestifyCall(info *types.Info, selector *ast.SelectorExpr) bool {
	object := info.ObjectOf(selector.Sel)
	return object != nil && object.Pkg() != nil &&
		(object.Pkg().Path() == "github.com/stretchr/testify/assert" ||
			object.Pkg().Path() == "github.com/stretchr/testify/require")
}

func isTimeType(typ types.Type) bool {
	return typ != nil && (types.TypeString(typ, nil) == "time.Time" || types.TypeString(typ, nil) == "*time.Time")
}

// containsTimeType reports whether a compared value's type is time.Time or
// transitively carries one — a struct with a timestamp field compared
// wholesale is just as dialect-sensitive as the field compared directly.
func containsTimeType(typ types.Type) bool {
	return typeContainsTime(typ, make(map[types.Type]bool))
}

func typeContainsTime(typ types.Type, seen map[types.Type]bool) bool {
	if typ == nil || seen[typ] {
		return false
	}
	seen[typ] = true
	if types.TypeString(typ, nil) == "time.Time" {
		return true
	}
	switch t := typ.(type) {
	case *types.Pointer:
		return typeContainsTime(t.Elem(), seen)
	case *types.Named:
		return typeContainsTime(t.Underlying(), seen)
	case *types.Struct:
		for field := range t.Fields() {
			if typeContainsTime(field.Type(), seen) {
				return true
			}
		}
	case *types.Slice:
		return typeContainsTime(t.Elem(), seen)
	case *types.Array:
		return typeContainsTime(t.Elem(), seen)
	case *types.Map:
		return typeContainsTime(t.Key(), seen) || typeContainsTime(t.Elem(), seen)
	}
	return false
}
