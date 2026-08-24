package storagetest

import (
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
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
// It also fails open for forms outside its scope: compound boolean conditions
// (a.Equal(b) && other), NotEqual, defined time types (type X time.Time), and
// interface- or any-typed arguments are not flagged.
func TestParityFamiliesAvoidExactTimeEquality(t *testing.T) {
	// Tests is deliberately left false: the parity families dispatched by Run
	// live only in the package's non-test files, and this package's own test
	// files are lint infrastructure rather than parity assertions.
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  ".",
	}
	pkgs, err := packages.Load(cfg, ".")
	require.NoError(t, err)
	require.Len(t, pkgs, 1)
	require.Empty(t, pkgs[0].Errors, "load storagetest package")

	pkg := pkgs[0]
	for _, file := range pkg.Syntax {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if detail, flagged := exactTimeAssertion(pkg.TypesInfo, call); flagged {
				pos := pkg.Fset.Position(call.Pos())
				assert.Fail(t, "exact time equality assertion",
					"%s:%d: %s: parity families must compare stored times with WithinDuration or ordering, not exact equality",
					filepath.Base(pos.Filename), pos.Line, detail)
			}
			return true
		})
	}
}

// exactTimeAssertion reports whether a call asserts exact time equality,
// returning a description of the offending time value so the failure names
// the field to fix, not just the line.
func exactTimeAssertion(info *types.Info, call *ast.CallExpr) (string, bool) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !isTestifyCall(info, selector) {
		return "", false
	}
	if isExactEqualityName(selector.Sel.Name) {
		// Only the expected/actual pair matters; a time.Time in the trailing
		// msgAndArgs diagnostics does not make the comparison time-sensitive.
		for _, arg := range valueArgs(info, selector, call.Args, 2) {
			if path, found := timeFieldPath(info.TypeOf(arg)); found {
				return path, true
			}
		}
		return "", false
	}
	if selector.Sel.Name != "True" && selector.Sel.Name != "False" {
		return "", false
	}

	condition := valueArgs(info, selector, call.Args, 1)
	if len(condition) != 1 {
		return "", false
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
		return "", false
	}
	equalSelector, ok := equalCall.Fun.(*ast.SelectorExpr)
	if ok && equalSelector.Sel.Name == "Equal" && isTimeType(info.TypeOf(equalSelector.X)) {
		return "time.Time.Equal", true
	}
	return "", false
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
	if typ == nil {
		return false
	}
	typ = types.Unalias(typ)
	if ptr, ok := typ.(*types.Pointer); ok {
		typ = types.Unalias(ptr.Elem())
	}
	return types.TypeString(typ, nil) == "time.Time"
}

// timeFieldPath returns a dotted path from a compared value's type to the
// first time.Time it carries (for example "storage.Lock.CreatedAt"), or false
// when it carries none. A struct with a timestamp field compared wholesale is
// just as dialect-sensitive as the field compared directly.
func timeFieldPath(typ types.Type) (string, bool) {
	return timePath(typ, make(map[types.Type]bool))
}

func timePath(typ types.Type, seen map[types.Type]bool) (string, bool) {
	if typ == nil {
		return "", false
	}
	typ = types.Unalias(typ)
	if seen[typ] {
		return "", false
	}
	seen[typ] = true
	if types.TypeString(typ, nil) == "time.Time" {
		return "time.Time", true
	}
	switch t := typ.(type) {
	case *types.Pointer:
		return timePath(t.Elem(), seen)
	case *types.Named:
		path, found := timePath(t.Underlying(), seen)
		if !found {
			return "", false
		}
		return types.TypeString(t, packageNameQualifier) + "." + path, true
	case *types.Struct:
		for field := range t.Fields() {
			path, found := timePath(field.Type(), seen)
			if !found {
				continue
			}
			if path == "time.Time" {
				return field.Name(), true
			}
			return field.Name() + "." + path, true
		}
	case *types.Slice:
		return timePath(t.Elem(), seen)
	case *types.Array:
		return timePath(t.Elem(), seen)
	case *types.Map:
		if path, found := timePath(t.Key(), seen); found {
			return path, true
		}
		return timePath(t.Elem(), seen)
	}
	return "", false
}

func packageNameQualifier(p *types.Package) string {
	return p.Name()
}
