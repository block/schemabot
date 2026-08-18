package storagetest

import (
	"go/ast"
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
		for _, arg := range call.Args {
			if isTimeType(info.TypeOf(arg)) {
				return true
			}
		}
		return false
	}
	if selector.Sel.Name != "True" || len(call.Args) == 0 {
		return false
	}

	for _, arg := range call.Args {
		equalCall, ok := arg.(*ast.CallExpr)
		if !ok {
			continue
		}
		equalSelector, ok := equalCall.Fun.(*ast.SelectorExpr)
		if ok && equalSelector.Sel.Name == "Equal" && isTimeType(info.TypeOf(equalSelector.X)) {
			return true
		}
	}
	return false
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
