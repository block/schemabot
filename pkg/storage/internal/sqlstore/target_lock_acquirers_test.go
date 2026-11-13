package sqlstore

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/packages"
)

// applyTargetLockAcquirers names every exported entry point in this package
// that can take the apply target lock. TestApplyTargetLockPrecedesApplyRowLocks
// drives each of them and fails if any locks an apply row before it takes the
// target lock (OW-5), so the two lists are kept equal by the tests below
// rather than by review.
var applyTargetLockAcquirers = []string{
	"applyStore.AttachOperationWithTasks",
	"applyStore.ClaimApplyByID",
	"applyStore.Create",
	"applyStore.CreateWithGroupedOperations",
	"applyStore.CreateWithTasks",
	"applyStore.CreateWithTasksAndOperations",
	"applyStore.Update",
	"applyStore.WithExclusiveTarget",
	"applyStore.WithTargetHeld",
}

// packageLoadDeadline bounds the go/packages load, so a stalled go list
// subprocess fails this test instead of waiting for the suite timeout (go
// test's 10-minute default). The load needs export data for every dependency,
// and go list compiles any that are not in the build cache. After a dependency
// bump that is a cold compile of the changed module, Vitess included, while
// the rest of the suite runs in parallel. That takes over 30 seconds on a CI
// runner, so the bound sits well above it but below the suite timeout.
const packageLoadDeadline = 5 * time.Minute

// TestApplyTargetLockAcquirersAreListed walks this package's call graph and
// checks that the exported entry points reaching acquireApplyTargetLockConn
// are exactly applyTargetLockAcquirers. A new path to the target lock fails
// here until it is listed, and listing it fails the integration lock-order
// test until that test drives it, so no acquirer escapes the lock-order check.
// The walk covers the function bodies of this package: a call through an
// interface is followed to every package-level type here that implements it,
// and a function taken as a value counts as called where it is taken.
func TestApplyTargetLockAcquirersAreListed(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), packageLoadDeadline)
	defer cancel()
	cfg := &packages.Config{
		Context: ctx,
		Mode:    packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:     ".",
	}
	pkgs, err := packages.Load(cfg, ".")
	require.NoError(t, err)
	require.Len(t, pkgs, 1)
	require.Empty(t, pkgs[0].Errors, "load sqlstore package")
	pkg := pkgs[0]

	target, ok := pkg.Types.Scope().Lookup("acquireApplyTargetLockConn").(*types.Func)
	require.True(t, ok, "acquireApplyTargetLockConn is the one place the apply target lock is taken")

	listed := append([]string(nil), applyTargetLockAcquirers...)
	sort.Strings(listed)
	assert.Equal(t, listed, exportedFuncsReaching(pkg, target),
		"every exported path to the apply target lock must be in applyTargetLockAcquirers and driven by TestApplyTargetLockPrecedesApplyRowLocks")
}

// TestExportedFuncsReachingFollowsInterfaceCalls checks the call-graph walk on
// a small package. An exported function that reaches the target only through
// an interface the package implements is reported beside the one that calls
// it directly, and a call through an interface whose implementations never
// reach the target is not.
func TestExportedFuncsReachingFollowsInterfaceCalls(t *testing.T) {
	const src = `package p

type holder interface{ Hold() }

type rester interface{ Rest() }

type store struct{}

func (store) Hold() { acquire() }

func (store) Rest() {}

func acquire() {}

func Direct() { acquire() }

func ViaInterface(h holder) { h.Hold() }

func Unrelated(r rester) { r.Rest() }
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "p.go", src, parser.SkipObjectResolution)
	require.NoError(t, err)
	info := &types.Info{
		Defs: make(map[*ast.Ident]types.Object),
		Uses: make(map[*ast.Ident]types.Object),
	}
	typesPkg, err := (&types.Config{}).Check("p", fset, []*ast.File{file}, info)
	require.NoError(t, err)
	target, ok := typesPkg.Scope().Lookup("acquire").(*types.Func)
	require.True(t, ok)

	pkg := &packages.Package{Types: typesPkg, TypesInfo: info, Syntax: []*ast.File{file}}
	assert.Equal(t, []string{"Direct", "ViaInterface", "store.Hold"}, exportedFuncsReaching(pkg, target))
}

// exportedFuncsReaching returns the sorted names of the exported functions and
// methods declared in pkg whose calls can reach target.
func exportedFuncsReaching(pkg *packages.Package, target *types.Func) []string {
	callers := packageCallers(pkg)
	reached := map[*types.Func]bool{target: true}
	queue := []*types.Func{target}
	for len(queue) > 0 {
		callee := queue[0]
		queue = queue[1:]
		for _, caller := range callers[callee] {
			if !reached[caller] {
				reached[caller] = true
				queue = append(queue, caller)
			}
		}
	}

	var names []string
	for fn := range reached {
		if fn.Exported() {
			names = append(names, qualifiedFuncName(fn))
		}
	}
	sort.Strings(names)
	return names
}

// packageCallers maps each function declared in pkg to the functions in pkg
// that call it or take it as a value. A use of an interface method counts as
// a use of every method in pkg that can run behind it.
func packageCallers(pkg *packages.Package) map[*types.Func][]*types.Func {
	callers := make(map[*types.Func][]*types.Func)
	behind := make(map[*types.Func][]*types.Func)
	for _, file := range pkg.Syntax {
		for _, decl := range file.Decls {
			fnDecl, ok := decl.(*ast.FuncDecl)
			if !ok || fnDecl.Body == nil {
				continue
			}
			caller, ok := pkg.TypesInfo.Defs[fnDecl.Name].(*types.Func)
			if !ok {
				continue
			}
			ast.Inspect(fnDecl.Body, func(node ast.Node) bool {
				ident, ok := node.(*ast.Ident)
				if !ok {
					return true
				}
				used, ok := pkg.TypesInfo.Uses[ident].(*types.Func)
				if !ok {
					return true
				}
				used = used.Origin()
				callees, resolved := behind[used]
				if !resolved {
					callees = packageFuncsBehind(pkg.Types, used)
					behind[used] = callees
				}
				for _, callee := range callees {
					callers[callee] = append(callers[callee], caller)
				}
				return true
			})
		}
	}
	return callers
}

// packageFuncsBehind returns the functions declared in pkg that a use of fn
// can run. A function or concrete method is itself, when pkg declares it. An
// interface method, whichever package declares the interface, is the
// same-named method of every package-level type in pkg that implements the
// interface.
func packageFuncsBehind(pkg *types.Package, fn *types.Func) []*types.Func {
	recv := fn.Signature().Recv()
	if recv == nil || !types.IsInterface(recv.Type()) {
		if fn.Pkg() != pkg {
			return nil
		}
		return []*types.Func{fn}
	}
	iface, ok := recv.Type().Underlying().(*types.Interface)
	if !ok {
		return nil
	}

	var funcs []*types.Func
	scope := pkg.Scope()
	for _, name := range scope.Names() {
		typeName, ok := scope.Lookup(name).(*types.TypeName)
		if !ok || types.IsInterface(typeName.Type()) {
			continue
		}
		// A pointer's method set includes the value receiver's, so one check
		// covers both receiver kinds.
		ptr := types.NewPointer(typeName.Type())
		if !types.Implements(ptr, iface) {
			continue
		}
		method, _, _ := types.LookupFieldOrMethod(ptr, true, fn.Pkg(), fn.Name())
		if impl, ok := method.(*types.Func); ok && impl.Pkg() == pkg {
			funcs = append(funcs, impl.Origin())
		}
	}
	return funcs
}

// qualifiedFuncName names a function as "receiverType.Method" for methods and
// by its bare name otherwise.
func qualifiedFuncName(fn *types.Func) string {
	sig, ok := fn.Type().(*types.Signature)
	if !ok || sig.Recv() == nil {
		return fn.Name()
	}
	recv := types.Unalias(sig.Recv().Type())
	if ptr, ok := recv.(*types.Pointer); ok {
		recv = types.Unalias(ptr.Elem())
	}
	if named, ok := recv.(*types.Named); ok {
		return named.Obj().Name() + "." + fn.Name()
	}
	return fn.Name()
}
