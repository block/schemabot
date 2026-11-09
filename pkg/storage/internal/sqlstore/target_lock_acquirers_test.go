package sqlstore

import (
	"context"
	"go/ast"
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
// subprocess fails this test instead of waiting for the suite timeout.
const packageLoadDeadline = 30 * time.Second

// TestApplyTargetLockAcquirersAreListed walks this package's call graph and
// checks that the exported entry points reaching acquireApplyTargetLockConn
// are exactly applyTargetLockAcquirers. A new path to the target lock fails
// here until it is listed, and listing it fails the integration lock-order
// test until that test drives it, so no acquirer escapes the lock-order check.
// Calls made through an interface are not followed; every current path
// reaches the lock through concrete calls.
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

	var acquirers []string
	for fn := range reached {
		if fn.Exported() {
			acquirers = append(acquirers, qualifiedFuncName(fn))
		}
	}
	sort.Strings(acquirers)
	listed := append([]string(nil), applyTargetLockAcquirers...)
	sort.Strings(listed)
	assert.Equal(t, listed, acquirers,
		"every exported path to the apply target lock must be in applyTargetLockAcquirers and driven by TestApplyTargetLockPrecedesApplyRowLocks")
}

// packageCallers maps each function declared in pkg to the functions in pkg
// that call it or take it as a value.
func packageCallers(pkg *packages.Package) map[*types.Func][]*types.Func {
	callers := make(map[*types.Func][]*types.Func)
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
				callee, ok := pkg.TypesInfo.Uses[ident].(*types.Func)
				if ok && callee.Pkg() == pkg.Types {
					callee = callee.Origin()
					callers[callee] = append(callers[callee], caller)
				}
				return true
			})
		}
	}
	return callers
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
