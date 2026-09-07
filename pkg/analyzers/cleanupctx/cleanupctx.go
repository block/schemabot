// Package cleanupctx provides a go/analysis analyzer that flags the test
// context being used inside a t.Cleanup callback.
//
// The testing package cancels the context returned by t.Context() just before
// it runs the Cleanup-registered functions, so teardown that reaches for it
// gets a context that is already in the Canceled state. Every operation the
// callback then performs — DROP DATABASE, DROP TABLE, a lock release, a
// provider shutdown — fails immediately. The failure is invisible whenever the
// call site discards its error, which teardown code usually does, so the
// databases and tables the test created are simply left behind on the shared
// test server, where they accumulate and interfere with later tests.
//
// Both spellings of the mistake are reported:
//
//	t.Cleanup(func() {
//		db.ExecContext(t.Context(), "DROP DATABASE ...") // called inside
//	})
//
//	ctx := t.Context()
//	t.Cleanup(func() {
//		db.ExecContext(ctx, "DROP DATABASE ...") // captured outside
//	})
//
// The fix is a context with a lifetime of its own, which teardown gets from
// testctx.Cleanup:
//
//	t.Cleanup(func() {
//		ctx, cancel := testctx.Cleanup(t, 30*time.Second)
//		defer cancel()
//		db.ExecContext(ctx, "DROP DATABASE ...")
//	})
//
// A bare context.WithoutCancel(t.Context()) is accepted too, since detaching
// the test context from its cancellation is the operation testctx.Cleanup
// performs. Note that plain context.Background() is not a usable third option
// here: it is correct at runtime, but the usetesting linter rewrites it back
// to t.Context(), which is how this defect gets reintroduced.
package cleanupctx

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer flags uses of the test context inside t.Cleanup callbacks.
var Analyzer = &analysis.Analyzer{
	Name:     "cleanupctx",
	Doc:      "flags t.Context() inside t.Cleanup callbacks, where it is already cancelled; use testctx.Cleanup(t, timeout) so teardown outlives the test",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	// Variables initialized from a test context, so a callback that captures
	// one instead of calling t.Context() itself is reported the same way.
	testCtxVars := collectTestContextVars(pass, insp)

	insp.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node) {
		call := n.(*ast.CallExpr)
		recv, body, ok := cleanupCallback(pass, call)
		if !ok {
			return
		}
		reportTestContextUses(pass, body, recv, testCtxVars)
	})

	return nil, nil
}

// cleanupCallback reports whether call is `<tb>.Cleanup(func() { ... })` on a
// testing type, returning the receiver's object and the callback body.
func cleanupCallback(pass *analysis.Pass, call *ast.CallExpr) (types.Object, *ast.BlockStmt, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Cleanup" || len(call.Args) != 1 {
		return nil, nil, false
	}
	recv, ok := testingReceiver(pass, sel.X)
	if !ok {
		return nil, nil, false
	}
	lit, ok := call.Args[0].(*ast.FuncLit)
	if !ok {
		return nil, nil, false
	}
	return recv, lit.Body, true
}

// reportTestContextUses walks a cleanup callback body and reports each use of
// the test context that is not detached from the test's cancellation.
func reportTestContextUses(pass *analysis.Pass, body *ast.BlockStmt, recv types.Object, testCtxVars map[types.Object]bool) {
	// Ancestors of the node currently being visited, so a use can be tested
	// for an enclosing context.WithoutCancel call.
	var stack []ast.Node

	ast.Inspect(body, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		defer func() { stack = append(stack, n) }()

		if detachedBy(pass, stack) {
			return true
		}

		if call, ok := n.(*ast.CallExpr); ok && isContextCallOn(pass, call, recv) {
			pass.Reportf(call.Pos(), "%s.Context() is already cancelled inside %s.Cleanup — use testctx.Cleanup(%s, timeout) so teardown outlives the test",
				recv.Name(), recv.Name(), recv.Name())
			return true
		}

		// A context variable captured from the enclosing test function is
		// cancelled by the time the callback runs, exactly as a fresh
		// t.Context() call would be.
		if id, ok := n.(*ast.Ident); ok && testCtxVars[pass.TypesInfo.Uses[id]] {
			pass.Reportf(id.Pos(), "%s is the test context and is already cancelled inside %s.Cleanup — use testctx.Cleanup(%s, timeout) so teardown outlives the test",
				id.Name, recv.Name(), recv.Name())
		}
		return true
	})
}

// collectTestContextVars returns the variables whose value comes from a
// testing type's Context method, e.g. `ctx := t.Context()`.
func collectTestContextVars(pass *analysis.Pass, insp *inspector.Inspector) map[types.Object]bool {
	vars := map[types.Object]bool{}

	record := func(lhs, rhs []ast.Expr) {
		if len(lhs) != 1 || len(rhs) != 1 {
			return
		}
		call, ok := rhs[0].(*ast.CallExpr)
		if !ok || !isContextCallOn(pass, call, nil) {
			return
		}
		id, ok := lhs[0].(*ast.Ident)
		if !ok {
			return
		}
		if obj := pass.TypesInfo.Defs[id]; obj != nil {
			vars[obj] = true
		}
	}

	insp.Preorder([]ast.Node{(*ast.AssignStmt)(nil), (*ast.ValueSpec)(nil)}, func(n ast.Node) {
		switch decl := n.(type) {
		case *ast.AssignStmt:
			record(decl.Lhs, decl.Rhs)
		case *ast.ValueSpec:
			lhs := make([]ast.Expr, 0, len(decl.Names))
			for _, name := range decl.Names {
				lhs = append(lhs, name)
			}
			record(lhs, decl.Values)
		}
	})

	return vars
}

// detachedBy reports whether any ancestor is a context.WithoutCancel call,
// which gives the test context a lifetime independent of the test.
func detachedBy(pass *analysis.Pass, stack []ast.Node) bool {
	for _, anc := range stack {
		call, ok := anc.(*ast.CallExpr)
		if !ok {
			continue
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "WithoutCancel" {
			continue
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok {
			continue
		}
		if name, ok := pass.TypesInfo.Uses[pkg].(*types.PkgName); ok && name.Imported().Path() == "context" {
			return true
		}
	}
	return false
}

// isContextCallOn reports whether call is a no-argument Context() method call
// on a testing type. When recv is non-nil the call must be on that exact
// object; otherwise any testing receiver matches.
func isContextCallOn(pass *analysis.Pass, call *ast.CallExpr, recv types.Object) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Context" || len(call.Args) != 0 {
		return false
	}
	obj, ok := testingReceiver(pass, sel.X)
	if !ok {
		return false
	}
	return recv == nil || obj == recv
}

// testingReceiver resolves expr to the object it names when that object is one
// of the testing package's test types (*testing.T, *testing.B, *testing.F, or
// the testing.TB interface).
func testingReceiver(pass *analysis.Pass, expr ast.Expr) (types.Object, bool) {
	id, ok := expr.(*ast.Ident)
	if !ok {
		return nil, false
	}
	obj := pass.TypesInfo.Uses[id]
	if obj == nil {
		return nil, false
	}
	if !isTestingType(obj.Type()) {
		return nil, false
	}
	return obj, true
}

func isTestingType(t types.Type) bool {
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}
	named, ok := t.(*types.Named)
	if !ok {
		return false
	}
	pkg := named.Obj().Pkg()
	if pkg == nil || pkg.Path() != "testing" {
		return false
	}
	switch named.Obj().Name() {
	case "T", "B", "F", "TB":
		return true
	}
	return false
}
