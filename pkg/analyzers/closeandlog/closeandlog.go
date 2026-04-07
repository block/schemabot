// Package closeandlog provides a go/analysis analyzer that flags Close() calls
// where the error is silently discarded. The project convention is to use
// utils.CloseAndLog from github.com/block/spirit/pkg/utils instead.
package closeandlog

import (
	"go/ast"
	"go/types"
	"sync"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

const message = "use utils.CloseAndLog(x) instead of discarding Close() error"
const bareMessage = "use utils.CloseAndLog(x) instead of bare defer x.Close() (error is silently discarded)"

// Analyzer flags patterns where .Close() errors are silently discarded:
//   - _ = x.Close()
//   - defer func() { _ = x.Close() }()
//   - defer x.Close()
//   - x.Close() (as a standalone statement)
//
// Only flags when Close() returns error — void-returning Close methods are fine.
var Analyzer = &analysis.Analyzer{
	Name:     "closeandlog",
	Doc:      "flags Close() calls that discard errors; use utils.CloseAndLog instead",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	nodeFilter := []ast.Node{
		(*ast.AssignStmt)(nil),
		(*ast.DeferStmt)(nil),
		(*ast.ExprStmt)(nil),
	}

	insp.Preorder(nodeFilter, func(n ast.Node) {
		switch stmt := n.(type) {
		case *ast.AssignStmt:
			checkBlankAssign(pass, stmt)
		case *ast.DeferStmt:
			checkDefer(pass, stmt)
		case *ast.ExprStmt:
			checkExprStmt(pass, stmt)
		}
	})

	return nil, nil
}

// checkBlankAssign flags `_ = x.Close()`.
func checkBlankAssign(pass *analysis.Pass, stmt *ast.AssignStmt) {
	if len(stmt.Lhs) != 1 || len(stmt.Rhs) != 1 {
		return
	}
	ident, ok := stmt.Lhs[0].(*ast.Ident)
	if !ok || ident.Name != "_" {
		return
	}
	call, ok := stmt.Rhs[0].(*ast.CallExpr)
	if !ok {
		return
	}
	if isCloseCall(pass, call) {
		pass.Reportf(stmt.Pos(), message)
	}
}

// checkDefer flags `defer x.Close()` (bare defer without error handling).
// Deferred anonymous functions like `defer func() { _ = x.Close() }()` are
// handled by checkBlankAssign/checkExprStmt visiting the inner statements.
func checkDefer(pass *analysis.Pass, stmt *ast.DeferStmt) {
	if isCloseCall(pass, stmt.Call) {
		pass.Reportf(stmt.Pos(), bareMessage)
	}
}

// checkExprStmt flags `x.Close()` as a standalone statement (return value ignored).
func checkExprStmt(pass *analysis.Pass, stmt *ast.ExprStmt) {
	call, ok := stmt.X.(*ast.CallExpr)
	if !ok {
		return
	}
	if isCloseCall(pass, call) {
		pass.Reportf(stmt.Pos(), message)
	}
}

// isCloseCall returns true if the call expression is a method call to Close()
// on a type where Close() returns error.
func isCloseCall(pass *analysis.Pass, call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Close" {
		return false
	}
	if len(call.Args) != 0 {
		return false
	}

	// Skip resp.Body.Close() — HTTP response body close errors are
	// rarely actionable; the standard defer resp.Body.Close() is fine.
	if isHTTPResponseBody(pass, sel.X) {
		return false
	}

	methodObj := pass.TypesInfo.ObjectOf(sel.Sel)
	if methodObj == nil {
		return false
	}
	sig, ok := methodObj.Type().(*types.Signature)
	if !ok {
		return false
	}

	results := sig.Results()
	if results.Len() != 1 {
		return false
	}
	return types.Implements(results.At(0).Type(), errorInterface())
}

// isHTTPResponseBody reports whether expr is resp.Body where resp is *http.Response.
func isHTTPResponseBody(pass *analysis.Pass, expr ast.Expr) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Body" {
		return false
	}
	t := pass.TypesInfo.TypeOf(sel.X)
	if t == nil {
		return false
	}
	ptr, ok := t.(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := ptr.Elem().(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj.Name() == "Response" && obj.Pkg() != nil && obj.Pkg().Path() == "net/http"
}

var (
	errIfaceOnce sync.Once
	errIface     *types.Interface
)

func errorInterface() *types.Interface {
	errIfaceOnce.Do(func() {
		errIface = types.Universe.Lookup("error").Type().Underlying().(*types.Interface)
	})
	return errIface
}
