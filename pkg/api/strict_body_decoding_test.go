package api

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// strictBodyDecoder is the one function allowed to read a request body.
const strictBodyDecoder = "decodeStrictJSONBody"

// requestBodyReaders are the *http.Request fields and methods that read or
// parse the request body.
var requestBodyReaders = []string{
	"Body", "GetBody",
	"FormValue", "PostFormValue", "FormFile",
	"ParseForm", "ParseMultipartForm", "MultipartReader",
}

// Every request body in this package is read through decodeStrictJSONBody,
// so an unknown field or data after the JSON value answers 400 instead of
// being dropped without a word (AV-6). A handler added later that reads its
// body any other way, whether json.NewDecoder(r.Body), io.ReadAll(r.Body),
// or form parsing, fails this test.
//
// Each top-level declaration is checked, so a handler func literal held in a
// package-level var is covered as well as a function. A request is a
// parameter declared as an *http.Request, of the function or of any func
// literal inside the declaration, a var of that type, or a variable assigned
// or declared from another request, including through any chain of
// WithContext and Clone calls. A body read straight off such a copy, as in
// r.Clone(ctx).Body, counts too. Matching is by name within a declaration,
// so a different variable that shares a request's name is also checked,
// which is the safe direction. Two uses of the body are not reads: comparing
// it with nil, and wrapping it in place with http.MaxBytesReader.
func TestEveryRequestBodyIsDecodedStrictly(t *testing.T) {
	fset := token.NewFileSet()
	foundDecoder := false
	var lenient []string
	for _, file := range parseAPISourceFiles(t, fset) {
		httpName := importedName(file, "net/http")
		for _, decl := range file.Decls {
			// A package-level var can hold a handler func literal, so every
			// declaration is a scope, not only functions.
			var scope ast.Node = decl
			var params []*ast.FieldList
			if fn, ok := decl.(*ast.FuncDecl); ok {
				if fn.Body == nil {
					continue
				}
				if fn.Recv == nil && fn.Name.Name == strictBodyDecoder {
					foundDecoder = true
					continue
				}
				scope = fn.Body
				params = []*ast.FieldList{fn.Recv, fn.Type.Params}
			}
			for _, pos := range requestBodyReads(scope, params, httpName) {
				lenient = append(lenient, fset.Position(pos).String())
			}
		}
	}

	require.True(t, foundDecoder, "%s no longer exists; update this test", strictBodyDecoder)
	for _, at := range lenient {
		t.Errorf("%s reads a request body outside %s; decode it with %s so an unknown field or data after the JSON value answers 400",
			at, strictBodyDecoder, strictBodyDecoder)
	}
}

// parseAPISourceFiles parses this package's non-test source.
func parseAPISourceFiles(t *testing.T, fset *token.FileSet) []*ast.File {
	t.Helper()
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		files = append(files, file)
	}
	return files
}

// importedName returns the name file refers to the package at path by, or ""
// when file does not import it.
func importedName(file *ast.File, path string) string {
	for _, spec := range file.Imports {
		if value, err := strconv.Unquote(spec.Path.Value); err != nil || value != path {
			continue
		}
		if spec.Name != nil {
			return spec.Name.Name
		}
		return filepath.Base(path)
	}
	return ""
}

// requestBodyReads returns where scope, with the given parameter lists,
// reads a request body.
func requestBodyReads(scope ast.Node, params []*ast.FieldList, httpName string) []token.Pos {
	requests := requestVariables(scope, params, httpName)
	if len(requests) == 0 {
		return nil
	}
	allowed := bodyUsesThatAreNotReads(scope, requests, httpName)

	var reads []token.Pos
	ast.Inspect(scope, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok || allowed[sel] || !slices.Contains(requestBodyReaders, sel.Sel.Name) {
			return true
		}
		if derivesFromRequest(sel.X, requests) {
			reads = append(reads, sel.Pos())
		}
		return true
	})
	return reads
}

// requestVariables names the variables in scope that hold an *http.Request.
func requestVariables(scope ast.Node, params []*ast.FieldList, httpName string) map[string]bool {
	requests := map[string]bool{}
	addFields := func(fields *ast.FieldList) {
		if fields == nil {
			return
		}
		for _, field := range fields.List {
			if isHTTPRequestType(field.Type, httpName) {
				for _, name := range field.Names {
					requests[name.Name] = true
				}
			}
		}
	}
	for _, fields := range params {
		addFields(fields)
	}

	for {
		before := len(requests)
		ast.Inspect(scope, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.FuncLit:
				addFields(node.Type.Params)
			case *ast.ValueSpec:
				if isHTTPRequestType(node.Type, httpName) {
					for _, name := range node.Names {
						requests[name.Name] = true
					}
				}
				if len(node.Names) == len(node.Values) {
					for i, value := range node.Values {
						if derivesFromRequest(value, requests) {
							requests[node.Names[i].Name] = true
						}
					}
				}
			case *ast.AssignStmt:
				if len(node.Lhs) != len(node.Rhs) {
					return true
				}
				for i, rhs := range node.Rhs {
					lhs, ok := node.Lhs[i].(*ast.Ident)
					if ok && derivesFromRequest(rhs, requests) {
						requests[lhs.Name] = true
					}
				}
			}
			return true
		})
		if len(requests) == before {
			return requests
		}
	}
}

// derivesFromRequest reports whether expr is a request variable, or a copy
// of one made with any chain of WithContext and Clone calls, such as
// r.WithContext(ctx).Clone(ctx), since both keep the request body.
func derivesFromRequest(expr ast.Expr, requests map[string]bool) bool {
	for {
		call, ok := expr.(*ast.CallExpr)
		if !ok {
			break
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (sel.Sel.Name != "WithContext" && sel.Sel.Name != "Clone") {
			return false
		}
		expr = sel.X
	}
	if paren, ok := expr.(*ast.ParenExpr); ok {
		return derivesFromRequest(paren.X, requests)
	}
	ident, ok := expr.(*ast.Ident)
	return ok && requests[ident.Name]
}

// isHTTPRequestType reports whether expr is http.Request or *http.Request.
func isHTTPRequestType(expr ast.Expr, httpName string) bool {
	if httpName == "" || expr == nil {
		return false
	}
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Request" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == httpName
}

// bodyUsesThatAreNotReads returns the request.Body selectors in body that
// only compare the body with nil or wrap it in place with
// http.MaxBytesReader.
func bodyUsesThatAreNotReads(scope ast.Node, requests map[string]bool, httpName string) map[*ast.SelectorExpr]bool {
	allowed := map[*ast.SelectorExpr]bool{}
	ast.Inspect(scope, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.BinaryExpr:
			if node.Op != token.EQL && node.Op != token.NEQ {
				return true
			}
			if sel := requestBody(node.X, requests); sel != nil && isNil(node.Y) {
				allowed[sel] = true
			}
			if sel := requestBody(node.Y, requests); sel != nil && isNil(node.X) {
				allowed[sel] = true
			}
		case *ast.AssignStmt:
			if len(node.Lhs) != 1 || len(node.Rhs) != 1 {
				return true
			}
			target := requestBody(node.Lhs[0], requests)
			call, ok := node.Rhs[0].(*ast.CallExpr)
			if target == nil || !ok || len(call.Args) != 3 || !isPackageFunc(call.Fun, httpName, "MaxBytesReader") {
				return true
			}
			wrapped := requestBody(call.Args[1], requests)
			if wrapped != nil && sameIdent(wrapped.X, target.X) {
				allowed[target] = true
				allowed[wrapped] = true
			}
		}
		return true
	})
	return allowed
}

// requestBody returns expr as a request.Body selector, or nil.
func requestBody(expr ast.Expr, requests map[string]bool) *ast.SelectorExpr {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Body" {
		return nil
	}
	if derivesFromRequest(sel.X, requests) {
		return sel
	}
	return nil
}

// sameIdent reports whether a and b are the same identifier.
func sameIdent(a, b ast.Expr) bool {
	identA, okA := a.(*ast.Ident)
	identB, okB := b.(*ast.Ident)
	return okA && okB && identA.Name == identB.Name
}

func isNil(expr ast.Expr) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == "nil"
}

// isPackageFunc reports whether expr is pkg.name.
func isPackageFunc(expr ast.Expr, pkg, name string) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && pkg != "" && ident.Name == pkg
}
