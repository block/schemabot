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
// A request is any variable declared as an *http.Request in a function: a
// parameter of the function or of a closure inside it, a var of that type,
// or one assigned from another request, including through WithContext or
// Clone. Matching is by name within a function, so a different variable that
// shares a request's name is also checked, which is the safe direction. Two
// uses of the body are not reads: comparing it with nil, and wrapping it in
// place with http.MaxBytesReader.
func TestEveryRequestBodyIsDecodedStrictly(t *testing.T) {
	fset := token.NewFileSet()
	foundDecoder := false
	var lenient []string
	for _, file := range parseAPISourceFiles(t, fset) {
		httpName := importedName(file, "net/http")
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if fn.Recv == nil && fn.Name.Name == strictBodyDecoder {
				foundDecoder = true
				continue
			}
			for _, pos := range requestBodyReads(fn, httpName) {
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

// requestBodyReads returns where fn reads a request body.
func requestBodyReads(fn *ast.FuncDecl, httpName string) []token.Pos {
	requests := requestVariables(fn, httpName)
	if len(requests) == 0 {
		return nil
	}
	allowed := bodyUsesThatAreNotReads(fn.Body, requests, httpName)

	var reads []token.Pos
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok || allowed[sel] || !slices.Contains(requestBodyReaders, sel.Sel.Name) {
			return true
		}
		if ident, ok := sel.X.(*ast.Ident); ok && requests[ident.Name] {
			reads = append(reads, sel.Pos())
		}
		return true
	})
	return reads
}

// requestVariables names the variables in fn that hold an *http.Request.
func requestVariables(fn *ast.FuncDecl, httpName string) map[string]bool {
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
	addFields(fn.Recv)
	addFields(fn.Type.Params)

	for {
		before := len(requests)
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.FuncLit:
				addFields(node.Type.Params)
			case *ast.ValueSpec:
				if isHTTPRequestType(node.Type, httpName) {
					for _, name := range node.Names {
						requests[name.Name] = true
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
// of one made with WithContext or Clone.
func derivesFromRequest(expr ast.Expr, requests map[string]bool) bool {
	if call, ok := expr.(*ast.CallExpr); ok {
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (sel.Sel.Name != "WithContext" && sel.Sel.Name != "Clone") {
			return false
		}
		expr = sel.X
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
func bodyUsesThatAreNotReads(body *ast.BlockStmt, requests map[string]bool, httpName string) map[*ast.SelectorExpr]bool {
	allowed := map[*ast.SelectorExpr]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
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
	if ident, ok := sel.X.(*ast.Ident); ok && requests[ident.Name] {
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
