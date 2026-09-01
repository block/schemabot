// Package severityglyphs provides a go/analysis analyzer that flags severity
// glyph literals (🚨 ⛔ ❌ ⚠️ ℹ️) in non-test source files. The project
// convention is that the severity vocabulary lives in pkg/glyph — one glyph
// per meaning, one meaning per glyph — and every rendering site references
// the named constant, so the vocabulary cannot drift surface by surface.
//
// The analyzer inspects decoded string values, so escape-spelled glyphs
// (e.g. "❌") are caught the same as literal ones. It matches on the
// base codepoints (⚠ U+26A0, ℹ U+2139), so variation-selector forms are
// caught too. Apply-state glyphs (🚫 ⏹️ ⏸ ⏳ ↩️ 🔁 ✅) are a separate
// vocabulary owned by pkg/presentation and are not flagged.
//
// The analyzer reports on every matching string literal in every package it
// is invoked against — it does not filter by import path. Callers (Makefile,
// CI, pre-commit script) are responsible for passing only the package set
// where the rule should apply: everything except ./pkg/glyph, the vocabulary's
// legitimate home.
package severityglyphs

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// severityGlyphs pairs each severity glyph's base codepoint with the
// pkg/glyph constant that rendering code must use instead. Matching on the
// base codepoint (not the emoji-presentation sequence) also catches literals
// that omit or add the variation selector.
var severityGlyphs = []struct {
	glyph    string
	constant string
}{
	{"🚨", "glyph.Escalation"},
	{"⛔", "glyph.Refused"},
	{"❌", "glyph.Failed"},
	{"⚠", "glyph.Attention"},
	{"ℹ", "glyph.Info"},
}

// Analyzer flags string literals containing a severity glyph in non-test
// source files. Test files are skipped because assertions on rendered output
// deliberately pin the literal glyphs — they must break when the vocabulary
// drifts.
var Analyzer = &analysis.Analyzer{
	Name:     "severityglyphs",
	Doc:      "flags severity glyph literals (🚨 ⛔ ❌ ⚠️ ℹ️) in non-test files; use the named pkg/glyph constants (callers exclude pkg/glyph itself from the package set)",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	nodeFilter := []ast.Node{(*ast.BasicLit)(nil)}

	insp.Preorder(nodeFilter, func(n ast.Node) {
		lit := n.(*ast.BasicLit)
		if lit.Kind != token.STRING {
			return
		}
		if isTestFile(pass, lit.Pos()) {
			return
		}
		val, err := strconv.Unquote(lit.Value)
		if err != nil {
			return
		}
		for _, sg := range severityGlyphs {
			if strings.Contains(val, sg.glyph) {
				pass.Reportf(lit.Pos(), "severity glyph %s in string literal — use %s from pkg/glyph", sg.glyph, sg.constant)
			}
		}
	})

	return nil, nil
}

func isTestFile(pass *analysis.Pass, pos token.Pos) bool {
	return strings.HasSuffix(pass.Fset.Position(pos).Filename, "_test.go")
}
