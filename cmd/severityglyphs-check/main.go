// Command severityglyphs-check runs the severityglyphs analyzer as a
// standalone tool.
//
// Usage:
//
//	go run ./cmd/severityglyphs-check ./pkg/...
//
// The analyzer reports on every package it is given. The caller is
// responsible for excluding ./pkg/glyph (the severity vocabulary's
// legitimate home for the glyph literals).
package main

import (
	"github.com/block/schemabot/pkg/analyzers/severityglyphs"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	singlechecker.Main(severityglyphs.Analyzer)
}
