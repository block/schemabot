// Command cleanupctx-check runs the cleanupctx analyzer as a standalone tool.
//
// Usage:
//
//	go run ./cmd/cleanupctx-check ./...
//
// The rule only concerns test files, but the analyzer must be given the build
// tags the test files carry — integration and e2e tests are invisible to a
// plain run — so callers invoke it once per tag set.
package main

import (
	"github.com/block/schemabot/pkg/analyzers/cleanupctx"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	singlechecker.Main(cleanupctx.Analyzer)
}
