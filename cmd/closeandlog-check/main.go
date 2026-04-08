// Command closeandlog-check runs the closeandlog analyzer as a standalone tool.
//
// Usage:
//
//	go run ./cmd/closeandlog-check ./...
package main

import (
	"github.com/block/schemabot/pkg/analyzers/closeandlog"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	singlechecker.Main(closeandlog.Analyzer)
}
