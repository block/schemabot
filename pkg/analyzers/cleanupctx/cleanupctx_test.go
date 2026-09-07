package cleanupctx_test

import (
	"testing"

	"github.com/block/schemabot/pkg/analyzers/cleanupctx"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, cleanupctx.Analyzer, "example")
}
