package severityglyphs_test

import (
	"testing"

	"github.com/block/schemabot/pkg/analyzers/severityglyphs"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, severityglyphs.Analyzer, "example")
}
