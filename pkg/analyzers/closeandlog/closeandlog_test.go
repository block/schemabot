package closeandlog_test

import (
	"testing"

	"github.com/block/schemabot/pkg/analyzers/closeandlog"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, closeandlog.Analyzer, "example")
}

func TestAnalyzer_HTTPBodyExempt(t *testing.T) {
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, closeandlog.Analyzer, "httpexample")
}
