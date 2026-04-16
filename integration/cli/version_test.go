//go:build integration

package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/e2eutil"
)

func TestCLI_Version(t *testing.T) {
	var cli e2eutil.CLIFinder
	binPath := cli.FindOrBuild(t, "../..", "./pkg/cmd", "../../bin/schemabot")

	out, err := e2eutil.RunCLIWithError(binPath, "", "--version")
	require.NoError(t, err)
	assert.Contains(t, out, "(commit:")
}
