package commands

import (
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/cliname"
)

// TestCLINameFlagSpelling pins the two independent spellings of the
// --cli-name flag to each other: the hidden kong declaration on Globals,
// which makes kong accept the flag at any position, and the raw-args scan in
// cliname.FromArgs, which consumes the value before kong parses so it can
// feed kong's usage name. A rename of either spelling without the other
// fails one of these assertions.
func TestCLINameFlagSpelling(t *testing.T) {
	var cli struct {
		Globals
		Status struct{} `cmd:""`
	}
	parser, err := kong.New(&cli, kong.Name("schemabot"))
	require.NoError(t, err)

	args := []string{"status", "--cli-name", "acme schemabot"}
	_, err = parser.Parse(args)
	require.NoError(t, err, "kong must accept the hidden flag after a subcommand")
	assert.Equal(t, "acme schemabot", cli.CLIName, "kong parses the value into Globals")
	assert.Equal(t, "acme schemabot", cliname.FromArgs(args), "the raw-args scan finds the same value")
}
