package main

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// renderTopLevelHelp returns `schemabot --help` as a user sees it, built from
// the same CLI struct and the same help options main passes to kong.
func renderTopLevelHelp(t *testing.T) string {
	t.Helper()

	var cli CLI
	var out bytes.Buffer
	parser, err := kong.New(&cli,
		kong.Name("schemabot"),
		kong.Description("Declarative schema GitOps orchestrator"),
		kong.Writers(&out, io.Discard),
		kong.Exit(func(int) {}),
		kong.ConfigureHelp(helpOptions()),
		kong.Vars{"version": "test", "cli_name": "schemabot"},
	)
	require.NoError(t, err)

	// kong prints help from the --help flag and then reports the missing
	// command, so the error is expected and the written page is the subject.
	_, _ = parser.Parse([]string{"--help"})
	require.Contains(t, out.String(), "Commands:", "help page did not render")
	return out.String()
}

// helpCommandListing returns the command listing of a help page, without the
// usage, description, flags, and closing hint around it.
func helpCommandListing(t *testing.T, help string) string {
	t.Helper()

	_, listing, found := strings.Cut(help, "Commands:\n")
	require.True(t, found, "help page has no command listing")
	listing, _, _ = strings.Cut(listing, "\nRun \"")
	return listing
}

// helpCommandNames returns the command named by each row of a command
// listing, in the order the page prints them. A row is indented two spaces
// and starts with the command name; section headings sit at the left margin,
// and a help string that wraps is indented under its column.
func helpCommandNames(t *testing.T, listing string) []string {
	t.Helper()

	var names []string
	for line := range strings.SplitSeq(listing, "\n") {
		if !strings.HasPrefix(line, "  ") || strings.HasPrefix(line, "   ") {
			continue
		}
		names = append(names, strings.Fields(line)[0])
	}
	require.NotEmpty(t, names, "command listing had no rows")
	return names
}

// The top-level listing is the first page a newcomer reads, so it stays one
// screen tall: one line per command, subcommands left to each command's own
// --help, and nothing the CLI does not advertise.
func TestTopLevelHelpIsOneLinePerCommand(t *testing.T) {
	help := renderTopLevelHelp(t)
	listing := helpCommandListing(t, help)

	assert.Subset(t, helpCommandNames(t, listing), []string{"plan", "apply", "status", "storage", "local", "serve"},
		"the listing should name what SchemaBot does")

	for _, unlisted := range []string{"preview", "fix-lint"} {
		assert.NotContains(t, helpCommandNames(t, listing), unlisted,
			"%s is hidden and must stay out of the listing", unlisted)
	}

	// Compact: a command's help shares its row rather than taking the next line.
	assert.Regexp(t, `(?m)^  plan +Create a schema change plan$`, listing)

	// Unexpanded: no subcommand reaches the top-level page, hidden or not.
	for _, nested := range []string{
		"storage plan", "freeze create", "checks backfill",
		"configure setup", "local status", "local managed", "local serve",
	} {
		assert.NotContains(t, listing, nested, "%q belongs on its parent's own --help", nested)
	}
}
