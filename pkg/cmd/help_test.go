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

// renderHelp returns a help page as a user sees it, built from the same CLI
// struct and the same help options and groups main passes to kong. With no
// command it renders the top-level page; with one it renders that command's.
func renderHelp(t *testing.T, command ...string) string {
	t.Helper()

	var cli CLI
	var out bytes.Buffer
	parser, err := kong.New(&cli,
		kong.Name("schemabot"),
		kong.Description("Declarative schema GitOps orchestrator"),
		kong.Writers(&out, io.Discard),
		kong.Exit(func(int) {}),
		kong.ConfigureHelp(helpOptions()),
		commandGroups(),
		kong.Vars{"version": "test", "cli_name": "schemabot"},
	)
	require.NoError(t, err)

	// kong prints help from the --help flag and then reports the missing
	// command, so the error is expected and the written page is the subject.
	_, _ = parser.Parse(append(command, "--help"))
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
	help := renderHelp(t)
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

// helpSection is one titled block of a command listing, with the commands it
// lists in the order the page prints them.
type helpSection struct {
	title    string
	commands []string
}

// helpSections splits a command listing into its titled sections. A section
// title sits at the left margin; the rows under it are indented two spaces.
// Rows kong prints before any title are the ungrouped commands it lists under
// the "Commands:" header the listing is cut from.
func helpSections(t *testing.T, listing string) []helpSection {
	t.Helper()

	var sections []helpSection
	for line := range strings.SplitSeq(listing, "\n") {
		switch {
		case strings.TrimSpace(line) == "":
		case !strings.HasPrefix(line, " "):
			sections = append(sections, helpSection{title: strings.TrimSpace(line)})
		case strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "   "):
			if len(sections) == 0 {
				sections = append(sections, helpSection{title: "Commands:"})
			}
			last := &sections[len(sections)-1]
			last.commands = append(last.commands, strings.Fields(line)[0])
		}
	}
	require.NotEmpty(t, sections, "command listing had no sections")
	return sections
}

// The top-level listing is sorted into task-named sections so a newcomer can
// find a command by what they are trying to do. The sections print in the
// order the CLI struct declares them, and each command sits under the one
// whose name describes it.
func TestTopLevelHelpSections(t *testing.T) {
	sections := helpSections(t, helpCommandListing(t, renderHelp(t)))

	want := []helpSection{
		{"Set up the CLI:", []string{"init", "configure", "login", "local"}},
		{"Explore your databases:", []string{"databases", "pull", "onboard", "status", "list-plans", "logs"}},
		{"Plan and apply a change:", []string{"plan", "apply"}},
		{"Follow and control a change:", []string{"progress", "stop", "start", "cutover", "cancel", "release", "revert", "skip-revert", "rollback"}},
		{"Manage an environment:", []string{"freeze", "unfreeze", "locks", "unlock", "settings"}},
		{"Run SchemaBot:", []string{"serve", "checks", "storage"}},
	}
	assert.Equal(t, want, sections)
}

// A section title names a group of top-level commands, so it has no business
// heading the subcommands of one of them. Each command's own help page heads
// what it lists with "Commands:", whichever section its parent sits in.
func TestSubcommandHelpHeadsItsOwnCommands(t *testing.T) {
	for _, command := range []string{"configure", "local", "freeze", "checks", "storage"} {
		t.Run(command, func(t *testing.T) {
			sections := helpSections(t, helpCommandListing(t, renderHelp(t, command)))

			require.Len(t, sections, 1, "a command page lists one section")
			assert.Equal(t, "Commands:", sections[0].title)
			for _, name := range sections[0].commands {
				assert.Equal(t, command, name, "the rows are this command's own subcommands")
			}
		})
	}
}
