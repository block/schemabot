package main

import (
	"io"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/webhook"
	"github.com/block/schemabot/pkg/webhook/action"
)

// prCommandsWithoutCLICommand lists the PR comment commands that deliberately
// have no CLI command of the same name, each mapped to the CLI capability that
// covers it. Parity between the two surfaces is of capability, not of spelling,
// and this table is where a difference in spelling has to be justified.
var prCommandsWithoutCLICommand = map[string]string{
	// Kong renders help for the whole CLI itself, so there is no command field
	// on the CLI struct to match.
	action.Help: "kong provides the CLI's own --help",

	// The PR's two-step consent for a destructive change is a second command
	// because a comment has no prompt to answer. On the CLI the same consent is
	// the answer to apply's interactive prompt, or its --auto-approve flag.
	action.ApplyConfirm: "consent is apply's confirmation prompt / --auto-approve",

	// Same two-step consent, on the CLI's rollback.
	action.RollbackConfirm: "consent is rollback's confirmation prompt / --auto-approve",
}

// Every command a PR comment accepts must be reachable from the CLI, so an
// operator can still drive a schema change when GitHub is unavailable. A new PR
// comment command therefore has to ship with either a CLI command of the same
// name or an entry in prCommandsWithoutCLICommand naming the CLI capability
// that covers it.
//
// The reverse does not hold: the CLI is deliberately a superset, carrying
// commands (status, logs, storage, serve, and others) that have no reason to be
// issued from a PR comment.
//
// Only the spelling half of that parity is mechanized here: this asserts that a
// CLI command exists to answer each comment command, not that the two do the
// same thing. A CLI apply whose behavior drifted from comment apply still
// passes, so the capability half remains a convention.
func TestEveryPRCommandHasACLIEquivalent(t *testing.T) {
	prCommands := webhook.CommandNames()
	require.NotEmpty(t, prCommands, "webhook registers no PR comment commands")

	cliCommands := cliCommandNames(t)
	require.NotEmpty(t, cliCommands, "CLI struct exposes no commands")

	for _, name := range prCommands {
		if _, exempt := prCommandsWithoutCLICommand[name]; exempt {
			continue
		}
		assert.Contains(t, cliCommands, name,
			"PR comment command %q has no CLI equivalent; add a cmd-tagged field to CLI, "+
				"or add %q to prCommandsWithoutCLICommand naming the CLI capability that covers it",
			name, name)
	}
}

// The exemption table must not outlive the commands it excuses: a stale entry
// would silently exempt a command name that is no longer registered, and would
// go on exempting it if that name were later reused for a different command.
func TestPRCommandExemptionsNameRegisteredCommands(t *testing.T) {
	prCommands := webhook.CommandNames()
	require.NotEmpty(t, prCommands, "webhook registers no PR comment commands")

	for name, reason := range prCommandsWithoutCLICommand {
		assert.Contains(t, prCommands, name,
			"prCommandsWithoutCLICommand exempts %q, which is not a registered PR comment command; remove the entry",
			name)
		assert.NotEmpty(t, reason,
			"exemption for %q has no reason; name the CLI capability that covers it", name)
	}
}

// cliCommandNames returns the name of every top-level command on the CLI
// struct. The names come from kong's own model rather than from the struct tags
// directly, so they are exactly the words the binary accepts: an explicit
// name: tag where one is set, and kong's derivation from the field name
// otherwise.
func cliCommandNames(t *testing.T) []string {
	t.Helper()

	var cli CLI
	parser, err := kong.New(&cli,
		kong.Name("schemabot"),
		kong.Writers(io.Discard, io.Discard),
		kong.Vars{"cli_name": "schemabot"},
	)
	require.NoError(t, err)

	names := make([]string, 0, len(parser.Model.Children))
	for _, child := range parser.Model.Children {
		if child.Type == kong.CommandNode {
			names = append(names, child.Name)
		}
	}
	return names
}
