package templates

import "github.com/block/schemabot/pkg/cmd/cliname"

// cliCommand renders a command meant to be run in a terminal rather than
// commented on the PR: command after the tool name the deployment's operators
// run the CLI as, the server's cli_name. Operators who run the CLI through a
// wrapper paste the hint as printed and reach the wrapper, not an unconfigured
// binary. An empty cliName renders the CLI's own default. Commands a PR author
// comments keep "schemabot", the bot's trigger word, and never render through
// here.
func cliCommand(cliName, command string) string {
	if cliName == "" {
		cliName = cliname.DefaultName
	}
	return cliName + " " + command
}

// environmentFlag scopes a CLI command hint to environment, so a wrapper that
// routes by environment reaches the right server. An unknown environment
// renders the placeholder the operator fills in.
func environmentFlag(environment string) string {
	if environment == "" {
		environment = "<environment>"
	}
	return "-e " + environment
}
