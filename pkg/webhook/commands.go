package webhook

import (
	"regexp"
	"strings"
)

// CommandParser parses SchemaBot commands from PR comments.
type CommandParser struct {
	commandRegex           *regexp.Regexp
	mentionRegex           *regexp.Regexp
	helpRegex              *regexp.Regexp
	commandWithoutEnvRegex *regexp.Regexp
	rollbackRegex          *regexp.Regexp // rollback <apply-id>
	databaseRegex          *regexp.Regexp
	skipRevertRegex        *regexp.Regexp
	deferCutoverRegex      *regexp.Regexp
	allowUnsafeRegex       *regexp.Regexp
	autoConfirmRegex       *regexp.Regexp
}

// NewCommandParser creates a new command parser.
func NewCommandParser() *CommandParser {
	return &CommandParser{
		commandRegex:           regexp.MustCompile(`(?i)schemabot\s+(plan|apply|apply-confirm|unlock|stop|revert|skip-revert|cutover|rollback-confirm)\s+(?:.*?-e\s+(staging|production))`),
		mentionRegex:           regexp.MustCompile(`(?i)\bschemabot\b`),
		helpRegex:              regexp.MustCompile(`(?i)schemabot\s+help\b`),
		commandWithoutEnvRegex: regexp.MustCompile(`(?i)schemabot\s+(plan|apply|apply-confirm|unlock|stop|revert|skip-revert|cutover|rollback|rollback-confirm|fix-lint)\b`),
		rollbackRegex:          regexp.MustCompile(`(?i)schemabot\s+rollback\s+(apply[_-][a-f0-9]+)`),
		databaseRegex:          regexp.MustCompile(`(?i)-d\s+([a-zA-Z0-9_-]+)`),
		skipRevertRegex:        regexp.MustCompile(`(?i)--skip-revert\b`),
		deferCutoverRegex:      regexp.MustCompile(`(?i)--defer-cutover\b`),
		allowUnsafeRegex:       regexp.MustCompile(`(?i)--allow-unsafe\b`),
		autoConfirmRegex:       regexp.MustCompile(`(?i)(?:--yes\b|-y\b)`),
	}
}

// CommandResult represents the result of parsing a command.
type CommandResult struct {
	Action       string
	ApplyID      string // Positional arg for rollback <apply-id>
	Environment  string
	Database     string // Optional -d flag value
	SkipRevert   bool
	DeferCutover bool
	AllowUnsafe  bool
	AutoConfirm  bool
	Found        bool
	IsHelp       bool
	IsMention    bool
	MissingEnv   bool
}

// ParseCommand parses a SchemaBot command from a comment body.
func (p *CommandParser) ParseCommand(body string) CommandResult {
	// Check help first
	if p.helpRegex.MatchString(body) {
		return CommandResult{Action: "help", IsHelp: true, IsMention: true}
	}

	// Check rollback <apply-id> (positional arg, no -e flag)
	rollbackMatches := p.rollbackRegex.FindStringSubmatch(body)
	if len(rollbackMatches) >= 2 {
		result := CommandResult{
			Action:       "rollback",
			ApplyID:      rollbackMatches[1],
			Found:        true,
			IsMention:    true,
			DeferCutover: p.deferCutoverRegex.MatchString(body),
		}
		dbMatches := p.databaseRegex.FindStringSubmatch(body)
		if len(dbMatches) >= 2 {
			result.Database = dbMatches[1]
		}
		return result
	}

	// Check valid command with environment
	matches := p.commandRegex.FindStringSubmatch(body)
	if len(matches) >= 3 {
		action := strings.ToLower(matches[1])
		result := CommandResult{
			Action:       action,
			Environment:  strings.ToLower(matches[2]),
			Found:        true,
			IsMention:    true,
			SkipRevert:   p.skipRevertRegex.MatchString(body),
			DeferCutover: p.deferCutoverRegex.MatchString(body),
			AllowUnsafe:  p.allowUnsafeRegex.MatchString(body),
			AutoConfirm:  action == "apply" && p.autoConfirmRegex.MatchString(body),
		}

		dbMatches := p.databaseRegex.FindStringSubmatch(body)
		if len(dbMatches) >= 2 {
			result.Database = dbMatches[1]
		}

		return result
	}

	// Check recognized command without -e flag
	envMatches := p.commandWithoutEnvRegex.FindStringSubmatch(body)
	if len(envMatches) >= 2 {
		action := strings.ToLower(envMatches[1])

		// unlock and fix-lint don't require -e flag
		if action == "unlock" || action == "fix-lint" {
			result := CommandResult{
				Action:    action,
				Found:     true,
				IsMention: true,
			}
			if action == "fix-lint" {
				dbMatches := p.databaseRegex.FindStringSubmatch(body)
				if len(dbMatches) >= 2 {
					result.Database = dbMatches[1]
				}
			}
			return result
		}

		// plan without -e runs for all configured environments
		if action == "plan" {
			result := CommandResult{
				Action:     action,
				IsMention:  true,
				MissingEnv: true,
			}
			dbMatches := p.databaseRegex.FindStringSubmatch(body)
			if len(dbMatches) >= 2 {
				result.Database = dbMatches[1]
			}
			return result
		}

		return CommandResult{
			Action:     action,
			IsMention:  true,
			MissingEnv: true,
		}
	}

	// Check if schemabot was mentioned at all
	if p.mentionRegex.MatchString(body) {
		return CommandResult{IsMention: true}
	}

	return CommandResult{}
}
