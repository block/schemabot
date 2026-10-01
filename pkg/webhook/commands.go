package webhook

import (
	"regexp"
	"strings"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

// CommandSpec declares the parse and dispatch shape of a SchemaBot command.
//
// Adding a new command means appending one entry to commandSpecs — the parser,
// unsupported-flag handling, and missing-env behavior all derive from the spec.
// Adding ad-hoc parsing logic anywhere else for a known command is a sign the
// spec is missing a field, not that the parser needs another special case.
type CommandSpec struct {
	// Name is the command word that follows "schemabot ", e.g. "plan".
	Name string

	// RequiresEnv means the command needs `-e <env>` to be runnable.
	// When env is missing the parser returns MissingEnv=true; the dispatcher
	// decides whether to post a "missing env" comment or take a multi-env
	// branch (currently only plan does the latter).
	RequiresEnv bool

	// HasApplyID means the command takes a positional `apply_<id>` argument
	// (currently only rollback).
	HasApplyID bool

	// SupportsDB means `-d <db>` is recognized.
	SupportsDB bool

	// SupportsSkipRevert means `--skip-revert` is recognized.
	SupportsSkipRevert bool

	// SupportsDeferCutover means `--defer-cutover` is recognized.
	SupportsDeferCutover bool

	// SupportsAllowUnsafe means `--allow-unsafe` is recognized.
	SupportsAllowUnsafe bool

	// SupportsForce means `--force` is recognized.
	SupportsForce bool
}

// commandSpecs is the registry of all SchemaBot commands. Order does not
// affect parsing: the command word is matched whole, so "apply" never matches
// the start of "apply-confirm".
var commandSpecs = []CommandSpec{
	{Name: action.Help},
	{Name: action.Plan, RequiresEnv: true, SupportsDB: true},
	{Name: action.Apply, RequiresEnv: true, SupportsDB: true,
		SupportsSkipRevert: true, SupportsDeferCutover: true,
		SupportsAllowUnsafe: true},
	{Name: action.ApplyConfirm, RequiresEnv: true, SupportsDB: true,
		SupportsSkipRevert: true, SupportsDeferCutover: true, SupportsAllowUnsafe: true},
	{Name: action.Unlock, SupportsDB: true, SupportsForce: true},
	{Name: action.FixLint, SupportsDB: true},
	{Name: action.Stop, RequiresEnv: true, HasApplyID: true},
	{Name: action.Cancel, RequiresEnv: true, HasApplyID: true},
	{Name: action.Start, RequiresEnv: true, HasApplyID: true},
	{Name: action.Release, RequiresEnv: true, HasApplyID: true},
	{Name: action.Revert, RequiresEnv: true, HasApplyID: true},
	{Name: action.SkipRevert, RequiresEnv: true, HasApplyID: true},
	{Name: action.Cutover, RequiresEnv: true, HasApplyID: true},
	{Name: action.Rollback, RequiresEnv: true, HasApplyID: true},
	{Name: action.RollbackConfirm, RequiresEnv: true, SupportsDeferCutover: true},
}

// CommandNames returns the command word of every registered PR comment
// command.
//
// Exported so the CLI's own command surface can be checked against it: every
// command a PR comment accepts has to have a CLI equivalent, because both
// surfaces converge on the same service methods and the CLI is the fallback
// when GitHub is unavailable. A fallback that covers only part of the surface
// is not a fallback.
func CommandNames() []string {
	names := make([]string, 0, len(commandSpecs))
	for _, s := range commandSpecs {
		names = append(names, s.Name)
	}
	return names
}

// specByName indexes commandSpecs for O(1) lookup by command word.
var specByName = func() map[string]CommandSpec {
	m := make(map[string]CommandSpec, len(commandSpecs))
	for _, s := range commandSpecs {
		m[s.Name] = s
	}
	return m
}()

func commandSupportsDatabaseFlag(actionName string) bool {
	spec, ok := specByName[actionName]
	return ok && spec.SupportsDB
}

// CommandParser parses SchemaBot commands from PR comments.
type CommandParser struct {
	mentionRegex         *regexp.Regexp
	applyIDRegex         *regexp.Regexp
	applyIDPrefixRegex   *regexp.Regexp
	environmentNameRegex *regexp.Regexp
	databaseNameRegex    *regexp.Regexp
	tenantRegex          *regexp.Regexp
}

// NewCommandParser creates a new command parser.
func NewCommandParser() *CommandParser {
	return &CommandParser{
		mentionRegex:         regexp.MustCompile(`(?im)^ {0,3}schemabot(?:[ \t]+|$)`),
		applyIDRegex:         regexp.MustCompile(`(?i)^apply[_-][a-f0-9]+$`),
		applyIDPrefixRegex:   regexp.MustCompile(`(?i)^apply[_-][a-f0-9]`),
		environmentNameRegex: regexp.MustCompile(`^[a-z0-9][a-z0-9_]*(?:-[a-z0-9_]+)*$`),
		databaseNameRegex:    regexp.MustCompile(`^[a-zA-Z0-9_-]+$`),
		tenantRegex:          regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`),
	}
}

// CommandResult represents the result of parsing a command.
type CommandResult struct {
	Action string
	// DeliveryID identifies the webhook delivery that carried the command for
	// logs emitted by asynchronous command work.
	DeliveryID string
	// SuppressRetryComments is set by the durable driver so retryable failures
	// do not post an answer the driver is about to supersede. The driver posts
	// the single terminal answer after exhaustion; synchronous handling leaves
	// it false.
	SuppressRetryComments bool
	// CommentID is the PR comment that carried this command. Handlers
	// acknowledge it with a reaction once they commit to acting, so on a
	// fan-out only the deployments actually doing work acknowledge.
	CommentID    int64
	ApplyID      string // Positional apply identifier for apply-scoped commands.
	Environment  string
	Database     string // Optional -d flag value
	Tenant       string // Optional --tenant/-t routing target for this command.
	TenantError  bool   // True when --tenant/-t is present without a valid routing target.
	SkipRevert   bool
	DeferCutover bool
	AllowUnsafe  bool
	Force        bool
	Found        bool
	IsHelp       bool
	IsMention    bool
	// ProseMention is true when the comment has a line opening with
	// `schemabot` that reads as a sentence about SchemaBot, and no line
	// addressed to it. Such a comment is not answered.
	ProseMention bool
	MissingEnv   bool
	// EnvironmentError is true when `-e` is present but its value is not a
	// valid environment name (for example a flag glued onto the value:
	// `-e production--allow-unsafe`). The dispatcher posts a usage comment;
	// such a command must never fall back to missing-env handling, which
	// would run `plan` against every configured environment.
	EnvironmentError bool
}

// ParseCommand parses a SchemaBot command from a comment body.
//
// A comment addresses SchemaBot only on a non-code line that opens with
// `schemabot` and holds nothing but a command word, flags, the values of flags
// that take one, and an apply ID. Each line that opens with `schemabot` is one
// of three kinds:
//   - A sentence about SchemaBot ("SchemaBot apply -e staging succeeded") has
//     a plain word on it. It is skipped: it never runs a command or gets an
//     answer, and it does not hide a command on a later line.
//   - A malformed command has no plain word but has a token SchemaBot does not
//     accept exactly, such as `--allow-unsafe.` or `-d billing,`. It is the
//     directive and gets the "invalid command" answer: the parser never trims
//     a token into one it accepts, so a typo can never run a command, least of
//     all an unsafe one.
//   - A command has every token accepted exactly. The first one is the
//     directive:
//     1. Help (`schemabot help`) short-circuits with IsHelp=true so the
//     dispatcher can branch on it without consulting the full spec table.
//     2. A registered command word is looked up in specByName and routed
//     through applySpec.
//     3. Any other word, or `schemabot` alone, is a bare IsMention so the
//     dispatcher can post a friendly "invalid command" comment under the
//     respond_to_unscoped policy.
func (p *CommandParser) ParseCommand(body string) CommandResult {
	d, ok, prose := p.firstDirective(markdownDirectiveText(body))
	if !ok {
		return CommandResult{ProseMention: prose}
	}
	tenant, tenantErr := p.tenantOf(d)

	if d.kind == lineMalformed {
		return CommandResult{Tenant: tenant, TenantError: tenantErr, IsMention: true}
	}
	if d.name == action.Help {
		return CommandResult{Action: action.Help, Tenant: tenant, TenantError: tenantErr, IsHelp: true, IsMention: true}
	}

	spec, ok := specByName[d.name]
	if !ok {
		return CommandResult{Tenant: tenant, TenantError: tenantErr, IsMention: true}
	}
	return p.applySpec(spec, d, tenant, tenantErr)
}

// lineKind classifies a line that opens with `schemabot`.
type lineKind int

const (
	// lineProse is a sentence about SchemaBot: it has a plain word on it.
	lineProse lineKind = iota
	// lineMalformed is a command attempt with a token SchemaBot does not
	// accept exactly.
	lineMalformed
	// lineCommand is a command whose every token SchemaBot accepts exactly.
	lineCommand
)

// directive is a line that opens with `schemabot`, split into whole tokens.
// The command's flags are read from these tokens, never by searching the
// line, so a flag only counts when it is a token on its own.
type directive struct {
	kind lineKind
	// name is the command word, lowercased; empty for a bare `schemabot`.
	name string
	// flags holds the boolean flags present, by canonical spelling.
	flags map[string]bool
	// values holds the value flags present, by canonical spelling. A flag
	// with no value maps to "".
	values map[string]string
	// repeated holds the value flags given more than once, by canonical
	// spelling; which value was meant is ambiguous.
	repeated map[string]bool
	applyID  string
}

// booleanFlags maps every accepted spelling of a flag that takes no value to
// its canonical spelling.
var booleanFlags = map[string]string{
	"--skip-revert":   "--skip-revert",
	"--defer-cutover": "--defer-cutover",
	"--allow-unsafe":  "--allow-unsafe",
	"--force":         "--force",
	"-y":              "-y",
	"--yes":           "-y",
}

// valueFlags maps every accepted spelling of a flag that takes the following
// word as its value to its canonical spelling.
var valueFlags = map[string]string{
	"-e":       "-e",
	"-d":       "-d",
	"-t":       "-t",
	"--tenant": "-t",
}

// firstDirective returns the first line that opens with `schemabot` and is
// not a sentence. prose reports whether a sentence was skipped on the way, so
// a caller can say why a comment naming SchemaBot was not answered.
func (p *CommandParser) firstDirective(body string) (d directive, ok, prose bool) {
	for line := range strings.Lines(body) {
		line = strings.TrimRight(line, "\r\n")
		loc := p.mentionRegex.FindStringIndex(line)
		if loc == nil {
			continue
		}
		d := p.parseDirective(strings.Fields(line[loc[1]:]))
		if d.kind == lineProse {
			prose = true
			continue
		}
		return d, true, prose
	}
	return directive{}, false, prose
}

// parseDirective splits words, everything after `schemabot` on a line, into a
// directive. The first word is the command, matched as a whole word so
// "planned" or "plan." is not `plan`. Every later word must be an accepted
// flag, the value of a flag that takes one, an apply ID, or a usage
// placeholder such as `<apply-id>` copied from help text.
//
// A plain word anywhere makes the line a sentence. Otherwise the line is
// malformed when a token is close to one SchemaBot accepts but is not exactly
// it (an unknown or punctuated flag, a punctuated apply ID, an invalid or
// missing database), or when it is ambiguous (a value flag given twice, two
// apply IDs). The values of -e and -t are checked later, where an invalid one
// gets its own usage answer.
func (p *CommandParser) parseDirective(words []string) directive {
	d := directive{kind: lineCommand, flags: map[string]bool{}, values: map[string]string{}, repeated: map[string]bool{}}
	if len(words) == 0 {
		return d
	}
	d.name = strings.ToLower(words[0])
	malformed := false
	for i := 1; i < len(words); i++ {
		word := words[i]
		lower := strings.ToLower(word)
		if flag, ok := valueFlags[lower]; ok {
			if _, given := d.values[flag]; given {
				d.repeated[flag] = true
				malformed = true
			}
			value := ""
			if i+1 < len(words) && !strings.HasPrefix(words[i+1], "-") {
				i++
				value = words[i]
			}
			d.values[flag] = value
			continue
		}
		if flag, ok := booleanFlags[lower]; ok {
			d.flags[flag] = true
			continue
		}
		switch {
		case strings.HasPrefix(word, "-"):
			malformed = true
		case p.applyIDRegex.MatchString(word):
			if d.applyID != "" {
				malformed = true
			}
			d.applyID = word
		case p.applyIDPrefixRegex.MatchString(word):
			malformed = true
		case isUsagePlaceholder(word):
		default:
			d.kind = lineProse
			return d
		}
	}
	if database, given := d.values["-d"]; given && !p.databaseNameRegex.MatchString(database) {
		malformed = true
	}
	if malformed {
		d.kind = lineMalformed
	}
	return d
}

// isUsagePlaceholder reports whether word is a placeholder like `<apply-id>`,
// which only appears in a command copied from usage text.
func isUsagePlaceholder(word string) bool {
	return len(word) > 2 && strings.HasPrefix(word, "<") && strings.HasSuffix(word, ">")
}

// tenantOf returns the directive's --tenant/-t routing target, and whether
// the flag is present without a single valid one.
func (p *CommandParser) tenantOf(d directive) (string, bool) {
	tenant, given := d.values["-t"]
	if !given {
		return "", false
	}
	if d.repeated["-t"] || !p.tenantRegex.MatchString(tenant) {
		return "", true
	}
	return tenant, false
}

// markdownDirectiveText returns the lines of body that render as the
// commenter's own text, dropping everything that only shows a command: fenced
// and indented code, and quotes. A quote covers its `>` lines, the lines that
// continue its paragraph without a `>` (which Markdown renders inside the
// quote), and an HTML <blockquote>. A quote-reply to a SchemaBot command
// therefore never runs that command.
func markdownDirectiveText(body string) string {
	var b strings.Builder
	inFence, inQuote, inHTMLQuote := false, false, false
	for line := range strings.Lines(body) {
		leadingSpaces := len(line) - len(strings.TrimLeft(line, " "))
		rest := line[leadingSpaces:]
		markdownIndent := leadingSpaces <= 3
		if inHTMLQuote {
			inHTMLQuote = !closesHTMLQuote(line)
			continue
		}
		if strings.TrimSpace(line) == "" {
			inQuote = false
			continue
		}
		if markdownIndent && isMarkdownFence(rest) {
			inFence = !inFence
			inQuote = false
			continue
		}
		if inFence {
			continue
		}
		if markdownIndent && strings.HasPrefix(rest, ">") {
			inQuote = true
			continue
		}
		if inQuote {
			continue
		}
		if markdownIndent && opensHTMLQuote(rest) {
			inHTMLQuote = !closesHTMLQuote(rest)
			continue
		}
		if strings.HasPrefix(line, "    ") || strings.HasPrefix(line, "\t") {
			continue
		}
		b.WriteString(line)
	}
	return b.String()
}

func opensHTMLQuote(line string) bool {
	return strings.HasPrefix(strings.ToLower(line), "<blockquote")
}

func closesHTMLQuote(line string) bool {
	return strings.Contains(strings.ToLower(line), "</blockquote>")
}

func isMarkdownFence(line string) bool {
	return strings.HasPrefix(line, "```") || strings.HasPrefix(line, "~~~")
}

// applySpec populates CommandResult from a command directive using the
// per-command spec. Each spec field gates the corresponding flag, so flags
// only affect commands that opted in via the registry.
func (p *CommandParser) applySpec(spec CommandSpec, d directive, tenant string, tenantErr bool) CommandResult {
	result := CommandResult{
		Action:      spec.Name,
		Tenant:      tenant,
		TenantError: tenantErr,
		IsMention:   true,
	}

	if spec.HasApplyID {
		result.ApplyID = d.applyID
	}
	if spec.SupportsDB && d.values["-d"] != "" {
		result.Database = storage.CanonicalKey(d.values["-d"])
	}
	if spec.SupportsSkipRevert {
		result.SkipRevert = d.flags["--skip-revert"]
	}
	if spec.SupportsDeferCutover {
		result.DeferCutover = d.flags["--defer-cutover"]
	}
	if spec.SupportsAllowUnsafe {
		result.AllowUnsafe = d.flags["--allow-unsafe"]
	}
	if spec.SupportsForce {
		result.Force = d.flags["--force"]
	}
	// The -e value is the whole following token and its validity is checked
	// here: a malformed value like `production--allow-unsafe` or `staging.`
	// must be rejected as a whole, never trimmed into an environment.
	if env := d.values["-e"]; env != "" {
		env = storage.CanonicalKey(env)
		if p.environmentNameRegex.MatchString(env) {
			result.Environment = env
		} else {
			result.EnvironmentError = true
		}
	}

	switch {
	case result.EnvironmentError:
		// The command is recognized; the dispatcher rejects it with a usage
		// comment instead of treating the environment as missing.
		result.Found = true
	case !spec.RequiresEnv:
		result.Found = true
	case result.Environment != "":
		result.Found = true
	default:
		result.MissingEnv = true
	}

	return result
}

// commandDirective returns the comment's directive when it is a well-formed
// command. A malformed directive carries no flags: it is answered as an
// invalid command before any flag is considered.
func (p *CommandParser) commandDirective(body string) (directive, bool) {
	d, ok, _ := p.firstDirective(markdownDirectiveText(body))
	if !ok || d.kind != lineCommand {
		return directive{}, false
	}
	return d, true
}

// HasAutoConfirmFlag reports whether the command carries the `-y` / `--yes`
// flag. No comment command takes it: a comment has no prompt to skip, and the
// gates that stop an apply — a discarded copy, a re-plan that differs from the
// comment the operator was shown — stop it because the operator has to see
// what they are consenting to, which a flag cannot express. The dispatcher uses this to say so rather than accept the
// flag and ignore it, which would read as consent that was never recorded.
// This is distinct from the CLI's own `-y` (`--auto-approve`), which skips an
// interactive terminal prompt that genuinely exists.
//
// The answer decides whether a command is rejected, so it is read off the
// directive line the command was parsed from rather than the whole comment: a
// reader who mentions the flag in prose, or pastes a CLI example in a fence, is
// describing it, not passing it.
func (p *CommandParser) HasAutoConfirmFlag(body string) bool {
	d, ok := p.commandDirective(body)
	return ok && d.flags["-y"]
}

// HasDatabaseFlag reports whether the command carries a `-d <database>` flag,
// regardless of which command it accompanies. Like HasAutoConfirmFlag, the
// answer decides whether a command is rejected, so it is read off the
// directive line the command was parsed from: prose or a fenced CLI example
// mentioning the flag describes it, not passes it.
func (p *CommandParser) HasDatabaseFlag(body string) bool {
	d, ok := p.commandDirective(body)
	return ok && d.values["-d"] != ""
}

// HasDeferCutoverFlag reports whether the command carries `--defer-cutover`,
// regardless of which command it accompanies. Read off the directive line for
// the same reason as HasDatabaseFlag.
func (p *CommandParser) HasDeferCutoverFlag(body string) bool {
	d, ok := p.commandDirective(body)
	return ok && d.flags["--defer-cutover"]
}
