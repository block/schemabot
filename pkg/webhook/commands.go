package webhook

import (
	"regexp"
	"sort"
	"strings"

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

	// SupportsAutoConfirm means `-y` / `--yes` is recognized. Only apply uses
	// this today; other commands have the flag silently dropped from the
	// CommandResult so the dispatcher can post an "unsupported flag" comment
	// via HasAutoConfirmFlag.
	SupportsAutoConfirm bool

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
// affect parsing — the parser builds an alternation regex sorted by command
// name length so longer names (e.g. "apply-confirm") match before shorter
// prefixes ("apply").
var commandSpecs = []CommandSpec{
	{Name: action.Help},
	{Name: action.Plan, RequiresEnv: true, SupportsDB: true},
	{Name: action.Apply, RequiresEnv: true, SupportsDB: true,
		SupportsSkipRevert: true, SupportsDeferCutover: true,
		SupportsAllowUnsafe: true, SupportsAutoConfirm: true},
	{Name: action.ApplyConfirm, RequiresEnv: true, SupportsDB: true,
		SupportsSkipRevert: true, SupportsDeferCutover: true, SupportsAllowUnsafe: true},
	{Name: action.Unlock, SupportsDB: true, SupportsForce: true},
	{Name: action.FixLint, SupportsDB: true},
	{Name: action.Stop, RequiresEnv: true, HasApplyID: true},
	{Name: action.Cancel, RequiresEnv: true, HasApplyID: true},
	{Name: action.Start, RequiresEnv: true, HasApplyID: true},
	{Name: action.Release, RequiresEnv: true, HasApplyID: true},
	{Name: action.Revert, RequiresEnv: true},
	{Name: action.SkipRevert, RequiresEnv: true},
	{Name: action.Cutover, RequiresEnv: true, HasApplyID: true},
	{Name: action.Rollback, RequiresEnv: true, HasApplyID: true},
	{Name: action.RollbackConfirm, RequiresEnv: true, SupportsDeferCutover: true},
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

// commandNamePattern is the alternation of every registered command name,
// sorted by length descending so "apply-confirm" wins over "apply" at the same
// start position under RE2's leftmost-first semantics.
func commandNamePattern() string {
	names := make([]string, 0, len(commandSpecs))
	for _, s := range commandSpecs {
		names = append(names, regexp.QuoteMeta(s.Name))
	}
	sort.Slice(names, func(i, j int) bool { return len(names[i]) > len(names[j]) })
	return strings.Join(names, "|")
}

// CommandParser parses SchemaBot commands from PR comments.
type CommandParser struct {
	commandRegex      *regexp.Regexp
	mentionRegex      *regexp.Regexp
	helpRegex         *regexp.Regexp
	applyIDRegex      *regexp.Regexp
	environmentRegex  *regexp.Regexp
	databaseRegex     *regexp.Regexp
	tenantRegex       *regexp.Regexp
	tenantFlagRegex   *regexp.Regexp
	skipRevertRegex   *regexp.Regexp
	deferCutoverRegex *regexp.Regexp
	allowUnsafeRegex  *regexp.Regexp
	forceRegex        *regexp.Regexp
	autoConfirmRegex  *regexp.Regexp
}

// NewCommandParser creates a new command parser.
func NewCommandParser() *CommandParser {
	return &CommandParser{
		commandRegex:      regexp.MustCompile(`(?im)^ {0,3}schemabot[ \t]+(` + commandNamePattern() + `)\b`),
		mentionRegex:      regexp.MustCompile(`(?im)^ {0,3}schemabot(?:[ \t]+|$)`),
		helpRegex:         regexp.MustCompile(`(?im)^ {0,3}schemabot[ \t]+help\b`),
		applyIDRegex:      regexp.MustCompile(`(?i)\b(apply[_-][a-f0-9]+)\b`),
		environmentRegex:  regexp.MustCompile(`(?i)-e\s+(staging|production)`),
		databaseRegex:     regexp.MustCompile(`(?i)-d\s+([a-zA-Z0-9_-]+)`),
		tenantRegex:       regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`),
		tenantFlagRegex:   regexp.MustCompile(`(?i)(?:^|\s)(?:--tenant|-t)(?:[ \t]+([^\s]+))?`),
		skipRevertRegex:   regexp.MustCompile(`(?i)--skip-revert\b`),
		deferCutoverRegex: regexp.MustCompile(`(?i)--defer-cutover\b`),
		allowUnsafeRegex:  regexp.MustCompile(`(?i)--allow-unsafe\b`),
		forceRegex:        regexp.MustCompile(`(?i)--force\b`),
		autoConfirmRegex:  regexp.MustCompile(`(?i)(?:--yes\b|-y\b)`),
	}
}

// CommandResult represents the result of parsing a command.
type CommandResult struct {
	Action       string
	ApplyID      string // Positional apply identifier for apply-scoped commands.
	Environment  string
	Database     string // Optional -d flag value
	Tenant       string // Optional --tenant/-t routing target for this command.
	TenantError  bool   // True when --tenant/-t is present without a valid routing target.
	SkipRevert   bool
	DeferCutover bool
	AllowUnsafe  bool
	Force        bool
	AutoConfirm  bool
	Found        bool
	IsHelp       bool
	IsMention    bool
	MissingEnv   bool
}

// ParseCommand parses a SchemaBot command from a comment body.
//
// Resolution order:
//  1. Help (`schemabot help`) is detected first and short-circuits with
//     IsHelp=true so the dispatcher can branch on it without consulting the
//     full spec table.
//  2. The first registered command word that follows `schemabot ` is looked
//     up in specByName and routed through applySpec. Commands must begin a
//     non-code comment line so prose, filenames, URLs, and examples are not
//     treated as directives.
//  3. If a line starts with `schemabot` but no registered command follows, the
//     result is a bare IsMention so the dispatcher can post a friendly
//     "invalid command" comment under the respond_to_unscoped policy.
func (p *CommandParser) ParseCommand(body string) CommandResult {
	body = markdownDirectiveText(body)
	directive, ok := p.firstDirectiveLine(body)
	if !ok {
		return CommandResult{}
	}
	tenant, tenantErr := p.extractTenant(directive)

	if p.helpRegex.MatchString(directive) {
		return CommandResult{Action: action.Help, Tenant: tenant, TenantError: tenantErr, IsHelp: true, IsMention: true}
	}

	matches := p.commandRegex.FindStringSubmatch(directive)
	if len(matches) < 2 {
		return CommandResult{Tenant: tenant, TenantError: tenantErr, IsMention: true}
	}

	name := strings.ToLower(matches[1])
	spec, ok := specByName[name]
	if !ok {
		return CommandResult{Tenant: tenant, TenantError: tenantErr, IsMention: true}
	}
	return p.applySpec(spec, directive, tenant, tenantErr)
}

func (p *CommandParser) firstDirectiveLine(body string) (string, bool) {
	for line := range strings.Lines(body) {
		line = strings.TrimRight(line, "\r\n")
		if p.mentionRegex.MatchString(line) {
			return line, true
		}
	}
	return "", false
}

func (p *CommandParser) extractTenant(body string) (string, bool) {
	match := p.tenantFlagRegex.FindStringSubmatch(body)
	if len(match) == 0 {
		return "", false
	}
	if len(match) < 2 || match[1] == "" || !p.tenantRegex.MatchString(match[1]) {
		return "", true
	}
	return match[1], false
}

func markdownDirectiveText(body string) string {
	var b strings.Builder
	inFence := false
	for line := range strings.Lines(body) {
		leadingSpaces := len(line) - len(strings.TrimLeft(line, " "))
		if leadingSpaces <= 3 && isMarkdownFence(line[leadingSpaces:]) {
			inFence = !inFence
			continue
		}
		if inFence || strings.HasPrefix(line, "    ") || strings.HasPrefix(line, "\t") {
			continue
		}
		b.WriteString(line)
	}
	return b.String()
}

func isMarkdownFence(line string) bool {
	return strings.HasPrefix(line, "```") || strings.HasPrefix(line, "~~~")
}

// applySpec populates CommandResult from a body using the per-command spec.
// Each spec field gates the corresponding regex extraction, so flags only
// affect commands that opted in via the registry.
func (p *CommandParser) applySpec(spec CommandSpec, body, tenant string, tenantErr bool) CommandResult {
	result := CommandResult{
		Action:      spec.Name,
		Tenant:      tenant,
		TenantError: tenantErr,
		IsMention:   true,
	}

	if spec.HasApplyID {
		if m := p.applyIDRegex.FindStringSubmatch(body); len(m) >= 2 {
			result.ApplyID = m[1]
		}
	}
	if spec.SupportsDB {
		if m := p.databaseRegex.FindStringSubmatch(body); len(m) >= 2 {
			result.Database = m[1]
		}
	}
	if spec.SupportsSkipRevert {
		result.SkipRevert = p.skipRevertRegex.MatchString(body)
	}
	if spec.SupportsDeferCutover {
		result.DeferCutover = p.deferCutoverRegex.MatchString(body)
	}
	if spec.SupportsAllowUnsafe {
		result.AllowUnsafe = p.allowUnsafeRegex.MatchString(body)
	}
	if spec.SupportsForce {
		result.Force = p.forceRegex.MatchString(body)
	}
	if spec.SupportsAutoConfirm {
		result.AutoConfirm = p.autoConfirmRegex.MatchString(body)
	}

	if m := p.environmentRegex.FindStringSubmatch(body); len(m) >= 2 {
		result.Environment = strings.ToLower(m[1])
	}

	switch {
	case !spec.RequiresEnv:
		result.Found = true
	case result.Environment != "":
		result.Found = true
	default:
		result.MissingEnv = true
	}

	return result
}

// HasAutoConfirmFlag reports whether the body contains the `-y` / `--yes`
// flag, regardless of which command it accompanies. The dispatcher uses this
// to post an "unsupported flag" comment when an operator pairs `-y` with a
// command whose spec does not opt into SupportsAutoConfirm.
func (p *CommandParser) HasAutoConfirmFlag(body string) bool {
	return p.autoConfirmRegex.MatchString(body)
}

// HasDatabaseFlag reports whether the body contains a `-d <database>` flag,
// regardless of which command it accompanies.
func (p *CommandParser) HasDatabaseFlag(body string) bool {
	return p.databaseRegex.MatchString(body)
}

// HasDeferCutoverFlag reports whether the body contains `--defer-cutover`,
// regardless of which command it accompanies.
func (p *CommandParser) HasDeferCutoverFlag(body string) bool {
	return p.deferCutoverRegex.MatchString(body)
}
