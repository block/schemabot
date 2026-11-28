package main

import (
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/webhook"
	"github.com/block/schemabot/pkg/webhook/action"
)

// agentSkillDir is the agent skill shipped with the repository, relative to
// this package.
const agentSkillDir = "../../skills/schemabot"

// skillPlaceholders are the placeholders the skill may use in a command
// example, each mapped to a value the command's surface accepts. A placeholder
// outside this table fails the test, so a new one is a deliberate addition.
var skillPlaceholders = map[string]string{
	"<apply-id>":   "apply-0123456789abcdef",
	"<env>":        "staging",
	"<target>":     "orders-002",
	"<database>":   "shop",
	"<deployment>": "east",
}

// skillCommand is one `schemabot …` command the skill shows an agent.
type skillCommand struct {
	file string
	line int
	text string
	// fenced is true for a command inside a fenced block, which the skill
	// presents as a whole command to run, rather than a mention in prose.
	fenced bool
	// prComment is true when the command is shown as a PR comment, false when
	// it is shown as a terminal command.
	prComment bool
}

// An agent copies the commands in its skill. A command the server no longer
// accepts fails silently on a PR: SchemaBot answers it as invalid, or does not
// answer at all, and the apply the agent announced never starts. Every command
// the skill shows is therefore parsed by the surface it is shown on: the PR
// comment parser for comment commands, the CLI's own kong model for terminal
// commands.
func TestAgentSkillCommandsParse(t *testing.T) {
	commands := agentSkillCommands(t)
	require.NotEmpty(t, commands, "no schemabot commands found in %s", agentSkillDir)

	prCommands := policyCommandWords()
	cli := newCLIParser(t)
	var sawPR, sawCLI bool

	for _, c := range commands {
		where := fmt.Sprintf("%s:%d: %s", c.file, c.line, c.text)
		args, ok := substitutePlaceholders(t, where, strings.Fields(c.text)[1:])
		if !ok {
			continue
		}
		if isPRCommentExample(c, args[0], prCommands) {
			sawPR = true
			assertParsesAsPRComment(t, where, args)
			continue
		}
		sawCLI = true
		if c.fenced {
			_, err := cli.Parse(args)
			assert.NoError(t, err, "%s: the CLI does not accept this command", where)
			continue
		}
		assertCLIMentionResolves(t, where, cli, args)
	}

	assert.True(t, sawPR, "the skill shows no PR comment commands; the extraction is broken")
	assert.True(t, sawCLI, "the skill shows no CLI commands; the extraction is broken")
}

// The command policy is the skill's safety contract: it is what keeps an agent
// from posting a production command, an unsafe one, or one that answers a
// question SchemaBot asked the user. These assertions pin that contract, so a
// rewrite of the skill cannot drop a rule without failing CI, and a new PR
// comment command cannot ship without the policy saying who may post it.
func TestAgentSkillCommandPolicy(t *testing.T) {
	policy := readSkillFile(t, "references/command-policy.md")
	skill := readSkillFile(t, "SKILL.md")

	t.Run("pins who posts each command", func(t *testing.T) {
		rows := policyRows(t, markdownSection(t, policy, "## Every command at a glance"))
		assert.Equal(t, expectedPolicy, rows,
			"the command policy's at-a-glance table changed. If the change is intended, "+
				"update expectedPolicy in the same PR so the new rule is reviewed as a safety change")

		for _, word := range policyCommandWords() {
			assert.Contains(t, webhook.CommandNames(), word,
				"the command policy covers %q, which the PR comment parser no longer accepts; "+
					"remove it from the skill and from expectedPolicy", word)
		}
		for _, name := range webhook.CommandNames() {
			covered := slices.ContainsFunc(slices.Collect(maps.Keys(rows)), func(entry string) bool {
				return entry == name || strings.HasPrefix(entry, name+" ")
			})
			assert.True(t, covered, "PR comment command %q is not in the command policy's at-a-glance table; "+
				"say whether an agent may post it", name)
		}
	})

	t.Run("states a rule for every PR comment flag", func(t *testing.T) {
		flags := webhook.CommandFlags()
		for _, flag := range flags {
			_, pinned := expectedPolicy[flag]
			_, neutral := policyNeutralFlags[flag]
			assert.True(t, pinned || neutral, "PR comment flag %s has no rule in the command policy; add it to the "+
				"at-a-glance table and expectedPolicy, or to policyNeutralFlags with the reason it changes nothing "+
				"about who may post the command", flag)
			assert.False(t, pinned && neutral, "flag %s is both pinned by the policy and listed as neutral", flag)
		}
		for flag := range policyNeutralFlags {
			assert.Contains(t, flags, flag, "policyNeutralFlags lists %s, which the PR comment parser no longer accepts", flag)
		}
	})

	t.Run("hands over terminal commands that change SchemaBot itself", func(t *testing.T) {
		section := markdownSection(t, policy, "## Terminal commands that change SchemaBot itself")
		handOverPart, _, found := strings.Cut(section, "Their read-only forms")
		require.True(t, found, "the section must keep the read-only forms apart from the commands to hand over")
		cli := newCLIParser(t)
		assert.Contains(t, handOverPart, "hand over any terminal `apply` or `rollback`, in every environment")
		for name, flags := range cliSafeguardFlags {
			cmd := childCommand(cli.Model.Node, name)
			if !assert.NotNil(t, cmd, "the CLI has no %s command; update cliSafeguardFlags", name) {
				continue
			}
			for _, flag := range flags {
				assert.True(t, hasFlag(cmd, flag),
					"the CLI's %s no longer has %s; update cliSafeguardFlags and the command policy", name, flag)
				assert.Contains(t, handOverPart, "`"+flag+"`",
					"the command policy must name %s among the safeguards a terminal %s can drop", flag, name)
			}
		}
		var names []string
		for _, child := range cli.Model.Children {
			if child.Type != kong.CommandNode {
				continue
			}
			names = append(names, child.Name)
			kind, classified := cliCommandKinds[child.Name]
			if !classified && slices.Contains(webhook.CommandNames(), child.Name) {
				continue
			}
			if !assert.True(t, classified, "CLI command %q is neither a PR comment command nor in cliCommandKinds; "+
				"say whether it only reads, works locally, or changes SchemaBot itself", child.Name) {
				continue
			}
			if kind == cliWatchHasControls {
				assert.True(t, hasFlag(child, "--no-watch"), "the CLI's %s has no --no-watch; update cliCommandKinds", child.Name)
				assert.Contains(t, handOverPart, "`"+child.Name+" --no-watch`",
					"the command policy must tell an agent to run %s with --no-watch", child.Name)
				// --json takes one snapshot and never opens the watcher, so
				// the policy may name it as the other safe form.
				assert.True(t, hasFlag(child, "--json"), "the CLI's %s has no --json; update cliCommandKinds", child.Name)
				assert.Contains(t, handOverPart, "`"+child.Name+" --json`",
					"the command policy must name %s --json as a safe snapshot", child.Name)
				continue
			}
			if kind == cliChangesSchemaBot {
				named := strings.Contains(handOverPart, "`"+child.Name+"`") || strings.Contains(handOverPart, "`"+child.Name+" ")
				assert.True(t, named, "the command policy must tell an agent to hand over %q", child.Name)
			}
		}
		for name := range cliCommandKinds {
			assert.Contains(t, names, name, "cliCommandKinds lists %q, which is no longer a CLI command", name)
		}
	})

	t.Run("never posts a production command", func(t *testing.T) {
		production := markdownSection(t, policy, "## Production: never post")
		assert.Contains(t, production, "Never post a production command")
		assert.Contains(t, skill, "**Production: never post.**")
	})

	t.Run("never posts an unsafe, skip-revert, override, or forced command", func(t *testing.T) {
		never := markdownSection(t, policy, "## Never post these, in any environment")
		for _, flag := range []string{"--allow-unsafe", "--skip-revert", "--override", "unlock --force"} {
			assert.Contains(t, never, "`"+flag+"`", "the never-post list must name %s", flag)
			assert.Contains(t, skill, "`"+flag+"`", "SKILL.md's summary must name %s", flag)
		}
	})

	t.Run("asks before commands that answer the user's question", func(t *testing.T) {
		ask := markdownSection(t, policy, "## Ask before posting these, in any environment")
		for _, name := range []string{
			action.ApplyConfirm, action.Release, action.Unlock,
			action.Rollback, action.RollbackConfirm, action.Revert, action.SkipRevert,
		} {
			assert.Contains(t, ask, "`"+name+"`", "the ask-first list must name %s", name)
			assert.Contains(t, skill, "`"+name+"`", "SKILL.md's summary must name %s", name)
		}
	})

	t.Run("hands over when the blast radius is unknown", func(t *testing.T) {
		other := markdownSection(t, policy, "## Other environments: decide who it affects")
		assert.Contains(t, other, "treat anything you cannot confirm as shared")
	})

	t.Run("stops when SchemaBot refuses", func(t *testing.T) {
		refused := markdownSection(t, policy, "## When SchemaBot blocks or refuses a change")
		assert.Contains(t, refused, "Never get past a refusal by repeating the command, adding a flag, or trying another environment")
		assert.Contains(t, refused, "make it only once the user agrees")
		assert.Contains(t, refused, "never try other credentials or turn off authentication")
		assert.Contains(t, skill, "**Stop when SchemaBot refuses**")
		assert.Contains(t, skill, "only into a form SchemaBot names or the user agrees to")
	})

	t.Run("never acts outside SchemaBot", func(t *testing.T) {
		outside := markdownSection(t, policy, "## Never act outside SchemaBot")
		assert.Contains(t, outside, "Never run DDL against the database or cluster yourself")
		assert.Contains(t, outside, "never drop an invalid index, cancel a backend, or run the `GRANT` a refusal names")
		assert.Contains(t, outside, "never act in the PlanetScale console")
		assert.Contains(t, skill, "**Never act outside SchemaBot.**")
	})
}

// disposition is what an agent does with a command: the at-a-glance table in
// the command policy states one per command per environment kind.
type disposition string

const (
	postFreely disposition = "post freely"
	handOver   disposition = "hand over"
	askFirst   disposition = "ask first"
	// byWhoItAffects posts or hands over depending on who else the database
	// affects, and hands over when that cannot be confirmed.
	byWhoItAffects disposition = "by who it affects"
	inTerminal     disposition = "run in the terminal"
)

// policyRule is one row of the command policy's at-a-glance table.
type policyRule struct {
	production disposition
	other      disposition
}

// expectedPolicy is the command policy an agent follows, keyed by each command
// or flag the at-a-glance table names. A change here is a change to what an
// agent may post from a user's account, and is reviewed as one.
var expectedPolicy = map[string]policyRule{
	"plan":             {production: postFreely, other: postFreely},
	"help":             {production: postFreely, other: postFreely},
	"apply":            {production: handOver, other: byWhoItAffects},
	"stop":             {production: handOver, other: byWhoItAffects},
	"start":            {production: handOver, other: byWhoItAffects},
	"cutover":          {production: handOver, other: byWhoItAffects},
	"cancel":           {production: handOver, other: byWhoItAffects},
	"apply-confirm":    {production: handOver, other: askFirst},
	"release":          {production: handOver, other: askFirst},
	"unlock":           {production: handOver, other: askFirst},
	"rollback":         {production: handOver, other: askFirst},
	"rollback-confirm": {production: handOver, other: askFirst},
	"revert":           {production: handOver, other: askFirst},
	"skip-revert":      {production: handOver, other: askFirst},
	"--allow-unsafe":   {production: handOver, other: handOver},
	"--skip-revert":    {production: handOver, other: handOver},
	"--override":       {production: handOver, other: handOver},
	"--force":          {production: handOver, other: handOver},
	"freeze approve":   {production: handOver, other: handOver},
	"fix-lint":         {production: inTerminal, other: inTerminal},
}

// policyNeutralFlags are the PR comment flags that only say where or how a
// command runs, so the command's own row decides who may post it. Every other
// flag the parser accepts needs a row of its own in expectedPolicy.
var policyNeutralFlags = map[string]string{
	"-e":              "names the environment, which the command's row already splits on",
	"-d":              "picks one database of the PR",
	"-t":              "picks one tenant of the environment",
	"--target":        "narrows the command to one rollout member",
	"--defer-cutover": "waits for a separate cutover, which has its own row",
	"-y":              "is refused on a PR comment",
}

// cliSafeguardFlags are, per CLI command, the flags that drop a safeguard the
// PR comment form always keeps. The command policy names each one when it
// tells an agent to hand over the terminal command.
var cliSafeguardFlags = map[string][]string{
	action.Apply:    {"--no-lock", "--force", "--auto-approve"},
	action.Rollback: {"--auto-approve"},
}

// hasFlag reports whether a CLI command accepts the flag, in its long form or
// its negated form.
func hasFlag(cmd *kong.Node, flag string) bool {
	return slices.ContainsFunc(cmd.Flags, func(f *kong.Flag) bool {
		return "--"+f.Name == flag || ((f.Negated || f.Tag.Negatable != "") && "--no-"+f.Name == flag)
	})
}

// cliKind is what a CLI command with no PR comment form does.
type cliKind string

const (
	cliReadsOnly cliKind = "reads only"
	// cliWatchHasControls reads only with --no-watch or --json; watching,
	// its keys stop or cancel the apply, or run its cutover.
	cliWatchHasControls cliKind = "reads only with --no-watch or --json"
	cliWorksLocally     cliKind = "works on the local machine or the worktree"
	cliRunsAServer      cliKind = "runs a SchemaBot server"
	cliChangesSchemaBot cliKind = "changes SchemaBot itself"
)

// cliCommandKinds classifies each CLI command that has no PR comment form, or
// whose CLI form does more than the comment. A command that changes SchemaBot
// itself must be named in the command policy, which tells an agent to hand it
// over; a new CLI command fails the test until it is classified here.
var cliCommandKinds = map[string]cliKind{
	"init":       cliWorksLocally,
	"onboard":    cliWorksLocally,
	"pull":       cliReadsOnly,
	"progress":   cliWatchHasControls,
	"databases":  cliReadsOnly,
	"locks":      cliReadsOnly,
	"logs":       cliReadsOnly,
	"status":     cliReadsOnly,
	"list-plans": cliReadsOnly,
	"preview":    cliWorksLocally,
	"configure":  cliWorksLocally,
	"login":      cliWorksLocally,
	"freeze":     cliChangesSchemaBot,
	"unfreeze":   cliChangesSchemaBot,
	"settings":   cliChangesSchemaBot,
	"checks":     cliChangesSchemaBot,
	"storage":    cliChangesSchemaBot,
	"serve":      cliRunsAServer,
	"local":      cliRunsAServer,
}

// policyRows reads the at-a-glance table into one rule per command or flag
// named in a row's first cell.
func policyRows(t *testing.T, table string) map[string]policyRule {
	t.Helper()
	code := regexp.MustCompile("`([^`]+)`")
	rows := map[string]policyRule{}
	for line := range strings.SplitSeq(table, "\n") {
		cells := strings.Split(line, "|")
		if len(cells) != 5 || strings.HasPrefix(strings.TrimSpace(cells[1]), "---") || strings.TrimSpace(cells[1]) == "Command" {
			continue
		}
		production := readDisposition(t, cells[2], "")
		rule := policyRule{production: production, other: readDisposition(t, cells[3], production)}
		for _, m := range code.FindAllStringSubmatch(cells[1], -1) {
			_, dup := rows[m[1]]
			assert.False(t, dup, "%s is in more than one row of the at-a-glance table", m[1])
			rows[m[1]] = rule
		}
	}
	return rows
}

// readDisposition maps the wording of a table cell to the disposition it
// states. "Same" repeats the production column.
func readDisposition(t *testing.T, cell string, production disposition) disposition {
	t.Helper()
	text := strings.TrimPrefix(strings.TrimSpace(cell), "[")
	switch {
	case strings.HasPrefix(text, "Same") && production != "":
		return production
	case strings.HasPrefix(text, "Post or hand over"):
		return byWhoItAffects
	case strings.HasPrefix(text, "Post freely"):
		return postFreely
	case strings.HasPrefix(text, "Hand over"):
		return handOver
	case strings.HasPrefix(text, "Ask first"):
		return askFirst
	case strings.HasPrefix(text, "Run it in the terminal"):
		return inTerminal
	}
	assert.Fail(t, "unrecognized disposition in the command policy table", "cell: %q", cell)
	return ""
}

// agentSkillCommands returns every `schemabot …` command in the skill's
// markdown. Lines of a bash, sh, or console fence are terminal commands; lines
// of a text fence are PR comments; inline code spans are PR comments when
// their command word is one the command policy pins (see isPRCommentExample),
// and terminal commands otherwise.
func agentSkillCommands(t *testing.T) []skillCommand {
	t.Helper()
	inline := regexp.MustCompile("`(schemabot [^`]+)`")
	var out []skillCommand

	err := filepath.WalkDir(agentSkillDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".md" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(agentSkillDir, path)
		if err != nil {
			return err
		}

		fence := ""
		inFence := false
		for i, line := range strings.Split(string(data), "\n") {
			trimmed := strings.TrimSpace(line)
			if marker := fenceMarker(trimmed); marker != "" {
				inFence = !inFence
				fence = strings.TrimPrefix(trimmed, marker)
				if marker != "```" {
					fence = marker
				}
				continue
			}
			if inFence {
				cmd := strings.TrimPrefix(trimmed, "$ ")
				if !strings.HasPrefix(cmd, "schemabot ") {
					continue
				}
				switch fence {
				case "bash", "sh", "console":
					out = append(out, skillCommand{file: rel, line: i + 1, text: cmd, fenced: true})
				case "text":
					out = append(out, skillCommand{file: rel, line: i + 1, text: cmd, fenced: true, prComment: true})
				default:
					assert.Fail(t, "a schemabot command in a fence the test cannot check",
						"%s:%d: open the fence as ```bash for a terminal command or ```text for a PR comment, "+
							"so the command is parsed by the surface it is shown on", rel, i+1)
				}
				continue
			}
			for _, m := range inline.FindAllStringSubmatch(line, -1) {
				out = append(out, skillCommand{file: rel, line: i + 1, text: m[1], prComment: true})
			}
		}
		return nil
	})
	require.NoError(t, err, "read the agent skill at %s", agentSkillDir)
	return out
}

// fenceMarker returns the marker that opens or closes a fenced block on the
// line, or "" when the line is not a fence.
func fenceMarker(trimmed string) string {
	for _, marker := range []string{"```", "~~~"} {
		if strings.HasPrefix(trimmed, marker) {
			return marker
		}
	}
	return ""
}

// substitutePlaceholders replaces each known placeholder with a value the
// command's surface accepts, and fails on an unknown one.
func substitutePlaceholders(t *testing.T, where string, args []string) ([]string, bool) {
	t.Helper()
	out := make([]string, 0, len(args))
	for _, arg := range args {
		if strings.HasPrefix(arg, "<") {
			value, ok := skillPlaceholders[arg]
			if !assert.True(t, ok, "%s: unknown placeholder %s; use one of the skillPlaceholders keys", where, arg) {
				return nil, false
			}
			arg = value
		}
		out = append(out, arg)
	}
	if !assert.NotEmpty(t, out, "%s: no command word", where) {
		return nil, false
	}
	return out, true
}

// assertParsesAsPRComment parses the command exactly as SchemaBot parses a
// comment, and checks that every flag on it took effect: a flag the command
// does not support is dropped by the parser, which would leave the agent
// believing it asked for something it did not.
func assertParsesAsPRComment(t *testing.T, where string, args []string) {
	t.Helper()
	body := "schemabot " + strings.Join(args, " ")
	result := webhook.NewCommandParser().ParseCommand(body)

	if args[0] == action.Help {
		assert.True(t, result.IsHelp, "%s: not parsed as help", where)
		return
	}
	assert.Equal(t, args[0], result.Action, "%s: parsed as a different command", where)
	assert.False(t, result.EnvironmentError, "%s: invalid -e value", where)
	assert.False(t, result.TenantError, "%s: invalid --tenant value", where)
	// A plan with no -e plans every environment, which the parser reports as
	// not found; its flags are still checked below.
	if !isMultiEnvPlan(result) && !assert.True(t, result.Found, "%s: the PR comment parser does not accept this command", where) {
		return
	}

	for _, arg := range args[1:] {
		switch {
		case arg == "--target":
			assert.NotEmpty(t, result.Target, "%s: --target is not supported by %s", where, args[0])
		case arg == "-d":
			assert.NotEmpty(t, result.Database, "%s: -d is not supported by %s", where, args[0])
		case arg == "--defer-cutover":
			assert.True(t, result.DeferCutover, "%s: --defer-cutover is not supported by %s", where, args[0])
		case arg == "--allow-unsafe":
			assert.True(t, result.AllowUnsafe, "%s: --allow-unsafe is not supported by %s", where, args[0])
		case arg == "--skip-revert":
			assert.True(t, result.SkipRevert, "%s: --skip-revert is not supported by %s", where, args[0])
		case arg == "--force":
			assert.True(t, result.Force, "%s: --force is not supported by %s", where, args[0])
		case arg == "-e":
			assert.NotEmpty(t, result.Environment, "%s: -e was not read by %s", where, args[0])
		case arg == "-t" || arg == "--tenant":
			assert.NotEmpty(t, result.Tenant, "%s: %s was not read by %s", where, arg, args[0])
		case arg == "--override":
			assert.True(t, result.OverrideGiven, "%s: --override is not supported by %s", where, args[0])
		case strings.HasPrefix(arg, "-"):
			assert.Fail(t, "a flag this test does not check", "%s: add a case for %s so the test proves %s accepts it", where, arg, args[0])
		case strings.HasPrefix(arg, "apply-"):
			assert.Equal(t, arg, result.ApplyID, "%s: the apply ID was not read", where)
		}
	}
}

// isPRCommentExample reports whether a command is shown as a PR comment. A
// command in a text fence always is. An inline mention is when its command
// word is one the command policy pins, which is independent of the parser's
// registry, so a PR comment command dropped from the parser still gets parsed
// as one here, and fails.
func isPRCommentExample(c skillCommand, word string, prCommands []string) bool {
	if !c.prComment {
		return false
	}
	return c.fenced || slices.Contains(prCommands, word)
}

// policyCommandWords returns the PR comment command words the command policy
// pins, such as "freeze" for "freeze approve". Flags are left out.
func policyCommandWords() []string {
	var words []string
	for entry := range expectedPolicy {
		if strings.HasPrefix(entry, "-") {
			continue
		}
		word, _, _ := strings.Cut(entry, " ")
		if !slices.Contains(words, word) {
			words = append(words, word)
		}
	}
	slices.Sort(words)
	return words
}

// isMultiEnvPlan reports whether the command is `plan` with no -e and no
// --target, which SchemaBot answers by planning every configured environment.
// The parser reports it as missing -e; every other command missing -e is a
// usage error.
func isMultiEnvPlan(result webhook.CommandResult) bool {
	return result.Action == action.Plan && result.MissingEnv && result.Target == ""
}

// assertCLIMentionResolves checks a terminal command mentioned in prose. A
// mention can leave out arguments a full invocation needs, so instead of
// parsing it, the command path is resolved in the CLI's kong model and every
// flag on it must exist on that command.
func assertCLIMentionResolves(t *testing.T, where string, cli *kong.Kong, args []string) {
	t.Helper()
	node := cli.Model.Node
	i := 0
	for ; i < len(args) && !strings.HasPrefix(args[i], "-"); i++ {
		child := childCommand(node, args[i])
		if child == nil {
			break
		}
		node = child
	}
	if !assert.NotSame(t, cli.Model.Node, node, "%s: %q is not a CLI command", where, args[0]) {
		return
	}

	known := map[string]bool{}
	for _, group := range node.AllFlags(false) {
		for _, f := range group {
			known["--"+f.Name] = true
			if f.Negated || f.Tag.Negatable != "" {
				known["--no-"+f.Name] = true
			}
			if f.Short != 0 {
				known["-"+string(f.Short)] = true
			}
			for _, alias := range f.Aliases {
				known["--"+alias] = true
			}
		}
	}
	for _, arg := range args[i:] {
		if strings.HasPrefix(arg, "-") {
			name, _, _ := strings.Cut(arg, "=")
			assert.True(t, known[name], "%s: %s has no flag %s", where, node.Path(), name)
		}
	}
}

func childCommand(node *kong.Node, name string) *kong.Node {
	for _, child := range node.Children {
		if child.Type == kong.CommandNode && (child.Name == name || slices.Contains(child.Aliases, name)) {
			return child
		}
	}
	return nil
}

func newCLIParser(t *testing.T) *kong.Kong {
	t.Helper()
	var cli CLI
	parser, err := kong.New(&cli,
		kong.Name("schemabot"),
		kong.Writers(io.Discard, io.Discard),
		kong.Exit(func(int) {}),
		kong.Vars{"version": "test", "cli_name": "schemabot"},
	)
	require.NoError(t, err)
	return parser
}

func readSkillFile(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(agentSkillDir, name))
	require.NoError(t, err, "read %s from the agent skill", name)
	return string(data)
}

// markdownSection returns the text under heading, up to the next heading of
// the same or a higher level.
func markdownSection(t *testing.T, doc, heading string) string {
	t.Helper()
	start := strings.Index(doc, "\n"+heading+"\n")
	require.NotEqual(t, -1, start, "the command policy has no %q section", heading)
	level, _, _ := strings.Cut(heading, " ")
	var body []string
	for line := range strings.SplitSeq(doc[start+len(heading)+2:], "\n") {
		marker, _, isHeading := strings.Cut(line, " ")
		if isHeading && strings.Trim(marker, "#") == "" && marker != "" && len(marker) <= len(level) {
			break
		}
		body = append(body, line)
	}
	return strings.Join(body, "\n")
}
