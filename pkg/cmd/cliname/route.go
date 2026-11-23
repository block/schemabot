package cliname

import (
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"unicode"
)

// The route is the set of global flags that selected the control plane this
// run talked to, such as --profile or --endpoint, plus any a wrapper adds of
// its own. A hint that sends the operator back to the same server renders
// through Command so the pasted command reaches it again rather than the
// default route. The route is recorded from what the operator typed, before
// any endpoint resolution rewrites it: a wrapper that port-forwards, or a local
// runtime, replaces the endpoint with one that does not outlive the run.
var route atomic.Pointer[[]Flag]

// Flag is one routing flag and the value the operator gave it, such as
// {Name: "--profile", Value: "prod"}.
type Flag struct {
	Name  string
	Value string
}

// SetRoute records the routing flags hints carry, in command-line order. The
// CLI records its own with Globals.RouteFlags after parsing; a wrapper that adds
// a routing flag of its own appends it and calls SetRoute again. Callers must
// never pass a credential: everything recorded here is printed. An empty route
// clears it. A hint that already passes a recorded flag, under any spelling the
// CLI accepts for it (flagSpellings), keeps its own value and does not get the
// recorded one.
func SetRoute(flags ...Flag) {
	recorded := append([]Flag(nil), flags...)
	route.Store(&recorded)
}

// Route returns the recorded routing flags, or nil when none were recorded.
func Route() []Flag {
	if p := route.Load(); p != nil {
		return append([]Flag(nil), (*p)...)
	}
	return nil
}

// Command renders a pasteable command that reaches the server this run used:
// the tool name, args, and the recorded routing flags. Args are rendered as
// given, so a caller can pass a placeholder such as <environment>; the routing
// values came from the operator and are rendered so a shell passes them through
// unchanged.
//
// A routing flag the caller already passes is left out, so the caller's value
// stands alone: a wrapper that routes on -e would otherwise turn a hint
// written as "start <id> -e <environment>" into one naming -e twice.
func Command(args ...string) string {
	parts := append([]string{Name()}, args...)
	for _, flag := range Route() {
		if passesFlag(args, flag.Name) {
			continue
		}
		rendered, ok := renderFlag(flag)
		if !ok {
			slog.Debug("leaving a routing flag with a control character out of command hints", "flag", flag.Name)
			continue
		}
		parts = append(parts, rendered)
	}
	return strings.Join(parts, " ")
}

// flagSpellings lists the spellings of one routing flag that the CLI accepts
// interchangeably. A hint that passes either spelling already names the flag,
// whichever spelling the route recorded: commands take the environment as -e
// or --environment, and a hint that passes -e staging must not also get a
// recorded --environment development.
var flagSpellings = [][]string{
	{"-e", "--environment"},
}

// spellings returns every spelling of the flag name, the name itself first.
func spellings(name string) []string {
	for _, group := range flagSpellings {
		if slices.Contains(group, name) {
			return group
		}
	}
	return []string{name}
}

// passesFlag reports whether args already pass the flag name under any of its
// spellings, either as its own word ("-e", "staging") or joined to its value
// ("--environment=staging").
func passesFlag(args []string, name string) bool {
	for _, spelling := range spellings(name) {
		for _, arg := range args {
			if arg == spelling || strings.HasPrefix(arg, spelling+"=") {
				return true
			}
		}
	}
	return false
}

// renderFlag renders a routing flag for a shell. A value that starts with a
// hyphen, or is empty, is joined with "=", since the CLI's parser reads a
// separate hyphen-leading word as another flag. A value holding a control
// character has no portable shell spelling, so it is reported as unrenderable.
func renderFlag(flag Flag) (string, bool) {
	if strings.ContainsFunc(flag.Value, unicode.IsControl) {
		return "", false
	}
	value := shellEscape(flag.Value)
	if value == "" || strings.HasPrefix(flag.Value, "-") {
		return flag.Name + "=" + value, true
	}
	return flag.Name + " " + value, true
}

// shellEscape backslash-escapes every character a shell would otherwise split
// on, expand, or treat as syntax. Backslashes rather than quotes, because no
// quoting style is safe on its own: double quotes still expand "$" and, in an
// interactive bash, "!" history references, and callers embed the whole
// command in single quotes.
//
// A leading "=" is escaped too: zsh expands a word that starts with one into
// the path of the command it names.
func shellEscape(value string) string {
	var b strings.Builder
	for i, r := range value {
		if !isShellSafe(r) || (i == 0 && r == '=') {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// isShellSafe reports whether a shell passes r through unescaped anywhere in
// a word.
func isShellSafe(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	default:
		return strings.ContainsRune("_@%+=:,./-", r)
	}
}
