package cliname

import (
	"regexp"
	"strings"
	"sync/atomic"
)

// The route is the set of global flags that selected the control plane this
// run talked to, such as --profile or --endpoint, plus any a wrapper adds of
// its own. A hint that sends the operator back to the same server renders
// through Command so the pasted command reaches it again rather than the
// default route. The route is recorded from what the operator typed, before
// any endpoint resolution rewrites it: a wrapper that port-forwards, or a local
// runtime, replaces the endpoint with one that does not outlive the run.
var route atomic.Pointer[[]string]

// SetRoute records the routing flags hints carry, as flag and value pairs in
// command-line order (for example "--profile", "prod"). The CLI records its own
// with Globals.RouteFlags after parsing; a wrapper that adds a routing flag of
// its own appends it and calls SetRoute again. Callers must never pass a
// credential: everything recorded here is printed. An empty route clears it.
func SetRoute(flags ...string) {
	recorded := append([]string(nil), flags...)
	route.Store(&recorded)
}

// Route returns the recorded routing flags, or nil when none were recorded.
func Route() []string {
	if p := route.Load(); p != nil {
		return append([]string(nil), (*p)...)
	}
	return nil
}

// Command renders a pasteable command that reaches the server this run used:
// the tool name, args, and the recorded routing flags. Args are rendered as
// given, so a caller can pass a placeholder such as <environment>; the routing
// values came from the operator and are quoted when a shell would split them.
func Command(args ...string) string {
	parts := append([]string{Name()}, args...)
	for _, flag := range Route() {
		parts = append(parts, quoteArg(flag))
	}
	return strings.Join(parts, " ")
}

// shellSafe matches an argument a shell passes through unchanged.
var shellSafe = regexp.MustCompile(`^[A-Za-z0-9_@%+=:,./-]+$`)

// quoteArg double-quotes an argument a shell would otherwise split or expand.
// Double quotes, not single, because callers embed the whole command in
// single quotes.
func quoteArg(arg string) string {
	if shellSafe.MatchString(arg) {
		return arg
	}
	escaped := strings.NewReplacer(`\`, `\\`, `"`, `\"`, `$`, `\$`, "`", "\\`").Replace(arg)
	return `"` + escaped + `"`
}
