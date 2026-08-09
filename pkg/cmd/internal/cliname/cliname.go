// Package cliname resolves the tool name rendered in CLI command hints.
//
// Every pasteable command the CLI prints ("Force unlock: schemabot unlock
// ...") starts with the tool name. When the CLI runs behind a wrapper, the
// bare binary name is wrong: pasting it invokes an unconfigured binary
// instead of the wrapper the operator actually uses. The wrapper passes its
// own invocation on every call via the --cli-name flag, and every hint
// renders through Name so pasted commands work as printed.
package cliname

import (
	"strings"
	"sync/atomic"
)

// defaultName is the bare binary name, rendered when no --cli-name is passed.
const defaultName = "schemabot"

// flagName is the global flag a wrapper uses to pass its invocation.
const flagName = "--cli-name"

var name atomic.Value

// Set records the tool name command hints render, typically a wrapper
// invocation such as "sq schemabot". An empty name is ignored so an absent
// --cli-name flag keeps the default rather than clearing the name.
func Set(n string) {
	if n == "" {
		return
	}
	name.Store(n)
}

// Name returns the tool name to render at the start of CLI command hints.
func Name() string {
	if v, ok := name.Load().(string); ok {
		return v
	}
	return defaultName
}

// FromArgs extracts the --cli-name flag value from raw command-line args,
// returning "" when the flag is absent. It scans the args directly because
// the name feeds kong's usage text, which must be fixed before kong parses;
// kong still declares the flag so it is accepted at any position.
func FromArgs(args []string) string {
	value := ""
	for i := 0; i < len(args); i++ {
		if args[i] == "--" {
			break
		}
		if args[i] == flagName && i+1 < len(args) {
			value = args[i+1]
			i++
			continue
		}
		if rest, ok := strings.CutPrefix(args[i], flagName+"="); ok {
			value = rest
		}
	}
	return value
}
