package cliname

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestName covers the full lifecycle in one test because the name is
// process-global: the default before any Set, an empty Set leaving the
// default in place, and a wrapper-passed name taking effect. The global is
// reset to its unset state around the test so ordering against other tests
// in the package cannot change the outcome.
func TestName(t *testing.T) {
	name.Store(nil)
	t.Cleanup(func() { name.Store(nil) })

	assert.Equal(t, "schemabot", Name(), "default before any Set")

	Set("")
	assert.Equal(t, "schemabot", Name(), "empty Set keeps the default")

	Set("acme schemabot")
	assert.Equal(t, "acme schemabot", Name(), "wrapper-passed name is rendered")
}

func TestFromArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"absent", []string{"status", "-e", "staging"}, ""},
		{"equals form", []string{"--cli-name=acme schemabot", "status"}, "acme schemabot"},
		{"space form", []string{"--cli-name", "acme schemabot", "status"}, "acme schemabot"},
		{"after subcommand", []string{"rollback", "--cli-name", "acme schemabot"}, "acme schemabot"},
		{"missing value at end", []string{"status", "--cli-name"}, ""},
		{"flag-like space-form value not consumed", []string{"--cli-name", "-e", "staging"}, ""},
		{"flag-like equals-form value accepted", []string{"--cli-name=-x"}, "-x"},
		{"not scanned past double dash", []string{"status", "--", "--cli-name=acme schemabot"}, ""},
		{"last occurrence wins", []string{"--cli-name=first", "--cli-name", "second"}, "second"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, FromArgs(tt.args))
		})
	}
}

// TestCommand covers how hints render the recorded route. Like the name, the
// route is process-global, so both are reset around the test. A run with no
// routing flags renders the bare command; a wrapper that records a routing
// flag of its own gets it on every hint, quoted when a shell would split it;
// and recording an empty route clears it.
func TestCommand(t *testing.T) {
	name.Store(nil)
	route.Store(nil)
	t.Cleanup(func() {
		name.Store(nil)
		route.Store(nil)
	})

	assert.Equal(t, "schemabot progress apply-1", Command("progress", "apply-1"), "no route renders the bare command")

	Set("acme schemabot")
	SetRoute("--profile", "prod", "--context", "west cluster")
	assert.Equal(t, `acme schemabot progress apply-1 -e production --profile prod --context "west cluster"`,
		Command("progress", "apply-1", "-e", "production"))
	assert.Equal(t, "acme schemabot start -e <environment> apply-1 --profile prod --context \"west cluster\"",
		Command("start", "-e", "<environment>", "apply-1"), "caller args, including a placeholder, render as given")

	SetRoute()
	assert.Equal(t, "acme schemabot progress apply-1", Command("progress", "apply-1"), "an empty route clears it")
}

func TestQuoteArg(t *testing.T) {
	tests := []struct {
		arg  string
		want string
	}{
		{"prod", "prod"},
		{"https://schemabot.example:8443/api", "https://schemabot.example:8443/api"},
		{"west cluster", `"west cluster"`},
		{`a"b`, `"a\"b"`},
		{"$HOME", `"\$HOME"`},
		{"", `""`},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, quoteArg(tt.arg), "quoteArg(%q)", tt.arg)
	}
}
