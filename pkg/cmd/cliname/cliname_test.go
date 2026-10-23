package cliname

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
// flag of its own gets it on every hint; and recording an empty route clears
// it.
func TestCommand(t *testing.T) {
	name.Store(nil)
	route.Store(nil)
	t.Cleanup(func() {
		name.Store(nil)
		route.Store(nil)
	})

	assert.Equal(t, "schemabot progress apply-1", Command("progress", "apply-1"), "no route renders the bare command")

	Set("acme schemabot")
	SetRoute(Flag{"--profile", "prod"}, Flag{"--context", "west cluster"})
	assert.Equal(t, `acme schemabot progress apply-1 -e production --profile prod --context west\ cluster`,
		Command("progress", "apply-1", "-e", "production"))
	assert.Equal(t, `acme schemabot start -e <environment> apply-1 --profile prod --context west\ cluster`,
		Command("start", "-e", "<environment>", "apply-1"), "caller args, including a placeholder, render as given")

	SetRoute(Flag{"--profile", "bad\nname"}, Flag{"--context", "west"})
	assert.Equal(t, "acme schemabot progress apply-1 --context west", Command("progress", "apply-1"),
		"a value with a control character has no shell spelling and is left out")

	SetRoute()
	assert.Equal(t, "acme schemabot progress apply-1", Command("progress", "apply-1"), "an empty route clears it")
}

// Every rendered value must reach the CLI byte for byte when the operator
// pastes it. Each case renders a flag, hands the result to a real shell, and
// compares the words the shell produced with the flag the operator typed.
func TestRenderFlag_ShellRoundTrip(t *testing.T) {
	tests := []struct {
		value    string
		rendered string
	}{
		{"prod", "--profile prod"},
		{"https://schemabot.example:8443/api", "--profile https://schemabot.example:8443/api"},
		{"-prod", "--profile=-prod"},
		{"", "--profile="},
		{"west cluster", `--profile west\ cluster`},
		{"prod!cluster", `--profile prod\!cluster`},
		{`a"b'c`, `--profile a\"b\'c`},
		{"$HOME`id`", "--profile \\$HOME\\`id\\`"},
		{"~prod", `--profile \~prod`},
		{"=prod", `--profile \=prod`},
		{`back\slash`, `--profile back\\slash`},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			rendered, ok := renderFlag(Flag{Name: "--profile", Value: tt.value})
			require.True(t, ok)
			assert.Equal(t, tt.rendered, rendered)

			out, err := exec.CommandContext(t.Context(), "/bin/sh", "-c", `printf '%s\0' `+rendered).Output()
			require.NoError(t, err)
			words := strings.Split(strings.TrimSuffix(string(out), "\x00"), "\x00")
			want := []string{"--profile", tt.value}
			if strings.Contains(rendered, "=") && !strings.Contains(rendered, " ") {
				want = []string{"--profile=" + tt.value}
			}
			assert.Equal(t, want, words)
		})
	}
}
