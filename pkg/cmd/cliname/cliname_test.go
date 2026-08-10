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
		{"not scanned past double dash", []string{"status", "--", "--cli-name=acme schemabot"}, ""},
		{"last occurrence wins", []string{"--cli-name=first", "--cli-name", "second"}, "second"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, FromArgs(tt.args))
		})
	}
}
