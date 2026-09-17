package commands

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/require"
)

func TestLocalRuntimeSelection(t *testing.T) {
	const profiles = `default_profile: development
profiles:
  development:
    local_runtime: project
  other:
    local_runtime: another
  remote:
    endpoint: https://example.test
`
	for _, tt := range []struct{ name, config, env, flag, id, want, errorText string }{
		{name: "unconfigured", want: "local"},
		{name: "configured default", config: profiles, want: "project"},
		{name: "environment", config: profiles, env: "other", want: "another"},
		{name: "flag wins", config: profiles, env: "remote", flag: "other", want: "another"},
		{name: "explicit ID wins", config: profiles, flag: "remote", id: "chosen", want: "chosen"},
		{name: "explicit ID ignores broken config", config: "[bad yaml", id: "chosen", want: "chosen"},
		{name: "remote profile", config: profiles, flag: "remote", errorText: "does not select a local runtime"},
		{name: "missing flag profile", flag: "missing", errorText: "unknown profile"},
		{name: "missing environment profile", env: "missing", errorText: "unknown profile"},
		{name: "missing configured default", config: "default_profile: missing\n", errorText: "unknown profile"},
		{name: "profiles without a selected default", config: "profiles:\n  remote:\n    endpoint: https://example.test\n", errorText: "unknown profile"},
		{name: "invalid config", config: "[bad yaml", errorText: "parse"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("SCHEMABOT_PROFILE", tt.env)
			if tt.config != "" {
				require.NoError(t, os.MkdirAll(filepath.Join(home, ".schemabot"), 0700))
				require.NoError(t, os.WriteFile(filepath.Join(home, ".schemabot", "config.yaml"), []byte(tt.config), 0600))
			}
			got, err := resolveLocalRuntimeID(tt.id, tt.flag)
			if tt.errorText != "" {
				require.ErrorContains(t, err, tt.errorText)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
			require.NoDirExists(t, filepath.Join(home, ".schemabot", "runtimes"))
		})
	}
}

func TestLocalCommandsAcceptOptionalRuntimeID(t *testing.T) {
	for _, command := range []string{"stop", "status"} {
		for _, id := range []string{"", "project"} {
			t.Run(command+"/"+id, func(t *testing.T) {
				var cli struct {
					Local LocalCmd `cmd:""`
					Globals
				}
				parser, err := kong.New(&cli)
				require.NoError(t, err)
				args := []string{"local", command}
				if id != "" {
					args = append(args, id)
				}
				_, err = parser.Parse(args)
				require.NoError(t, err)
				if command == "stop" {
					require.Equal(t, id, cli.Local.Stop.ID)
				} else {
					require.Equal(t, id, cli.Local.Status.ID)
				}
			})
		}
	}
}
