package commands

import (
	"fmt"
	"testing"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/stretchr/testify/require"
)

func TestConfigureShowProfileSelection(t *testing.T) {
	for _, tt := range []struct{ name, flag, env, configured, want, source string }{
		{name: "fallback", want: "default", source: "default"},
		{name: "configured", configured: "team", want: "team", source: "from config default_profile"},
		{name: "environment", configured: "team", env: "sandbox", want: "sandbox", source: "from SCHEMABOT_PROFILE env"},
		{name: "flag", configured: "team", env: "sandbox", flag: "chosen", want: "chosen", source: "from --profile flag"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			t.Setenv("SCHEMABOT_PROFILE", tt.env)
			t.Setenv("SCHEMABOT_ENDPOINT", "")
			cfg := &client.Config{DefaultProfile: tt.configured, Profiles: map[string]client.Profile{}}
			for _, name := range []string{"default", "team", "sandbox", "chosen"} {
				cfg.Profiles[name] = client.Profile{Endpoint: "https://" + name + ".example.test"}
			}
			require.NoError(t, client.SaveConfig(cfg))
			var runErr error
			output := captureOutput(t, func() { runErr = (&ConfigureShowCmd{}).Run(&Globals{Profile: tt.flag}) })
			require.NoError(t, runErr)
			require.Contains(t, output, fmt.Sprintf("  Active profile: %s (%s)\n", tt.want, tt.source))
			require.Contains(t, output, fmt.Sprintf("  Endpoint: https://%s.example.test (from profile)\n", tt.want))
			require.Contains(t, output, fmt.Sprintf("    * %s: https://%s.example.test\n", tt.want, tt.want))
		})
	}
}
