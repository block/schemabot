package client

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/block/schemabot/pkg/localruntime"
)

// RegisterLocalProfile adds a local connection without replacing a remote
// profile, credentials, or the user's default. Repeating the same input is safe.
// It does not provision or start the runtime.
func RegisterLocalProfile(name, runtimeID string) (bool, error) {
	if strings.TrimSpace(name) == "" {
		return false, fmt.Errorf("profile name is required")
	}
	if _, err := localruntime.Directory(runtimeID); err != nil {
		return false, err
	}
	cfg, err := LoadConfig()
	if err != nil {
		return false, err
	}
	wanted := Profile{LocalRuntime: runtimeID}
	if existing, ok := cfg.Profiles[name]; ok {
		// Treat unexpected fields as a conflict. Local profiles must not
		// retain remote credentials or acquire new behavior silently.
		if reflect.DeepEqual(existing, wanted) {
			return false, nil
		}
		return false, fmt.Errorf("profile %q already has a different connection; choose another profile name", name)
	}
	cfg.Profiles[name] = wanted
	if err := SaveConfig(cfg); err != nil {
		return false, err
	}
	return true, nil
}
