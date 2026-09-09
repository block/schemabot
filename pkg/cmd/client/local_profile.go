package client

import (
	"context"
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
	wanted := Profile{LocalRuntime: runtimeID}
	changed := false
	err := UpdateConfig(context.Background(), func(cfg *Config) error {
		changed = false
		if existing, ok := cfg.Profiles[name]; ok {
			if reflect.DeepEqual(existing, wanted) {
				return nil
			}
			return fmt.Errorf("profile %q already has a different connection; choose another profile name", name)
		}
		cfg.Profiles[name] = wanted
		changed = true
		return nil
	})
	return changed && err == nil, err
}
