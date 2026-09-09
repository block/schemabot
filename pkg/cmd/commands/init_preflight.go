package commands

import (
	"fmt"
	"os"
	"path/filepath"
)

// Probe only private staging paths, before registration or runtime startup.
// Publication still uses an exclusive rename and never falls back to overwrite.
func checkInitPublication(stage string, rename func(string, string) error) error {
	from, to := filepath.Join(stage, ".publish-probe"), filepath.Join(stage, ".publish-probe-done")
	if err := os.Mkdir(from, 0700); err != nil {
		return fmt.Errorf("prepare schema publication check: %w", err)
	}
	if err := rename(from, to); err != nil {
		return fmt.Errorf("schema directory must support exclusive directory rename; choose --schema-dir on a local filesystem such as APFS or ext4: %w", err)
	}
	if err := os.Remove(to); err != nil {
		return fmt.Errorf("remove schema publication check: %w", err)
	}
	return nil
}
