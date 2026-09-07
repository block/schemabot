package localruntime

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/block/spirit/pkg/utils"
)

// UpdateConfig transforms the private configuration under the runtime lifetime
// lock. The caller validates its content; the supervisor treats it as opaque.
// transform receives nil when no config exists and must return the original
// bytes for a no-op. Only no-ops are allowed while the runtime is active.
func (m Manager) UpdateConfig(transform func([]byte) ([]byte, error)) (bool, error) {
	if err := privateDirectory(m.Dir); err != nil {
		return false, err
	}
	lease, available, err := lock(m.Dir)
	if err != nil {
		return false, err
	}
	if available {
		defer utils.CloseAndLog(lease)
	}
	path := filepath.Join(m.Dir, "runtime.yaml")
	data, err := ReadPrivate(path)
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("read runtime configuration: %w", err)
	}
	updated, err := transform(bytes.Clone(data))
	if err != nil {
		return false, err
	}
	if bytes.Equal(data, updated) {
		return false, nil
	}
	if len(updated) > 1<<20 {
		return false, fmt.Errorf("runtime configuration exceeds 1 MiB")
	}
	if !available {
		return false, fmt.Errorf("runtime is active; finish active work and stop it before changing configuration")
	}
	if err := writeAtomic(path, updated); err != nil {
		return false, err
	}
	return true, nil
}
