package localruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/block/spirit/pkg/utils"
)

// UpdateConfig transforms the private configuration under the runtime lifetime
// lock. The caller validates its content; the supervisor treats it as opaque.
// transform receives nil when no config exists and must return the original
// bytes for a no-op. A live host validates and publishes additions itself.
func (m Manager) UpdateConfig(transform func([]byte) ([]byte, error), resolved ...map[string]map[string]string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		changed, err := m.updateConfig(ctx, transform, resolved...)
		if !errors.Is(err, errConfigChanged) {
			return changed, err
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

func (m Manager) updateConfig(ctx context.Context, transform func([]byte) ([]byte, error), resolved ...map[string]map[string]string) (bool, error) {
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
		if len(resolved) == 0 {
			return false, fmt.Errorf("runtime is active; configuration changes require validated registration")
		}
		r, err := m.Status(ctx)
		if err != nil {
			return false, err
		}
		if r.State == "starting" || r.State == "stopped" {
			return false, errConfigChanged
		}
		if r.State != "ready" {
			return false, fmt.Errorf("runtime is %s; existing work was left running", r.State)
		}
		if m.Binary != "" {
			binary, err := binaryDigest(m.Binary)
			if err != nil {
				return false, err
			}
			if r.Binary != binary {
				return false, fmt.Errorf("running runtime uses a different binary; existing work was left running")
			}
		}
		token, err := ReadPrivate(filepath.Join(m.Dir, "token"))
		if err != nil {
			return false, err
		}
		payload, err := json.Marshal(ConfigUpdate{Expected: digest(data), Config: updated, Resolved: resolved[0]})
		if err != nil {
			return false, err
		}
		err = m.callBody(ctx, r, string(token), http.MethodPost, "/config", nil, payload)
		return err == nil, err
	}
	if err := writeAtomic(path, updated); err != nil {
		return false, err
	}
	return true, nil
}

// ConfigUpdate carries declarations and ephemeral connections over the signed
// control channel. Resolved credentials are never written to runtime files.
type ConfigUpdate struct {
	Expected string                       `json:"expected"`
	Config   []byte                       `json:"config"`
	Resolved map[string]map[string]string `json:"resolved"`
}

// PrepareConfig validates an additive update and returns its infallible in-memory
// publication step. The host persists declarations before calling that step.
type PrepareConfig func([]byte, map[string]map[string]string) (func(), error)

var errConfigChanged = errors.New("runtime configuration changed; retry registration")
