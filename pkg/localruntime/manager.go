package localruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/block/spirit/pkg/utils"
)

const pollInterval = 100 * time.Millisecond

// Record contains process identity, not database apply state. A recorded PID is
// diagnostic only; ownership is proven by the lifetime lock and control endpoint.
type Record struct {
	ID         string `json:"id"`
	Generation string `json:"generation"`
	Binary     string `json:"binary"`
	Config     string `json:"config"`
	Version    string `json:"version"`
	PID        int    `json:"pid"`
	State      string `json:"state"`
	Control    string `json:"control,omitempty"`
	Endpoint   string `json:"endpoint,omitempty"`
}

// Connection is returned only after the runtime proves its identity and readiness.
type Connection struct {
	Record
	Token string `json:"-"`
}

// Manager uses a private runtime directory provisioned by shared setup. It never
// provisions databases or infers local mode from an unavailable remote endpoint.
type Manager struct {
	Dir     string
	Binary  string
	Version string
}

func (m Manager) record() (Record, error) {
	var r Record
	data, err := ReadPrivate(filepath.Join(m.Dir, "manifest.json"))
	if err != nil {
		return r, fmt.Errorf("read runtime manifest: %w", err)
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return r, fmt.Errorf("invalid runtime manifest: %w", err)
	}
	if r.ID != filepath.Base(m.Dir) || r.Generation == "" {
		return r, fmt.Errorf("runtime manifest identity mismatch")
	}
	return r, nil
}

// Ensure reconnects or starts exactly one child. A CLI deadline does not cancel
// the child; its manifest and inherited lock account for unfinished startup.
func (m Manager) Ensure(ctx context.Context) (Connection, error) {
	if err := ctx.Err(); err != nil {
		return Connection{}, err
	}
	if err := privateDirectory(m.Dir); err != nil {
		return Connection{}, err
	}
	config, err := ReadPrivate(filepath.Join(m.Dir, "runtime.yaml"))
	if err != nil {
		return Connection{}, fmt.Errorf("read runtime configuration: %w", err)
	}
	binary, err := binaryDigest(m.Binary)
	if err != nil {
		return Connection{}, err
	}
	lease, available, err := lock(m.Dir)
	if err != nil {
		return Connection{}, err
	}
	if available {
		err = m.start(ctx, lease, config, binary)
		utils.CloseAndLog(lease)
		if err != nil {
			return Connection{}, err
		}
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		r, err := m.record()
		if err == nil {
			if r.Config != digest(config) || r.Binary != binary {
				return Connection{}, fmt.Errorf("local runtime %s uses a different binary or configuration; finish active work and stop it before restarting", r.ID)
			}
			if r.Control != "" {
				token, readErr := ReadPrivate(filepath.Join(m.Dir, "token"))
				if readErr != nil {
					return Connection{}, readErr
				}
				var live Record
				if err = m.call(ctx, r, string(token), http.MethodGet, "/identity", &live); err == nil {
					if live.State == "ready" {
						return Connection{Record: live, Token: string(token)}, nil
					}
					if live.State == "degraded" || live.State == "stopping" {
						return Connection{}, fmt.Errorf("local runtime %s is %s; inspect its status and dependencies before retrying", r.ID, live.State)
					}
				}
			}
		}
		// A released lease proves the child exited. An unhealthy process holding it
		// is never replaced, even when its control listener is unreachable.
		probe, free, lockErr := lock(m.Dir)
		if lockErr != nil {
			return Connection{}, lockErr
		}
		if free {
			utils.CloseAndLog(probe)
			return Connection{}, fmt.Errorf("local runtime exited during startup; inspect %s", filepath.Join(m.Dir, "runtime.log"))
		}
		select {
		case <-ctx.Done():
			return Connection{}, fmt.Errorf("waiting for local runtime (startup remains accounted for; inspect its status): %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (m Manager) start(ctx context.Context, lease *os.File, config []byte, binary string) error {
	generation, err := randomID()
	if err != nil {
		return err
	}
	token, err := randomID()
	if err != nil {
		return err
	}
	r := Record{ID: filepath.Base(m.Dir), Generation: generation, Binary: binary, Config: digest(config), Version: m.Version, State: "starting"}
	if err := writeAtomic(filepath.Join(m.Dir, "token"), []byte(token)); err != nil {
		return err
	}
	if err := writeAtomic(filepath.Join(m.Dir, "active.yaml"), config); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(m.Dir, "manifest.json"), r); err != nil {
		return err
	}
	log, err := os.OpenFile(filepath.Join(m.Dir, "runtime.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(log)
	info, err := log.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("runtime log must be a private regular file")
	}
	cmd := exec.CommandContext(context.WithoutCancel(ctx), m.Binary, "local", "managed", "--directory", m.Dir, "--generation", generation)
	cmd.Dir = m.Dir
	cmd.Stdin = nil
	cmd.Stdout = log
	cmd.Stderr = log
	detach(cmd, lease)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start local runtime: %w", err)
	}
	// The supervisor CLI is not the lifetime owner of the child. Wait reaps it if
	// this caller happens to remain alive, without tying execution to its context.
	go func() {
		if err := cmd.Wait(); err != nil {
			slog.Debug("local runtime exited", "runtime", r.ID, "error", err)
		}
	}()
	return nil
}

// Status never starts a process, including when configuration or storage is gone.
func (m Manager) Status(ctx context.Context) (Record, error) {
	if _, err := os.Stat(m.Dir); os.IsNotExist(err) {
		return Record{ID: filepath.Base(m.Dir), State: "stopped"}, nil
	}
	if err := privateDirectory(m.Dir); err != nil {
		return Record{}, err
	}
	lease, free, err := lock(m.Dir)
	if err != nil {
		return Record{}, err
	}
	if free {
		utils.CloseAndLog(lease)
		return Record{ID: filepath.Base(m.Dir), State: "stopped"}, nil
	}
	r, err := m.record()
	if err != nil {
		return Record{}, fmt.Errorf("runtime is occupied but identity is unavailable: %w", err)
	}
	if r.Control == "" {
		return r, nil
	}
	token, err := ReadPrivate(filepath.Join(m.Dir, "token"))
	if err != nil {
		return Record{}, err
	}
	var live Record
	if err := m.call(ctx, r, string(token), http.MethodGet, "/identity", &live); err != nil {
		return r, fmt.Errorf("runtime is occupied but its identity could not be verified: %w", err)
	}
	return live, nil
}

// Stop asks the authenticated child to drain its existing server lifecycle.
// It never signals a PID or removes storage, and waits for the lease to release.
func (m Manager) Stop(ctx context.Context) error {
	r, err := m.Status(ctx)
	if err != nil {
		return err
	}
	if r.State == "stopped" {
		return nil
	}
	if r.Control == "" {
		return fmt.Errorf("runtime is still publishing its identity; retry stop")
	}
	token, err := ReadPrivate(filepath.Join(m.Dir, "token"))
	if err != nil {
		return err
	}
	if err := m.call(ctx, r, string(token), http.MethodPost, "/stop", nil); err != nil {
		return err
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		lease, free, err := lock(m.Dir)
		if err != nil {
			return err
		}
		if free {
			utils.CloseAndLog(lease)
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("runtime shutdown has not completed: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func loopback(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("invalid runtime endpoint: %w", err)
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme != "http" || ip == nil || !ip.IsLoopback() || u.Port() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("runtime endpoint must be a numeric loopback HTTP origin")
	}
	return nil
}

var controlClient = &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return fmt.Errorf("runtime redirects are not allowed") }}

func (m Manager) call(ctx context.Context, r Record, token, method, path string, result *Record) error {
	if err := loopback(r.Control); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, r.Control+path, nil)
	if err != nil {
		return err
	}
	nonce, err := randomID()
	if err != nil {
		return err
	}
	req.Header.Set("X-Runtime-Generation", r.Generation)
	req.Header.Set("X-Runtime-Nonce", nonce)
	req.Header.Set("X-Runtime-Signature", signature(token, requestMessage(req)))
	resp, err := controlClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("runtime control refused request (%d)", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil {
		return err
	}
	if len(data) > 65536 || !validSignature(token, "response\n"+nonce+"\n"+string(data), resp.Header.Get("X-Runtime-Signature")) {
		return fmt.Errorf("runtime response identity could not be verified")
	}
	if result != nil {
		if err := json.Unmarshal(data, result); err != nil {
			return err
		}
		if result.ID != r.ID || result.Generation != r.Generation || result.Binary != r.Binary || result.Config != r.Config {
			return fmt.Errorf("runtime identity mismatch")
		}
		if result.Endpoint != "" {
			return loopback(result.Endpoint)
		}
	}
	return nil
}
