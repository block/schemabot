package localruntime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/block/spirit/pkg/utils"
)

// Run owns the inherited lifetime lock until the hosted server fully closes.
// The control listener starts before storage bootstrap and remains available
// during dependency outages. The callback uses the normal server lifecycle.
func Run(ctx context.Context, dir, generation string, run func(context.Context, string, string, func(string) error) error) error {
	if err := privateDirectory(dir); err != nil {
		return err
	}
	lease, err := inheritedLock(dir)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(lease)
	m := Manager{Dir: dir}
	r, err := m.record()
	if err != nil {
		return err
	}
	if r.Generation != generation || r.State != "starting" {
		return fmt.Errorf("managed runtime startup identity mismatch")
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	binary, err := binaryDigest(executable)
	if err != nil {
		return err
	}
	config, err := ReadPrivate(filepath.Join(dir, "active.yaml"))
	if err != nil {
		return err
	}
	if binary != r.Binary || digest(config) != r.Config {
		return fmt.Errorf("managed runtime binary or configuration changed before startup")
	}
	token, err := ReadPrivate(filepath.Join(dir, "token"))
	if err != nil {
		return err
	}
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	r.Control = "http://" + listener.Addr().String()
	r.PID = os.Getpid()
	if err := writeJSON(filepath.Join(dir, "manifest.json"), r); err != nil {
		utils.CloseAndLog(listener)
		return err
	}
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	mux := http.NewServeMux()
	mux.HandleFunc("GET /identity", func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		snapshot := r
		mu.Unlock()
		if snapshot.State == "ready" {
			healthCtx, cancelHealth := context.WithTimeout(req.Context(), 500*time.Millisecond)
			defer cancelHealth()
			health, err := http.NewRequestWithContext(healthCtx, http.MethodGet, snapshot.Endpoint+"/health", nil)
			if err != nil {
				http.Error(w, "invalid health endpoint", http.StatusInternalServerError)
				return
			}
			health.Header.Set("Authorization", "Bearer "+string(token))
			resp, err := controlClient.Do(health)
			if err != nil {
				snapshot.State = "degraded"
			} else {
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					snapshot.State = "degraded"
				}
			}
		}
		if err := writeControl(w, req, string(token), snapshot); err != nil {
			slog.Error("write runtime identity", "error", err)
		}
	})
	mux.HandleFunc("POST /stop", func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		r.State = "stopping"
		mu.Unlock()
		cancel()
		if err := writeControl(w, req, string(token), nil); err != nil {
			slog.Error("acknowledge runtime shutdown", "error", err)
		}
	})
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if len(req.Header.Get("X-Runtime-Nonce")) != 64 || !validSignature(string(token), requestMessage(req), req.Header.Get("X-Runtime-Signature")) {
			http.Error(w, "invalid runtime credential", http.StatusUnauthorized)
			return
		}
		if req.Header.Get("X-Runtime-Generation") != generation {
			http.Error(w, "runtime generation mismatch", http.StatusConflict)
			return
		}
		mux.ServeHTTP(w, req)
	})
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 10 * time.Second}
	serveErr := make(chan error, 1)
	go func() { err := server.Serve(listener); serveErr <- err; cancel() }()
	runErr := run(childCtx, filepath.Join(dir, "active.yaml"), string(token), func(endpoint string) error {
		if err := loopback(endpoint); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		if r.State == "stopping" {
			return fmt.Errorf("runtime stopped during startup")
		}
		r.State = "ready"
		r.Endpoint = endpoint
		return writeJSON(filepath.Join(dir, "manifest.json"), r)
	})
	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancelShutdown()
	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, server.Close())
	}
	err = <-serveErr
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return errors.Join(runErr, shutdownErr, err)
}
