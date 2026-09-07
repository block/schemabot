//go:build darwin || linux

package localruntime

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDeadline = 10 * time.Second

// TestMain hosts an actual detached child to exercise inherited leases and
// control transport independently of database bootstrap. Real engines are
// covered in integration/localruntime.
func TestMain(m *testing.M) {
	if len(os.Args) == 7 && os.Args[1] == "local" && os.Args[2] == "managed" {
		err := Run(context.Background(), os.Args[4], os.Args[6], func(ctx context.Context, config, token string, ready func(string) error) error {
			data, err := ReadPrivate(config)
			if err != nil {
				return err
			}
			if string(data) == "relative" {
				marker, err := os.ReadFile("proof")
				if err != nil {
					return err
				}
				if string(marker) != "runtime" {
					return fmt.Errorf("unexpected runtime directory")
				}
			}
			if string(data) == "starting" {
				<-ctx.Done()
				return nil
			}
			var lc net.ListenConfig
			listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
			if err != nil {
				return err
			}
			server := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer "+token {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if string(data) == "degraded" {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				w.WriteHeader(http.StatusOK)
			})}
			go func() {
				if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
					fmt.Fprintln(os.Stderr, err)
				}
			}()
			if err := ready("http://" + listener.Addr().String()); err != nil {
				return err
			}
			<-ctx.Done()
			return server.Close()
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func testManager(t *testing.T, config string) Manager {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "runtime.yaml"), []byte(config), 0600))
	binary, err := os.Executable()
	require.NoError(t, err)
	manager := Manager{Dir: dir, Binary: binary, Version: "test"}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), testDeadline)
		defer cancel()
		require.NoError(t, manager.Stop(ctx))
	})
	return manager
}

// Concurrent callers share a detached process and its verified identity. An
// explicit stop releases ownership; inspection alone never restarts the child.
func TestConcurrentReuse(t *testing.T) {
	manager := testManager(t, "ready")
	ctx, cancel := context.WithTimeout(t.Context(), testDeadline)
	defer cancel()
	type outcome struct {
		connection Connection
		err        error
	}
	results := make(chan outcome, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { c, err := manager.Ensure(ctx); results <- outcome{c, err} })
	}
	wg.Wait()
	close(results)
	generation := ""
	pid := 0
	for result := range results {
		require.NoError(t, result.err)
		assert.Equal(t, "ready", result.connection.State)
		if generation == "" {
			generation = result.connection.Generation
			pid = result.connection.PID
		}
		assert.Equal(t, generation, result.connection.Generation)
		assert.Equal(t, pid, result.connection.PID)
	}
	require.NoError(t, manager.Stop(ctx))
	r, err := manager.Status(ctx)
	require.NoError(t, err)
	assert.Equal(t, "stopped", r.State)
	next, err := manager.Ensure(ctx)
	require.NoError(t, err)
	assert.NotEqual(t, generation, next.Generation)
}

// Dependency failure and configuration changes must not replace a live child.
func TestRefusesMismatchAndDegraded(t *testing.T) {
	for _, config := range []string{"ready", "degraded"} {
		t.Run(config, func(t *testing.T) {
			manager := testManager(t, config)
			ctx, cancel := context.WithTimeout(t.Context(), testDeadline)
			defer cancel()
			_, err := manager.Ensure(ctx)
			if config == "degraded" {
				require.ErrorContains(t, err, "degraded")
			} else {
				require.NoError(t, err)
			}
			original, err := manager.Status(ctx)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(manager.Dir, "runtime.yaml"), []byte("changed"), 0600))
			_, err = manager.Ensure(ctx)
			require.ErrorContains(t, err, "different binary or configuration")
			after, err := manager.Status(ctx)
			require.NoError(t, err)
			assert.Equal(t, original.Generation, after.Generation)
			assert.Equal(t, config, after.State)
		})
	}
}

// A caller timing out during bootstrap leaves a tracked child whose control
// endpoint can still report starting and accept a graceful stop.
func TestStartupDeadlinePreservesChild(t *testing.T) {
	manager := testManager(t, "starting")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := manager.Ensure(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	inspectCtx, cancelInspect := context.WithTimeout(t.Context(), testDeadline)
	defer cancelInspect()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		r, err := manager.Status(inspectCtx)
		if assert.NoError(c, err) {
			assert.Equal(c, "starting", r.State)
			assert.NotEmpty(c, r.Control)
		}
	}, testDeadline, pollInterval)
	require.NoError(t, manager.Stop(inspectCtx))
}

// The child rejects unauthenticated control and stale process generations.
// A crash releases the lifetime lock and permits a fresh, separately identified
// process; changing the selected binary never restarts a live one.
func TestIdentityAndCrashRecovery(t *testing.T) {
	manager := testManager(t, "ready")
	ctx, cancel := context.WithTimeout(t.Context(), testDeadline)
	defer cancel()
	connection, err := manager.Ensure(ctx)
	require.NoError(t, err)
	for _, token := range []string{"", "wrong"} {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, connection.Control+"/stop", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := controlClient.Do(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		require.NoError(t, resp.Body.Close())
	}
	stale := connection.Record
	stale.Generation = "old-generation"
	require.ErrorContains(t, manager.call(ctx, stale, connection.Token, http.MethodPost, "/stop", nil), "409")
	changedBinary := filepath.Join(t.TempDir(), "other-binary")
	require.NoError(t, os.WriteFile(changedBinary, []byte("different"), 0600))
	changed := manager
	changed.Binary = changedBinary
	_, err = changed.Ensure(ctx)
	require.ErrorContains(t, err, "different binary or configuration")
	same, err := manager.Status(ctx)
	require.NoError(t, err)
	assert.Equal(t, connection.Generation, same.Generation)
	child, err := os.FindProcess(connection.PID)
	require.NoError(t, err)
	require.NoError(t, child.Kill())
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		r, err := manager.Status(ctx)
		if assert.NoError(c, err) {
			assert.Equal(c, "stopped", r.State)
		}
	}, testDeadline, pollInterval)
	replacement, err := manager.Ensure(ctx)
	require.NoError(t, err)
	assert.NotEqual(t, connection.Generation, replacement.Generation)
}

// A different process on a stale control port cannot see the token or prove
// identity by copying public manifest fields into an unsigned response.
func TestStaleControlListenerCannotCaptureCredential(t *testing.T) {
	token := "private-runtime-credential"
	var sawToken atomic.Bool
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawToken.Store(strings.Contains(fmt.Sprint(r.Header), token))
		w.Header().Set("Content-Type", "application/json")
		_, err := fmt.Fprint(w, `{"id":"sample","generation":"stale","binary":"binary","config":"config","state":"ready"}`)
		assert.NoError(t, err)
	}))
	defer fake.Close()
	var result Record
	err := (Manager{}).call(t.Context(), Record{ID: "sample", Generation: "stale", Binary: "binary", Config: "config", Control: fake.URL}, token, http.MethodGet, "/identity", &result)
	require.ErrorContains(t, err, "identity could not be verified")
	assert.False(t, sawToken.Load())
}

// Project working directories do not change how runtime-relative paths resolve.
func TestRuntimeUsesOwnDirectory(t *testing.T) {
	manager := testManager(t, "relative")
	require.NoError(t, os.WriteFile(filepath.Join(manager.Dir, "proof"), []byte("runtime"), 0600))
	ctx, cancel := context.WithTimeout(t.Context(), testDeadline)
	defer cancel()
	connection, err := manager.Ensure(ctx)
	require.NoError(t, err)
	assert.Equal(t, "ready", connection.State)
}
