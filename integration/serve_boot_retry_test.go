//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/serve"
)

// bootRecoveryDeadline bounds each wait on the boot goroutine: reaching the
// retry loop, coming up after the credential heals, and stopping on cancel.
const bootRecoveryDeadline = 30 * time.Second

// storageRetryLogMessage is the warning bootStorage logs between attempts; the
// tests synchronize on it to know the boot has entered its retry loop.
const storageRetryLogMessage = "storage not ready, retrying"

// buildResult carries Build's outcome across the goroutine boundary.
type buildResult struct {
	srv *serve.Server
	err error
}

// msgSignalHandler wraps a slog.Handler and closes ch the first time a record
// with the given message is handled, letting a test synchronize on a specific
// server-side event without sleeping.
type msgSignalHandler struct {
	slog.Handler
	msg  string
	once *sync.Once
	ch   chan struct{}
}

func (h *msgSignalHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == h.msg {
		h.once.Do(func() { close(h.ch) })
	}
	return h.Handler.Handle(ctx, r)
}

// newRetrySignalLogger returns a logger for serve.Build plus a channel that is
// closed the first time the storage boot logs that it is retrying.
func newRetrySignalLogger() (*slog.Logger, chan struct{}) {
	retrying := make(chan struct{})
	logger := slog.New(&msgSignalHandler{
		Handler: slog.NewTextHandler(io.Discard, nil),
		msg:     storageRetryLogMessage,
		once:    &sync.Once{},
		ch:      retrying,
	})
	return logger, retrying
}

// fileDSNConfig writes dsn to a file in the test's temp dir and returns a
// server config whose storage DSN is the file reference, the same shape a
// deployment uses for a secret mounted into the pod.
func fileDSNConfig(t *testing.T, dsn string) (*api.ServerConfig, string) {
	t.Helper()
	dsnPath := filepath.Join(t.TempDir(), "dsn")
	require.NoError(t, os.WriteFile(dsnPath, []byte(dsn), 0o600))
	return &api.ServerConfig{Storage: api.StorageConfig{DSN: "file:" + dsnPath}}, dsnPath
}

// TestBuildWaitsOutStorageCredentialRotation exercises a server booting while
// its storage credential is mid-rotation: the mounted DSN file still holds the
// old password, so the database rejects every new connection. The boot must
// keep retrying inside its budget — re-resolving the DSN each attempt — and
// come up on its own once the refreshed credential lands in the file, with no
// pod restart.
func TestBuildWaitsOutStorageCredentialRotation(t *testing.T) {
	staleCfg, err := mysql.ParseDSN(schemabotDSN)
	require.NoError(t, err)
	staleCfg.Passwd = "stale-rotated-password"
	cfg, dsnPath := fileDSNConfig(t, staleCfg.FormatDSN())

	logger, retrying := newRetrySignalLogger()
	done := make(chan buildResult, 1)
	go func() {
		srv, buildErr := serve.Build(t.Context(), cfg, serve.WithLogger(logger))
		done <- buildResult{srv: srv, err: buildErr}
	}()

	// Wait for the boot to observe the stale credential and start retrying,
	// then heal the DSN file the way a secret-mount refresh would.
	select {
	case <-retrying:
	case r := <-done:
		t.Fatalf("Build returned before retrying: err=%v", r.err)
	case <-time.After(bootRecoveryDeadline):
		t.Fatal("Build never started retrying against the stale credential")
	}
	require.NoError(t, os.WriteFile(dsnPath, []byte(schemabotDSN), 0o600))

	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.NotNil(t, r.srv)
		require.NoError(t, r.srv.Close())
	case <-time.After(bootRecoveryDeadline):
		t.Fatal("Build did not come up after the storage credential healed")
	}
}

// TestBuildStorageBootStopsOnContextCancel exercises shutdown while storage is
// unreachable: a pod told to stop mid-boot must exit the retry loop promptly
// instead of spending the rest of the boot budget.
func TestBuildStorageBootStopsOnContextCancel(t *testing.T) {
	unreachable := mysql.NewConfig()
	unreachable.User = "root"
	unreachable.Passwd = "testpassword"
	unreachable.Net = "tcp"
	unreachable.Addr = "127.0.0.1:1" // nothing listens here; every dial is refused
	unreachable.DBName = "schemabot_test"
	cfg, _ := fileDSNConfig(t, unreachable.FormatDSN())

	logger, retrying := newRetrySignalLogger()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan buildResult, 1)
	go func() {
		srv, buildErr := serve.Build(ctx, cfg, serve.WithLogger(logger))
		done <- buildResult{srv: srv, err: buildErr}
	}()

	select {
	case <-retrying:
	case r := <-done:
		t.Fatalf("Build returned before retrying: err=%v", r.err)
	case <-time.After(bootRecoveryDeadline):
		t.Fatal("Build never started retrying against the unreachable database")
	}
	cancel()

	select {
	case r := <-done:
		require.ErrorIs(t, r.err, context.Canceled)
		require.Nil(t, r.srv)
	case <-time.After(bootRecoveryDeadline):
		t.Fatal("Build did not stop after context cancellation")
	}
}
