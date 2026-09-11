//go:build integration

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// serveStartupMarker is the first line `serve` logs once it begins bringing
	// storage up, which is the point from which the process is inside startup
	// and has something to be signalled out of.
	serveStartupMarker = "ensuring storage schema"

	// serveSignalExitDeadline bounds how long the signalled process may take to
	// exit. It is generous next to the exit it is checking for — a signalled
	// startup unwinds in well under a second — and small next to the minutes a
	// process that ignores the signal spends finishing its startup budget, so
	// there is no value in between where the result is ambiguous.
	serveSignalExitDeadline = 20 * time.Second

	// serveStartupMarkerDeadline bounds waiting for the marker above. It covers
	// config parsing and the first log write on a loaded machine; past that, the
	// process is not starting up at all and the test has nothing to signal.
	serveStartupMarkerDeadline = 30 * time.Second
)

// A SchemaBot server that is signalled while its storage is unreachable exits,
// rather than spending its whole startup retry budget on a database it has
// already been told to stop waiting for. An orchestrator that sends the signal
// is not asking the process to finish starting; a process that treats it that
// way outlives its termination grace period and is killed instead of stopping.
func TestServeExitsWhenSignalledDuringFailingStartup(t *testing.T) {
	binary := buildSchemabotBinary(t)
	storageAddr := unresponsiveStorageAddr(t)
	configPath := writeServeConfig(t, storageAddr)

	cmd := exec.CommandContext(t.Context(), binary, "serve")
	cmd.Env = append(os.Environ(),
		"SCHEMABOT_CONFIG_FILE="+configPath,
		// A port of 0 keeps a failed test from colliding with anything else on
		// the machine; startup never reaches the listener either way.
		"PORT=0",
		"GRPC_PORT=",
	)
	// Merge both streams: the server logs to stdout and the CLI reports a failed
	// command on stderr, and a test that has to say why a process did not stop
	// wants whichever one it produced.
	output, sink, err := os.Pipe()
	require.NoError(t, err)
	cmd.Stdout = sink
	cmd.Stderr = sink
	t.Cleanup(func() { assert.NoError(t, output.Close()) })

	require.NoError(t, cmd.Start())
	// Drop the parent's handle so the scanner sees EOF when the child exits.
	require.NoError(t, sink.Close())
	t.Cleanup(func() {
		// The process has normally exited by now; killing an exited process is
		// an error worth ignoring, and a test that failed before the signal
		// must not leave the server behind.
		_ = cmd.Process.Kill()
	})

	logs := scanFor(t, output, serveStartupMarker)
	select {
	case line := <-logs.matched:
		require.Contains(t, line, serveStartupMarker)
	case <-time.After(serveStartupMarkerDeadline):
		t.Fatalf("server did not reach storage startup within %s; logs so far:\n%s",
			serveStartupMarkerDeadline, logs.text())
	}

	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))

	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()

	select {
	case err := <-exited:
		// A server that could not finish starting failed, and says so with its
		// exit code: an orchestrator restarts it rather than routing to it.
		require.Error(t, err, "a server signalled during a failing startup must exit non-zero")
		var exitErr *exec.ExitError
		require.ErrorAs(t, err, &exitErr)
		assert.NotEqual(t, 0, exitErr.ExitCode())
	case <-time.After(serveSignalExitDeadline):
		t.Fatalf("server did not exit within %s of SIGTERM; logs so far:\n%s",
			serveSignalExitDeadline, logs.text())
	}
}

// unresponsiveStorageAddr returns the address of a listener that accepts
// connections and then says nothing, standing in for a database that is
// reachable but not answering — the case that keeps startup blocked for its
// whole budget rather than failing fast.
func unresponsiveStorageAddr(t *testing.T) string {
	t.Helper()

	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var mu sync.Mutex
	var conns []net.Conn
	t.Cleanup(func() {
		assert.NoError(t, listener.Close())
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			assert.NoError(t, conn.Close())
		}
	})

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()

	return listener.Addr().String()
}

func writeServeConfig(t *testing.T, storageAddr string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "schemabot.yaml")
	config := fmt.Sprintf(`storage:
  dsn: "root:test@tcp(%s)/schemabot"
databases:
  testapp:
    type: mysql
    environments:
      production:
        dsn: "root:test@tcp(%s)/testapp"
`, storageAddr, storageAddr)
	require.NoError(t, os.WriteFile(path, []byte(config), 0o600))
	return path
}

// scannedLogs collects a process's stderr so a failing test can print what the
// process was doing, and reports the first line matching a marker.
type scannedLogs struct {
	matched chan string

	mu    sync.Mutex
	lines []string
}

func (l *scannedLogs) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func scanFor(t *testing.T, r io.Reader, marker string) *scannedLogs {
	t.Helper()

	logs := &scannedLogs{matched: make(chan string, 1)}
	go func() {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()
			logs.mu.Lock()
			logs.lines = append(logs.lines, line)
			logs.mu.Unlock()
			if strings.Contains(line, marker) {
				select {
				case logs.matched <- line:
				default:
				}
			}
		}
	}()
	return logs
}

// buildSchemabotBinary builds the CLI from the tree under test. It never falls
// back to a prebuilt bin/schemabot: the behavior being asserted is the one in
// this source, and a binary from an earlier build would answer for a different
// one.
func buildSchemabotBinary(t *testing.T) string {
	t.Helper()

	root, err := moduleRoot()
	require.NoError(t, err)

	binary := filepath.Join(t.TempDir(), "schemabot")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./pkg/cmd")
	build.Dir = root
	var stderr bytes.Buffer
	build.Stderr = &stderr
	require.NoErrorf(t, build.Run(), "build schemabot: %s", stderr.String())
	return binary
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getwd: %w", err)
	}
	for {
		if fileExists(filepath.Join(dir, "go.mod")) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod above %s", dir)
		}
		dir = parent
	}
}
