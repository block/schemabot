package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/livee2e"
)

// A deploy gate reads the exit code: 0 promotes the release, 1 blocks it, and
// 2 retries the run.
func TestReportMapsEachOutcomeToItsExitCode(t *testing.T) {
	inconclusive := fmt.Errorf("apply apply-00000000000000aa finished before it reached In Progress: %w", livee2e.ErrInconclusive)
	cancelled := fmt.Errorf("wait for apply: run cancelled: %w", context.Canceled)
	cleanupFailed := errors.New("clean up run e2e_20261105103000: leaving table e2e_20261105103000 in place")
	// Run joins a failed cleanup by message only, so the steps' chain is gone.
	withCleanup := func(err error) error { return fmt.Errorf("%s; %w", err.Error(), cleanupFailed) }

	for _, tt := range []struct {
		name      string
		err       error
		signalled bool
		code      int
		out       string
	}{
		{name: "passed", code: exitPassed, out: "PASS"},
		{name: "failed", err: errors.New("column unchanged"), code: exitFailed, out: "FAIL"},
		{name: "inconclusive", err: inconclusive, code: exitInconclusive, out: "INCONCLUSIVE"},
		{name: "inconclusive with a failed cleanup", err: withCleanup(inconclusive), code: exitFailed, out: "FAIL"},
		{name: "stopped by a signal", err: cancelled, signalled: true, code: exitInconclusive, out: "INCONCLUSIVE"},
		{name: "stopped by a signal with a failed cleanup", err: withCleanup(cancelled), signalled: true, code: exitFailed, out: "FAIL"},
		{name: "cancelled without a signal", err: cancelled, code: exitFailed, out: "FAIL"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			code := report(slog.New(slog.NewTextHandler(io.Discard, nil)), &out, tt.err, tt.signalled)
			assert.Equal(t, tt.code, code)
			assert.Equal(t, tt.out, strings.TrimSpace(out.String()))
		})
	}
}

// A signal can stop the binary before the runner starts. Stopped while the
// test database opens, it proved nothing about the release, so it exits 2. A
// database it cannot reach still fails.
func TestRunMapsAStartupFailureToItsExitCode(t *testing.T) {
	args := []string{"schemabot-e2e",
		"--repo", "octo/schema", "--table-dir", "schema/testdb", "--database", "testdb",
		"--environment", "staging", "--check-name", "SchemaBot (staging)",
		"--app-slug", "schemabot", "--seed-table", "seed"}
	env := map[string]string{
		"GITHUB_TOKEN": "test-token",
		// Nothing listens on port 1, so a connection is refused at once.
		"SCHEMABOT_E2E_DSN": "e2e:e2e@tcp(127.0.0.1:1)/testdb",
	}
	stopped, stop := context.WithCancel(t.Context())
	stop()

	for _, tt := range []struct {
		name string
		ctx  context.Context
		args []string
		code int
		out  string
	}{
		{name: "stopped while the database opens", ctx: stopped, args: args, code: exitInconclusive, out: "INCONCLUSIVE"},
		{name: "database unreachable", ctx: t.Context(), args: args, code: exitFailed, out: "FAIL"},
		{name: "help", ctx: t.Context(), args: []string{"schemabot-e2e", "-h"}, code: exitFailed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			code := run(tt.ctx, tt.args, func(k string) string { return env[k] }, &out, io.Discard)
			assert.Equal(t, tt.code, code)
			assert.Equal(t, tt.out, strings.TrimSpace(out.String()))
		})
	}
}
