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
